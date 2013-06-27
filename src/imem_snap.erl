-module(imem_snap).

-include("imem.hrl").
-include_lib("kernel/include/file.hrl").

-export([ snap_info/0
        , snap_info/1
        , format/1
        , restore_snap/2
        , zip_snap/1
        , snapshot/1
        ]).

%% ----- SNAPSHOT INTERFACE ------------------------------------------------

zip_snap({files, SnapFiles}) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    ZipCandidates = [filename:join([SnapDir, SF])
                    || SF <- SnapFiles
                    , filelib:file_size(filename:join([SnapDir, SF])) > 0],
    if ZipCandidates =:= [] -> ok;
        true ->
            {{Y,M,D}, {H,Mn,S}} = calendar:local_time(),
            Sec = S + element(3, erlang:now()) / 1000000,
            ZipFileName = filename:join([SnapDir
                                        , lists:flatten(["snapshot_"
                                                        , io_lib:format("~4..0B~2..0B~2..0B_~2..0B~2..0B~9.6.0f", [Y,M,D,H,Mn,Sec])
                                                        , ".zip"
                                                        ])
                                        ]),
            % to make the file name valid for windows
            GoodZipFileName = re:replace(ZipFileName, "[<>:\"\\\\|?*]", "", [global, {return, list}]),
            case zip:zip(GoodZipFileName, ZipCandidates) of
                {error, Reason} ->
                    lists:flatten(io_lib:format("old snapshot backup to ~p failed reason : ~p~n"
                                                , [GoodZipFileName, Reason]));
                _ ->
                    lists:flatten(io_lib:format("old snapshots are backed up to ~p~n"
                                                , [GoodZipFileName]))
            end
    end;
zip_snap({re, MatchPattern}) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    SnapFiles =
    [filename:basename(File)
    || File <- filelib:wildcard(filename:join([SnapDir, MatchPattern]))
    ],
    zip_snap({files, SnapFiles}).

snap_info() ->
    MTabs = mnesia:system_info(tables),
    BytesPerWord =  erlang:system_info(wordsize),
    MnesiaTables = [{M, mnesia:table_info(M, size), mnesia:table_info(M, memory) * BytesPerWord} || M <- MTabs],
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    case filelib:is_dir(SnapDir) of
        true ->
            SnapTables = [
                case re:run(F,"(.*)\.bkp", [{capture, [1], list}]) of
                    {match, [T|_]} ->
                        Fn = filename:join(SnapDir,F),
                        {list_to_existing_atom(T), filelib:file_size(Fn), filelib:last_modified(Fn)};
                    _ -> throw({error, "bad snapshot"})
                end
                || F <- filelib:wildcard("*.bkp", SnapDir), re:run(F,"(.*)\.bkp", [{capture, [1], list}]) =/= nomatch
            ],
            STabs = [S || {S, _, _} <- SnapTables],
            RestorableTables = sets:to_list(sets:intersection(sets:from_list(MTabs), sets:from_list(STabs))),
            {bkp, [ {dbtables, lists:sort(MnesiaTables)}
                  , {snaptables, lists:sort(SnapTables)}
                  , {restorabletables, lists:sort(RestorableTables)}]
            };
        false -> throw({error, "no snapshot dir found", SnapDir})
    end.

snap_info(zip) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    ZipFiles = filelib:wildcard(filename:join([SnapDir, "*.zip"])),
    snap_info({zip, ZipFiles}, []).
snap_info({zip, []}, ContentFiles) -> {zip, ContentFiles};
snap_info({zip, [Z|ZipFiles]}, ContentFiles) ->
    {ok, CntFiles} = zip:foldl(fun(F, IF, _, Acc) ->
                                   [{F, IF()} | Acc]
                               end
                               , []
                               , Z),
    snap_info({zip, ZipFiles}, [{Z,CntFiles}|ContentFiles]).

-define(FMTTIME(DT),
(fun() ->
    {{_Y,_M,_D},{_H,_Mm,_S}} = DT,
    lists:flatten(io_lib:format("~4..0B.~2..0B.~2..0B ~2..0B:~2..0B:~2..0B", [_Y,_M,_D,_H,_Mm,_S]))
end)()
).
format({bkp, [ {dbtables, DbTables}
                  , {snaptables, SnapTables}
                  , {restorabletables, RestorableTables}]
            }) ->
    MTLen = lists:max([length(atom_to_list(MTab)) || {MTab, _, _} <- DbTables]),
    Header = lists:flatten(io_lib:format("~*s ~-10s ~-10s ~-10s  ~-20s ~7s", [-MTLen, "name", "rows", "memory", "snap_size", "snap_time", "restore"])),
    Sep = lists:duplicate(length(Header),$-),
    lists:flatten([
        io_lib:format("~s~n", [Sep]),
        io_lib:format("~s~n", [Header]),
        io_lib:format("~s~n", [Sep]),
        [(fun() ->
            {SnapSize,SnapTime} = case proplists:lookup(_MTab, SnapTables) of
                {_MTab, Sz, Tm} -> {integer_to_list(Sz), ?FMTTIME(Tm)};
                none -> {"", ""}
            end,
            Restotable = case lists:member(_MTab, RestorableTables) of
                true -> "Y";
                _ -> ""
            end,
            io_lib:format("~*s ~-10B ~-10B ~-10s ~20s ~7s~n", [-MTLen, atom_to_list(_MTab), Rows, Mem, SnapSize, SnapTime, Restotable])
        end)() || {_MTab, Rows, Mem} <- DbTables],
    io_lib:format("~s~n", [Sep])
    ]);
format({zip, ContentFiles}) ->
    lists:flatten(
    [(fun() ->
        FLen = lists:max([length(filename:basename(_F)) || {_F,_} <- CntFiles]),
        Header = lists:flatten(io_lib:format("~*s ~-10s  ~-20s ~-20s ~-20s", [-FLen, "name", "size", "created", "accessed", "modified"])),
        Sep = lists:duplicate(length(Header),$-),
        [io_lib:format("~s~n", [Sep]),
         io_lib:format("File : ~s~n", [Z]),
         io_lib:format("~s~n", [Header]),
         io_lib:format("~s~n", [Sep]),
         [(fun()->
             io_lib:format("~*s ~-10B ~20s ~20s ~20s~n",
                                 [-FLen, filename:basename(F), Fi#file_info.size
                                 , ?FMTTIME(Fi#file_info.ctime)
                                 , ?FMTTIME(Fi#file_info.atime)
                                 , ?FMTTIME(Fi#file_info.mtime)])
         end)()
         || {F,Fi} <- CntFiles],
         io_lib:format("~s~n", [Sep])]
    end)()
    || {Z,CntFiles} <- ContentFiles]
    ).

restore_snap([], _) -> ?Log("restore finished!~n", []);
restore_snap([T|Tabs], Replace) when is_atom(T) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    SnapFile = filename:join([SnapDir, atom_to_list(T) ++ ".bkp"]),
    case filelib:is_file(SnapFile) of
        true ->
            {ok, Bin} = file:read_file(SnapFile),
            ?Log("------------ processing ~s @ ~p~n", [SnapFile, T]),
            TableSize = proplists:get_value(size,mnesia:table_info(T, all)),
            TableType = proplists:get_value(type,mnesia:table_info(T, all)),
            {IdExt, ExDiff, Append} =
            lists:foldl(fun(R, {I, E, A}) ->
                if (TableSize > 0) andalso (TableType =/= bag) ->
                    K = element(2, R),
                    {atomic, RepRows} = mnesia:sync_transaction(fun() ->
                        case mnesia:read(T, K) of
                            [R] -> %?Log("found identical existing row ~p~n", [K]),
                                {[R|I], E, A};
                            [R1] -> %?Log("existing row with different content ~p~n", [K]),
                                if Replace =:= true -> ok = mnesia:write(R); true -> ok end,
                                {I, [{R,R1}|E], A};
                            [] -> %?Log("row not found, appending ~p~n", [K]),
                                ok = mnesia:write(R),
                                {I, E, [R|A]}
                        end
                    end),
                    RepRows;
                true ->
                    {atomic, ok} = mnesia:sync_transaction(fun() ->
                        mnesia:write(R)
                    end),
                    {I, E, [R|A]}
                end
            end
            , {[],[],[]}
            , binary_to_term(Bin)),
            %?Log("rows ~s ~p~nfull match ~p~nappended ~p~n", [if Replace =:= true -> "replaced"; true -> "collided" end, ExDiff, IdExt, Append]),
            ?Log("rows ~s ~p, full match ~p, appended ~p~n", [if Replace =:= true -> "replaced"; true -> "collided" end,
                                                            length(ExDiff), length(IdExt), length(Append)]);
        _ ->
            ?Log("file not found ~s~n", [SnapFile])
    end,
    restore_snap(Tabs, Replace).

snapshot(Tab) when is_atom(Tab) -> snapshot(atom_to_list(Tab));
snapshot(TabRegExp) when is_list(TabRegExp) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    Tabs = [T || T <- mnesia:system_info(tables), re:run(atom_to_list(T), TabRegExp) =/= nomatch],
    [{atomic, ok} = mnesia:transaction(fun() ->
                        Rows = mnesia:select(T, [{'$1', [], ['$1']}], write),
                        BackFile = filename:join([SnapDir, atom_to_list(T)++".bkp"]),
                        NewBackFile = filename:join([SnapDir, atom_to_list(T)++".bkp.new"]),
                        ok = file:write_file(NewBackFile, term_to_binary(Rows)),
                        {ok, _} = file:copy(NewBackFile, BackFile),
                        ?Log("snapshot created for ~p~n", [T]),
                        ok = file:delete(NewBackFile)
                    end)
    || T <- Tabs].
