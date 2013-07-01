-module(imem_snap).

-include("imem.hrl").
-include_lib("kernel/include/file.hrl").

-export([ info/1
        , format/1
        , restore/4
        , restore/5
        , zip/1
        , take/1
        ]).

-define(BKP_EXTN, ".bkp").
-define(BKP_TMP_EXTN, ".bkp.new").

%% ----- SNAPSHOT INTERFACE ------------------------------------------------

% backup existing snapshot
zip(all) -> zip({re, "*"++?BKP_EXTN});
zip({files, SnapFiles}) ->
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
zip({re, MatchPattern}) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    SnapFiles =
    [filename:basename(File)
    || File <- filelib:wildcard(filename:join([SnapDir, MatchPattern]))
    ],
    zip({files, SnapFiles}).

% display information of existing snapshot or a snapshot bundle (.zip)
info(bkp) ->
    MTabs = mnesia:system_info(tables),
    BytesPerWord =  erlang:system_info(wordsize),
    MnesiaTables = [{atom_to_list(M), mnesia:table_info(M, size), mnesia:table_info(M, memory) * BytesPerWord} || M <- MTabs],
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    case filelib:is_dir(SnapDir) of
        true ->
            SnapTables = [
                case re:run(F,"(.*)\\"++?BKP_EXTN, [{capture, [1], list}]) of
                    {match, [T|_]} ->
                        Fn = filename:join(SnapDir,F),
                        {T, filelib:file_size(Fn), filelib:last_modified(Fn)};
                    _ -> throw({error, "bad snapshot"})
                end
                || F <- filelib:wildcard("*"++?BKP_EXTN, SnapDir), re:run(F,"(.*)\\"++?BKP_EXTN, [{capture, [1], list}]) =/= nomatch
            ],
            STabs = [S || {S, _, _} <- SnapTables],
            RestorableTables = sets:to_list(sets:intersection(sets:from_list([atom_to_list(M) || M <- MTabs])
                                                             , sets:from_list(STabs))),
            {bkp, [ {dbtables, lists:sort(MnesiaTables)}
                  , {snaptables, lists:sort(SnapTables)}
                  , {restorabletables, lists:sort(RestorableTables)}]
            };
        false -> throw({error, "no snapshot dir found", SnapDir})
    end;
info(zip) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    ZipFiles = filelib:wildcard(filename:join([SnapDir, "*.zip"])),
    info({zip, ZipFiles}, []).

info({zip, []}, ContentFiles) -> {zip, ContentFiles};
info({zip, [Z|ZipFiles]}, ContentFiles) ->
    {ok, CntFiles} = zip:foldl(fun(F, IF, _, Acc) ->
                                   [{F, IF()} | Acc]
                               end
                               , []
                               , Z),
    info({zip, ZipFiles}, [{Z,CntFiles}|ContentFiles]).

% formatting
% - snapshot info
% - restore info
% for better display
% (a wrapper around info/1 and restore/* interfaces)
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
    MTLen = lists:max([length(MTab) || {MTab, _, _} <- DbTables]),
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
            io_lib:format("~*s ~-10B ~-10B ~-10s ~20s ~7s~n", [-MTLen, _MTab, Rows, Mem, SnapSize, SnapTime, Restotable])
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
    );
format({restore, RestoreRes}) ->
    FLen = lists:max([length(atom_to_list(_F)) || {_F, _} <- RestoreRes]),
    Header = lists:flatten(io_lib:format("~*s ~-10s ~-10s ~-10s", [-FLen, "name", "identical", "replaced", "added"])),
    Sep = lists:duplicate(length(Header),$-),
    lists:flatten(
        [io_lib:format("~s~n", [Sep]),
         io_lib:format("~s~n", [Header]),
         io_lib:format("~s~n", [Sep]),
         [case Res of
            {atomic, {I,E,A}} ->
                io_lib:format("~*s ~-10B ~-10B ~-10B~n", [-FLen, atom_to_list(T), length(I), length(E), length(A)]);
            Error -> io_lib:format("~*s ~p~n", [-FLen, atom_to_list(T), Error])
         end
         || {T, Res} <- RestoreRes],
        io_lib:format("~s~n", [Sep])]
    ).


% take snapshot of all/some of the current in memory mnesia table
take([all]) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    take({ tabs
             , SnapDir
             , mnesia:system_info(tables) -- [schema]});

% multiple tables as list of strings or regex strings
take({tabs, [_R|_] = RegExs}) when is_list(_R) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    take({tabs
         , SnapDir
         , lists:flatten([[T || R <- RegExs, re:run(atom_to_list(T), R, []) /= nomatch]
                         || T <- mnesia:system_info(tables)])
        });

% single table as atom (internal use)
take(Tab) when is_atom(Tab) -> take({tabs, [atom_to_list(Tab)]});

% list of tables as atoms
take({tabs, SnapDir, Tabs}) ->
    lists:flatten([
        case mnesia:transaction(fun() ->
                TblContent = [
                    {prop, mnesia:table_info(T, user_properties)},
                    {rows, mnesia:select(T, [{'$1', [], ['$1']}], write)}
                ],
                BackFile = filename:join([SnapDir, atom_to_list(T)++?BKP_EXTN]),
                NewBackFile = filename:join([SnapDir, atom_to_list(T)++?BKP_TMP_EXTN]),
                ok = file:write_file(NewBackFile, term_to_binary(TblContent)),
                {ok, _} = file:copy(NewBackFile, BackFile),
                ok = file:delete(NewBackFile)
            end) of
        {atomic, ok}      -> io_lib:format("snapshot created for ~p~n", [T]);
        {aborted, Reason} -> io_lib:format("snapshot failed for ~p, reason ~p~n", [T, Reason])
    end
    || T <- Tabs]).

% snapshot restore interface
restore(bkp, [_T|_] = Tabs, Strategy, Simulate) when is_list(_T) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    [(fun() ->
        Table = filename:rootname(filename:basename(Tab)),
        SnapFile = filename:join([SnapDir, Table++?BKP_EXTN]),
        {ok, Bin} = file:read_file(SnapFile),
        [{prop, TabProp}, {rows, Rows}] = binary_to_term(Bin),
        restore(list_to_atom(Table), {prop, TabProp}, {rows, Rows}, Strategy, Simulate)
    end)()
    || Tab <- Tabs].

restore(zip, ZFile, TabRegEx, Strategy, Simulate) when is_list(ZFile) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    ZipFile = filename:join([SnapDir, ZFile]),
    case filelib:is_file(ZipFile) of
        true ->
            case zip:foldl(fun(File, _, GBin, Acc) ->
                    case [File || R <- TabRegEx, re:run(File, R) /= nomatch] of
                        [] when length(TabRegEx) > 0 -> Acc;
                        _ ->
                            Tab = list_to_atom(filename:rootname(filename:basename(File))),
                            [{prop, TabProp}, {rows, Rows}] = binary_to_term(GBin()),
                            [restore(Tab, {prop, TabProp}, {rows, Rows}, Strategy, Simulate) | Acc]
                    end
                end, [], ZipFile) of
                {ok, Res} -> Res;
                Error -> {filename:absname(ZipFile), Error}
            end;
        _ -> {filename:absname(ZipFile), "not found"}
    end;

% private real restore function
restore(Tab, {prop, TabProp}, {rows, Rows}, Strategy, Simulate) when is_atom(Tab) ->
    Ret = mnesia:sync_transaction(fun() ->
        % restore the properties
        [mnesia:write_table_properties(Tab,P) || P <- TabProp],
        if (Simulate /= true) andalso (Strategy =:= destroy)
            -> {atomic, ok} = mnesia:clear_table(Tab);
            true -> ok
        end,
        TableSize = proplists:get_value(size,mnesia:table_info(Tab, all)),
        TableType = proplists:get_value(type,mnesia:table_info(Tab, all)),
        lists:foldl(fun(Row, {I, E, A}) ->
            if (TableSize > 0) andalso (TableType =/= bag) ->
                K = element(2, Row),
                case mnesia:read(Tab, K) of
                    [Row] ->    % found identical existing row
                            {[Row|I], E, A}; 
                    [RowN] ->   % existing row with different content,
                        case Strategy of
                            replace ->
                                if Simulate /= true -> ok = mnesia:write(Row); true -> ok end,
                                {I, [{Row,RowN}|E], A};
                            destroy ->
                                if Simulate /= true -> ok = mnesia:write(Row); true -> ok end,
                                {I, E, [Row|A]};
                            _ -> {I, E, A}
                        end;
                    [] -> % row not found, appending
                        if Simulate /= true -> ok = mnesia:write(Row); true -> ok end,
                        {I, E, [Row|A]}
                end;
            true ->
                if Simulate /= true -> ok = mnesia:write(Row); true -> ok end,
                {I, E, [Row|A]}
            end
        end,
        {[], [], []},
        Rows)
    end),
    {Tab, Ret}.


%%
%%----- TESTS ------------------------------------------------
%%

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(TABLES, lists:sort([atom_to_list(T) || T <- mnesia:system_info(tables) -- [schema]])).
-define(FILENAMES(__M, __Dir), lists:sort([filename:rootname(filename:basename(F)) || F <- filelib:wildcard(__M, __Dir)])).
-define(EMPTY_DIR(__Dir), [{F, file:delete(filename:absname(filename:join([__Dir,F])))} || F <- filelib:wildcard("*.*", __Dir)]).

setup() ->
    application:load(imem),
    application:set_env(imem, mnesia_node_type, ram),
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    imem:start(),
    ?EMPTY_DIR(SnapDir),
    ?Log("after deleteing files ~p~n", [?FILENAMES("*.*", SnapDir)]).

teardown(_) ->
    imem:stop().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun test_snapshot/1
        ]}}.

test_snapshot(_) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    ?Log("snapshots :~n~s", [snapshot(all)]),
    ?assertEqual(?TABLES, ?FILENAMES("*"++?BKP_EXTN, SnapDir)),
    ?EMPTY_DIR(SnapDir),
    ?Log("snapshot tests complted!~n", []).

-endif.

