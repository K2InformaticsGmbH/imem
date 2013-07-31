-module(imem_snap).
-behavior(gen_server).

-include_lib("kernel/include/file.hrl").

-include("imem.hrl").
-include("imem_meta.hrl").

-record(state, { snapdir    = ""        :: list()
               , snapFun    = undefined :: any()
               , snapHash   = undefined :: any()
               }).

% snapshot interface
-export([ info/1
        , restore/4
        , restore/5
        , zip/1
        , take/1
        ]).

% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        , start_link/1
        ]).

% fun access exports
-export([ timestamp/1
        , get_snap_timestamps/1
        , set_snap_timestamps/2
        , snap_log/2
        , snap_err/2
        , do_snapshot/1
        ]).

-define(BKP_EXTN, ".bkp").
-define(BKP_TMP_EXTN, ".bkp.new").

-define(GET_SNAPSHOT_CYCLE_WAIT,?GET_IMEM_CONFIG(snapshotCycleWait,[],10000)).
-define(GET_SNAPSHOT_SCRIPT,?GET_IMEM_CONFIG(snapshotScript,[],true)).
-define(GET_SNAPSHOT_SCRIPT_FUN,?GET_IMEM_CONFIG(snapshotScriptFun,[],
<<"fun() ->
    [(fun() ->
        case imem_snap:get_snap_timestamps(T) of
            [] -> ok;
            {Wt,St} ->
                LastWriteTime = imem_snap:timestamp(Wt),
                LastSnapTime = imem_snap:timestamp(St),
                if 
                    LastSnapTime < LastWriteTime ->
                        Res = imem_snap:take(T),
                        [case R of
                            {ok, T} ->
                                Str = lists:flatten(io_lib:format(\"snapshot created for ~p\", [T])),
                                imem_snap:snap_log(Str++\"~n\",[]),
                                imem_meta:log_to_db(info,imem_snap,handle_info,[snapshot],Str);
                            {error, T, Reason}  -> imem_snap:snap_err(\"snapshot of ~p failed for ~p\", [T, Reason])
                        end || R <- Res],
                        true = imem_snap:set_snap_timestamps(T, erlang:now());
                    true -> 
                        ok % no backup needed
                end
        end
      end)()
    || T <- imem_meta:all_tables(), imem_meta:is_local_table(T) =:= true, imem_meta:is_local_time_partitioned_table(T) =/= true],
    ok
end.">>)).

%% ----- SERVER INTERFACE ------------------------------------------------
start_link(Params) ->
    ets:new(?MODULE, [public, named_table, {keypos,2}]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

init(_) ->
    ?Info("~p starting...~n", [?MODULE]),
    zip({re, "*.bkp"}),
    erlang:send_after(10000, self(), imem_snap_loop),
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    SnapshotDir = filename:absname(SnapDir),
    case filelib:is_dir(SnapDir) of
        false ->
            case filelib:ensure_dir(SnapshotDir) of
                ok ->
                    case file:make_dir(SnapDir) of
                        ok -> ok;
                        {error, eexists} -> ok;
                        {error, Error} ->
                            ?Warn("unable to create snapshot directory ~p : ~p~n", [SnapDir, Error])
                    end;
                {error, Error} ->
                    ?Warn("unable to create snaoshot directory ~p : ~p~n", [SnapDir, Error])
            end;
        _ -> ok
    end,
    ?Info("SnapshotDir ~p", [SnapshotDir]),
    ?Info("~p started!", [?MODULE]),
    {ok,#state{snapdir=SnapshotDir}}.

handle_info(imem_snap_loop, #state{snapFun=SFun,snapHash=SHash} = State) ->
    ?Debug("snapshot loop"),
    case ?GET_SNAPSHOT_CYCLE_WAIT of
        MCW when (is_integer(MCW) andalso (MCW >= 100)) ->
            {SnapHash,SnapFun} = case {?GET_SNAPSHOT_SCRIPT, ?GET_SNAPSHOT_SCRIPT_FUN} of
                {false, _} ->       {undefined,undefined};
                {true, <<"">>} ->   {undefined,undefined};
                {true, SFunStr} ->
                    case erlang:phash2(SFunStr) of
                        SHash   -> {SHash,SFun};
                        H1      -> {H1,imem_meta:compile_fun(SFunStr)}
                    end
            end,
            ?Debug("snapshot snap hash ~p, snap fun ~p", [SnapHash, SnapFun]),
            do_snapshot(SnapFun),
            erlang:send_after(MCW, self(), imem_snap_loop),
            {noreply, State#state{snapFun=SnapFun,snapHash=SnapHash}};
        Other ->
            ?Debug("snapshot unknown timeout ~p", [Other]),
            erlang:send_after(10000, self(), imem_snap_loop),
            {noreply, State}
    end;

handle_info(Info, State) ->
    ?Info("Unknown info ~p!", [Info]),
    {noreply, State}.

handle_call(Request, From, State) ->
    ?Info("Unknown request ~p from ~p!", [Request, From]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?Info("Unknown cast ~p!", [Request]),
    {noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

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
    MTabs = imem_meta:all_tables(tables),
    BytesPerWord =  erlang:system_info(wordsize),
    MnesiaTables = [{atom_to_list(M), imem_meta:table_size(M), imem_meta:table_memory(M) * BytesPerWord} || M <- MTabs],
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
        false -> {error, lists:flatten(io_lib:format("snapshot dir ~p found", [SnapDir]))}
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
    info({zip, ZipFiles}, [{filename:absname(Z),CntFiles}|ContentFiles]).


% take snapshot of all/some of the current in memory imem table
take([all]) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    take({ tabs
         , SnapDir
         , imem_meta:all_tables(tables)});

% multiple tables as list of strings or regex strings
take({tabs, [_R|_] = RegExs}) when is_list(_R) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    take({tabs
         , SnapDir
         , lists:flatten([[T || R <- RegExs, re:run(atom_to_list(T), R, []) /= nomatch]
                         || T <- imem_meta:all_tables(tables)])
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
        {atomic, ok}      -> {ok, T};
        {aborted, Reason} -> {error, lists:flatten(io_lib:format("snapshot ~p failed for ~p~n", [T, Reason]))}
    end
    || T <- Tabs]).

% snapshot restore interface
restore(bkp, Tabs, Strategy, Simulate) when is_list(Tabs) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    [(fun() ->
        Table = if
            is_atom(Tab) -> filename:rootname(filename:basename(atom_to_list(Tab)));
            is_list(Tab) -> filename:rootname(filename:basename(Tab))
        end,
        SnapFile = filename:join([SnapDir, Table++?BKP_EXTN]),
        {ok, Bin} = file:read_file(SnapFile),
        [{prop, TabProp}, {rows, Rows}] = 
            case binary_to_term(Bin) of
                [{prop,_},{rows,_}] = R -> R;
                R -> [{prop,[]},{rows,R}]
            end,
        restore(list_to_atom(Table), {prop, TabProp}, {rows, Rows}, Strategy, Simulate)
    end)()
    || Tab <- Tabs].

restore(zip, ZipFile, TabRegEx, Strategy, Simulate) when is_list(ZipFile) ->
    case filelib:is_file(ZipFile) of
        true ->
            case zip:foldl(fun(File, _, GBin, Acc) ->
                    case [File || R <- TabRegEx, re:run(File, R) /= nomatch] of
                        [] when length(TabRegEx) > 0 -> Acc;
                        _ ->
                            Tab = list_to_atom(filename:rootname(filename:basename(File))),
                            [{prop, TabProp}, {rows, Rows}] = 
                                case binary_to_term(GBin()) of
                                    [{prop,_},{rows,_}] = R -> R;
                                    R -> [{prop,[]},{rows,R}]
                                end,
                            [restore(Tab, {prop, TabProp}, {rows, Rows}, Strategy, Simulate) | Acc]
                    end
                end, [], ZipFile) of
                {ok, Res} -> Res;
                Error -> {error, lists:flatten(io_lib:format("restore error ~p for file ~p~n", [Error, filename:absname(ZipFile)]))}
            end;
        _ ->
            {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
            PossibleZipFile = filename:join([SnapDir, filename:basename(ZipFile)]),
            case filelib:is_file(PossibleZipFile) of
                true -> restore(zip, PossibleZipFile, TabRegEx, Strategy, Simulate);
                _ -> {error, lists:flatten(io_lib:format("file ~p not found~n", [filename:absname(PossibleZipFile)]))}
            end
    end;

% private real restore function
restore(Tab, {prop, TabProp}, {rows, Rows}, Strategy, Simulate) when is_atom(Tab) ->
    % restore the properties
    [mnesia:write_table_properties(Tab,P) || P <- TabProp],
    if (Simulate /= true) andalso (Strategy =:= destroy)
        -> {atomic, ok} = mnesia:clear_table(Tab);
        true -> ok
    end,
    Ret = mnesia:transaction(fun() ->
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
    _ = ?EMPTY_DIR(SnapDir),
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
    ?Log("snapshots :~n~p", [take([all])]),
    ?assertEqual(?TABLES, ?FILENAMES("*"++?BKP_EXTN, SnapDir)),
    _ = ?EMPTY_DIR(SnapDir),
    ?Log("snapshot tests completed!~n", []).

-endif.

%% ----- PRIVATE APIS ------------------------------------------------

timestamp({Mega, Secs, Micro}) -> Mega*1000000000000 + Secs*1000000 + Micro.

do_snapshot(SnapFun) ->
    try  
        case SnapFun of
            undefined ->    ok;
            E ->            ok = E()
        end,
        ok
    catch
        _:Err ->
            ?Error("cannot snap ~p", [Err]),
            {error,{"cannot snap",Err}}
    end.

get_snap_timestamps(Tab) ->
    case ets:lookup(?SNAP_ETS_TAB, Tab) of
        [] -> [];
        [#snap_properties{table=Tab, last_write=Wt, last_snap=St}|_] -> {Wt,St}
    end.
set_snap_timestamps(Tab,Time) ->
    case ets:lookup(?SNAP_ETS_TAB, Tab) of
        [] -> [];
        [#snap_properties{table=Tab}=Up|_] -> ets:insert(?SNAP_ETS_TAB, Up#snap_properties{last_snap=Time})
    end.
snap_log(P,A) -> ?Log(P,A).
snap_err(P,A) -> ?Error(P,A).

