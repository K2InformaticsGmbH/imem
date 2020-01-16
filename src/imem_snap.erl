-module(imem_snap).
-behavior(gen_server).

-include_lib("kernel/include/file.hrl").

-include("imem.hrl").
-include("imem_meta.hrl").

-record(state, { snapdir    = ""        :: list()
               , snapFun    = undefined :: any()
               , snapHash   = undefined :: any()
               , snap_timer = undefined :: reference()
               , csnap_pid  = undefined :: pid()
               }).

% snapshot interface
-export([ info/1
        , restore/4
        , restore/5
        , restore_as/5
        , zip/1
        , take/1
        , restore_chunked/3
        , del_dirtree/1
        , snap_file_count/0
        , exclude_table_pattern/1
        , filter_candidate_list/2
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
        , get_snap_properties/1
        , set_snap_properties/1
        , snap_log/2
        , snap_err/2
        , do_snapshot/0
        , do_snapshot/1
        , all_snap_tables/0
        , all_local_time_partitioned_tables/0
        ]).

-export([do_cluster_snapshot/0, filter_cluster_snapshot/1]).

-safe([all_snap_tables, filter_candidate_list, get_snap_properties,
       set_snap_properties, snap_log,snap_err,take, do_snapshot,
       do_cluster_snapshot]).

-define(BKP_EXTN, ".bkp").
-define(BKP_TMP_EXTN, ".bkp.new").
-define(BKP_ZIP_PREFIX, "backup_snapshot_").

-define(GET_SNAPSHOT_CYCLE_WAIT,
            ?GET_CONFIG(snapshotCycleWait, [], 10000,
                        "Wait time between snapshot cycles in msec.")).
-define(GET_SNAPSHOT_CHUNK_MAX_SIZE,
            ?GET_CONFIG(snapshotChunkMaxSize, [], 500,
                        "Maximum snapshot chunk size (number of rows).")).
-define(GET_SNAPSHOT_CHUNK_FETCH_TIMEOUT,
            ?GET_CONFIG(snapshotChunkFetchTimeout, [], 20000,
                        "Timeout in msec for fetching the next chunk from DB.")).
-define(GET_SNAPSHOT_SCRIPT,
            ?GET_CONFIG(snapshotScript, [], true,
                        "Do we want to use a specialized snapshot script function?")).
-define(GET_SNAPSHOT_EXCLUSION_PATTERNS,
            ?GET_CONFIG(snapshotExclusuionPatterns, [],
                        ["Dyn@.*", "Dyn$", "Audit_.*", "Audit$", "Hist$", "Idx$"],
                        "Snapshot excusion table name patterns")).
-define(PUT_SNAPSHOT_EXCLUSION_PATTERNS(__TablePatterns, __Remark),
            ?PUT_CONFIG(snapshotExclusuionPatterns, [], __TablePatterns, __Remark)).
-define(GET_SNAPSHOT_SCRIPT_FUN,
            ?GET_CONFIG(snapshotScriptFun, [],
                        <<"fun(ExcludePatterns) ->
                            ExcludeList = [dual, ddSize, ddNode | [imem_meta:physical_table_name(T) || T <- [ddCache@,ddSeCo@,ddPerm@]]],
                            Candidates = imem_snap:filter_candidate_list(ExcludePatterns,imem_snap:all_snap_tables() -- ExcludeList),
                            [(fun() ->
                                case imem_snap:get_snap_properties(T) of
                                    {} ->               ok;
                                    {Prop, Wt, St} ->
                                        if
                                            St < Wt ->
                                                Res = imem_snap:take(T),
                                                [case R of
                                                    {ok, T} ->
                                                        Str = lists:flatten(io_lib:format(\"snapshot created for ~p\", [T])),
                                                        imem_snap:snap_log(Str++\"~n\",[]),
                                                        imem_meta:log_to_db(info,imem_snap,handle_info,[snapshot],Str);
                                                    {error, T, Reason}  -> imem_snap:snap_err(\"snapshot of ~p failed for ~p\", [T, Reason])
                                                end || R <- Res],
                                                true = imem_snap:set_snap_properties(Prop);
                                            true ->
                                                ok % no backup needed
                                        end
                                end
                              end)()
                            || T <- Candidates],
                            ok
                        end.">>,"Function to perform customized snapshotting.")).
-define(GET_CLUSTER_SNAPSHOT,
            ?GET_CONFIG(snapshotCluster, [], true,
                        "Do we need periodic snapshotting of important tables an all nodes?")).
-define(GET_CLUSTER_SNAPSHOT_TABLES,
            ?GET_CONFIG(snapshotClusterTables, [], [ddAccount,ddRole,ddConfig],
                        "List of important tables to be regularily snapshotted.")).
-define(GET_CLUSTER_SNAPSHOT_TOD,
            ?GET_CONFIG(snapshotClusterHourOfDay, [], 14,
                        "Hour of (00..23)day in which important tables must be snapshotted.")).

-define(GET_SNAPSHOT_COPY_FILTER(_Table),
    ?GET_CONFIG(
        snapshotCopyFilter, [_Table], [["*"]],
        "Filters to apply during copying cluster snapshot."
    )
).

-define(B(_B), <<??_B>>).
-define(GET_SNAPSHOT_COPY_FILTER_FUN(_Table),
    ?GET_CONFIG(
        snapshotCopyFilterFun, [_Table],
        ?B(
            fun
                ({prop, DDTableProperties}, #{foldFnAcc := Acc} = Ctx) ->
                    if
                        copy -> {true, Ctx};
                        change ->
                            NewDDTableProperties = DDTableProperties,
                            {true, NewDDTableProperties, Ctx};
                        true ->
                            Acc1 = Acc,
                            {true, DDTableProperties, Ctx#{foldFnAcc := Acc1}}
                    end;
                (#{ckey := _Key, cvalue := _Map} = SkvhMap, #{foldFnAcc := Acc} = Ctx) ->
                    if
                        copy -> {true, Ctx};
                        skip -> {false, Ctx};
                        change ->
                            SkvhMap1 = SkvhMap,
                            {true, SkvhMap1, Ctx};
                        true ->
                            Acc1 = Acc,
                            {true, SkvhMap, Ctx#{foldFnAcc := Acc1}}
                    end;
                (RecTuple, #{foldFnAcc := Acc} = Ctx) ->
                    if
                        copy -> {true, Ctx};
                        skip -> {false, Ctx};
                        change ->
                            RecTuple1 = RecTuple,
                            {true, RecTuple1, Ctx};
                        true ->
                            Acc1 = Acc,
                            {true, RecTuple, Ctx#{foldFnAcc := Acc1}}
                    end
            end
        ),
        "Function to apply on every record of the table."
    )
).

-spec(
    start_snap_loop() -> ok | pid()
).
-ifdef(TEST).
    start_snap_loop() -> ok.
-else.
    start_snap_loop() ->
        erlang:whereis(?MODULE) ! imem_snap_loop.
-endif.

-spec(
    suspend_snap_loop() -> term()
).
suspend_snap_loop() ->
    erlang:whereis(?MODULE) ! imem_snap_loop_cancel.

%% ----- SERVER INTERFACE ------------------------------------------------
%% ?SERVER_START_LINK.
start_link(Params) ->
    ?Info("~p starting...~n", [?MODULE]),
    catch ets:new(?MODULE, [public, named_table, {keypos,2}]),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]) of
        {ok, _} = Success ->
            ?Info("~p started!~n", [?MODULE]),
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [?MODULE, Error]),
            Error
    end.

init(_) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    SnapshotDir = filename:absname(SnapDir),
    ?Info("snapshot directory ~s~n", [SnapshotDir]),
    process_flag(trap_exit, true),
    catch ?Info("~s~n", [zip({re, "*.bkp"})]),
    start_snap_loop(),
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
                    ?Warn("unable to create snapshot directory ~p : ~p~n", [SnapDir, Error])
            end;
        _ ->
            case maybe_coldstart_restore(SnapshotDir) of
                true ->
                    no_op;
                false ->
                    start_snap_loop()
            end
    end,
    erlang:send_after(
      1000, ?MODULE,
      {cluster_snap, '$replace_with_timestamp', '$create_when_needed'}),
    {ok,#state{snapdir = SnapshotDir}}.

-spec create_clean_dir(list()) -> list().
create_clean_dir(Prefix) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    {Secs, Micros} = ?TIMESTAMP,
    {{Y,M,D},{H,Mn,S}} = calendar:now_to_local_time({Secs div 1000000, Secs rem 1000000, Micros}),
    FSec = S + Micros / 1000000,
    Dir = lists:flatten(io_lib:format("~s~4..0B~2..0B~2..0B_~2..0B~2..0B~9.6.0f", [Prefix,Y,M,D,H,Mn,FSec])),
    BackupDir = filename:join(filename:absname(SnapDir), Dir),
    case filelib:is_dir(BackupDir) of
        true ->
            lists:foreach(fun(F) ->
                                  ok = file:delete(F)
                          end, filelib:wildcard("*.*", BackupDir));
        false ->
            ?Info("creating cluster snap dir ~s", [BackupDir]),
            ok = file:make_dir(BackupDir)
    end,
    BackupDir.

-spec cluster_snap(list(), '$replace_with_timestamp' | ddTimestamp(), '$create_when_needed' | list()) -> ok.
cluster_snap(Tabs, '$replace_with_timestamp', Dir) ->
    cluster_snap(Tabs, ?TIMESTAMP, Dir);
cluster_snap([], StartTime, Dir) ->
    ZipFile = filename:join(filename:dirname(Dir), filename:basename(Dir)++".zip"),
    ZipCandidates = filelib:wildcard("*.bkp", Dir),
    ?Debug("zip:zip(~p, ~p)", [ZipFile, ZipCandidates]),
    case zip:zip(ZipFile, ZipCandidates, [{cwd, Dir}]) of
        {error, Reason} ->
            ?Error("cluster snapshot ~s failed reason : ~p", [ZipFile, Reason]);
        _ ->
            lists:foreach(
              fun(F) -> ok = file:delete(filename:join(Dir,F)) end,
              filelib:wildcard("*.*", Dir)),
            ok = file:del_dir(Dir),
            ?Info("cluster snapshot ~s", [ZipFile])
    end,
    ?Info("cluster snapshot took ~p ms", [?TIMESTAMP_DIFF(StartTime, ?TIMESTAMP) div 1000]),
    erlang:send_after(1000, ?MODULE, {cluster_snap, '$replace_with_timestamp', '$create_when_needed'}),
    ok;
cluster_snap(Tabs, StartTime, '$create_when_needed') ->
    cluster_snap(Tabs, StartTime, create_clean_dir(?BKP_ZIP_PREFIX));
cluster_snap([T|Tabs], {_,_} = StartTime, Dir) ->
    NextTabs = case catch take_chunked(imem_meta:physical_table_name(T), Dir) of
                   ok ->
                       ?Info("cluster snapshot ~p", [T]),
                        Tabs;
                   {'ClientError', {"Table does not exist", T}} ->
                       ?Warn("cluster snapshot - Table : ~p does not exist", [T]),
                       Tabs;
                   [{error,Error}] ->
                       ?Error("cluster snapshot failed for ~p : ~p", [T, Error]),
                       Tabs++[T];
                   Error ->
                       ?Error("cluster snapshot failed for ~p : ~p", [T, Error]),
                       Tabs++[T]
               end,
    ?MODULE ! {cluster_snap, NextTabs, StartTime, Dir},
    ok.

handle_info({cluster_snap, StartTime, Dir}, State) ->
    handle_info({cluster_snap, ?GET_CLUSTER_SNAPSHOT_TABLES, StartTime, Dir}, State);
handle_info({cluster_snap, Tables, StartTime, Dir}, State) ->
    {noreply,
     case ?GET_CLUSTER_SNAPSHOT of
         true ->
             {{Y,M,D},{H,_,_}} = imem_datatype:timestamp_to_local_datetime(imem_meta:time()),
             {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
             ZipFilePattern = lists:flatten(io_lib:format(?BKP_ZIP_PREFIX"~4..0B~2..0B~2..0B_~2..0B*.zip", [Y,M,D,H])),
             BackupDir = filename:absname(SnapDir),
             case filelib:wildcard(ZipFilePattern, BackupDir) of
                 BFs when length(BFs) > 0 ->
                     case catch is_process_alive(State#state.csnap_pid) of
                         true -> State;
                         _ ->
                             erlang:send_after(
                               60 * 1000, ?MODULE,
                               {cluster_snap, '$replace_with_timestamp', '$create_when_needed'}),
                             State#state{csnap_pid = (#state{})#state.csnap_pid}
                     end;
                 _ ->
                     case ?GET_CLUSTER_SNAPSHOT_TOD of
                         H ->
                             if Dir == '$create_when_needed' ->
                                    ?Info("cluster snapshot ~p", [Tables]);
                                true -> ok
                             end,
                             State#state{csnap_pid=spawn(fun() -> cluster_snap(Tables, StartTime, Dir) end)};
                         _ ->
                             case catch is_process_alive(State#state.csnap_pid) of
                                 true -> State;
                                 _ ->
                                     erlang:send_after(
                                       60 * 1000, ?MODULE,
                                       {cluster_snap, '$replace_with_timestamp', '$create_when_needed'}),
                                     State#state{csnap_pid = (#state{})#state.csnap_pid}
                             end
                     end
             end;
         false ->
             erlang:send_after(
               5000, ?MODULE,
               {cluster_snap, '$replace_with_timestamp', '$create_when_needed'}),
             State
     end};
handle_info(imem_snap_loop, #state{snapFun=SFun,snapHash=SHash} = State) ->
    case ?GET_SNAPSHOT_CYCLE_WAIT of
        MCW when (is_integer(MCW) andalso (MCW >= 100)) ->
            {SnapHash,SnapFun} = case ?GET_SNAPSHOT_SCRIPT of
                false -> {undefined,undefined};
                true -> case ?GET_SNAPSHOT_SCRIPT_FUN of
                    <<"">> -> {undefined,undefined};
                    SFunStr ->
                        ?Debug("snapshot fun ~p~n", [SFunStr]),
                        case erlang:phash2(SFunStr) of
                            SHash   -> {SHash,SFun};
                            H1      -> {H1,imem_compiler:compile(SFunStr)}
                        end
                end
            end,
            do_snapshot(SnapFun),
            ?Debug("imem_snap_loop after GET_SNAPSHOT_CYCLE_WAIT ~p~n", [MCW]),
            SnapTimer = erlang:send_after(MCW, self(), imem_snap_loop),
            {noreply, State#state{snapFun=SnapFun,snapHash=SnapHash,snap_timer=SnapTimer}};
        _Other ->
            ?Info("snapshot unknown timeout ~p~n", [_Other]),
            SnapTimer = erlang:send_after(10000, self(), imem_snap_loop),
            {noreply, State#state{snap_timer = SnapTimer}}
    end;
handle_info(imem_snap_loop_cancel, #state{snap_timer=SnapTimer} = State) ->
    case SnapTimer of
        undefined ->
            ok;
        SnapTimer ->
            ?Debug("timer paused~n"),
            erlang:cancel_timer(SnapTimer)
    end,
    {noreply, State#state{snap_timer = undefined}};

handle_info(_Info, State) ->
    ?Info("Unknown info ~p!~n", [_Info]),
    {noreply, State}.

handle_call(_Request, _From, State) ->
    ?Info("Unknown request ~p from ~p!~n", [_Request, _From]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    ?Info("Unknown cast ~p!~n", [_Request]),
    {noreply, State}.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, _Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

%% ----- SNAPSHOT INTERFACE ------------------------------------------------

% backup existing snapshot
-spec(
    zip(all | {re, string()} | {files, [string()]}) ->
        ok | string()
).
zip(all) -> zip({re, "*"++?BKP_EXTN});
zip({re, MatchPattern}) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    SnapFiles =
    [filename:basename(File)
    || File <- filelib:wildcard(filename:join([SnapDir, MatchPattern]))
    ],
    zip({files, SnapFiles});
zip({files, SnapFiles}) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    ZipCandidates = [
        SF || SF <- SnapFiles,
        filelib:file_size(filename:join([SnapDir, SF])) > 0
    ],
    if ZipCandidates =:= [] -> ok;
        true ->
            {Secs, Micros} = ?TIMESTAMP,
            {{Y,M,D},{H,Mn,S}} = calendar:now_to_local_time({Secs div 1000000, Secs rem 1000000, Micros}),
            FSec = S + Micros / 1000000,
            ZipFileName = re:replace(lists:flatten(["snapshot_"
                                         , io_lib:format("~4..0B~2..0B~2..0B_~2..0B~2..0B~9.6.0f", [Y,M,D,H,Mn,FSec])
                                         , ".zip"
                                        ]), "[<>:\"\\\\|?*]", "", [global, {return, list}]),
            % to make the file name valid for windows
            ZipFileFullPath = filename:join(SnapDir, ZipFileName),
            case zip:zip(ZipFileFullPath, ZipCandidates, [{cwd, SnapDir}]) of
                {error, Reason} ->
                    lists:flatten(io_lib:format("old snapshot backup to ~s failed reason : ~p"
                                                , [ZipFileFullPath, Reason]));
                _ ->
                    lists:flatten(io_lib:format("old snapshots are backed up to ~s"
                                                , [ZipFileFullPath]))
            end
    end.

% display information of existing snapshot or a snapshot bundle (.zip)
-spec(
    info(bkp | zip) ->
        {bkp, [{dbtables | snaptables | restorabletables, list()}]} |
        {zip, list()} | {error, string()}
).
info(bkp) ->
    MTabs = lists:filter(fun(T) -> imem_meta:is_readable_table(T) end, imem_meta:all_tables()),
    MnesiaTables = [{atom_to_list(M), imem_meta:table_size(M), imem_meta:table_memory(M)} || M <- MTabs],
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
     info({zip, ZipFiles},
          case zip:foldl(fun(F, IF, _, Acc) ->
                                 [{F, IF()} | Acc]
                         end, [], Z) of
              {ok, CntFiles} ->
                  [{filename:absname(Z),CntFiles}|ContentFiles];
              {error, Error} ->
                  ?Error("processing ~p: ~p", [Z, Error]),
                  ContentFiles
          end).

% take snapshot of all/some of the current in memory imem tables
-spec(
    take([all] | [local] | {tabs, [atom() | string()]}) ->
        {ok, atom() | string()} | {error, atom() | string() | term()}
).
take([all]) ->                                      % standard snapshot tables (no local time partitioned tables)
    take({tabs, all_snap_tables()});
take([local]) ->                                    % local time partitioned tables
    take({tabs, all_local_time_partitioned_tables()});
take({tabs, [_R|_] = RegExs}) when is_list(_R) ->   % multiple tables as list of strings or regex strings
    FilteredSnapReadTables = lists:filter(fun(T) -> imem_meta:is_readable_table(T) end, imem_meta:all_tables()),
    ?Debug("tables readable for snapshot ~p~n", [FilteredSnapReadTables]),

    SelectedSnapTables = lists:flatten([[T || R <- RegExs, re:run(atom_to_list(T), R, []) /= nomatch]
                         || T <- FilteredSnapReadTables]),

    ?Debug("tables being snapshoted ~p~n", [SelectedSnapTables]),

    case SelectedSnapTables of
        []  -> {error, lists:flatten(io_lib:format(" ~p doesn't match any table in ~p~n", [RegExs, FilteredSnapReadTables]))};
        _   -> take({tabs, SelectedSnapTables})
    end;
take({tabs, Tabs}) ->                               % list of tables as atoms or snapshot transform maps
    lists:flatten([
        case take_chunked(Tab) of
            ok -> {ok, Tab};
            {error, Reason} -> {error, lists:flatten(io_lib:format("snapshot ~p failed for ~p~n", [Tab, Reason]))}
        end
    || Tab <- Tabs]);
take(Tab) when is_atom(Tab) ->                      % single table as atom (internal use)
    take({tabs, [Tab]});
take(Tab) when is_map(Tab) ->                       % single table using transform map
    take({tabs, [Tab]}).


% snapshot restore interface
%  - periodic snapshoting timer is paused during a restore operation
-spec(
    restore(bkp, [atom() | list()], destroy | replace, true | false) ->
        [{atom(), term()}]
).
restore(bkp, Tabs, Strategy, Simulate) when is_list(Tabs) ->
    suspend_snap_loop(),
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    Res = [(fun() ->
        Table = if
            is_atom(Tab) -> filename:rootname(filename:basename(atom_to_list(Tab)));
            is_list(Tab) -> filename:rootname(filename:basename(Tab))
        end,
        SnapFile = filename:join([SnapDir, Table++?BKP_EXTN]),
        {Tab, restore_chunked(list_to_atom(Table), SnapFile, Strategy, Simulate)}
    end)()
    || Tab <- Tabs],
    start_snap_loop(),
    Res.

% snapshot restore_as interface
%  - periodic snapshoting timer is paused during a restore operation
-spec(
    restore_as(bkp, atom() | string(), atom() | binary() | string(), destroy | replace, true | false) ->
        ok | {error, term()}
).
restore_as(Op, SrcTab, DstTab, Strategy, Simulate) when is_atom(SrcTab) ->
    restore_as(Op, atom_to_list(SrcTab), DstTab, Strategy, Simulate);
restore_as(Op, SrcTab, DstTab, Strategy, Simulate) when is_atom(DstTab) ->
    restore_as(Op, SrcTab, atom_to_list(DstTab), Strategy, Simulate);
restore_as(Op, SrcTab, DstTab, Strategy, Simulate) when is_binary(DstTab) ->
    restore_as(Op, SrcTab, binary_to_list(DstTab), Strategy, Simulate);
restore_as(bkp, SrcTab, DstTab, Strategy, Simulate) ->
    suspend_snap_loop(),
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    DstSnapFile = filename:join([SnapDir, DstTab++?BKP_EXTN]),
    SnapFile = case filelib:is_file(DstSnapFile) of
                   true -> DstSnapFile;
                   false -> filename:join([SnapDir, SrcTab++?BKP_EXTN])
               end,
    case restore_chunked(list_to_atom(DstTab), SnapFile, Strategy, Simulate) of
        {L1,L2,L3} ->
            ?Info("Restored table ~s as ~s from ~s with result ~p", [SrcTab, DstTab, SnapFile, {L1,L2,L3}]),
            start_snap_loop(),
            ok;
        Error ->
            start_snap_loop(),
            Error
    end.

-spec(
    restore(zip, string(), re:mp() | iodata() | unicode:charlist(), destroy | replace, true | false) ->
        list() | {error, string()}
).
restore(zip, ZipFile, TabRegEx, Strategy, Simulate) when is_list(ZipFile) ->
    case filelib:is_file(ZipFile) of
        true ->
            suspend_snap_loop(),
            {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
            {ok,Fs} = zip:unzip(ZipFile, [{cwd, SnapDir}]),
            ?Debug("unzipped ~p from ~p~n", [Fs,ZipFile]),
            Files = [F
                    || F <- Fs, re:run(F,TabRegEx,[{capture, all, list}]) =/= nomatch],
            ?Debug("restoring ~p from ~p~n", [Files,ZipFile]),
            Res = lists:foldl(
                fun(SnapFile, Acc) ->
                    case filelib:is_dir(SnapFile) of
                        false ->
                            Tab = list_to_atom(filename:basename(SnapFile, ?BKP_EXTN)),
                            [{Tab, restore_chunked(Tab, SnapFile, Strategy, Simulate)} | Acc];
                        _ -> Acc
                    end
                end,
                [], Files
            ),
            start_snap_loop(),
            Res;
        _ ->
            {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
            PossibleZipFile = filename:join([SnapDir, filename:basename(ZipFile)]),
            case filelib:is_file(PossibleZipFile) of
                true -> restore(zip, PossibleZipFile, TabRegEx, Strategy, Simulate);
                _ -> {error, lists:flatten(io_lib:format("file ~p not found~n", [filename:absname(PossibleZipFile)]))}
            end
    end.

-spec(
    restore_chunked(atom() | string(), destroy | replace, true | false) ->
        ok | {Inserted :: integer(), Edited :: integer(), Appended ::integer} |
        {error, term()}
).
restore_chunked(Tab, Strategy, Simulate) ->
    ?Debug("restoring ~p by ~p~n", [Tab, Strategy]),
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    Table = if
        is_atom(Tab) -> filename:rootname(filename:basename(atom_to_list(Tab)));
        is_list(Tab) -> filename:rootname(filename:basename(Tab))
    end,
    SnapFile = filename:join([SnapDir, Table++?BKP_EXTN]),
    restore_chunked(Tab, SnapFile, Strategy, Simulate).

restore_chunked(Tab, SnapFile, Strategy, Simulate) ->
    ?Debug("restoring ~p from ~p by ~p~n", [Tab, SnapFile, Strategy]),
    case file:open(SnapFile, [read, raw, binary]) of
       {ok, FHndl} ->
           if
                (Simulate /= true) andalso (Strategy =:= destroy) ->
                    catch imem_meta:truncate_table(Tab);
                true -> ok
           end,
           read_chunk(Tab, SnapFile, FHndl, Strategy, Simulate, {0,0,0});
       {error, Error} -> {error, Error}
   end.

read_chunk(Tab, SnapFile, FHndl, Strategy, Simulate, Opts) ->
    ?Debug("backup file header read"),
    case file:read(FHndl, 4) of
        eof ->
            ?Info("backup file ~p restored", [SnapFile]),
            file:close(FHndl),
            Opts;
        {ok, << Length:32 >>} ->
            ?Debug("backup chunk size ~p", [Length]),
            case file:read(FHndl, Length) of
                eof ->
                    ?Info("corrupted file ~p~n", [SnapFile]),
                    file:close(FHndl);
                {ok, Data} when is_binary(Data) ->
                    restore_chunk(Tab, binary_to_term(Data), SnapFile, FHndl, Strategy, Simulate, Opts);
                {error, Reason} ->
                    ?Error("reading ~p error ~p~n", [SnapFile, Reason]),
                    file:close(FHndl)
            end;
        {ok, Data} ->
            ?Error("reading ~p framing, header size ~p~n", [SnapFile, byte_size(Data)]),
            file:close(FHndl);
        {error, Reason} ->
            ?Error("reading ~p error ~p~n", [SnapFile, Reason]),
            file:close(FHndl)
    end.

restore_chunk(Tab, {prop, UserProperties}, SnapFile, FHndl, Strategy, Simulate, Opts) ->
    ?Info("restore properties of ~p~n", [Tab]),
    [mnesia:write_table_property(
       Tab,
       case P of
           #ddTable{columns = Cols, opts = TabOpts, owner = Owner} ->
               TOpts = proplists:delete(trigger, proplists:delete(index, TabOpts)),
               NewOpts = case proplists:get_value(record_name, TOpts, enoent) of
                             enoent ->
                                  [{record_name,
                                    list_to_atom(
                                      filename:rootname(
                                        filename:basename(SnapFile)))} | TOpts];
                              _ -> TOpts
                          end,
                _Res = (catch imem_meta:create_check_table(Tab, Cols, NewOpts, Owner)),
                P0 = P#ddTable{opts = NewOpts},
                ?Debug("creating table ~p with properties ~p result ~p", [Tab, P0, _Res]),
                P0;
            _ -> P
       end) || P <- UserProperties],
    ?Debug("all user_properties restored for ~p~n", [Tab]),
    read_chunk(Tab, SnapFile, FHndl, Strategy, Simulate, Opts);
restore_chunk(Tab, Rows, SnapFile, FHndl, Strategy, Simulate, {OldI, OldE, OldA}) when is_list(Rows) ->
    ?Debug("restore rows ~p~n", [length(Rows)]),
    {atomic, {NewI, NewE, NewA}} =
    imem_meta:transaction(fun() ->
        TableSize = imem_meta:table_size(Tab),
        TableType = imem_if_mnesia:table_info(Tab, type),
        lists:foldl(
          fun(Row, {I, E, A}) ->
            UpdatedRows = E + A,
            if (UpdatedRows > 0 andalso UpdatedRows rem 500 == 0) ->
                   ?Info("restoring ~p updated ~p rows~n", [Tab, UpdatedRows]);
               true -> ok
            end,
            if (TableSize > 0) andalso (TableType =/= bag) ->
                K = element(2, Row),
                case imem_meta:read(Tab, K) of
                    [Row] ->    % found identical existing row
                            {I+1, E, A};
                    [_] ->   % existing row with different content,
                        case Strategy of
                            replace ->
                                if Simulate /= true -> ok = imem_meta:write(Tab,Row); true -> ok end,
                                {I, E+1, A};
                            destroy ->
                                if Simulate /= true -> ok = imem_meta:write(Tab,Row); true -> ok end,
                                {I, E, A+1};
                            _ -> {I, E, A}
                        end;
                    [] -> % row not found, appending
                        if Simulate /= true -> ok = imem_meta:write(Tab,Row); true -> ok end,
                        {I, E, A+1}
                end;
            true ->
                if Simulate /= true -> ok = imem_meta:write(Tab,Row); true -> ok end,
                {I, E, A+1}
            end
        end,
        {OldI, OldE, OldA},
        Rows)
    end),
    read_chunk(Tab, SnapFile, FHndl, Strategy, Simulate, {NewI, NewE, NewA}).

-spec(all_snap_tables() -> list()).
all_snap_tables() ->
    lists:filter(fun(T) ->
            	    imem_meta:is_readable_table(T)
                    andalso not imem_meta:is_local_time_partitioned_table(T)
                end, imem_meta:all_tables()).

-spec(all_local_time_partitioned_tables() -> list()).
all_local_time_partitioned_tables() ->
    lists:filter(fun(T) ->
                    imem_meta:is_readable_table(T)
                    andalso imem_meta:is_local_time_partitioned_table(T)
                end, imem_meta:all_tables()).

%% ----- Helper function for snapshot script -------------------------
-spec filter_candidate_list([string()], [atom()]) -> [atom()].
filter_candidate_list(ExcludePatterns, Candidates) ->
    {ok, Compiled} = re:compile(string:join(ExcludePatterns, "|")),
    [C || C <- Candidates, re:run(atom_to_list(C), Compiled) =:= nomatch].

%% ----- PRIVATE APIS ------------------------------------------------

close_file(Me, FHndl) ->
    Ret = case file:close(FHndl) of
        ok -> done;
        {error,_} = Error -> Error
    end,
    Me ! Ret.

write_file(Me,Tab,FetchFunPid,RTrans,FHndl,NewRowCount,NewByteCount,RowsBin) ->
    PayloadSize = byte_size(RowsBin),
    case file:write(FHndl, << PayloadSize:32, RowsBin/binary >>) of
        ok ->
            FetchFunPid ! next,
            take_fun(Me,Tab,FetchFunPid,RTrans,NewRowCount,NewByteCount,FHndl);
        {error,_} = Error ->
            Me ! Error
    end.

write_close_file(Me, FHndl, RowsBin) ->
    PayloadSize = byte_size(RowsBin),
    Ret = case file:write(FHndl, << PayloadSize:32, RowsBin/binary >>) of
        ok ->
            case file:close(FHndl) of
                ok -> done;
                {error,_} = Error -> Error
            end;
        {error,_} = Error -> Error
    end,
    Me ! Ret.

take_fun(Me,Tab,FetchFunPid,RTrans,RowCount,ByteCount,FHndl) ->
    FetchFunPid ! next,
    receive
        {row, ?eot} ->
            ?Debug("table ~p fetch finished~n",[Tab]),
            close_file(Me, FHndl);
        {row, [?sot,?eot]} ->
            ?Debug("empty ~p~n",[Tab]),
            close_file(Me, FHndl);
        {row, [?sot,?eot|Rows]} ->
            RowsBin = term_to_binary([RTrans(R) || R <- Rows]),
            ?Debug("snap ~p all, total ~p rows ~p bytes~n",[Tab, RowCount+length(Rows), ByteCount+byte_size(RowsBin)]),
            write_close_file(Me, FHndl,RowsBin);
        {row, [?eot|Rows]} ->
            RowsBin = term_to_binary([RTrans(R) || R <- Rows]),
            ?Debug("snap ~p last, total ~p rows ~p bytes~n",[Tab, RowCount+length(Rows), ByteCount+byte_size(RowsBin)]),
            write_close_file(Me, FHndl,RowsBin);
        {row, [?sot|Rows]} ->
            NewRowCount = RowCount+length(Rows),
            RowsBin = term_to_binary([RTrans(R) || R <- Rows]),
            NewByteCount = ByteCount+byte_size(RowsBin),
            ?Debug("snap ~p first ~p rows ~p bytes~n",[Tab, NewRowCount, NewByteCount]),
            write_file(Me,Tab,FetchFunPid,RTrans,FHndl,NewRowCount,NewByteCount,RowsBin);
        {row, Rows} ->
            NewRowCount = RowCount+length(Rows),
            RowsBin = term_to_binary([RTrans(R) || R <- Rows]),
            NewByteCount = ByteCount+byte_size(RowsBin),
            ?Debug("snap ~p intermediate, total ~p rows ~p bytes~n",[Tab, NewRowCount, NewByteCount]),
            write_file(Me,Tab,FetchFunPid,RTrans,FHndl,NewRowCount,NewByteCount,RowsBin)
    after
        ?GET_SNAPSHOT_CHUNK_FETCH_TIMEOUT ->
            FetchFunPid ! abort,
            close_file(Me, FHndl)
    end.

take_chunked(Tab) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    take_chunked(Tab, SnapDir).

take_chunked(Tab, SnapDir) when is_atom(Tab) ->
    take_chunked(#{table=>Tab}, SnapDir);
take_chunked(Map, SnapDir) when is_map(Map) ->
    NTrans = fun(Name) -> atom_to_list(Name) end,
    PTrans = fun(Prop) -> Prop end,
    RTrans = fun(Row) -> Row end,
    take_chunked_transform(maps:merge(#{nTrans=>NTrans, pTrans=>PTrans, rTrans=>RTrans}, Map), SnapDir).

take_chunked_transform(#{table:=Tab, nTrans:=NTrans, pTrans:=PTrans, rTrans:=RTrans}, SnapDir) ->
    % Tab (atom) table name
    % NTrans (fun/1) name translation
    % PTrans (fun/1) property translation
    % RTrans (fun/1) row translation
    BackFile = filename:join([SnapDir, NTrans(Tab) ++ ?BKP_EXTN]),
    NewBackFile = filename:join([SnapDir, NTrans(Tab) ++ ?BKP_TMP_EXTN]),
    % truncates the file if already exists and writes the table props
    TblPropBin = case imem_if_mnesia:read(ddTable, {imem_meta:schema(),Tab}) of
        [] ->           term_to_binary({prop, PTrans(imem_if_mnesia:table_info(Tab, user_properties))});
        [DDTableRow] -> term_to_binary({prop, PTrans([DDTableRow])})
    end,
    PayloadSize = byte_size(TblPropBin),
    ok = file:write_file(NewBackFile, << PayloadSize:32, TblPropBin/binary >>),
    Me = self(),
    _Pid = spawn(fun() ->
        AvgRowSize = case imem_meta:table_size(Tab) of
            0 -> imem_meta:table_memory(Tab);
            Sz -> imem_meta:table_memory(Tab) / Sz
        end,
        ChunkSize = lists:min([erlang:round((element(2,imem:get_os_memory()) / 2)
                                 / AvgRowSize)
                    , ?GET_SNAPSHOT_CHUNK_MAX_SIZE]),
        ?Debug("[~p] snapshoting ~p of ~p rows ~p bytes~n", [self(), Tab, imem_meta:table_size(Tab)
                                                               , imem_meta:table_memory(Tab)]),
        FHndl = case file:open(NewBackFile, [append, raw
                            , {delayed_write, erlang:round(ChunkSize * AvgRowSize)
                              , 2 * ?GET_SNAPSHOT_CHUNK_FETCH_TIMEOUT}]) of
            {ok, FileHandle} -> FileHandle;
            {error, Error} ->
                ?Error("Error : ~p opening file : ~p", [Error, NewBackFile]),
                error(Error)
        end,
        FetchFunPid = imem_if_mnesia:fetch_start(self(), Tab, [{'$1', [], ['$1']}], ChunkSize, []),
        take_fun(Me,Tab,FetchFunPid,RTrans,0,0,FHndl)
    end),
    receive
        done    ->
            ?Debug("[~p] snapshoted ~p~n", [_Pid, Tab]),
            {ok, _} = file:copy(NewBackFile, BackFile),
            ok = file:delete(NewBackFile),
            ok;
        timeout ->
            ?Debug("[~p] timeout while snapshoting ~p~n", [_Pid, Tab]),
            {error, timeout};
        {error, Error} ->
            ?Debug("[~p] error while snapshoting ~p error ~p~n", [_Pid, Tab, Error]),
            {error, Error}
    end.

-spec(
    timestamp({integer(), integer(), integer()}) -> integer()
).
timestamp({Mega, Secs, Micro}) -> Mega*1000000000000 + Secs*1000000 + Micro.

-spec(
    del_dirtree(file:name_all()) ->
        ok | {error, term()}
).
del_dirtree(Path) ->
    case filelib:is_regular(Path) of
        true -> file:delete(Path);
        _ ->
            lists:foreach(fun(F) ->
                    case filelib:is_dir(F) of
                        true -> del_dirtree(F);
                        _ -> file:delete(F)
                    end
                end,
                filelib:wildcard(filename:join([Path,"**","*"]))
            ),
            file:del_dir(Path)
    end.

do_snapshot() ->
    do_snapshot(imem_compiler:compile(?GET_SNAPSHOT_SCRIPT_FUN)).

-spec(
    do_snapshot(function()) ->
        ok | term() | {error, {string(), term()}}
).
do_snapshot(SnapFun) ->
    try
        ok = case SnapFun of
            undefined -> ok;
            SnapFun when is_function(SnapFun) -> SnapFun(?GET_SNAPSHOT_EXCLUSION_PATTERNS)
        end
    catch
        _:Err ->
            ?Error("cannot snap ~p~n", [Err]),
            {error,{"cannot snap",Err}}
    end.

-spec(get_snap_properties(atom()) -> {} | #snap_properties{}).
get_snap_properties(Tab) ->
    case ets:lookup(?SNAP_ETS_TAB, Tab) of
        [] ->       {};
        [Prop] ->   {Prop, Prop#snap_properties.last_write, Prop#snap_properties.last_snap}
    end.

-spec(set_snap_properties(#snap_properties{}) -> true).
set_snap_properties(Prop) ->
    ets:insert(?SNAP_ETS_TAB, Prop#snap_properties{last_snap=?TIMESTAMP}).

-spec(snap_log(string(), list()) -> ok).
snap_log(_P,_A) -> ?Info(_P,_A).

-spec(snap_err(string(), list()) -> ok).
snap_err(P,A) -> ?Error(P,A).

-spec(snap_file_count() -> integer()).
snap_file_count() ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    length(filelib:wildcard("*.{bkp,zip}",SnapDir)).

-spec(exclude_table_pattern(binary() | list()) -> ok).
exclude_table_pattern(TablePattern) when is_binary(TablePattern) ->
    exclude_table_pattern(binary_to_list(TablePattern));
exclude_table_pattern(TablePattern) when is_list(TablePattern) ->
    ExPatterns = ?GET_SNAPSHOT_EXCLUSION_PATTERNS,
    Remark = list_to_binary(["Added ", TablePattern, " at ", imem_datatype:timestamp_to_io(?TIMESTAMP)]),
    ?PUT_SNAPSHOT_EXCLUSION_PATTERNS(lists:usort([TablePattern | ExPatterns]), Remark).

maybe_coldstart_restore(SnapDir) ->
    case {application:get_env(imem, cold_start_recover), imem_meta:nodes()} of
        {{ok, true}, []} ->
            case lists:reverse(lists:sort(filelib:wildcard(?BKP_ZIP_PREFIX"*.zip", SnapDir))) of
                [] ->
                    ?Warn("Cold Start : unable to auto restore, no "?BKP_ZIP_PREFIX"*.zip found in snapshot directory ~s", [SnapDir]),
                    false;
                [ZipFile | _ ] ->
                    ?Info("Cold Start : auto restoring ~s found at ~s", [ZipFile, SnapDir]),
                    restore(zip, filename:join(SnapDir, ZipFile), [], replace, false),
                    true
            end;
        {{ok, true}, _} ->
            ?Info("Not Cold Start : auto restore from cluster snapshot is skipped"),
            false;
        {_, []} ->
            ?Warn("Cold Start : auto restore from cluster snapshot is disabled"),
            false;
        _ ->
            false
    end.

%-------------------------------------------------------------------------------
% filter_cluster_snapshot and helpers
%-------------------------------------------------------------------------------

filter_cluster_snapshot(Target) ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    case lists:reverse(
        lists:sort(filelib:wildcard(?BKP_ZIP_PREFIX"*.zip", SnapDir))
    ) of
        [] ->
            ?Warn(
                "unable to filter cluster snapshot : no "?BKP_ZIP_PREFIX"*.zip"
                " found in snapshot directory ~s",
                [SnapDir]
            );
        [ZipFile | _ ] ->
            Source = filename:join(SnapDir, ZipFile),
            ?Info(
                "creating filtered copy of clusterd snapshot from ~s to ~s",
                [Source, Target]
            ),
            Path = filename:dirname(Target),
            File = filename:basename(Target),
            TempDir = filename:join(
                Path,
                "_temp" ++ filename:rootname(File)
            ),
            del_all_dir_tree(TempDir),
            ok = filelib:ensure_dir(filename:join(TempDir, "dummy.txt")),
            ?Info("TargetPath ~s", [TempDir]),
            {ok, Files} = zip:foldl(
                fun(FileInArchive, GetInfoFun, GetBinFun, Files) ->
                    Table = filename:basename(
                        FileInArchive, filename:extension(FileInArchive)
                    ),
                    case ?GET_SNAPSHOT_COPY_FILTER(Table) of
                        [] -> Files;
                        Filters ->
                            FoldFun = imem_compiler:compile(
                                ?GET_SNAPSHOT_COPY_FILTER_FUN(Table)
                            ),
                            TargetFile = filename:join(TempDir, FileInArchive),
                            ?Info("TargetFile ~p", [TargetFile]),
                            {ok, TargetFileHandle} = file:open(TargetFile, [write, raw]),
                            Stats = try
                                Sts = fold_snap(
                                    GetBinFun(), FoldFun, Filters, TargetFileHandle
                                ),
                                ok = file:close(TargetFileHandle),
                                Sts
                            catch
                                FoldError:Exception ->
                                    catch file:close(TargetFileHandle),
                                    error({FoldError, Exception})
                            end,
                            ?Info("processed '~s' : ~p", [Table, Stats]),
                            Files#{
                                Table => #{
                                    targetFile => TargetFile,
                                    fileName => FileInArchive,
                                    tableName => Table,
                                    getInfo => GetInfoFun,
                                    getBin => GetBinFun,
                                    filters => Filters,
                                    stats => Stats
                                }
                            }
                    end
                end,
                #{},
                Source
            ),
            FilesList = filelib:wildcard("*.bkp", TempDir),
            {ok, _} = zip:zip(Target, FilesList, [{cwd, TempDir}]),
            del_all_dir_tree(TempDir),
            Files
    end.

del_all_dir_tree(DirOrFile) ->
    case {filelib:is_dir(DirOrFile), filelib:is_regular(DirOrFile)} of
        {false, true} -> ok = file:delete(DirOrFile);
        {false, false} -> ok;
        {true, _} ->
            case file:del_dir(DirOrFile) of
                ok -> ok;
                {error, enoent} -> ok;
                {error, eexist} ->
                    {ok, Filenames} = file:list_dir(DirOrFile),
                    lists:foreach(
                        fun(F) ->
                            del_all_dir_tree(filename:join(DirOrFile, F))
                        end,
                        Filenames
                    ),
                    del_all_dir_tree(DirOrFile)
            end
    end.

fold_snap(Bytes, FoldFun, Filters, TargetFileHandle) ->
    fold_snap(
        Bytes, FoldFun, Filters,
        #{
            bytes => byte_size(Bytes), foldFnAcc => undefined,
            rows => #{total => 0, skipped => 0, copied => 0, changed => 0},
            chunks => #{total => 0, skipped => 0, copied => 0, changed => 0}
        },
        TargetFileHandle
    ).

fold_snap(<<>>, _FoldFun, _Filters, Ctx, _TargetFileHandle) -> Ctx;
fold_snap(
    <<Length:32, TermAndRest/binary>>, FoldFun, Filters, Ctx, TargetFileHandle
) ->
    <<ChunkBin:Length/binary, Rest/binary>> = TermAndRest,
    ChunkTerm = binary_to_term(ChunkBin),
    {NewChunkTerm, Ctx1} = process_chunk(
        ChunkTerm, FoldFun, Ctx#{filters => Filters}
    ),
    Ctx2 = case NewChunkTerm of
        skip -> incr(chunks, skipped, 1, Ctx1);
        ChunkTerm ->
            ok = file:write(
                TargetFileHandle,
                <<(byte_size(ChunkBin)):32, ChunkBin/binary>>
            ),
            incr(chunks, copied, 1, Ctx1);
        NewChunkTerm ->
            NewChunkBin = term_to_binary(NewChunkTerm),
            ok = file:write(
                TargetFileHandle,
                <<(byte_size(NewChunkBin)):32, NewChunkBin/binary>>
            ),
            incr(chunks, changed, 1, Ctx1)
    end,
    fold_snap(
        Rest, FoldFun, Filters, incr(chunks, total, 1, Ctx2), TargetFileHandle
    ).

process_chunk(
    RawRows, FoldFun,
    #{matchSpecCompiled := CompiledMatchSpec, skvh := Skvh} = Ctx
) when is_list(RawRows) ->
    Rows = if
        Skvh ->
            [imem_dal_skvh:skvh_rec_to_map(SkvhRec) || SkvhRec <- RawRows];
        true -> RawRows
    end,
    lists:foldr(
        fun(Row, {NewRows, ICtx}) ->
            case process_chunk(Row, FoldFun, ICtx) of
                {skip, ICtx1} ->
                    {NewRows, incr(rows, skipped, 1, ICtx1)};
                {copy, ICtx1} ->
                    {[Row | NewRows], incr(rows, copied, 1, ICtx1)};
                {write, Row, ICtx1} ->
                    {[Row | NewRows], incr(rows, copied, 1, ICtx1)};
                {write, NewRow, ICtx1} ->
                    {[NewRow | NewRows], incr(rows, changed, 1, ICtx1)}
            end
        end,
        {[], incr(rows, total, length(Rows), Ctx)},
        ets:match_spec_run(Rows, CompiledMatchSpec)
    );
process_chunk(
    {prop, [#ddTable{opts = Opts}]} = Prop, FoldFun,
    #{filters := Filters} = Ctx
) ->
    Skvh = is_skvh(Opts),
    {MatchSpec, MatchSpecCompiled} = case catch ets:match_spec_compile(Filters) of
        {'EXIT', _} ->
            MC = filters2ms(Skvh, Filters),
            {MC, ets:match_spec_compile(MC)};
        MSC -> {Filters, MSC}
    end,
    case apply_fold_fun(
        Prop, FoldFun,
        Ctx#{
            skvh => Skvh, matchSpec => MatchSpec,
            matchSpecCompiled => MatchSpecCompiled
        }
    ) of
        {copy, Ctx1} -> {Prop, Ctx1};
        {write, Prop1, Ctx1} -> {{prop, Prop1}, Ctx1}
    end;
process_chunk(SkvhMap, FoldFun, #{skvh := true} = Ctx) ->
    case apply_fold_fun(SkvhMap, FoldFun, Ctx) of
        {write, NewSkvhMap, Ctx1} ->
            {write, imem_dal_skvh:map_to_skvh_rec(NewSkvhMap), Ctx1};
        Other -> Other
    end;
process_chunk(Rec, FoldFun, Ctx) ->
    apply_fold_fun(Rec, FoldFun, Ctx).

filters2ms(true, Filters) ->
    [case hasWild(Filter) of
        false -> {#{ckey => Filter}, [], ['$_']};
        true -> {#{ckey => '$1'}, f2mc(Filter, '$1'), ['$_']}
    end || Filter <- Filters];
filters2ms(false, Filters) ->
    [{'$1', f2mc(Filter, {element, 2, '$1'}), ['$_']} || Filter <- Filters].

apply_fold_fun(Term, FoldFun, Ctx) ->
    case FoldFun(Term, Ctx) of
        {true, #{foldFnAcc := Priv}} ->
            {copy, Ctx#{foldFnAcc => Priv}};
        {false, #{foldFnAcc := Priv}} ->
            {skip, Ctx#{foldFnAcc => Priv}};
        {true, NewTerm, #{foldFnAcc := Priv}} ->
            {write, NewTerm, Ctx#{foldFnAcc => Priv}};
        Other ->
            ?Warn("Bad fun return ~p, skipping", [Other]),
            Ctx
    end.

is_skvh(Opts) ->
    case maps:from_list(Opts) of
        #{record_name := skvhTable} -> true;
        _ -> false
    end.

incr(GroupKey, Key, Amount,  Ctx) ->
    #{GroupKey := #{Key := Val} = Group} = Ctx,
    Ctx#{GroupKey => Group#{Key => Val + Amount}}.

f2mc('*', _) -> [];
f2mc('_', _) -> [];
f2mc(F, C) ->
    [case hasWild(F) of
        false -> {'==', C, {const, F}};
        true ->
            if
                is_tuple(F) andalso tuple_size(F) > 0 -> f2mc_tuple(F, C);
                is_list(F)  andalso length(F) > 0 -> f2mc_list(F, C);
                true ->  error({badfilter, F})
            end
    end].

hasWild(F) -> hasWild(F, false).
hasWild(_, true) -> true;
hasWild([], false) -> false;
hasWild('*',false) -> true;
hasWild(['_'|_], false) -> true;
hasWild(['*'|_], false) -> true;
hasWild([H|T], false) -> hasWild(T, hasWild(H, false));
hasWild(T, false) when is_tuple(T) -> hasWild(tuple_to_list(T), false);
hasWild(_, W) -> W.

f2mc_list(['*'], C) -> {is_list, C};
f2mc_list(F, C) ->
    NoUnderscore = lists:filter(fun('_') -> false; (_) -> true end, F),
    NoStar = lists:filter(fun('*') -> false; (_) -> true end, F),
    case {NoUnderscore, NoStar} of
        % only '_'
        {[], F} -> {'==', {length, C}, length(F)};
        % no '*' (also includes no '_' case)
        {_, F} ->
            L = {'==', {length, C}, length(F)},
            case f2mc_list(F, C, undefined) of
                undefined -> L;
                R -> {'andalso', L, R}
            end;
        % '*'
        {_, _} ->
            case lists:last(F) /= '*' orelse length(NoStar) + 1 /= length(F) of
                true -> error({badfilter, F});
                _ -> ok
            end,
            L = {'>=', {length, C}, length(NoStar)},
            case f2mc_list(F, C, undefined) of
                undefined -> L;
                R -> {'andalso', L, R}
            end
    end.
f2mc_list([], _C, M) -> M;
f2mc_list(['*'], _C, M) -> M;
f2mc_list(['_'|T], C, M) -> f2mc_list(T, {tl, C}, M);
f2mc_list([H|T], C, M) when is_tuple(H), tuple_size(H) > 0 ->
    L = f2mc_tuple(H, {hd, C}),
    if M == undefined -> f2mc_list(T, C, L);
    true -> f2mc_list(T, C, {'andalso', M, L})
    end;
f2mc_list([H|T], C, M) ->
    C1 = {hd, C},
    L = case io_lib:printable_unicode_list(H) of
        false when is_list(H) -> f2mc_list(H, C1);
        _ -> {'==', C1, {const, H}}
    end,
    if M == undefined -> f2mc_list(T, {tl, C}, L);
    true -> f2mc_list(T, {tl, C}, {'andalso', M, L})
    end.

f2mc_tuple({'*'}, C) -> {is_tuple, C};
f2mc_tuple(T, C) ->
    F = tuple_to_list(T),
    NoUnderscore = lists:filter(fun('_') -> false; (_) -> true end, F),
    NoStar = lists:filter(fun('*') -> false; (_) -> true end, F),
    C1 = {element, 1, C},
    case {NoUnderscore, NoStar} of
        % only '_'
        {[], F} -> {'andalso', {is_tuple, C}, {'==', {size, C}, length(F)}};
        % no '*' (also includes no '_' case)
        {_, F} ->
            L = {'andalso', {is_tuple, C}, {'==', {size, C}, length(NoStar)}},
            case f2mc_tuple(F, C1, undefined) of
                undefined -> L;
                R -> {'andalso', L, R}
            end;
        % '*'
        {_, _} ->
            L = {'andalso', {is_tuple, C}, {'>=', {size, C}, length(NoStar)}},
            case lists:last(F) /= '*' orelse length(NoStar) + 1 /= length(F) of
                true -> error({badfilter, T});
                _ -> ok
            end,
            case f2mc_tuple(F, C1, undefined) of
                undefined -> L;
                R -> {'andalso', L, R}
            end
    end.
f2mc_tuple([], _C, M) -> M;
f2mc_tuple(['*'], _C, M) -> M;
f2mc_tuple(['_'|T], {element, I, C}, M) ->
    f2mc_tuple(T, {element, I + 1, C}, M);
f2mc_tuple([H|T], {element, I, C}, M) ->
    L = case io_lib:printable_unicode_list(H) of
        false when is_list(H) -> f2mc_list(H, {element, I, C});
        false when is_tuple(H), tuple_size(H) > 0 ->
            f2mc_tuple(H, {element, I, C});
        _ -> {'==', {element, I, C}, {const, H}}
    end,
    C1 = {element, I + 1, C},
    if M == undefined -> f2mc_tuple(T, C1, L);
    true -> f2mc_tuple(T, C1, {'andalso', M, L})
    end.

%-------------------------------------------------------------------------------

do_cluster_snapshot() ->
    Start = os:timestamp(),
    process_flag(trap_exit, true),
    Dir = create_clean_dir(?BKP_ZIP_PREFIX),
    ZipFile = filename:join(
        filename:dirname(Dir), filename:basename(Dir) ++ ".zip"
    ),
    ?Info("starting cluster snapshot to ~s", [ZipFile]),
    Tables = ?GET_CLUSTER_SNAPSHOT_TABLES,
    Snappers = snap_tables(Tables, Dir),
    wait_snap_tables(Snappers),
    ?Info("finished snapshotting ~p tables, zipping...", [length(Tables)]),
    ZipCandidates = filelib:wildcard("*.bkp", Dir),
    ?Info("zipping ~p files", [length(ZipCandidates)]),
    case zip:zip(ZipFile, ZipCandidates, [{cwd, Dir}]) of
        {error, Reason} ->
            ?Error("zip ~s failed reason : ~p", [ZipFile, Reason]);
        _ ->
            ?Info("zipped to ~s", [ZipFile])
    end,
    ?Info("recursively delete ~s", [Dir]),
    lists:foreach(
        fun(File) ->
            ?Info("deleting ~s", [File]),
            ok = file:delete(filename:join(Dir, File))
        end,
        ZipCandidates
    ),
    ?Info("deleting ~s", [Dir]),
    ok = file:del_dir(Dir),
    Bytes = lists:sum([imem_meta:table_size(Table) || Table  <- Tables]),
    TSec = timer:now_diff(os:timestamp(), Start) div 1000000,
    TMin = TSec div 60,
    Hour = TMin div 60,
    Min = TMin rem 60,
    Sec = TSec rem 60,
    ?Info(
        "finished : added ~p tables (~p bytes) in ~2..0B:~2..0B:~2..0B",
        [length(Tables), Bytes, Hour, Min, Sec]
    ).

snap_tables([], _Dir) -> [];
snap_tables([Table | Tables], Dir) ->
    [
        spawn_link(
            fun() ->
                ?Info("snapshot process ~p started : ~p", [self(), Table]),
                take_chunked(imem_meta:physical_table_name(Table), Dir),
                exit(Table)
            end
        )
        | snap_tables(Tables, Dir)
    ].

wait_snap_tables([]) -> done;
wait_snap_tables(Snappers) ->
    receive
        {'EXIT', Pid, Table} ->
            ?Info("snapshot process ~p finished : ~p", [Pid, Table]),
            wait_snap_tables(Snappers -- [Pid])
    after
        10000 ->
            NewSnappers = [Pid || Pid <- Snappers, is_process_alive(Pid)],
            if
                length(NewSnappers) /= length(Snappers) ->
                    ?Warn(
                        "workers ~p, alive ~p",
                        [length(Snappers), length(NewSnappers)]
                    );
                true ->
                    ?Info(
                        "pending snapshot on ~p table(s)", [length(NewSnappers)]
                    )
            end,
            wait_snap_tables(NewSnappers)
    end.

%-------------------------------------------------------------------------------
% TESTS
%-------------------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

f2mc_test_() ->
    {
        inparallel,
        [{
            lists:flatten(io_lib:format("~s : ~p", [Title, Filter])),
            fun() ->
                case catch f2mc(Filter, '$1') of
                    {'EXIT', Exception} ->
                        ?debugFmt(
                            "~n~s : imem_snap:f2mc(~p, '$1').~n"
                            "Expected   : ~p~n"
                            "Exception  : ~p",
                            [Title, Filter, MatchSpec, Exception]
                        ),
                        error(failed);
                    CalculatedMs ->
                        if CalculatedMs /= MatchSpec ->
                                ?debugFmt(
                                    "~n~s : imem_snap:f2mc(~p, '$1').~n"
                                    "Expected   : ~p~n"
                                    "Got        : ~p",
                                    [Title, Filter, MatchSpec, CalculatedMs]
                                );
                            true -> ok
                        end,
                        ?assertEqual(MatchSpec, CalculatedMs)
                end
            end
        } || {Title, Filter, MatchSpec} <- [
                {"all",           '*',     []},
                {"list all",      ['*'],   [{is_list, '$1'}]},
                {"all tuple",     {'*'},   [{is_tuple, '$1'}]},
                {"list any one",  ['_'],   [{'==', {length, '$1'}, 1}]},
                {"list single",   ["one"], [{'==', '$1', {const, ["one"]}}]},
                {"tuple single",  {"one"}, [{'==', '$1', {const, {"one"}}}]},
                {"2 list both",   ["one", "two"],
                    [{'==', '$1', {const, ["one","two"]}}]
                },
                {"2 list second",  ['_', "two"],
                    [{
                        'andalso',
                        {'==', {length, '$1'}, 2},
                        {'==', {hd, {tl, '$1'}}, {const, "two"}}
                    }]
                },
                {"4 list first and last",  ["one", '_', '_', "three"],
                    [{
                        'andalso',
                        {'==', {length, '$1'}, 4},
                        {
                            'andalso',
                            {'==', {hd, '$1'}, {const, "one"}},
                            {'==', {hd, {tl, {tl, {tl, '$1'}}}}, {const, "three"}}
                        }
                    }]
                },
                {"open list second",  ['_', "two", '*'],
                    [{
                        'andalso',
                        {'>=', {length, '$1'}, 2},
                        {'==', {hd, {tl, '$1'}}, {const, "two"}}
                    }]
                }
            ]
        ]
    }.

filters2ms_test_() ->
  {
    inparallel,
    [{
      lists:flatten(io_lib:format("~s : ~p", [Title, Filter])),
      fun() ->
        case catch filters2ms(true,Filter) of
          {'EXIT', Exception} ->
            ?debugFmt(
              "~n~s : imem_snap:filters2ms(true,~p).~n"
              "Expected   : ~p~n"
              "Exception  : ~p",
              [Title, Filter, MatchSpec, Exception]
            ),
            error(failed);
          CalculatedMs ->
            if CalculatedMs /= MatchSpec ->
              ?debugFmt(
                "~n~s : imem_snap:filters2ms(~p, '$1').~n"
                "Expected   : ~p~n"
                "Got        : ~p",
                [Title, Filter, MatchSpec, CalculatedMs]
              );
              true -> ok
            end,
            ?assertEqual(MatchSpec, CalculatedMs)
        end
      end
    } || {Title, Filter, MatchSpec} <- [
      {"all",           ['*'],  [{#{ckey => '$1'},[],['$_']}]},
      {"list all",      [['*']],   [{#{ckey => '$1'},[{is_list,'$1'}],['$_']}] },
      {"all tuple",     [{'*'}],   [{#{ckey => '$1'},[{is_tuple,'$1'}],['$_']}]}
    ]
    ]
  }.

msrun_test_() ->
    Rows = [
        ["a"],
        ["a", 1],
        ["a", 1, c],
        ["b"],
        ["b", 1],
        ["b", 2],
        {"a"},
        {"a", 1},
        1,
        1.234,
        'a',
        <<"a">>,
        "a"
    ],
    {
        inparallel,
        [{
            Title,
            fun() ->
                MS = [{'$1', f2mc(Filter, '$1'), ['$_']} || Filter <- Filters],
                MSC = ets:match_spec_compile(MS),
                ?assertEqual(FilteredRows, ets:match_spec_run(Rows, MSC))
            end
        } ||
            {Title, Filters, FilteredRows} <- [
                {"all*",        ['*'],          Rows},
                {"all_",        ['_'],          Rows},
                {"integer1",    [1],            [1]},
                {"integer2",    [2],            []},
                {"float1.234",  [1.234],        [1.234]},
                {"float2.234",  [2.234],        []},
                {"mix",         ['a',1,5,a,<<"a">>],    [1,a,<<"a">>]},
                {"list",        [['*']],        [R || R <- Rows, is_list(R)]},
                {"tuple",       [{'*'}],        [R || R <- Rows, is_tuple(R)]},
                {"first",       [["a",'*']],    [["a"], ["a", 1], ["a", 1, c]]},
                {"first two",   [['_','_']],    [["a",1], ["b",1], ["b",2]]},
                {"second",      [['_',1]],      [["a",1], ["b",1]]}
            ]
        ]
    }.

-endif.
