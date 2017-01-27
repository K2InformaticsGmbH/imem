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
        , do_snapshot/1
        , all_snap_tables/0
        , all_local_time_partitioned_tables/0
        ]).

-safe([all_snap_tables,get_snap_properties,set_snap_properties,snap_log,
       snap_err,take]).

-define(BKP_EXTN, ".bkp").
-define(BKP_TMP_EXTN, ".bkp.new").

-define(GET_SNAPSHOT_CYCLE_WAIT,?GET_CONFIG(snapshotCycleWait,[],10000,"Wait time between snapshot cycles in msec.")).
-define(GET_SNAPSHOT_CHUNK_MAX_SIZE,?GET_CONFIG(snapshotChunkMaxSize,[],500,"Maximum snapshot chunk size (number of rows).")).
-define(GET_SNAPSHOT_CHUNK_FETCH_TIMEOUT,?GET_CONFIG(snapshotChunkFetchTimeout,[],20000,"Timeout in msec for fetching the next chunk from DB.")).
-define(GET_SNAPSHOT_SCRIPT,?GET_CONFIG(snapshotScript,[],true,"Do we want to use a specialized snapshot script function?")).
-define(GET_SNAPSHOT_EXCLUSION_PATTERNS,?GET_CONFIG(snapshotExclusuionPatterns,[], 
        ["Dyn@.*", "Dyn$", "Audit_.*", "Audit$", "Hist$", "Idx$"], "Snapshot excusion table name patterns")).
-define(PUT_SNAPSHOT_EXCLUSION_PATTERNS(__TablePatterns, __Remark), ?PUT_CONFIG(snapshotExclusuionPatterns, [], __TablePatterns, __Remark)).
-define(GET_SNAPSHOT_SCRIPT_FUN,?GET_CONFIG(snapshotScriptFun,[],
<<"fun(ExcludePatterns) ->
    ExcludeList = [dual, ddSize, ddNode
                  ,imem_meta:physical_table_name(ddCache@)
                  ,imem_meta:physical_table_name(ddSeCo@)
                  ,imem_meta:physical_table_name(ddPerm@)
                  ,imem_meta:physical_table_name(mproConnectionProbe@)
                  ],
    ExcludePred = fun(AN) -> SN = atom_to_list(AN), (lists:usort([re:run(SN, E) || E <- ExcludePatterns]) == [nomatch]) end,
    Candidates = lists:filter(ExcludePred,imem_snap:all_snap_tables() -- ExcludeList), 
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
-define(GET_CLUSTER_SNAPSHOT,?GET_CONFIG(snapshotCluster,[],true,"Do we need periodic snapshotting of important tables an all nodes?")).
-define(GET_CLUSTER_SNAPSHOT_TABLES,?GET_CONFIG(snapshotClusterTables,[],[ddAccount,ddRole,ddConfig],"List of important tables to be regularily snapshotted.")).
-define(GET_CLUSTER_SNAPSHOT_TOD,?GET_CONFIG(snapshotClusterHourOfDay,[],14,"Hour of (00..23)day in which important tables must be snapshotted.")).

-ifdef(TEST).
    start_snap_loop() -> ok.
    restart_snap_loop() -> ok.
-else.
    start_snap_loop() ->
        spawn(fun() ->
            catch ?Info("~s~n", [zip({re, "*.bkp"})]),
            restart_snap_loop()
        end).
    restart_snap_loop() ->
        erlang:whereis(?MODULE) ! imem_snap_loop.
-endif.

suspend_snap_loop() ->
    erlang:whereis(?MODULE) ! imem_snap_loop_cancel.

%% ----- SERVER INTERFACE ------------------------------------------------
%% ?SERVER_START_LINK.
start_link(Params) ->
    ?Info("~p starting...~n", [?MODULE]),
    ets:new(?MODULE, [public, named_table, {keypos,2}]),
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
        _ -> ok
    end,
    ?Info("snapshot directory ~s~n", [SnapshotDir]),

    process_flag(trap_exit, true),
    start_snap_loop(),
    erlang:send_after(
      1000, ?MODULE,
      {cluster_snap, ?GET_CLUSTER_SNAPSHOT_TABLES, '$replace_with_timestamp',
       '$create_when_needed'}),
    {ok,#state{snapdir = SnapshotDir}}.

create_clean_dir(Prefix) ->
    {_,_,Us} = Now = os:timestamp(),
    {{Y,M,D},{H,Mn,S}} = calendar:now_to_local_time(Now),
    Sec = S + Us / 1000000,
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    Dir = lists:flatten(io_lib:format("~s~4..0B~2..0B~2..0B_~2..0B~2..0B~9.6.0f", [Prefix,Y,M,D,H,Mn,Sec])),
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

cluster_snap(Tabs, '$replace_with_timestamp', Dir) ->
    cluster_snap(Tabs, os:timestamp(), Dir);
cluster_snap([], {_,_,_} = StartTime, Dir) ->
    ZipFile = filename:join(filename:dirname(Dir), filename:basename(Dir)++".zip"),
    ZipCandidates = [begin
                         {ok, Bin} = file:read_file(filename:join(Dir,F)),
                         {F, Bin}
                     end || F <- filelib:wildcard("*.bkp", Dir)],
    ?Debug("zip:zip(~p, ~p)", [ZipFile, ZipCandidates]),
    case zip:zip(ZipFile, ZipCandidates) of
        {error, Reason} ->
            ?Error("cluster snapshot ~s failed reason : ~p", [ZipFile, Reason]);
        _ ->
            lists:foreach(
              fun(F) -> ok = file:delete(filename:join(Dir,F)) end,
              filelib:wildcard("*.*", Dir)),
            ok = file:del_dir(Dir),
            ?Info("cluster snapshot ~s", [ZipFile])
    end,
    ?Info("cluster snapshot took ~pms",
          [timer:now_diff(os:timestamp(), StartTime) div 1000]),
    erlang:send_after(
      1000, ?MODULE,
      {cluster_snap, ?GET_CLUSTER_SNAPSHOT_TABLES, '$replace_with_timestamp',
       '$create_when_needed'});
cluster_snap(Tabs, StartTime, '$create_when_needed') ->
    cluster_snap(Tabs, StartTime, create_clean_dir("backup_snapshot_"));
cluster_snap([T|Tabs], {_,_,_} = StartTime, Dir) ->
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
    ?MODULE ! {cluster_snap, NextTabs, StartTime, Dir}.

handle_info({cluster_snap, Tables, StartTime, Dir}, State) ->
    {noreply,
     case ?GET_CLUSTER_SNAPSHOT of
         true ->
             {{Y,M,D},{H,_,_}} = calendar:local_time(),
             {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
             ZipFilePattern = lists:flatten(io_lib:format("backup_snapshot_~4..0B~2..0B~2..0B_~2..0B*.zip", [Y,M,D,H])),
             BackupDir = filename:absname(SnapDir),
             case filelib:wildcard(ZipFilePattern, BackupDir) of
                 BFs when length(BFs) > 0 ->
                     case catch is_process_alive(State#state.csnap_pid) of
                         true -> State;
                         _ ->
                             erlang:send_after(
                               1000, ?MODULE,
                               {cluster_snap, ?GET_CLUSTER_SNAPSHOT_TABLES, '$replace_with_timestamp', '$create_when_needed'}),
                             State#state{csnap_pid = (#state{})#state.csnap_pid}
                     end;
                 _ ->
                     ClusterSnapHour = ?GET_CLUSTER_SNAPSHOT_TOD,
                     case calendar:now_to_local_time(os:timestamp()) of
                         {{_,_,_},{ClusterSnapHour,_,_}} ->
                             if Dir == '$create_when_needed' ->
                                    ?Info("cluster snapshot ~p", [Tables]);
                                true -> ok
                             end,
                             State#state{
                               csnap_pid =
                               spawn(fun() -> cluster_snap(Tables, StartTime, Dir) end)};
                         _ ->
                             case catch is_process_alive(State#state.csnap_pid) of
                                 true -> State;
                                 _ ->
                                     erlang:send_after(
                                       1000, ?MODULE,
                                       {cluster_snap, Tables, '$replace_with_timestamp', '$create_when_needed'}),
                                     State#state{csnap_pid = (#state{})#state.csnap_pid}
                             end
                     end
             end;
         false ->
             erlang:send_after(
               1000, ?MODULE,
               {cluster_snap, Tables, '$replace_with_timestamp', '$create_when_needed'}),
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
    ZipCandidates = [filename:join([SnapDir, SF])
                    || SF <- SnapFiles
                    , filelib:file_size(filename:join([SnapDir, SF])) > 0],
    if ZipCandidates =:= [] -> ok;
        true ->
            {{Y,M,D}, {H,Mn,S}} = calendar:local_time(),
            Sec = S + element(3, os:timestamp()) / 1000000,
            ZipFileName = re:replace(lists:flatten(["snapshot_"
                                         , io_lib:format("~4..0B~2..0B~2..0B_~2..0B~2..0B~9.6.0f", [Y,M,D,H,Mn,Sec])
                                         , ".zip"
                                        ]), "[<>:\"\\\\|?*]", "", [global, {return, list}]),
            % to make the file name valid for windows
            ZipFileFullPath = filename:join(SnapDir, ZipFileName),
            case zip:zip(ZipFileFullPath, ZipCandidates) of
                {error, Reason} ->
                    lists:flatten(io_lib:format("old snapshot backup to ~s failed reason : ~p"
                                                , [ZipFileFullPath, Reason]));
                _ ->
                    lists:flatten(io_lib:format("old snapshots are backed up to ~s"
                                                , [ZipFileFullPath]))
            end
    end.

% display information of existing snapshot or a snapshot bundle (.zip)
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
    restart_snap_loop(),
    Res.

% snapshot restore_as interface
%  - periodic snapshoting timer is paused during a restore operation
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
            restart_snap_loop(),
            ok;
        Error -> 
            restart_snap_loop(),
            Error
    end.

restore(zip, ZipFile, TabRegEx, Strategy, Simulate) when is_list(ZipFile) ->
    case filelib:is_file(ZipFile) of
        true ->
            suspend_snap_loop(),
            {ok,Fs} = zip:unzip(ZipFile),
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
            restart_snap_loop(),
            Res;
        _ ->
            {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
            PossibleZipFile = filename:join([SnapDir, filename:basename(ZipFile)]),
            case filelib:is_file(PossibleZipFile) of
                true -> restore(zip, PossibleZipFile, TabRegEx, Strategy, Simulate);
                _ -> {error, lists:flatten(io_lib:format("file ~p not found~n", [filename:absname(PossibleZipFile)]))}
            end
    end.

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

all_snap_tables() ->
    lists:filter(fun(T) ->
            	    imem_meta:is_readable_table(T)
                    andalso not imem_meta:is_local_time_partitioned_table(T)
                end, imem_meta:all_tables()).

all_local_time_partitioned_tables() ->
    lists:filter(fun(T) ->
                    imem_meta:is_readable_table(T)
                    andalso imem_meta:is_local_time_partitioned_table(T)
                end, imem_meta:all_tables()).

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

timestamp({Mega, Secs, Micro}) -> Mega*1000000000000 + Secs*1000000 + Micro.

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

get_snap_properties(Tab) ->
    case ets:lookup(?SNAP_ETS_TAB, Tab) of
        [] ->       {};
        [Prop] ->   {Prop, Prop#snap_properties.last_write, Prop#snap_properties.last_snap}
    end.

set_snap_properties(Prop) ->
    ets:insert(?SNAP_ETS_TAB, Prop#snap_properties{last_snap= os:timestamp()}).

snap_log(_P,_A) -> ?Info(_P,_A).
snap_err(P,A) -> ?Error(P,A).

snap_file_count() ->
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    length(filelib:wildcard("*.{bkp,zip}",SnapDir)).

exclude_table_pattern(TablePattern) when is_binary(TablePattern) ->
    exclude_table_pattern(binary_to_list(TablePattern));
exclude_table_pattern(TablePattern) when is_list(TablePattern) ->
    ExPatterns = ?GET_SNAPSHOT_EXCLUSION_PATTERNS,
    Remark = list_to_binary("Added " ++ TablePattern ++ " at "),
    TimeStamp = imem_datatype:timestamp_to_io(os:timestamp()),
    ?PUT_SNAPSHOT_EXCLUSION_PATTERNS([TablePattern | ExPatterns], <<Remark/binary, TimeStamp/binary>>).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%%
%%----- TESTS ------------------------------------------------
%%

-define(TABLES, lists:sort([atom_to_list(T) || T <- mnesia:system_info(tables) -- [schema]])).
-define(FILENAMES(__M, __Dir), lists:sort([filename:rootname(filename:basename(F))
                                           || F <- filelib:wildcard(__M, __Dir)])).
-define(EMPTY_DIR(__Dir), [{F, file:delete(filename:absname(filename:join([__Dir,F])))}
                           || F <- filelib:wildcard("*.*", __Dir)]).

setup() ->
    ?imem_test_setup,
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    SnapDir.

teardown(_) ->
    ?imem_test_teardown.

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [fun test_snapshot/1]}
    }.

test_snapshot(SnapDir) ->
    ?LogDebug("---TEST--- ~p(~p)", [test_snapshot,SnapDir]),
    take(ddTable),
    % ?LogDebug("take snapshots :~n~p~n", [Take]),
    ?assert( lists:member("ddTable",?FILENAMES("*"++?BKP_EXTN, SnapDir))),
    % ?LogDebug("snapshot tests completed!~n", []),
    ok.

-endif.
