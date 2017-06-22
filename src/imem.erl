%%% -------------------------------------------------------------------
%%% Author      : Bikram Chatterjee
%%% Description : 
%%% Version     : 
%%% Created     : 30.09.2011
%%% -------------------------------------------------------------------

-module(imem).
-behaviour(application).

-include("imem.hrl").
-include("imem_if.hrl").

% shell start/stop helper
-export([ start/0
        , stop/0
        ]).

% application callbacks
-export([ start/2
        , stop/1
        ]).

% library functions
-export([ get_os_memory/0
        , get_vm_memory/0
        , get_swap_space/0
        , spawn_sync_mfa/3
        , priv_dir/0
        ]).

-safe([get_os_memory/0, get_vm_memory/0, get_swap_space/0]).


%% ====================================================================
%% External functions
%% ====================================================================
start() ->
    {ok, _} = application:ensure_all_started(?MODULE).

stop() ->
    ok = application:stop(?MODULE).

start(_Type, StartArgs) ->
    % cluster manager node itself may not run any apps
    % it only helps to build up the cluster
    % ?Notice("---------------------------------------------------~n"),
    ?Notice(" STARTING IMEM~n"),
    case application:get_env(erl_cluster_mgrs) of
        {ok, []} -> ?Info("cluster manager node(s) not defined!~n");
        {ok, CMNs} ->
            CMNodes = lists:usort(CMNs) -- [node()],
            ?Info("joining cluster with ~p~n", [CMNodes]),
            ok = join_erl_cluster(CMNodes)
    end,
    case application:get_env(start_time) of
        undefined ->
            % Setting a start time for the first node in cluster
            % or started without CMs
            ok = application:set_env(?MODULE,start_time,{?TIMESTAMP, node()});
        _ -> ok
    end,
    % If in a cluster, wait for other IMEM nodes to complete a full boot before
    % starting mnesia to serialize IMEM start in a cluster
    wait_remote_imem(),
    % Sets max_ets_tables application env parameter with maximum value either
    % configured with ERL_MAX_ETS_TABLES OS environment variable or to default
    % 1400
    ok = application:set_env(
           ?MODULE, max_ets_tables,
           case os:getenv("ERL_MAX_ETS_TABLES") of
               false -> 1400;
               MaxEtsTablesStr -> list_to_integer(MaxEtsTablesStr)
           end),
    % Mnesia should be loaded but not started
    AppInfo = application:info(),
    RunningMnesia = lists:member(mnesia,
                                 [A || {A, _} <- proplists:get_value(running, AppInfo)]),
    if RunningMnesia ->
           ?Error("Mnesia already started~n"),
           {error, mnesia_already_started};
       true ->
           LoadedMnesia = lists:member(
                            mnesia,
                            [A || {A, _, _} <- proplists:get_value(loaded, AppInfo)]),
           if LoadedMnesia -> ok; true -> application:load(mnesia) end,
           config_start_mnesia(),
           case imem_sup:start_link(StartArgs) of
               {ok, _} = Success ->
                   ?Notice(" IMEM STARTED~n"),
                   % ?Notice("---------------------------------------------------~n"),
                   Success;
               Error ->
                   ?Error(" IMEM FAILED TO START ~p~n", [Error]),
                   Error
           end
    end.

wait_remote_imem() ->
    AllNodes = nodes(),

    % Establishing full connection too all nodes of the cluster
    [net_adm:ping(N1)   % failures (pang) can be ignored
     || N1 <- lists:usort(  % removing duplicates
                lists:flatten(
                  [rpc:call(N, erlang, nodes, [])
                   || N <- AllNodes]
                 )
               ) -- [node()] % removing self
    ],

    % Building lists of nodes already running IMEM
    RunningImemNodes =
        [N || N <- AllNodes,
              true == lists:keymember(
                        imem, 1,
                        rpc:call(N, application, which_applications, []))
        ],
    ?Info("Nodes ~p already running IMEM~n", [RunningImemNodes]),

    % Building lists of nodes already loaded IMEM but not in running state
    % (ongoing IMEM boot)
    LoadedButNotRunningImemNodes =
        [N || N <- AllNodes,
              true == lists:keymember(
                        imem, 1,
                        rpc:call(N, application, loaded_applications, []))
        ] -- RunningImemNodes,
    ?Info("Nodes ~p loaded but not running IMEM~n",
          [LoadedButNotRunningImemNodes]),

    % Wait till imem application env parameter start_time for
    % all nodes in LoadedButNotRunningImemNodes are set
    (fun WaitStartTimeSet([]) -> ok;
         WaitStartTimeSet([N|Nodes]) ->
            case rpc:call(N, application, get_env, [?MODULE, start_time]) of
                undefined -> WaitStartTimeSet(Nodes++[N]);
                _ -> WaitStartTimeSet(Nodes)
            end
    end)(LoadedButNotRunningImemNodes),

    % Create a sublist from LoadedButNotRunningImemNodes
    % with the nodes which started before this node
    {ok,{SelfStartTime,_}} = application:get_env(start_time),
    NodesToWaitFor =
        lists:foldl(
          fun(Node, Nodes) ->
                  case rpc:call(Node, application, get_env, [?MODULE, start_time]) of
                      {ok, {StartTime,_}} when StartTime < SelfStartTime ->
                          [Node|Nodes];
                      % Ignoring the node which started after this node
                      % or if RPC fails for any reason
                      _ -> Nodes
                  end
          end, [],
          LoadedButNotRunningImemNodes),
    case NodesToWaitFor of
       [] -> % first node of the cluster
             % no need to wait
            ok;
        NodesToWaitFor ->
            %set_node_status(NodesToWaitFor),
            wait_remote_imem(NodesToWaitFor)
    end.

wait_remote_imem([]) -> ok;
wait_remote_imem([Node|Nodes]) ->
    case rpc:call(Node, application, which_applications, []) of
        {badrpc,nodedown} ->
            % Nodedown moving on
            wait_remote_imem(Nodes);
        RemoteStartedApplications ->
            case lists:keymember(imem,1,RemoteStartedApplications) of
                false ->
                    % IMEM loaded but not finished starting yet,
                    % waiting 500 ms before retrying
                    ?Info("Node ~p starting IMEM, waiting for it to finish starting~n",
                          [Node]),
                    %set_node_status([Node|Nodes]),
                    timer:sleep(500),
                    wait_remote_imem(Nodes++[Node]);
                true ->
                    % IMEM loaded and started, moving on
                    wait_remote_imem(Nodes)
            end
    end.

config_start_mnesia() ->
    {ok, SchemaName} = application:get_env(mnesia_schema_name),
    SDir = atom_to_list(SchemaName) ++ "." ++ atom_to_list(node()),
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    [_|Rest] = lists:reverse(filename:split(SnapDir)),
    RootParts = lists:reverse(Rest),
    SchemaDir = case ((length(RootParts) > 0) andalso
                      filelib:is_dir(filename:join(RootParts))) of
                    true -> filename:join(RootParts ++ [SDir]);
                    false ->
                        {ok, Cwd} = file:get_cwd(),
                        LastFolder = lists:last(filename:split(Cwd)),
                        if LastFolder =:= ".eunit" ->
                               filename:join([Cwd, "..", SDir]);
                           true ->  filename:join([Cwd, SDir])
                        end
                end,
    ?Info("schema path ~s~n", [SchemaDir]),
    application:set_env(mnesia, dir, SchemaDir),
    ok = mnesia:start().

stop(_State) ->
    imem_server:stop(),
	?Info("stopped imem_server~n"),
	?Info("stopping mnesia...~n"),
    spawn(
      fun() ->
              stopped = mnesia:stop(),
              ?Info("stopped mnesia~n")
      end),
	?Notice("SHUTDOWN IMEM~n"),
    ?Notice("---------------------------------------------------~n").

join_erl_cluster([]) -> ok;
join_erl_cluster([Node|Nodes]) ->
    case net_adm:ping(Node) of
        pong ->
            case application:get_env(start_time) of
                undefined -> set_start_time(Node);
                _ -> ok
            end,
            ?Info("joined node ~p~n", [Node]);
        pang ->
            ?Info("node ~p down!~n", [Node])
    end,
    join_erl_cluster(Nodes).

set_start_time(Node) ->
    % Setting a start time for this node in cluster requesting an unique time
    % from TimeKeeper
    case rpc:call(Node, application, get_env, [imem, start_time]) of
        {ok, {_,TimeKeeper}} ->
            ok = application:set_env(
                   ?MODULE, start_time,
                   {rpc:call(TimeKeeper, imem_if_mnesia, timestamp, []), TimeKeeper});
        undefined ->
            ok = application:set_env(
                   ?MODULE, start_time,
                   {rpc:call(Node, imem_if_mnesia, timestamp, []), Node})
    end.

-spec get_os_memory() -> {any(), integer(), integer()}.
get_os_memory() ->
    SysData = memsup:get_system_memory_data(),
    FreeMem = lists:foldl(
                fun({T, M}, A)
                      when T =:= free_memory; T =:= buffered_memory; T =:= cached_memory ->
                        A+M;
                   (_, A) -> A
                end, 0, SysData),
    TotalMemory = proplists:get_value(total_memory, SysData),
    case os:type() of
        {win32, _} = Win    -> {Win,        FreeMem,    TotalMemory};
        {unix, _} = Unix    -> {Unix,       FreeMem,    TotalMemory};
        Unknown             -> {Unknown,    FreeMem,    TotalMemory}
    end.

-spec get_vm_memory() -> {any(),integer()}.
get_vm_memory() -> get_vm_memory(os:type()).
get_vm_memory({win32, _} = Win) ->
    {Win, list_to_integer(
            re:replace(
              os:cmd("wmic process where processid=" ++ os:getpid() ++
                     " get workingsetsize | findstr /v \"WorkingSetSize\""),
              "[[:space:]]*", "", [global, {return,list}]))};
get_vm_memory({unix, _} = Unix) ->
    {Unix, erlang:round(
             element(3, get_os_memory())
                        * list_to_float(
                            re:replace(
                              os:cmd("ps -p "++os:getpid()++" -o pmem="),
                              "[[:space:]]*", "", [global, {return,list}])
                           ) / 100)}.

-spec get_swap_space() -> {integer(), integer()}.
get_swap_space() ->
    case maps:from_list(memsup:get_system_memory_data()) of
        #{free_swap := FreeSwap, total_swap := TotalSwap} ->
            {TotalSwap, FreeSwap};
        _ -> {0, 0}
    end.

% An MFA interface that is executed in a spawned short-lived process
% The result is synchronously collected and returned
% Restricts binary memory leakage within the scope of this process
spawn_sync_mfa(M,F,A) ->
    Self = self(),
    spawn(fun() -> Self ! (catch apply(M,F,A)) end),
    receive Result -> Result
    after 60000 -> {error, timeout}
    end.

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} ->
            filename:join(
              filename:dirname(
                filename:dirname(
                  code:which(?MODULE))), "priv");
        D -> D
    end.
