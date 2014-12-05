%%% -------------------------------------------------------------------
%%% Author      : Bikram Chatterjee
%%% Description : 
%%% Version     : 
%%% Created     : 30.09.2011
%%% -------------------------------------------------------------------

-module(imem).
-behaviour(application).

-include("imem.hrl").

-export([start/0
        , stop/0
        , start_test_writer/1
        , stop_test_writer/0
        , start_tcp/2
        , stop_tcp/0
        ]).

% application callbacks
-export([start/2, stop/1]).


%% ====================================================================
%% External functions
%% ====================================================================
start() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),
    application:start(sasl),
    application:start(os_mon),
    application:start(ranch),
    application:start(jsx),
    sqlparse:start(),
    ok = application:start(compiler),
    ok = application:start(syntax_tools),
    ok = application:start(goldrush),
    ok = application:load(lager),
    ok = application:set_env(lager, handlers, [{lager_console_backend, info},
                                               {lager_file_backend, [{file, "log/error.log"},
                                                                     {level, error},
                                                                     {size, 10485760},
                                                                     {date, "$D0"},
                                                                     {count, 5}]},
                                               {lager_file_backend, [{file, "log/console.log"},
                                                                     {level, info},
                                                                     {size, 10485760},
                                                                     {date, "$D0"},
                                                                     {count, 5}]}]),
    ok = application:set_env(lager, error_logger_redirect, false),
    ok = application:start(lager),
    erlhlc:start(),
    application:start(?MODULE).

stop() ->
    ok = application:stop(?MODULE),
    erlhlc:stop(),
    ok = application:stop(lager),
    ok = application:unload(lager),
    ok = application:stop(goldrush),
    ok = application:stop(syntax_tools),
    ok = application:stop(compiler),
    ok = sqlparse:stop(),
    ok = application:stop(jsx),
    ok = application:stop(ranch),
    ok = application:stop(os_mon),
    ok = application:stop(sasl),
    ok = application:unload(sasl).

start(_Type, StartArgs) ->
    % cluster manager node itself may not run any apps
    % it only helps to build up the cluster
    ?Notice("---------------------------------------------------~n"),
    ?Notice(" STARTING IMEM~n"),
    case application:get_env(erl_cluster_mgrs) of
        {ok, []} -> ?Info("cluster manager node(s) not defined!~n");
        {ok, CMNs} ->
            CMNodes = lists:usort(CMNs) -- [node()],
            ?Info("joining cluster with ~p~n", [CMNodes]),
            ok = join_erl_cluster(CMNodes)
    end,

    % Setting a start time for the first node in cluster
    % or started without CMs
    case application:get_env(start_time) of
        undefined ->
            ok = application:set_env(?MODULE,start_time,{erlang:now(),node()});
        _ -> ok
    end,

    % If in a cluser wait for other IMEM nodes to complete a full boot before
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
                   ?Notice("---------------------------------------------------~n"),
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

%set_node_status(Nodes) ->
%    ok = application:set_env(
%           imem, imem_starting_nodes,
%           lists:usort(
%             [{node(), element(2, application:get_env(start_time))}
%              | [ {N, element(2, rpc:call(N, application, get_env, [?MODULE, start_time]))}
%                  || N <- Nodes]
%             ])
%          ).

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

%% LAGER Disabled in test
%-ifndef(TEST).
%
%config_if_lager() ->
%    application:load(lager),
%    application:set_env(lager, handlers,
%                        [{lager_console_backend, info},
%                         {lager_file_backend, [{file, "log/error.log"},
%                                               {level, error},
%                                               {size, 10485760},
%                                               {date, "$D0"},
%                                               {count, 5}]},
%                         {lager_file_backend, [{file, "log/console.log"},
%                                               {level, info},
%                                               {size, 10485760},
%                                               {date, "$D0"},
%                                               {count, 5}]}]),
%    application:set_env(lager, error_logger_redirect, false),
%    lager:start(),
%    ?Info("IMEM starting with lager!").
%
%-else. % TEST
%
%% Lager disabled
%config_if_lager() ->
%    ?Info("IMEM starting without lager!").
%
%-endif. % TEST

stop(_State) ->
    stop_tcp(),
	?Info("stopped imem_server~n"),
	?Info("stopping mnesia...~n"),
    spawn(
      fun() ->
              stopped = mnesia:stop(),
              ?Info("stopped mnesia~n")
      end),
	?Notice("SHUTDOWN IMEM~n"),
    ?Notice("---------------------------------------------------~n").

% start stop query imem tcp server
start_tcp(Ip, Port) ->
    imem_server:start_link([{tcp_ip, Ip},{tcp_port, Port}]).

stop_tcp() -> imem_server:stop().

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
                   {rpc:call(TimeKeeper, erlang, now, []), TimeKeeper});
        undefined ->
            ok = application:set_env(
                   ?MODULE, start_time,
                   {rpc:call(Node, erlang, now, []), Node})
    end.

% start/stop test writer
start_test_writer(Param) ->
    {ok, ImemTimeout} = application:get_env(imem, imem_timeout),
    {ok, SupPid} = supervisor:start_child(imem_sup, {imem_test_writer
                                                    , {imem_test_writer, start_link, [Param]}
                                                    , permanent, ImemTimeout, worker, [imem_test_writer]}),
    [?Info("imem process ~p started pid ~p~n", [_Mod, _Pid]) || {_Mod,_Pid,_,_} <- supervisor:which_children(imem_sup)],
    {ok, SupPid}.
stop_test_writer() ->
    ok = supervisor:terminate_child(imem_sup, imem_test_writer),
    ok = supervisor:delete_child(imem_sup, imem_test_writer),
    [?Info("imem process ~p started pid ~p~n", [_Mod, _Pid]) || {_Mod,_Pid,_,_} <- supervisor:which_children(imem_sup)].
