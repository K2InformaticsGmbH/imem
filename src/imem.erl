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
    sqlparse:start(),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),
    application:start(sasl),
    application:start(os_mon),
    application:start(ranch),
    application:start(jsx),
    sqlparse:start(),
    config_if_lager(),
    application:start(?MODULE).

start(_Type, StartArgs) ->
    % cluster manager node itself may not run any apps
    % it only helps to build up the cluster
    ?Info("---------------------------------------------------~n", []),
    ?Info(" STARTING IMEM~n", []),
    case application:get_env(erl_cluster_mgrs) of
        {ok, []} -> ?Info("cluster manager node(s) not defined!~n");
        {ok, CMNs} ->
            CMNodes = lists:usort(CMNs) -- [node()],
            ?Info("joining cluster with ~p~n", [CMNodes]),
            [case net_adm:ping(CMNode) of
            pong -> ?Info("joined node ~p~n", [CMNode]);
            pang -> ?Info("node ~p down!~n", [CMNode])
            end || CMNode <- CMNodes]
    end,
    SupRes = case imem_sup:start_link(StartArgs) of
                 {ok, Pid} ->
                     % imem_server ranch listner is supervised by ranch so not
                     % added to supervison tree, started after imem_sup
                     % successful started to ensure booting complete of imem
                     % before listening for connections
                     %apps_start([asn1, crypto, public_key, ssl, ranch]),
                     case application:get_env(tcp_server) of
                         {ok, true} ->
                             {ok, TcpIf} = application:get_env(tcp_ip),
                             {ok, TcpPort} = application:get_env(tcp_port),
                             {ok, SSL} = application:get_env(ssl),
                             Pwd = case code:lib_dir(imem) of {error, _} -> "."; Path -> Path end,
                             imem_server:start_link([{tcp_ip, TcpIf},{tcp_port, TcpPort}
                                                     ,{pwd, Pwd}, {ssl, SSL}]);
                         _ -> ?Info("imem TCP is not configured to start!~n")
                     end,
                     {ok, Pid};
             	Error -> Error
             end,
    ?Info(" IMEM STARTED~n", []),
    ?Info("---------------------------------------------------~n", []),
    SupRes.

% LAGER Disabled in test
-ifndef(TEST).

config_if_lager() ->
    application:load(lager),
    application:set_env(lager, handlers, [{lager_console_backend, info},
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
    application:set_env(lager, error_logger_redirect, false),
    application:start(lager),
    ?Info("IMEM starting with lager!").

-else. % TEST

% Lager disabled
config_if_lager() ->
    ?Info("IMEM starting without lager!").

-endif. % TEST

stop()  ->
    stop_tcp(),
    application:stop(?MODULE).

stop(_State) ->
	?Info("SHUTDOWN IMEM~n", []),
	ok.

% start stop query imem tcp server
start_tcp(Ip, Port) ->
    imem_server:start_link([{tcp_ip, Ip},{tcp_port, Port}]).

stop_tcp() ->
    imem_server:stop().


% start/stop test writer
start_test_writer(Param) ->
    {ok, ImemTimeout} = application:get_env(imem, imem_timeout),
    {ok, SupPid} = supervisor:start_child(imem_sup, {imem_test_writer
                                                    , {imem_test_writer, start_link, [Param]}
                                                    , permanent, ImemTimeout, worker, [imem_test_writer]}),
    [?Info("imem process ~p started pid ~p~n", [Mod, Pid]) || {Mod,Pid,_,_} <- supervisor:which_children(imem_sup)],
    {ok, SupPid}.
stop_test_writer() ->
    ok = supervisor:terminate_child(imem_sup, imem_test_writer),
    ok = supervisor:delete_child(imem_sup, imem_test_writer),
    [?Info("imem process ~p started pid ~p~n", [Mod, Pid]) || {Mod,Pid,_,_} <- supervisor:which_children(imem_sup)].
