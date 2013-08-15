%%% -------------------------------------------------------------------
%%% Author      : Bikram Chatterjee
%%% Description : 
%%% Version     : 
%%% Created     : 30.09.2011
%%% -------------------------------------------------------------------

-module(imem).

-include("imem.hrl").

-export([start/0
        , stop/0
        , start_test_writer/1
        , stop_test_writer/0
        , start_tcp/2
        , stop_tcp/0
        ]).

%% ====================================================================
%% External functions
%% ====================================================================
start() ->
    application:start(sqlparse),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),
    application:start(sasl),
    application:start(os_mon),
    config_if_lager(),
    application:start(?MODULE).

-ifdef(LAGER).

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

-else. %LAGER

config_if_lager() ->
    ?Info("IMEM starting without lager!").

-endif. %LAGER

stop()  ->
    stop_tcp(),
    application:stop(?MODULE).

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
