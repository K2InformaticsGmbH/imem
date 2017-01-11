%%% -------------------------------------------------------------------
%%% Author    : Bikram Chatterjee
%%% Description    : 
%%%
%%% Created    : 30.09.2011
%%% -------------------------------------------------------------------

-module(imem_sup).
-behaviour(supervisor).

-include("imem.hrl").

% supervisor callbacks
-export([start_link/1, init/1]).

%% --------------------------------------------------------------------
%% Macros
%% --------------------------------------------------------------------

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args, Timeout), {I, {I, start_link, [Args]}, permanent, Timeout, Type, [I]}).

%% --------------------------------------------------------------------
%% Records
%% --------------------------------------------------------------------

%% ====================================================================
%% External functions
%% ====================================================================

start_link(Args) ->
    ?Info("~p starting...~n", [?MODULE]),
    case supervisor:start_link({local,?MODULE}, ?MODULE, Args) of
        {ok,_} = Success ->
            ?Info("~p started!~n", [?MODULE]),
            % imem_server ranch listner is supervised by ranch so not
            % added to supervison tree, started after imem_sup
            % successful started to ensure booting complete of imem
            % before listening for connections
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
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [?MODULE, Error]),
            Error
    end.

%% ====================================================================
%% Server functions
%% ====================================================================
%% --------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok,  {SupFlags,  [ChildSpec]}} |
%%          ignore                          |
%%          {error, Reason}
%% --------------------------------------------------------------------

init(_StartArgs) ->
    {ok, ImemTimeout} = application:get_env(imem_timeout),
    ?Info("~p starting children with timeout ~p~n", [?MODULE, ImemTimeout]),

    Children =
    [?CHILD(imem_if_mnesia, worker, [], ImemTimeout),
     ?CHILD(imem_if_csv,    worker, [], ImemTimeout),
     ?CHILD(imem_metrics,   worker, [], ImemTimeout)
     | lists:filtermap(
         fun({E,M}) ->
                 case application:get_env(E) of
                     {ok, true} -> {true, ?CHILD(M, worker, [], ImemTimeout)};
                     _ ->
                         ?Info("~p ~p disabled~n", [E, ?MODULE]),
                         false
                 end
         end,
         [{meta_server,         imem_meta},
          {config_server,       imem_config},
          {if_sys_conf_server,  imem_if_sys_conf},
          {monitor_server,      imem_monitor},
          {proll_server,        imem_proll},
          {purge_server,        imem_purge},
          {client_server,       imem_client},
          {seco_server,         imem_seco},
          {domain_server,       imem_domain},
          {snap_server,         imem_snap}])],
    {ok,
     {#{strategy => one_for_one, intensity => 3, period => 10},
      Children
     }}.

%% ====================================================================
%% Internal functions
%% ====================================================================
