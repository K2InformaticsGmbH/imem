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
    % imem_if_mnesia
    [?CHILD(imem_if_mnesia, worker, [], ImemTimeout)]
    ++
    % imem_if_csv
    [?CHILD(imem_if_csv, worker, [], ImemTimeout)]
    ++
    % imem_meta
    case application:get_env(meta_server) of
        {ok, true} -> [?CHILD(imem_meta, worker, [], ImemTimeout)];
        _ ->
            ?Info("~p meta_server disabled~n", [?MODULE]),
            []
    end
    ++
    % imem_if_sys_conf
    case application:get_env(if_sys_conf_server) of
        {ok, true} -> [?CHILD(imem_if_sys_conf, worker, [], ImemTimeout)];
        _ ->
            ?Info("~p if_sys_conf_server disabled~n", [?MODULE]),
            []
    end
    ++
    % imem_monitor
    case application:get_env(monitor_server) of
        {ok, true} -> [?CHILD(imem_monitor, worker, [], ImemTimeout)];
        _ ->
            ?Info("~p monitor_server disabled~n", [?MODULE]),
            []
    end
    ++
    % imem_proll
    case application:get_env(proll_server) of
        {ok, true} -> [?CHILD(imem_proll, worker, [], ImemTimeout)];
        _ ->
            ?Info("~p proll_server disabled~n", [?MODULE]),
            []
    end
    ++
    % imem_purge
    case application:get_env(purge_server) of
        {ok, true} -> [?CHILD(imem_purge, worker, [], ImemTimeout)];
        _ ->
            ?Info("~p purge_server disabled~n", [?MODULE]),
            []
    end
    ++
    % imem_client
    case application:get_env(client_server) of
        {ok, true} -> [?CHILD(imem_client, worker, [], ImemTimeout)];
        _ ->
            ?Info("~p client_server disabled~n", [?MODULE]),
            []
    end
    ++
    % imem_seco
    case application:get_env(seco_server) of
        {ok, true} -> [?CHILD(imem_seco, worker, [], ImemTimeout)];
        _ ->
            ?Info("~p seco_server disabled~n", [?MODULE]),
            []
    end
    ++
    % imem_domain
    case application:get_env(domain_server) of
        {ok, true} -> [?CHILD(imem_domain, worker, [], ImemTimeout)];
        _ ->
            ?Info("~p domain_server disabled~n", [?MODULE]),
            []
    end
    ++
    % imem_snap
    case application:get_env(snap_server) of
        {ok, true} -> [?CHILD(imem_snap, worker, [], ImemTimeout)];
        _ ->
            ?Info("~p snap_server disabled~n", [?MODULE]),
            []
    end 
    ++
    %imem_rest
    case application:get_env(imem_rest) of
        {ok, true} -> 
            {ok, RestPort} = application:get_env(rest_port),
            [?CHILD(imem_rest, worker, RestPort, ImemTimeout)];
        _ ->
            ?Info("~p imem_rest_server disabled~n", [?MODULE]),
            []
    end,

    {ok, {{one_for_one, 3, 10}, Children}}.

%% ====================================================================
%% Internal functions
%% ====================================================================
