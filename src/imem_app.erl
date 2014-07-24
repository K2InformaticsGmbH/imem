%%% -------------------------------------------------------------------
%%% Author	: Bikram Chatterjee
%%% Description	: 
%%%
%%% Created	: 30.09.2011
%%% -------------------------------------------------------------------

-module(imem_app).

-behaviour(application).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("imem.hrl").

%% --------------------------------------------------------------------
%% Behavioural exports
%% --------------------------------------------------------------------
-export([start/2, stop/1]).

%% --------------------------------------------------------------------
%% Macros
%% --------------------------------------------------------------------

%% --------------------------------------------------------------------
%% Records
%% --------------------------------------------------------------------

%% --------------------------------------------------------------------
%% API Functions
%% --------------------------------------------------------------------


%% ====================================================================!
%% External functions
%% ====================================================================!
%% --------------------------------------------------------------------
%% Func: start/2
%% Returns: {ok, Pid}        |
%%          {ok, Pid, State} |
%%          {error, Reason}
%% --------------------------------------------------------------------
apps_start([]) -> ok;
apps_start([A|Rest]) when is_atom(A) ->
    case (case application:start(A) of
             ok -> ok;
             {error, {already_started, A}} -> ok;
             Err -> {error, Err}
         end) of
        {error, Error} -> ?Error("~p start failed ~p~n", [A, Error]);
        ok -> apps_start(Rest)
    end.

start(_Type, StartArgs) ->
    % cluster manager node itself may not run any apps
    % it only helps to build up the cluster
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
    case imem_sup:start_link(StartArgs) of
    	{ok, Pid} ->
            % imem_server ranch listner
            % supervised by ranch so not added to supervison
            % tree started after imem_sup successful start start
            % to ensure imem complete booting before listening
            % for unside connections
            apps_start([asn1, crypto, public_key, ssl, ranch]),
            case application:get_env(tcp_server) of
                {ok, true} ->
                    {ok, TcpIf} = application:get_env(tcp_ip),
                    {ok, TcpPort} = application:get_env(tcp_port),
                    {ok, SSL} = application:get_env(ssl),
                    Pwd = case code:lib_dir(imem) of {error, _} -> "."; Path -> Path end,
                    imem_server:start_link([{tcp_ip, TcpIf},{tcp_port, TcpPort}, {pwd, Pwd}, {ssl, SSL}]);
                _ -> ?Info("imem TCP is not configured to start!~n")
            end,
            {ok, Pid};
    	Error -> Error
    end.

%% --------------------------------------------------------------------
%% Func: stop/1
%% Returns: any
%% --------------------------------------------------------------------
stop(_State) ->
	?Info("stopping ~p~n", [?MODULE]),
	ok.
