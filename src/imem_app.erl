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
start(_Type, StartArgs) ->
    case application:get_env(erl_cluster_mgr) of
        {ok, undefined} -> ?Log("~p - CM not defined!~n", [?MODULE]);
        {ok, CMNode} ->
            case net_adm:ping(CMNode) of
            pong -> ?Log("~p - ~p is CM~n", [?MODULE, CMNode]);
            pang -> ?Log("~p - CM ~p is not reachable!~n", [?MODULE, CMNode])
            end
    end,
    % imem_server ranch listner (started unsupervised)
    case application:start(ranch) of
        ok -> ok;
        {error, {already_started, ranch}} -> ok;
        Err -> ?Log("ranch start failed ~p~n", [Err])
    end,
    case application:get_env(tcp_server) of
        {ok, true} ->
            {ok, TcpIf} = application:get_env(tcp_ip),
            {ok, TcpPort} = application:get_env(tcp_port),
            imem_server:start_link([{tcp_ip, TcpIf},{tcp_port, TcpPort}]);
        _ -> ?Log("~p - imem TCP is not configured to start!~n", [?MODULE])
    end,
    case imem_sup:start_link(StartArgs) of
    	{ok, Pid} ->
    		{ok, Pid};
    	Error ->
    		Error
    end.

%% --------------------------------------------------------------------
%% Func: stop/1
%% Returns: any
%% --------------------------------------------------------------------
stop(_State) ->
	?Log("stopping ~p~n", [?MODULE]),
	ok.
