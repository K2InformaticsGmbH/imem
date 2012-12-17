%%% -------------------------------------------------------------------
%%% Author	: Bikram Chatterjee
%%% Description	: 
%%%
%%% Created	: 30.09.2011
%%% -------------------------------------------------------------------

-module(imem_app).

-behaviour(application).

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
    % application:load(lager),
    % application:set_env(lager, handlers, [{lager_console_backend, info},
    %                                       {lager_imem, [{db, "imemLog"},
    %                                                     {table, imemLog},
    %                                                     {level, info},
    %                                                     {user, <<"admin">>},
    %                                                     {password, <<"change_on_install">>}]},
    %                                       {lager_file_backend,
    %                                        [{"error.log", error, 10485760, "$D0", 5},
    %                                         {"console.log", info, 10485760, "$D0", 5}]}]),
    % application:set_env(lager, error_logger_redirect, false),
    % lager:start(),
    case application:get_env(erl_cluster_mgr) of
        {ok, undefined} -> io:format(user, "~p - CM not defined!~n", [?MODULE]);
        {ok, CMNode} ->
            case net_adm:ping(CMNode) of
            pong -> io:format(user, "~p - ~p is CM~n", [?MODULE, CMNode]);
            pang -> io:format(user, "~p - CM ~p is not reachable!~n", [?MODULE, CMNode])
            end
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
	io:format(user, "Stopping ~p~n", [?MODULE]),
	ok.
