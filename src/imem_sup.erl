%%% -------------------------------------------------------------------
%%% Author    : Bikram Chatterjee
%%% Description    : 
%%%
%%% Created    : 30.09.2011
%%% -------------------------------------------------------------------

-module(imem_sup).

-behaviour(supervisor).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("imem.hrl").

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------

-export([]).

%% --------------------------------------------------------------------
%% Internal exports
%% --------------------------------------------------------------------

-export([start/0
        , start_in_shell/0
        , start_link/1
        , init/1]).

%% --------------------------------------------------------------------
%% Macros
%% --------------------------------------------------------------------

-define(SERVER, ?MODULE).

%% --------------------------------------------------------------------
%% Records
%% --------------------------------------------------------------------

%% ====================================================================
%% External functions
%% ====================================================================

start() ->
    spawn(fun() ->
        {ok, _} = supervisor:start_link({local,?MODULE}, ?MODULE, _Arg = []),
        [?Log("imem process ~p started pid ~p~n", [Mod, Pid]) || {Mod,Pid,_,_} <- supervisor:which_children(?MODULE)]
    end).
    
start_in_shell() ->
    {ok, SupPid} = supervisor:start_link({local,?MODULE}, ?MODULE, _Arg = []),
    [?Log("imem process ~p started pid ~p~n", [Mod, Pid]) || {Mod,Pid,_,_} <- supervisor:which_children(?MODULE)],
    unlink(SupPid).
    
start_link(Args) ->
    case Result=supervisor:start_link({local,?MODULE}, ?MODULE, Args) of
        {ok,_} ->
            [?Log("imem process ~p started pid ~p~n", [Mod, Pid]) || {Mod,Pid,_,_} <- supervisor:which_children(?MODULE)],
            ?Log("~p started ~p~n", [?MODULE, Result]);
        Error ->    ?Log("~p startup failed with ~p~n", [?MODULE, Error])
    end,
    Result.

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
    {ok, SchemaName} = application:get_env(mnesia_schema_name),
    ?Log("~p initializing with ImemTimeout ~p~n", [?MODULE, ImemTimeout]),
    {ok, NodeType} = application:get_env(mnesia_node_type),
    {ok, Mod} = application:get_env(if_mod),
    {ok, IsSec} = application:get_env(if_sec),
    DefParams = [{schema_name, SchemaName}, {node_type, NodeType}, {if_mod, Mod}, {if_sec, IsSec}],
    {ok, Servers} = application:get_env(servers),
    Children = [
        {SMod, {SMod, start_link, [DefParams ++ SParams]}, permanent, ImemTimeout, worker, [SMod]}
        || {_SName, {SMod, SParams}} <- Servers],
    {ok, {{one_for_one, 3, 10}, Children}}.


%% ====================================================================
%% Internal functions
%% ====================================================================
