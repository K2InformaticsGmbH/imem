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

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------

-export([]).

%% --------------------------------------------------------------------
%% Internal exports
%% --------------------------------------------------------------------

-export([start/0, start_in_shell/0, start_link/1, init/1]).

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
        supervisor:start_link({local,?MODULE}, ?MODULE, _Arg = [])
    end).
    
start_in_shell() ->
    {ok, Pid} = supervisor:start_link({local,?MODULE}, ?MODULE, _Arg = []),
    unlink(Pid).
    
start_link(Args) ->
    io:format("~nStarting ~p~n", [?MODULE]),
    Result = supervisor:start_link({local,?MODULE}, ?MODULE, Args),
    io:format("~p started!~n~p~n", [?MODULE, Result]),
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
    {ok, MnesiaTimeout} = application:get_env(mnesia_timeout),
    {ok, SchemaName} = application:get_env(mnesia_schema_name),
    io:format("~pinitializing with MnesiaTimeout ~p~n", [?MODULE, MnesiaTimeout]),
    {ok, NodeType} = application:get_env(mnesia_node_type),
    {ok, Mod} = application:get_env(if_mod),
    {ok, IsSec} = application:get_env(if_sec),
    DefParams = [{schema_name, SchemaName}, {node_type, NodeType}, {if_mod, Mod}, {if_sec, IsSec}],
    {ok, Servers} = application:get_env(servers),
    Children = [
        {SMod, {SMod, start_link, [DefParams ++ SParams]}, permanent, MnesiaTimeout, worker, [SMod]}
        || {_SName, {SMod, SParams}} <- Servers],
    {ok, {{one_for_one, 3, 10}, Children}}.


%% ====================================================================
%% Internal functions
%% ====================================================================
