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
    {ok, NodeType} = application:get_env(mnesia_node_type),

    Children =
    % imem_if
    [{imem_if, {imem_if, start_link, [[{schema_name, SchemaName}, {node_type, NodeType}]]}, permanent, ImemTimeout, worker, [imem_if]}]
    % imem_meta
    ++
    case application:get_env(meta_server) of
        {ok, true} -> [{imem_meta, {imem_meta, start_link, [[]]}, permanent, ImemTimeout, worker, [imem_meta]}];
        _ -> []
    end
    % imem_seco
    ++
    case application:get_env(seco_server) of
        {ok, true} -> [{imem_seco, {imem_seco, start_link, [[]]}, permanent, ImemTimeout, worker, [imem_seco]}];
        _ -> []
    end
    % imem_server
    ++
    case application:get_env(tcp_server) of
        {ok, true} ->
            {ok, TcpIf} = application:get_env(tcp_ip),
            {ok, TcpPort} = application:get_env(tcp_port),
            [{imem_server, {imem_server, start_link, [[{tcp_ip, TcpIf},{tcp_port, TcpPort}]]}, permanent, ImemTimeout, worker, [imem_server]}];
        _ -> []
    end,

    {ok, {{one_for_one, 3, 10}, Children}}.

%% ====================================================================
%% Internal functions
%% ====================================================================
