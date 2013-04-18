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

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args, Timeout), {I, {I, start_link, [Args]}, permanent, Timeout, Type, [I]}).


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
    {ok, SnapInterval} = application:get_env(mnesia_snap_interval),

    Children =
    % imem_if    
    [?CHILD(imem_if, worker, [{schema_name, SchemaName}, {node_type, NodeType}, {snap_interval, SnapInterval}], ImemTimeout)]
    % imem_meta
    ++
    case application:get_env(meta_server) of
        {ok, true} -> [?CHILD(imem_meta, worker, [], ImemTimeout)];
        _ -> []
    end
    % imem_seco
    ++
    case application:get_env(seco_server) of
        {ok, true} -> [?CHILD(imem_seco, worker, [], ImemTimeout)];
        _ -> []
    end
    % imem_server
    ++
    case application:get_env(tcp_server) of
        {ok, true} ->
            {ok, TcpIf} = application:get_env(tcp_ip),
            {ok, TcpPort} = application:get_env(tcp_port),            
            [?CHILD(imem_server, worker, [{tcp_ip, TcpIf},{tcp_port, TcpPort}], ImemTimeout)];
        _ -> []
    end,

    {ok, {{one_for_one, 3, 10}, Children}}.

%% ====================================================================
%% Internal functions
%% ====================================================================
