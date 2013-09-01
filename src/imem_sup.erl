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
        [?Info("imem process ~p started pid ~p~n", [Mod, Pid]) || {Mod,Pid,_,_} <- supervisor:which_children(?MODULE)]
    end).

start_in_shell() ->
    {ok, SupPid} = supervisor:start_link({local,?MODULE}, ?MODULE, _Arg = []),
    [?Info("imem process ~p started pid ~p~n", [Mod, Pid]) || {Mod,Pid,_,_} <- supervisor:which_children(?MODULE)],
    unlink(SupPid).

start_link(Args) ->
    case Result=supervisor:start_link({local,?MODULE}, ?MODULE, Args) of
        {ok,_} ->
            [?Info("imem process ~p started pid ~p~n", [Mod, Pid]) || {Mod,Pid,_,_} <- supervisor:which_children(?MODULE)],
            ?Info("~p started ~p~n", [?MODULE, Result]);
        Error ->    ?Info("~p startup failed with ~p~n", [?MODULE, Error])
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
    ?Info("~p starting children with timeout ~p~n", [?MODULE, ImemTimeout]),

    Children =
    % imem_if
    [?CHILD(imem_if, worker, [], ImemTimeout)]
    % imem_meta
    ++
    case application:get_env(meta_server) of
        {ok, true} -> [?CHILD(imem_meta, worker, [], ImemTimeout)];
        _ -> []
    end
    % imem_monitor
    ++
    case application:get_env(monitor_server) of
        {ok, true} -> [?CHILD(imem_monitor, worker, [], ImemTimeout)];
        _ -> []
    end
    % imem_purge
    ++
    case application:get_env(purge_server) of
        {ok, true} -> [?CHILD(imem_purge, worker, [], ImemTimeout)];
        _ -> []
    end
    % imem_seco
    ++
    case application:get_env(seco_server) of
        {ok, true} -> [?CHILD(imem_seco, worker, [], ImemTimeout)];
        _ -> []
    end
    % imem_snap
    ++
    case application:get_env(snap_server) of
        {ok, true} -> [?CHILD(imem_snap, worker, [], ImemTimeout)];
        _ -> []
    end,

    {ok, {{one_for_one, 3, 10}, Children}}.

%% ====================================================================
%% Internal functions
%% ====================================================================
