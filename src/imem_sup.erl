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
    io:format("~ninitializing ~p..~n", [?MODULE]),
    {ok, MnesiaTimeout} = application:get_env(mnesia_timeout),
    CNode = case application:get_env(cluster_node) of
        {ok, undefined} -> node();
        {ok, CN} -> CN
    end,
    io:format(user, "Cluster node ~p~n", [CNode]),
    pong = net_adm:ping(CNode),
    %NodeList = imem_if:find_imem_nodes(),
    %create_schema_on_missing_nodes(NodeList),
    ok = mnesia:start(),
    io:format("~nMnesiaTimeout ~p..~n", [MnesiaTimeout]),
    ThreadList = [{imem, {imem, start_link, []}, permanent, MnesiaTimeout, worker, [imem]}],
    ThreadList0 = case application:get_env(start_monitor) of
        {ok, false} -> ThreadList;
        _ -> [{imem_if, {imem_if, start_link, []}, permanent, MnesiaTimeout, worker, [imem_if]} | ThreadList]
    end,
    {ok, {{one_for_one, 3, 10}, ThreadList0}}.

%create_schema_on_missing_nodes([]) -> ok;
%create_schema_on_missing_nodes([N|NodeList]) ->
%    case mnesia:create_schema([N]) of
%        ok ->
%            io:format(user, "imem:mnesia:create_schema created ~p~n", [N]);
%        {error, {N,{already_exists,N}}} ->
%            io:format(user, "imem:mnesia:create_schema schema exists on ~p skipping~n", [N]),
%            create_schema_on_missing_nodes(NodeList)
 %    end.

%% ====================================================================
%% Internal functions
 %% ====================================================================
