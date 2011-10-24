%%% -------------------------------------------------------------------
%%% Author	: Bikram Chatterjee
%%% Description	: 
%%%
%%% Created	: 30.09.2011
%%% -------------------------------------------------------------------

-module(sub_info_super).

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
	io:format("~nMnesiaTimeout ~p..~n", [MnesiaTimeout]),
	SubSync = {ss1, {'sub_info', start_link, []}, permanent, MnesiaTimeout, worker, ['sub_info']},
    ThreadList = [SubSync],
	{ok, {{one_for_one, 3, 10}, ThreadList}}.

%% ====================================================================
%% Internal functions
%% ====================================================================
