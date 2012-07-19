%%% -------------------------------------------------------------------
%%% Author		: Bikram Chatterjee
%%% Description	: 
%%% Version		: 
%%% Created		: 30.09.2011
%%% -------------------------------------------------------------------

-module(imem).

-behaviour(gen_server).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

-include("../include/imem_records.hrl"). 

-define(NODE_DISCOVERY_DELAY, 1000).

%% --------------------------------------------------------------------
%% External exports

-export([cluster/1
		, get_bulk_sleep_time/0
		]).

%% gen_server callbacks

-export([start_link/0
        , start/0
        , start/1
		, init/1
		, handle_call/3
		, handle_cast/2
		, handle_info/2
		, terminate/2
		, code_change/3
		]).

-record(state, {session = 0}).

%% ====================================================================
%% External functions
%% ====================================================================
start() -> start(node()).
start(Node) ->
    application:set_env(?MODULE, cluster_node, Node),
    application:start(?MODULE).

cluster(Node) when is_atom(Node) ->
	cluster([node(), Node]);
cluster(NodeList) when is_list(NodeList) ->
	mnesia:change_config(extra_db_nodes, [node() | NodeList]).
  
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_bulk_sleep_time() ->
	gen_server:call(?MODULE, bulk_sleep_time).

%get_datetime_stamp() ->
%    {{Year,Month,Day},{Hour,Min,Sec}} = erlang:localtime(),
%    lists:flatten(io_lib:format("~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",
%        [Year, Month, Day, Hour, Min, Sec])).

%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init([]) ->
 	timer:sleep(?NODE_DISCOVERY_DELAY),
    io:format("Starting imem...~n", []),
	NodeList = imem_if:find_imem_nodes(),
    io:format("Starting on nodes ~p~n", [NodeList]),
	mnesia:change_config(extra_db_nodes, NodeList),
	{ok, BulkSleepTime} = application:get_env(mnesia_bulk_sleep_time),
	put(mnesia_bulk_sleep_time, BulkSleepTime),
	mnesia:subscribe(system),
	%{ok, Timeout} = application:get_env(mnesia_timeout),
	io:format("~p started!~n", [?MODULE]),
	{ok, #state{session=[]}}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call(bulk_sleep_time, _From, State) ->
    {reply, get(mnesia_bulk_sleep_time), State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(_Msg, State) ->
	{noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info(Info, State) ->
	case Info of
		{mnesia_system_event,{mnesia_overload,Details}} ->
			BulkSleepTime0 = get(mnesia_bulk_sleep_time),
			BulkSleepTime = trunc(1.1 * BulkSleepTime0),
			put(mnesia_bulk_sleep_time, BulkSleepTime),
			io:format("Mnesia overload : ~p!~n",[Details]);
		{mnesia_system_event,{Event,Node}} ->
			io:format("Mnesia event ~p from Node ~p!~n",[Event, Node]);
		Error ->
			io:format("Mnesia error : ~p~n",[Error])
	end,
	{noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
	mnesia:unsubscribe(system),
	io:format("~p stopped!~n~n", [?MODULE]),
	shutdown.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%% Internal functions
%% --------------------------------------------------------------------
