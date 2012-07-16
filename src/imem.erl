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

-export([get_subscriber/1
		, get_payment_info/1
		, cluster/1
		, get_bulk_sleep_time/0
		, get_sync_count/0
		, delete_subscriber/3
		, write_subscriber/4
		, set_sub_counter/5
		, update_sub_counters/4
		, read_sub_counters/4
		]).

%% gen_server callbacks

-export([start_link/0
        , start/0
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
start() -> application:start(?MODULE).

cluster(Node) when is_atom(Node) ->
	cluster([node(), Node]);
cluster(NodeList) when is_list(NodeList) ->
	mnesia:change_config(extra_db_nodes, [node() | NodeList]).
  
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_bulk_sleep_time() ->
	gen_server:call(?MODULE, bulk_sleep_time).
  
get_sync_count() ->
	mnesia:dirty_read(syncinfo, 'change_count').

get_datetime_stamp() ->
    {{Year,Month,Day},{Hour,Min,Sec}} = erlang:localtime(),
    lists:flatten(io_lib:format("~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",
        [Year, Month, Day, Hour, Min, Sec])).

delete_subscriber(Msisdn, SyncDate, ChangeCount) ->
	Record = mnesia:dirty_read(subscriber, Msisdn),
	case Record of
		[] ->
			{0, 0, 0};
		_ ->
			case mnesia:sync_transaction(trans_fun(Record, SyncDate, ChangeCount, -1)) of
				{atomic, _} -> {0, 0, 1};
				{aborted, Reason} -> {error, Reason}
			end
	end.

write_subscriber(Msisdn, {IntMsisdn, PaymentType, PaymentMethod, IntStatus, IntChangeCount}, SyncDate, ChangeCount)
    when is_list(PaymentType), is_list(PaymentMethod)->
    Record = #subscriber{msisdn=IntMsisdn
                ,payment_type=trunc(list_to_integer(PaymentType))
                ,payment_method=trunc(list_to_integer(PaymentMethod))
                ,status=IntStatus
                ,change_count=IntChangeCount
            },
	DbRecord = mnesia:dirty_read(subscriber, Msisdn),
	case DbRecord of
		[] ->
			case mnesia:sync_transaction(trans_fun(Record, SyncDate, ChangeCount, 1)) of
				{atomic, _} -> {1, 0, 0};
				{aborted, Reason} -> {error, Reason}
			end;
		[Record] ->
			{0, 0, 0};
		_ ->			
			case mnesia:sync_transaction(trans_fun(Record, SyncDate, ChangeCount, 0)) of
				{atomic, _} -> {0, 1, 0};
				{aborted, Reason} -> {error, Reason}
			end
	end.

read_sub_counters(PartitionKey, ShardKey, BaseId, KeysToRead) ->
	case mnesia:sync_transaction(read_sub_counters_fun(PartitionKey, ShardKey, BaseId, KeysToRead)) of
		{atomic, Result} -> Result;
		{aborted, Reason} -> {error, Reason}
	end.

read_sub_counters_fun([], [], BaseId, KeysToRead) -> 
	read_sub_counters_fun(sub_counter, BaseId, KeysToRead);
read_sub_counters_fun(PartitionKey, ShardKey, BaseId, KeysToRead) -> 
    Table = list_to_atom(lists:flatten(["sub_counter_", PartitionKey, "_", ShardKey])), 
	read_sub_counters_fun(Table, BaseId, KeysToRead).

read_sub_counters_fun(Table, BaseId, KeysToRead) -> 
	fun() ->
			Result = lists:foldl(fun(K, Acc) ->
							  Key = case BaseId of
										[] -> K;
										_ -> lists:flatten([BaseId,".",K])
									end,
							  case mnesia:read(Table, Key) of
								  [#sub_counter{counter_value = Val}] ->
									  [{K, Val}| Acc];
								  _ -> [{K, undefined}| Acc]
							  end
					  end,
					  [],
					  KeysToRead),
			lists:reverse(Result)
	end.


set_sub_counter(PartitionKey, ShardKey, BaseId, CounterName, CounterValue) ->
	set_sub_counters(PartitionKey, ShardKey, BaseId, [{CounterName, CounterValue}]).

set_sub_counters(PartitionKey, ShardKey, BaseId, SetList) ->
	case mnesia:sync_transaction(set_sub_counters_fun(PartitionKey, ShardKey, BaseId, SetList)) of
		{atomic, Result} -> Result;
		{aborted, Reason} -> {error, Reason}
	end.

set_sub_counters_fun([], [], BaseId, SetList) -> 
	set_sub_counters_fun(sub_counter, BaseId, SetList);
set_sub_counters_fun(PartitionKey, ShardKey, BaseId, SetList) -> 
    Table = list_to_atom(lists:flatten(["sub_counter_", PartitionKey, "_", ShardKey])), 
	set_sub_counters_fun(Table, BaseId, SetList).

set_sub_counters_fun(Table, BaseId, SetList) -> 
	fun() ->
			Result = lists:foldl(fun({K,V}, Acc) ->
										 Key = lists:flatten([BaseId,".",K]),
										 mnesia:write(Table, #sub_counter{counter_name=Key, counter_value=V}, write),
										 [{K, V}| Acc]
								 end, [],
								 SetList),
			lists:reverse(Result)
	end.


update_sub_counters(PartitionKey, ShardKey, BaseId, UpdateList) ->
	case mnesia:sync_transaction(update_sub_counters_fun(PartitionKey, ShardKey, BaseId, UpdateList)) of
		{atomic, Result} -> Result;
		{aborted, Reason} -> {error, Reason}
	end.

update_sub_counters_fun([], [], BaseId, UpdateList) -> 
	update_sub_counters_fun(sub_counter, BaseId, UpdateList);
update_sub_counters_fun(PartitionKey, ShardKey, BaseId, UpdateList) -> 
    Table = list_to_atom(lists:flatten(["sub_counter_", PartitionKey, "_", ShardKey])), 
	update_sub_counters_fun(Table, BaseId, UpdateList).

update_sub_counters_fun(Table, BaseId, UpdateList) -> 
	fun() ->
			Result = lists:foldl(fun({K,Incr}, Acc) ->
							  Key = lists:flatten([BaseId,".",K]),
							  case mnesia:read(Table, Key) of
								  [#sub_counter{counter_value = Val}] ->
									  mnesia:write(Table, #sub_counter{counter_name=Key, counter_value=Val+Incr}, write),
									  [{K, Val+Incr}| Acc];
								  _ -> mnesia:write(Table, #sub_counter{counter_name=Key, counter_value=Incr}, write),
									   [{K, Incr}| Acc]
							  end
					  end, [],
					  UpdateList),
			lists:reverse(Result)
	end.

trans_fun(Record, SyncDate, ChangeCount, Incr) ->
	fun() ->
			if Incr < 0 ->  mnesia:delete(subscriber, Record, write);
			   true -> mnesia:write(subscriber, Record, write)
			end,
			mnesia:write(syncinfo, #syncinfo{key='sync_time', val=SyncDate}, write),
			mnesia:write(syncinfo, #syncinfo{key='update_time', val=get_datetime_stamp()}, write),
			mnesia:write(syncinfo, #syncinfo{key='change_count', val=ChangeCount}, write),
			[#syncinfo{val = Count}] = mnesia:read(syncinfo, 'record_count', read), 
			mnesia:write(syncinfo, #syncinfo{key='record_count', val=Count+Incr}, write)
	end.

-spec get_subscriber(integer()) -> [#subscriber{}] | [] | {error,_}.
get_subscriber(Msisdn) ->
	case catch(mnesia:dirty_read(subscriber, Msisdn)) of
		[] -> [];
		[Sub] -> [Sub];
		Error -> {error, Error}
	end.

-spec get_payment_info(integer()) -> {atom(),atom()}.
get_payment_info(Msisdn) ->
	get_payment_info_result(get_subscriber(Msisdn)).
 
-spec get_payment_info_result([#subscriber{}] | [] | {error,_}) -> {atom(),atom()}.
get_payment_info_result([]) ->
	{?SUB_PAYMENT_TYPE_UNKNOWN, ?SUB_PAYMENT_METHOD_UNKNOWN};
get_payment_info_result([SubInfo]) ->
	{erlang:element(#subscriber.payment_type, SubInfo), erlang:element(#subscriber.payment_method, SubInfo)};
get_payment_info_result({error, _Error}) ->
	{?SUB_PAYMENT_TYPE_ERROR, ?SUB_PAYMENT_METHOD_ERROR}.

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
%% 	timer:sleep(?NODE_DISCOVERY_DELAY),
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
