-module(imem_metrics).

-include("imem.hrl").

-behaviour(imem_gen_metrics).

-export([start_link/1
        ,get_metric/1
        ,get_metric/2
        ,request_metric/3
        ]).

-export([init/0
        ,handle_metric_req/3
        ,request_metric/1
        ,terminate/2
        ]).

-safe([get_metric/1]).

-define(CACHE_STALE_AFTER, 60000000). % 1 minute (in micro second 60 * 1e-6)

-spec start_link(list()) -> {ok, pid()} | {error, term()}.
start_link(_) ->
    imem_gen_metrics:start_link(?MODULE).

-spec get_metric(term()) -> term().
get_metric(MetricKey) ->
    imem_gen_metrics:get_metric(?MODULE, MetricKey).

-spec get_metric(term(), integer()) -> term().
get_metric(MetricKey, Timeout) ->
    imem_gen_metrics:get_metric(?MODULE, MetricKey, Timeout).

-spec request_metric(term(), term(), pid()) -> ok.
request_metric(MetricKey, ReqRef, ReplyTo) ->
    imem_gen_metrics:request_metric(?MODULE, MetricKey, ReqRef, ReplyTo).

-spec request_metric(any()) -> {ok, any()} | noreply.
request_metric(_) -> noreply.

%% imem_gen_metrics callback
init() -> {ok, undefined}.

handle_metric_req(system_information, ReplyFun, State) ->
    {_, FreeMemory, TotalMemory} = imem:get_os_memory(),
    Result = #{
        free_memory => FreeMemory,
        total_memory => TotalMemory,
        erlang_memory => erlang:memory(total),
        process_count => erlang:system_info(process_count),
        port_count => erlang:system_info(port_count), 
        run_queue => erlang:statistics(run_queue)
    },
    ReplyFun(Result),
    State;
handle_metric_req(erlang_nodes, ReplyFun, State) ->
    {ok, RequiredNodes} = application:get_env(imem, erl_cluster_mgrs),
    ReplyFun(#{nodes => [node() | imem_meta:nodes()], required_nodes => RequiredNodes}),
    State;
handle_metric_req(data_nodes, ReplyFun, State) ->
    {ok, RequiredNodes} = application:get_env(imem, erl_cluster_mgrs),
    DataNodes = [#{schema => Schema, node => Node} || {Schema, Node} <- imem_meta:data_nodes()],
    ReplyFun(#{data_nodes => DataNodes, required_nodes => RequiredNodes}),
    State;
handle_metric_req(process_statistics, ReplyFun, State) ->
    Now = os:timestamp(),
    case State of
        #{last := Last, stats := Stats} ->
            case timer:now_diff(Now, Last) of
                Diff when Diff > ?CACHE_STALE_AFTER ->
                    NewStats = get_process_stats(),
                    ReplyFun(NewStats),
                    #{last => Now, stats => NewStats};
                _ ->
                    ReplyFun(Stats),
                    #{last => Last, stats => Stats}
            end;
        _ ->
            NewStats = get_process_stats(),
            ReplyFun(NewStats),
            #{last => Now, stats => NewStats}
    end;
handle_metric_req(UnknownMetric, ReplyFun, State) ->
    ?Error("Unknow metric requested ~p", [UnknownMetric]),
    ReplyFun({error, unknown_metric}),
    State.

terminate(_Reason, _State) -> ok.

get_process_stats() ->
    ProcessInfos = [
        erlang:process_info(
            erlang:whereis(P),
            [heap_size, message_queue_len, stack_size, total_heap_size]
        ) || P <- erlang:registered()],
    lists:foldl(
        fun(
            Pi,
            #{max_stack_size := MaxStackSize,
              max_heap_size := MaxHeapSize,
              max_total_heap_size := MaxTotalHeapSz,
              max_message_queue_len := MaxMQLen}
        ) ->
            #{heap_size := HeapSize, message_queue_len := MQLen,
              stack_size := StackSize, total_heap_size := TotalHeapSz}
                = maps:from_list(Pi),
            #{max_heap_size => lists:max([MaxHeapSize, HeapSize]),
              max_message_queue_len => lists:max([MaxMQLen, MQLen]),
              max_stack_size => lists:max([MaxStackSize, StackSize]),
              max_total_heap_size
                => lists:max([MaxTotalHeapSz, TotalHeapSz])};
        (Pi, _) ->
            #{heap_size := HeapSize, message_queue_len := MQLen,
              stack_size := StackSize, total_heap_size := TotalHeapSz}
                = maps:from_list(Pi),
            #{max_heap_size => HeapSize, max_message_queue_len => MQLen,
              max_stack_size => StackSize,
              max_total_heap_size => TotalHeapSz}
        end,
        #{}, ProcessInfos
    ).
