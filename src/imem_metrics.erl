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
    process_statistics(ReplyFun, State);
handle_metric_req(UnknownMetric, ReplyFun, State) ->
    ?Error("Unknow metric requested ~p", [UnknownMetric]),
    ReplyFun({error, unknown_metric}),
    State.

terminate(_Reason, _State) -> ok.

process_statistics(
    ReplyFun,
    #{process_statistics := #{last := Last, value := Stats}}
) ->
    Now = os:timestamp(),
    case timer:now_diff(Now, Last) of
        Diff when Diff > ?CACHE_STALE_AFTER ->
            NewStats = get_process_stats(),
            ReplyFun(NewStats),
            #{process_statistics => #{last => Now, value => NewStats}};
        _ ->
            ReplyFun(Stats),
            #{process_statistics => #{last => Last, value => Stats}}
    end;
process_statistics(ReplyFun, _) ->
    NewStats = get_process_stats(),
    ReplyFun(NewStats),
    #{process_statistics => #{last => os:timestamp(), value => NewStats}}.

get_process_stats() ->
    ProcessInfoMaps = [
        {Registered,
         maps:from_list(
            erlang:process_info(
                erlang:whereis(Registered),
                [heap_size, message_queue_len, stack_size,
                 total_heap_size]
            )
         )} || Registered <- erlang:registered()
    ],
    process_info_max(ProcessInfoMaps).

process_info_max(ProcessInfoMaps) ->
    process_info_max(ProcessInfoMaps, #{}).

process_info_max([], MaxProcessInfos) -> MaxProcessInfos;
process_info_max([PiMap | ProcessInfoMaps], Stat) ->
    process_info_max(ProcessInfoMaps, process_max(PiMap, Stat)).

-define(PROCESS_MAX(_PiProp, _StatProp),
    process_max(
        {Process, #{_PiProp := Value} = Pi},
        #{_StatProp := #{value := OldV}} = Stat
    ) ->
        process_max(
            % remove processed property from map to process next recursively
            {Process, maps:without([_PiProp], Pi)},
            if OldV < Value -> % only if new value is greater                
                Stat#{_StatProp => #{process => Process, value => Value}};
                true -> Stat
            end
        );
    % generate the stat property for first time
    process_max({Process, #{_PiProp := Value} = Pi}, Stat) when Value > 0 ->
        process_max(
            {Process, maps:without([_PiProp], Pi)},
            Stat#{_StatProp => #{process => Process, value => Value}}
        );
    % skip all zero values
    process_max({Process, #{_PiProp := _} = Pi}, Stat) ->
        process_max({Process, maps:without([_PiProp], Pi)}, Stat)
).

?PROCESS_MAX(heap_size, max_heap_size);
?PROCESS_MAX(message_queue_len, max_message_queue_len);
?PROCESS_MAX(stack_size, max_stack_size);
?PROCESS_MAX(total_heap_size, max_total_heap_size);
process_max({_, Pi}, Stat) when map_size(Pi) == 0 -> Stat.
