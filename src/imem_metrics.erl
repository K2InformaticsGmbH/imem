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
    ReplyFun(imem_meta:nodes()),
    State;
handle_metric_req(data_nodes, ReplyFun, State) ->
    ReplyFun([#{schema => Schema, node => Node} || {Schema, Node} <- imem_meta:data_nodes()]),
    State;
handle_metric_req(UnknownMetric, ReplyFun, State) ->
    ?Error("Unknow metric requested ~p", [UnknownMetric]),
    ReplyFun(undefined),
    State.

terminate(_Reason, _State) -> ok.
