-module(imem_metrics).

-include("imem.hrl").

-behaviour(imem_gen_metrics).

-export([start_link/1
        ,get_metric/1
        ]).

-export([init/0
        ,handle_metric_req/2
        ]).

-spec start_link(list()) -> {ok, pid()} | {error, term()}.
start_link(_) ->
    imem_gen_metrics:start_link(?MODULE).

-spec get_metric(atom()) -> [map()].
get_metric(MetricKey) ->
    imem_gen_metrics:get_metric(?MODULE, MetricKey).


%% imem_gen_metrics callback
init() -> {ok, undefined}.

handle_metric_req(system_information, State) ->
    {_, FreeMemory, TotalMemory} = imem:get_os_memory(),
    {#{free_memory => FreeMemory,
       total_memory => TotalMemory,
       erlang_memory => erlang:memory(total),
       process_count => erlang:system_info(process_count),
       port_count => erlang:system_info(port_count), 
       run_queue => erlang:statistics(run_queue)}, State};
handle_metric_req(UnknownMetric, State) ->
    ?Error("Unknow metric requested ~p", [UnknownMetric]),
    {undefined, State}.
