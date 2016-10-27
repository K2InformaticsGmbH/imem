-module(imem_metrics).

-include("imem.hrl").

-behaviour(imem_gen_metrics).

-export([start_link/0
        ,get_metric/1
        ]).

-export([init/0
        ,handle_metric_req/2
        ]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    imem_gen_metrics:start_link(?MODULE).

-spec get_metric(atom()) -> [map()].
get_metric(MetricKey) ->
    imem_gen_metrics:get_metric(?MODULE, MetricKey).


%% imem_gen_metrics callback
init() -> {ok, undefined}.

handle_metric_req(system_information, State) ->
    %% Not implemented.
    {[], State};
handle_metric_req(UnknownMetric, State) ->
    ?Error("Unknow metric requested ~p", [UnknownMetric]),
    {undefined, State}.
