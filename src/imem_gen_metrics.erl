-module(imem_gen_metrics).

-include("imem.hrl").
-include("imem_meta.hrl").

-behaviour(gen_server).

-export([get_metric/2]).

-export([start_link/1
        ,init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-record(state, {impl_state :: term()
               ,mod :: atom()
               ,reductions :: integer()
               ,system_time :: integer()
               ,system_state = normal :: atom()}).

-callback init() -> {ok, term()} | {error, term()}.
-callback handle_metric_req(MetricKey :: term(), State :: term()) -> Result :: [map()].

-spec start_link(atom()) -> {ok, pid()} | {error, term()}.
start_link(Mod) ->
    gen_server:start_link({local, Mod}, ?MODULE, [Mod], []).

-spec get_metric(atom(), term()) -> [map()].
get_metric(Mod, MetricKey) ->
    gen_server:call(Mod, {get_metric, MetricKey}).

%% Gen server callback implementations.
init([Mod]) ->
    case Mod:init() of
        {ok, State} ->
            Reductions = element(1,erlang:statistics(reductions)),
            Time = os:system_time(micro_seconds),
            {ok, #state{mod = Mod, impl_state = State, reductions = Reductions,
                        system_time = Time, system_state = normal}};
        {error, Reason} -> {stop, Reason}
    end.

handle_call({get_metric, MetricKey}, _From, #state{mod = Mod, impl_state = ImplState, system_state = SysState} = State) ->
    Time = os:system_time(micro_seconds),
    ElapsedSeconds = (Time - State#state.system_time) / 1000000,
    case {ElapsedSeconds < 1, SysState} of
        {true, normal} ->
            {Metric, NewImplState} = Mod:handle_metric_req(MetricKey, ImplState),
            {reply, Metric, State#state{impl_state = NewImplState}};
        {true, _} -> {reply, SysState, State};
        {false, _} ->
            MaxReductions = ?GET_CONFIG(maxReductions,[Mod],100000000,"Max number of reductions per second before considering the system as overloaded."),
            MaxMemory = ?GET_CONFIG(maxMemory,[Mod],90,"Memory usage before considering the system as overloaded."),
            Reductions = element(1,erlang:statistics(reductions)),
            ElapsedReductions = Reductions - State#state.reductions,
            ReductionsRate = ElapsedReductions/ElapsedSeconds,
            {_, FreeMemory, TotalMemory} = imem:get_os_memory(),
            PctMemoryUsed = 100 - FreeMemory / TotalMemory * 100,
            {{Metric, NewImplState}, NewSysState} = case {ReductionsRate > MaxReductions, PctMemoryUsed > MaxMemory} of
                {true, _} -> {{cpu_overload, ImplState}, cpu_overload};
                {_, true} -> {{memory_overload, ImplState}, memory_overload};
                _ -> {Mod:handle_metric_req(MetricKey, ImplState), normal}
            end,
            {reply, Metric, State#state{impl_state = NewImplState
                                       ,reductions = Reductions
                                       ,system_time = Time
                                       ,system_state = NewSysState}}
    end.

handle_cast(Unknown, State) ->
    ?Error("~p implementing ~p pid ~p received unknown cast ~p", [?MODULE, self(), Unknown]),
    {noreply, State}.

handle_info(Message, State) ->
    ?Error("~p doesn't message unexpected: ~p", [?MODULE, Message]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
