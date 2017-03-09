-module(imem_gen_metrics).

-include("imem.hrl").
-include("imem_meta.hrl").

-behaviour(gen_server).

-export([get_metric/2
        ,get_metric/3
        ,request_metric/4]).

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

-define(DEFAULT_REQ_TIMEOUT, 10000).

-callback init() -> {ok, term()} | {error, term()}.
-callback request_metric(MetricKey :: any()) -> noreply | {ok, any()}.
-callback handle_metric_req(MetricKey :: term(), ReplyFun :: fun(), State :: term()) -> NewState :: term().
-callback terminate(Reason :: term(), State :: term()) -> ok.

-spec start_link(atom()) -> {ok, pid()} | {error, term()}.
start_link(Mod) ->
    gen_server:start_link({local, Mod}, ?MODULE, [Mod], []).

-spec get_metric(atom(), term()) -> term() | {error, timeout}.
get_metric(Mod, MetricKey) ->
    get_metric(Mod, MetricKey, ?DEFAULT_REQ_TIMEOUT).

-spec get_metric(atom(), term(), integer()) -> term() | timeout.
get_metric(Mod, MetricKey, Timeout) ->
    ReqRef = make_ref(),
    gen_server:cast(Mod, {request_metric, MetricKey,
                          build_reply_fun(ReqRef, self())}),
    receive {metric, ReqRef, _Timestamp, Metric} -> Metric
    after Timeout -> timeout
    end.

-spec request_metric(atom(), term(), term(), pid()) -> ok.
request_metric(Mod, MetricKey, ReqRef, ReplyTo) ->
    ReplyFun = build_reply_fun(ReqRef, ReplyTo),
    case Mod:request_metric(MetricKey) of
        noreply ->
            gen_server:cast(Mod, {request_metric, MetricKey, ReplyFun});
        {ok, Reply} -> ReplyFun(Reply)
    end.

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

handle_call(UnknownReq, _From, #state{mod = Mod} = State) ->
    ?Error("~p implementing ~p pid ~p received unknown call ~p", [Mod, ?MODULE, self(), UnknownReq]),
    {noreply, State}.

handle_cast({request_metric, MetricKey, ReplyFun}, #state{} = State) ->
    {noreply, internal_get_metric(MetricKey, ReplyFun, State)};
handle_cast(UnknownReq, #state{mod = Mod} = State) ->
    ?Error("~p implementing ~p pid ~p received unknown cast ~p", [Mod, ?MODULE, self(), UnknownReq]),
    {noreply, State}.

handle_info(Message, State) ->
    ?Error("~p doesn't message unexpected: ~p", [?MODULE, Message]),
    {noreply, State}.

terminate(Reason, #state{mod=Mod, impl_state=ImplState}) ->
    Mod:terminate(Reason, ImplState).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% Helper functions
-spec internal_get_metric(term(), fun(), #state{}) -> #state{}.
internal_get_metric(MetricKey, ReplyFun, #state{mod=Mod, impl_state=ImplState, system_state=SysState} = State) ->
    Time = os:system_time(micro_seconds),
    ElapsedSeconds = (Time - State#state.system_time) / 1000000,
    case {ElapsedSeconds < 1, SysState} of
        {true, normal} ->
            NewImplState = Mod:handle_metric_req(MetricKey, ReplyFun, ImplState),
            State#state{impl_state = NewImplState};
        {true, _} ->
            ReplyFun(SysState),
            State;
        {false, _} ->
            MaxReductions = ?GET_CONFIG(maxReductions,[Mod],100000000,"Max number of reductions per second before considering the system as overloaded."),
            MaxMemory = ?GET_CONFIG(maxMemory,[Mod],90,"Memory usage before considering the system as overloaded."),
            Reductions = element(1,erlang:statistics(reductions)),
            ElapsedReductions = Reductions - State#state.reductions,
            ReductionsRate = ElapsedReductions/ElapsedSeconds,
            {_, FreeMemory, TotalMemory} = imem:get_os_memory(),
            PctMemoryUsed = 100 - FreeMemory / TotalMemory * 100,
            {NewImplState, NewSysState} = case {ReductionsRate > MaxReductions, PctMemoryUsed > MaxMemory} of
                {true, _} ->
                    ReplyFun(cpu_overload),
                    {ImplState, cpu_overload};
                {_, true} ->
                    ReplyFun(memory_overload),
                    {ImplState, memory_overload};
                _ ->
                    {Mod:handle_metric_req(MetricKey, ReplyFun, ImplState), normal}
            end,
            State#state{impl_state = NewImplState
                       ,reductions = Reductions
                       ,system_time = Time
                       ,system_state = NewSysState}
    end.

-spec build_reply_fun(term(), pid() | tuple()) -> fun().
build_reply_fun(ReqRef, ReplyTo) when is_pid(ReplyTo) ->
    fun(Result) -> ReplyTo ! {metric, ReqRef, os:timestamp(), Result} end;
build_reply_fun(ReqRef, Sock) when is_tuple(Sock) ->
    fun(Result) ->
        imem_server:send_resp({metric, ReqRef, os:timestamp(), Result}, Sock)
    end.
