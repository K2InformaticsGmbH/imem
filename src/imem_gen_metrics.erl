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
               ,last_system_check :: integer()
               ,last_request :: integer()
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
    metric_cast(Mod, MetricKey, build_reply_fun(ReqRef, self())),
    receive {metric, ReqRef, _Timestamp, _Node, Metric} -> Metric
    after Timeout -> timeout
    end.

-spec request_metric(atom(), term(), term(), pid()) -> ok.
request_metric(Mod, MetricKey, ReqRef, ReplyTo) ->
    ReplyFun = build_reply_fun(ReqRef, ReplyTo),
    case Mod:request_metric(MetricKey) of
        noreply ->
            metric_cast(Mod, MetricKey, ReplyFun);
        {noreply, NewMetricKey} ->
            metric_cast(Mod, NewMetricKey, ReplyFun);
        {ok, Reply} ->
            ReplyFun(Reply),
            ok
    end.

-spec metric_cast(atom(), term(), fun()) -> ok.
metric_cast(Mod, MetricKey, ReplyFun) ->
    case whereis(Mod) of
        P when not is_pid(P) ->
            ReplyFun({error, no_proc}),
            ok;
        _ ->
            gen_server:cast(Mod, {request_metric, MetricKey, ReplyFun})
    end.

%% Gen server callback implementations.
init([Mod]) ->
    case Mod:init() of
        {ok, State} ->
            Reductions = element(1,erlang:statistics(reductions)),
            Time = os:system_time(micro_seconds),
            {ok, #state{mod = Mod, impl_state = State, reductions = Reductions,
                        last_system_check = Time, system_state = normal}};
        {error, Reason} -> {stop, Reason}
    end.

handle_call({impl, Req}, From, #state{mod = Mod, impl_state = ImplState} = State) ->
    {Reply, ImplState1} = impl_mfa(Mod, handle_call, [Req, From], ImplState),
    {reply, Reply, State#state{impl_state = ImplState1}};
handle_call(UnknownReq, From, #state{mod = Mod} = State) ->
    ?Error("~p unexpected handle_call ~p from ~p", [Mod, UnknownReq, From]),
    {reply, {error, badreq}, State}.

handle_cast({impl, Req}, #state{mod = Mod, impl_state = ImplState} = State) ->
    {_, ImplState1} = impl_mfa(Mod, handle_cast, [Req], ImplState),
    {noreply, State#state{impl_state = ImplState1}};
handle_cast({request_metric, MetricKey, ReplyFun}, #state{} = State) ->
    {noreply, internal_get_metric(MetricKey, ReplyFun, State)};
handle_cast(UnknownReq, #state{mod = Mod} = State) ->
    ?Error("~p unexpected handle_cast ~p", [Mod, UnknownReq]),
    {noreply, State}.

handle_info({impl, Info}, #state{mod = Mod, impl_state = ImplState} = State) ->
    {_, ImplState1} = impl_mfa(Mod, handle_info, [Info], ImplState),
    {noreply, State#state{impl_state = ImplState1}};
handle_info(Message, #state{mod = Mod} = State) ->
    ?Error("~p unexpected handle_info ~p", [Mod, Message]),
    {noreply, State}.

terminate(Reason, #state{mod=Mod, impl_state=ImplState}) ->
    Mod:terminate(Reason, ImplState).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

impl_mfa(Mod, Fun, Args, State) ->
    try apply(Mod, Fun, lists:concat([Args, [State]])) of
        {reply,   IR,       IS}    -> {IR,               IS};
        {reply,   IR,       IS, _} -> {IR,               IS};
        {noreply,           IS}    -> {noreply,          IS};
        {noreply,           IS, _} -> {noreply,          IS};
        {stop,    IRsn,     IS}    -> {{stop, IRsn},     IS};
        {stop,    IRsn, IR, IS}    -> {{stop, IRsn, IR}, IS}
    catch
        Class:Exception ->
            ?Error("CRASH ~p:~p(~p, ~p) -> ~p", [Mod, Fun, Args, State, {Class,Exception}]),
            {{error, {Class, Exception}}, State}
    end.

%% Helper functions
-spec internal_get_metric(term(), fun(), #state{}) -> #state{}.
internal_get_metric(MetricKey, ReplyFun, #state{mod=Mod, impl_state=ImplState} = State) ->
    Time = os:system_time(micro_seconds),
    ElapsedSeconds = (Time - State#state.last_system_check) / 1000000,
    case {ElapsedSeconds < 1, State#state.system_state} of
        {_, eval_crash_suspend} ->
            case is_crash_suspend_timeout((Time - State#state.last_request) / 1000000) of
                true ->
                    ReplyFun(eval_crash_suspend),
                    State;
                false ->
                    internal_get_metric(MetricKey, ReplyFun, State#state{system_state = normal})
            end;
        {true, normal} ->
            {NewImplState, NewSysState} = safe_request_metric(Mod, MetricKey, ReplyFun, ImplState),
            State#state{impl_state = NewImplState
                       ,system_state = NewSysState
                       ,last_request = Time};
        {true, SysState} ->
            ReplyFun(SysState),
            State#state{last_request = Time};
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
                    safe_request_metric(Mod, MetricKey, ReplyFun, ImplState)
            end,
            State#state{impl_state = NewImplState
                       ,reductions = Reductions
                       ,last_system_check = Time
                       ,last_request = Time
                       ,system_state = NewSysState}
    end.

-spec safe_request_metric(atom(), term(), fun(), term()) -> {term(), atom()}.
safe_request_metric(Mod, MetricKey, ReplyFun, ImplState) ->
    try Mod:handle_metric_req(MetricKey, ReplyFun, ImplState) of
        NewImplState -> {NewImplState, normal}
    catch
        Error:Reason ->
            ?Error("~p:~p crash on metric request, called as ~p:handle_metric_req(~p, ~p, ~p)~n~p~n",
                [Error, Reason, Mod, MetricKey, ReplyFun, ImplState, erlang:get_stacktrace()]),
            ReplyFun(eval_crash),
            {ImplState, eval_crash_suspend}
    end.

-spec build_reply_fun(term(), pid() | tuple()) -> fun().
build_reply_fun(ReqRef, ReplyTo) when is_pid(ReplyTo) ->
    fun(Result) -> ReplyTo ! {metric, ReqRef, os:timestamp(), node(), Result} end;
build_reply_fun(ReqRef, Sock) when is_tuple(Sock) ->
    fun(Result) ->
        imem_server:send_resp({imem_async, {metric, ReqRef, os:timestamp(), node(), Result}}, Sock)
    end.

-spec is_crash_suspend_timeout(integer()) -> boolean().
is_crash_suspend_timeout(ElapsedSeconds) when ElapsedSeconds > 2 ->
    % Minimum suspend time after a crash is 2 seconds regardless of configuration.
    SuspendTimeout = ?GET_CONFIG(evalCrashTimeout, [], 10, "Seconds the agent will refuse to respond after an evaluation crash, minimum value 2."),
    ElapsedSeconds < SuspendTimeout;
is_crash_suspend_timeout(_ElapsedSeconds) -> true.
