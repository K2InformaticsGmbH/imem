-module(imem_tracer).

-include("imem.hrl").
-include("imem_tracer.hrl").

% interface API
-export([trace_whitelist/2]).

% imem interfaces
-export([init/0, subscribe/0, unsubscribe/0, trigger/5]).
-safe([trigger/5]).

-define(MAX_MSG_PER_SEC, 10000).
-define(MAX_TRACER_PROC_MQ_LEN, 1000).

-export([tracer_proc/1, handler_fun/0]).

%------------------------------------------------------------------------------
% imem_meta callback
%------------------------------------------------------------------------------

init() ->
    imem_meta:init_create_check_table(
        ddTrace,
        {record_info(fields, ddTrace), ?ddTrace, ddTraceDef()},
        [{trigger,
            <<"fun(__OR,__NR,__T,__U,__O) ->"
              " imem_tracer:trigger(__OR,__NR,__T,__U,__O) "
              "end.">>}],
        system
    ).

trigger(OldRec, NewRec, ddTrace, User, _TrOpts) ->
    case {OldRec,NewRec} of
        {{}, {}} ->
            ?Warn("truncated stopping ddTrace"),
            dbg:stop_clear();
        {{}, #ddTrace{enable = Enabled, mod = M, func = F, args = A,
                      match_spec = MS} = Trace} ->
            case Enabled == true andalso is_trace_running() of
                true ->
                    check_start_trace(User, Trace),
                    MatchSpec = ms_lookup(MS, User),
                    send_sync({add, M, F, A, MatchSpec}),
                    dbg:tpl(M, F, A, MatchSpec),
                    dbg:p(all, [c]);
                false when Enabled == true -> ?Warn("trace not running");
                _ -> ok
            end;
        {#ddTrace{enable = Enabled, mod = M, func = F, args = A}, {}} ->
            case Enabled == true andalso is_trace_running() of
                true ->
                    send_sync({del, M, F, A}),
                    dbg:ctpl(M, F, A);
                _ -> ok
            end;
        {#ddTrace{enable = true, mod = M, func = F, args = A},
         #ddTrace{enable = false}} ->
            case is_trace_running() of
                true ->
                    send_sync({del, M, F, A}),
                    dbg:ctpl(M, F, A);
                _ -> ok
            end;
        {#ddTrace{mod = M, func = F, args = A},
         #ddTrace{enable = true, mod = M1, func = F1, args = A1,
                  match_spec = MS} = Trace} ->
            case is_trace_running() of
                true ->
                    check_start_trace(User, Trace),
                    send_sync({del, M, F, A}),
                    dbg:ctpl(M, F, A),
                    MatchSpec = ms_lookup(MS, User),
                    send_sync({add, M1, F1, A1, MatchSpec}),
                    dbg:tpl(M1, F1, A1, MatchSpec),
                    dbg:p(all, [c]);
                _ -> ?Warn("trace not running")
            end;
        _ -> ok
    end.

%------------------------------------------------------------------------------
% imem_if_mnesia interceptors
%------------------------------------------------------------------------------

-record(tracer_cb_st, {pid, count = 0, last_s = 0}).

subscribe() ->
    ?Warn("stopping all previous traces"),
    catch dbg:stop_clear(),
    Pid = spawn(?MODULE, tracer_proc, [self()]),
    catch exit(whereis(?MODULE), kill),
    catch erlang:unregister(?MODULE),
    true = erlang:register(?MODULE, Pid),
    disable_tps(),
    ok.

unsubscribe() ->
    ?Info("stop all traces"),
    catch dbg:stop_clear(),
    catch exit(whereis(?MODULE), kill),
    catch erlang:unregister(?MODULE).

%------------------------------------------------------------------------------
% erlang dbg tracer callback
%------------------------------------------------------------------------------

tracer(Trace, #tracer_cb_st{last_s = LastSec, count = Count} = St) ->
    Now = timestamp_sec(),
    NewCount =
        if 
            LastSec < Now ->    0;
            true ->             Count + 1
        end,
    if 
        NewCount > ?MAX_MSG_PER_SEC ->
            ?Warn("tracer callback overflow > ~p traces / sec", [?MAX_MSG_PER_SEC]),
            dbg:stop_clear(), % stop debugging
            St; % dropped
        true ->
            send_trace_event(Trace, St#tracer_cb_st{count = NewCount, last_s = Now})
    end.


handler_fun() ->
    {ok, fun tracer/2}.

%------------------------------------------------------------------------------
% imem_tracer singleton process
%------------------------------------------------------------------------------

-record(state, {filters = #{}, reply}).
tracer_proc(ReplyPid) when is_pid(ReplyPid) ->
    erlang:monitor(process, ReplyPid),
    tracer_proc(#state{reply = ReplyPid});
tracer_proc(#state{reply = ReplyPid} = State) ->
    NewState =
        receive
            {tracer_pid, TracerPid} ->
                ?Info("imem_tracer ~p, dbg:tracer ~p, imem_statement ~p~n",
                      [self(), TracerPid, ReplyPid]),
                true = erlang:link(TracerPid),
                State;
            {trace_event, Trace} ->
                process_trace(Trace, State);
            {From, {add, M, F, A, Rate}} ->
                From ! ok,
                State#state{
                    filters =
                        (State#state.filters)#{
                            {M,F,A} => #{rate => Rate, stat => #{},
                                         overflow => false}}
                };
            {From, {del, M, F, A}} ->
                From ! ok,
                State#state{
                    filters = maps:remove({M,F,A}, State#state.filters)
                };
            {_, _, process, _, _} = Mon ->
                ?SystemExceptionNoLogging({process_died, Mon});
            Other ->
                ?Error("got unexpected message : ~p", [Other]),
                exit({badarg, Other, State})
        end,
    tracer_proc(NewState).

%------------------------------------------------------------------------------
% internal functions
%------------------------------------------------------------------------------

check_start_trace(_User, #ddTrace{trace_node = Node, enable = true})
    when Node /= node() ->
        check_start_tracer(Node);
check_start_trace(User,  #ddTrace{trace_node = Node, enable = true, mod = M,
                                  func = F, args = A}) when Node == node() ->
    case
        imem_meta:select_count(
            ddTrace,
            [{#ddTrace{event_type = register, enable = true, trace_node = '$1',
                       _ = '_'},
             [{'/=', {node}, '$1'}],
             [true]}]
        ) > 0 of
        true -> ?ClientErrorNoLogging("one or more remote traces are active");
        _ ->
            check_start_tracer(Node)
    end,
    LocalSafe = trace_whitelist(User, []),
    case LocalSafe -- lists:usort([M, {M, F}, {M, '_'}, {M, F, A},
                                   {M, '_', '_'}]) of
        LocalSafe ->
            ?ClientErrorNoLogging(
                "mod / func / args unsafe for local node tracing"
                " (not whitelisted)"
            );
        _ -> true
    end;
check_start_trace(_User, _Trace) -> ok.

check_start_tracer(Node) ->
    case dbg:get_tracer(Node) of
        {ok, Pid} when is_pid(Pid) -> ok;
        {error,{no_tracer_on_node,Node}} ->
            case whereis(?MODULE) of
                Pid when is_pid(Pid) ->
                    case is_process_alive(Pid) of
                        true -> check_start_tracer(Node, Pid);
                        false ->
                            ?ClientErrorNoLogging("tracer process not alive")
                    end;
                _ -> ?ClientErrorNoLogging("tracer process not found")
            end
    end.
check_start_tracer(Node, Pid) ->
    {ok, F} = rpc:call(Node, imem_tracer, handler_fun, []),
    case dbg:tracer(Node, process, {F, #tracer_cb_st{pid = Pid}}) of
        {ok, Node} ->
            {ok, TPid} = dbg:get_tracer(Node),
            Pid ! {tracer_pid, TPid};
        {error, Error} ->
            ?ClientErrorNoLogging({"on trace activate", Node, Error})
    end.

process_trace({trace, From, Type, MFA}, State) ->
    process_trace({trace, From, Type, MFA, <<>>}, State);
process_trace({trace, FromTo, Type, {M, F, ArityArgs}, Extra}, State) ->
    case apply_filter(M, F, ArityArgs, State) of
        {ok, NewState} ->
            Row = #ddTrace{
                trace_key = timestamp(), process = FromTo, event_type = Type,
                mod = M, func = F, args = ArityArgs, extra = Extra,
                trace_node = node(FromTo)
            },
            State#state.reply !
                {mnesia_table_event, {write, ddTrace, Row, [], undefined}},
            NewState;
        {overflow, NewState} ->
            Row = #ddTrace{
                trace_key = timestamp(), process = FromTo, event_type = Type,
                mod = M, func = F, args = ArityArgs, extra = Extra,
                overflow = true, trace_node = node(FromTo)
            },
            State#state.reply !
                {mnesia_table_event, {write, ddTrace, Row, [], undefined}},
            NewState;
        {drop, NewState} ->
            NewState
    end.

send_trace_event(Trace, #tracer_cb_st{pid = Pid} = State) ->
    {messages, Messages} = rpc:call(node(Pid), erlang, process_info, [Pid, messages]),
    if length(Messages) < ?MAX_TRACER_PROC_MQ_LEN ->
        case catch Pid ! {trace_event, Trace} of
            {'EXIT', Error} ->
                ?Error("send_trace_event failed : ~p : ~p", [Error, Trace]),
                dbg:stop_clear();
            _ ->
                State
        end;
        true -> State
    end.

is_trace_running() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) ->
            is_process_alive(Pid);
        _ -> false
    end.

send_sync(Message) ->
    {messages, Messages} = process_info(whereis(?MODULE), messages),
    if length(Messages) < ?MAX_TRACER_PROC_MQ_LEN ->
        case catch ?MODULE ! {self(), Message} of
            {'EXIT', Error} ->
                ?Error("send_sync failed : ~p : ~p", [Error, Message]),
                dbg:stop_clear();
            _ ->
                receive Reply -> Reply
                after 1000 ->
                    ?SystemExceptionNoLogging("tracer reply timeout")
                end
        end;
        true -> ?SystemExceptionNoLogging("tracer process overloaded")
    end.

ddTraceDef() ->
    (#ddTrace{})#ddTrace{
        trace_key =
            list_to_binary(
                io_lib:format("fun(_,_R) -> imem_meta:record_hash(_R,~p) end.",
                              [[#ddTrace.event_type, #ddTrace.mod,
                                #ddTrace.func, #ddTrace.args,
                                #ddTrace.trace_node]])
            ),
        trace_node = node()
    }.

ms_lookup([A|_] = MS, _) when is_atom(A); is_tuple(A) -> MS;
ms_lookup("default", User) ->
    ?GET_CONFIG({trace_ms, "default"}, [User], [{'_', [], []}],
                "default smallest match specification for debug tracing");
ms_lookup(MS, User) -> ?LOOKUP_CONFIG(MS, User).

apply_filter(Mod, Fun, Args, State) when is_list(Args) ->
    apply_filter(Mod, Fun, length(Args), State);
apply_filter(M, F, A, #state{filters = Filters} = State) ->
    MFA = {M,   F,   A},
    MF_ = {M,   F, '_'},
    M__ = {M, '_', '_'},
    {Key, Rate, Stat, Overflow} =
        case Filters of
            #{MFA := #{rate := R, stat := S, overflow := O}} -> {MFA, R, S, O};
            #{MF_ := #{rate := R, stat := S, overflow := O}} -> {MF_, R, S, O};
            #{M__ := #{rate := R, stat := S, overflow := O}} -> {M__, R, S, O};
            _ ->
                ?Error("No filters for ~p:~p/~p in ~p", [M, F, A, Filters]),
                dbg:ctpl(M, F, A),
                {'$nokey', -1, undefined, undefined}
        end,
    if 
        Rate < 1 -> 
            {ok, State};
        true ->
            Now = timestamp_sec(),
            Last = maps:get(last, Stat, Now),
            case (Last < Now) of
                true ->     % New Second, reset everything
                    {ok,
                        State#state{
                            filters =
                                Filters#{Key =>
                                    #{rate => Rate,
                                      stat => #{last => Now, count => 1},
                                      overflow => false}}
                        }
                    };
                % Within same second, count up
                false ->
                    NC = maps:get(count, Stat, 0) + 1,
                    Status =
                        if
                            % propagate once
                            NC > Rate andalso Overflow == false -> overflow;
                            % DON'T propagate
                            NC > Rate -> drop;
                            % propagate
                            true -> ok
                        end,
                    Filter = #{rate => Rate, stat => Stat#{count => NC, last => Last}},
                    NewFilter = case Status of
                                    overflow -> Filter#{overflow => true};
                                    drop -> Filter#{overflow => true};
                                    _ -> Filter#{overflow => false}
                                end,
                    {Status, State#state{filters = Filters#{Key => NewFilter}}}
            end
    end.

disable_tps() ->
    {Rows, _} = imem_meta:select(
                    ddTrace,
                    [{#ddTrace{event_type = register, enable= true, _ = '_'},
                    [], ['$_']}]
                ),
    lists:foreach(
        fun(Row) ->
            case imem_meta:write(ddTrace, Row#ddTrace{enable = false}) of
                ok -> ok;
                Error -> ?SystemExceptionNoLogging({Error, Row})
            end
        end,
        Rows
    ).

trace_whitelist(U, M) when is_atom(M) -> trace_whitelist(U, [M]);
trace_whitelist(U, {M, F} = W) when is_atom(M), is_atom(F) ->
    trace_whitelist(U, [W]);
trace_whitelist(U, {M, F, A} = W)
 when is_atom(M) andalso is_atom(F) andalso
      (is_integer(A) orelse A == '_') ->
    trace_whitelist(U, [W]);
trace_whitelist(U, W) when is_list(W) ->
    lists:foreach(
        fun(Wi) when is_atom(Wi) -> ok;
           ({M, F}) when is_atom(M), is_atom(F) -> ok;
           ({M, F, A}) when is_atom(M) andalso is_atom(F) andalso
                            (is_integer(A) orelse A == '_') -> ok;
           (Wi) -> ?SystemExceptionNoLogging({badarg, Wi})
        end, W),
    case catch ?LOOKUP_CONFIG(trace_local_wl,U) of
        Wl when is_list(Wl) ->
            Wlus = lists:usort(Wl),
            case lists:usort(Wlus ++ W) of
                Wlus -> Wlus; % already added
                NewWlus ->
                    ?GET_CONFIG(trace_local_wl, [U], NewWlus,
                        "list of MFAs safe to trace on local node")
            end;
        _ ->
            ?GET_CONFIG(trace_local_wl, [U], lists:usort(W),
                        "list of MFAs safe to trace on local node")
    end.

timestamp() ->
    erlang:system_time(microsecond).

timestamp_sec() ->
    erlang:system_time(second).

-ifdef(CONSOLE).

dbg:stop_clear().
dbg:tracer(process, {fun(T, _) -> io:format("~p~n", [T]) end, ok}).
dbg:tpl(erlang, now, '_', [{'_', [], [{return_trace}]}]).
dbg:p(all, [c]).


f().
dbg:stop_clear().
F = fun(T, _) ->
        io:format("~p -- ~p~n", [T, catch imem_tracer:tracer_dummy(T, ok)])
        %io:format("~p -- ~p~n", [T, node()])
    end.
dbg:tracer(hd(nodes()), process, {F, ok}).
dbg:tpl(erlang, now, '_', [{'_', [], [{return_trace}]}]).
dbg:p(all, [c]).

-endif.