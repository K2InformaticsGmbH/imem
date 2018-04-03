-module(imem_tracer).

-include("imem.hrl").
-include("imem_tracer.hrl").

% imem interfaces
-export([init/0, subscribe/0, unsubscribe/0, trigger/5]).
-safe([trigger/5]).

-define(MAX_MSG_PER_SEC, 10000).
-define(MAX_TRACER_PROC_MQ_LEN, 5000).

-export([tracer_proc/1]).

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
        {{}, #ddTrace{enable = Enabled, match_spec = MS} = Trace} ->
            case Enabled == true andalso is_trace_running() of
                true ->
                    send_sync(
                        {add,
                         Trace#ddTrace{match_spec = ms_lookup(MS, User)}}
                    );
                false when Enabled == true -> ?Warn("trace not running");
                _ -> ok
            end;
        {#ddTrace{enable = Enabled} = Trace, {}} ->
            case Enabled == true andalso is_trace_running() of
                true -> send_sync({del, Trace});
                _ -> ok
            end;
        {#ddTrace{enable = true} = OldTrace,
         #ddTrace{enable = false}} ->
            case is_trace_running() of
                true -> send_sync({del, OldTrace});
                _ -> ok
            end;
        {#ddTrace{} = OldTrace,
         #ddTrace{enable = true, match_spec = MS} = NewTrace} ->
            case is_trace_running() of
                true ->
                    send_sync({del, OldTrace}),
                    send_sync(
                        {add,
                         NewTrace#ddTrace{match_spec = ms_lookup(MS, User)}}
                    );
                _ -> ?Warn("trace not running")
            end;
        _ -> ok
    end.

%------------------------------------------------------------------------------
% imem_if_mnesia interceptors
%------------------------------------------------------------------------------

-record(tracer_cb_st, {count = 0, last_s = 0}).

subscribe() ->
    catch dbg:stop_clear(),
    Pid = spawn_link(?MODULE, tracer_proc, [self()]),
    catch exit(whereis(?MODULE), kill),
    catch erlang:unregister(?MODULE),
    true = erlang:register(?MODULE, Pid),
    disable_tps(),
    case dbg:tracer(process, {fun tracer/2, #tracer_cb_st{}}) of
        {ok, TracerPid} ->
            ?MODULE ! {tracer_pid, TracerPid},
            ?Info("Statement ~p~n", [self()]),
            ok;
        Error -> Error
    end.

unsubscribe() ->
    catch dbg:stop_clear(),
    catch exit(whereis(?MODULE), kill),
    catch erlang:unregister(?MODULE).

%------------------------------------------------------------------------------
% erlang dbg tracer callback
%------------------------------------------------------------------------------

tracer(Trace, #tracer_cb_st{last_s = LastSec, count = Count} = St) ->
    {_,{_,_,Sec}} = calendar:now_to_datetime(os:timestamp()),
    NewCount =
        if LastSec < Sec -> 0;
            true -> Count + 1
        end,
    if NewCount > ?MAX_MSG_PER_SEC -> St; % dropped
        true ->
            send_trace_event(Trace, St#tracer_cb_st{count = NewCount, last_s = Sec})
    end.

%------------------------------------------------------------------------------
% imem_tracer singleton process
%------------------------------------------------------------------------------

-record(state, {filters = #{}, reply}).
tracer_proc(ReplyPid) when is_pid(ReplyPid) ->
    tracer_proc(#state{reply = ReplyPid});
tracer_proc(#state{} = State) ->
    NewState =
        receive
            {tracer_pid, TracerPid} ->
                ?Info("imem_tracer ~p, dbg:tracer ~p~n", [self(), TracerPid]),
                true = erlang:link(TracerPid),
                dbg:p(all, [c]), %% TODO REVISIT etc etc
                State;
            {trace_event, Other} ->
                process_trace(Other, State);
            {From, {add, #ddTrace{event_type = register, rate = Rate, mod = M,
                                  func = F, args = A, match_spec = MS}}} ->
                From ! apply(dbg, tpl, [M, F, A, MS]),
                ?Info("ADD trace pattern : Module ~p, Function ~p, Arity ~p"
                      " and Rate ~p~n", [M, F, A, Rate]),
                State#state{
                    filters =
                        (State#state.filters)#{
                            {M,F,A} => #{rate => Rate, stat => #{}}
                        }
                };
            {From, {del, #ddTrace{event_type = register, rate = Rate,
                                  mod = M, func = F, args = A}}} ->
                From ! apply(dbg, ctpl, [M, F, A]),
                ?Info("DEL trace pattern : Module ~p, Function ~p, Arity ~p"
                      " and Rate ~p~n", [M, F, A, Rate]),
                State#state{filters = maps:remove({M,F,A}, State#state.filters)};
            Other ->
                ?Error("got unexpected message : ~p", [Other]),
                exit({badarg, Other, State})
        end,
    tracer_proc(NewState).

%------------------------------------------------------------------------------
% internal functions
%------------------------------------------------------------------------------

process_trace({trace, From, Type, MFA}, State) ->
    process_trace({trace, From, Type, MFA, <<>>}, State);
process_trace({trace, FromTo, Type, {M, F, ArityArgs}, Extra}, State) ->
    case apply_filter(M, F, ArityArgs, State) of
        {ok, NewState} ->
            Row = #ddTrace{process = FromTo, event_type = Type, mod = M,
                           func = F, args = ArityArgs, extra = Extra},
            State#state.reply !
                {mnesia_table_event, {write, ddTrace, Row, [], undefined}},
            NewState;
        {_, NewState} -> NewState
    end.

send_trace_event(Trace, State) ->
    {messages, Messages} = process_info(whereis(?MODULE), messages),
    if length(Messages) < ?MAX_TRACER_PROC_MQ_LEN ->
        case catch ?MODULE ! {trace_event, Trace} of
            {'EXIT', _} -> dbg:stop_clear();
            _ -> State
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
            {'EXIT', _} -> dbg:stop_clear();
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
                                #ddTrace.func, #ddTrace.args]])
            )
    }.

ms_lookup([A|_] = MS, _) when is_atom(A); is_tuple(A) -> MS;
ms_lookup("default", User) ->
    ?GET_CONFIG({trace_ms, "default"}, User, [{'_', [], []}],
                "default smallest match specification for debug tracing");
ms_lookup(MS, User) -> ?LOOKUP_CONFIG(MS, User).

apply_filter(Mod, Fun, Args, State) when is_list(Args) ->
    apply_filter(Mod, Fun, length(Args), State);
apply_filter(M, F, A, #state{filters = Filters} = State) ->
    MFA = {M,   F,   A},
    MF_ = {M,   F, '_'},
    M__ = {M, '_', '_'},
    {Key, Rate, Stat} = case Filters of
                            #{MFA := #{rate := R, stat := S}} -> {MFA, R, S};
                            #{MF_ := #{rate := R, stat := S}} -> {MF_, R, S};
                            #{M__ := #{rate := R, stat := S}} -> {M__, R, S};
                            _ ->
                                ?SystemExceptionNoLogging(
                                    lists:flatten(
                                        io_lib:format(
                                            "No filter found for ~p:~p/~p in ~p",
                                            [M, F, A, Filters]
                                        )
                                    )
                                )
                        end,
    if Rate < 1 -> {ok, State};
        true ->
            Now = os:timestamp(),
            Last = maps:get(last, Stat, Now),
            case Last < Now andalso
                 timer:now_diff(Now, Last) / 1000000 >= 1.0 of
                % New Second, reset everything
                true ->
                    {ok,
                        State#state{
                            filters =
                                Filters#{Key =>
                                    #{rate => Rate,
                                      stat => #{last => Now, count => 1}}}
                        }
                    };
                % Within same second, count up
                false ->
                    NewCount = maps:get(count, Stat, 0) + 1,
                    {if NewCount < Rate -> ok;  % propagate
                        true            -> drop % DON'T propagate
                     end,
                     State#state{filters =
                        Filters#{Key =>
                                    #{rate => Rate,
                                    stat => Stat#{count => NewCount}}}}}
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

-ifdef(CONSOLE).

spawn(
    fun() ->
        (fun F(0) -> ok;
             F(C) when C > 0 ->
                _ = imem_meta:data_nodes(),
                F(C - 1)
         end)(1000000)
    end
).

-endif.