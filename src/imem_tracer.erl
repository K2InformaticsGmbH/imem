-module(imem_tracer).
-include("imem_tracer.hrl").

% debug interface wrapper
-export([tp/2, tp/3, tp/4, tpl/2, tpl/3, tpl/4, p/1, p/2]).

% imem interfaces
-export([subscribe/1, tracer_proc/1, trigger/5]).

-define(KILL_WAIT, 1000).
-define(MAX_MSG_PER_SEC, 1000).
-define(MAX_TRACER_PROC_MQ_LEN, 2000).

-safe([tp/2, tp/3, tp/4, tpl/2, tpl/3, tpl/4, p/1, p/2, trigger/5]).

-ifdef(CONSOLE).

    f().
    Match_spec = [{'_', [], [{message,{process_dump}}, {exception_trace}]}].
    Match_spec = [{'_', [], ['_']}].
    Trace_spec = {erlang, now, 0}.
    Trace_spec = {lists, last, 1}.
    imem_tracer:subscribe({table, ddTrace, undefined}).
    imem_tracer:tpl(Trace_spec, Match_spec).
    imem_tracer:p(all, [c]). % should be a pre-subscribe step

-endif.

-record(tracer_cb_st, {count = 0, last_s = 0}).

subscribe({table, ddTrace, _}) ->
    catch dbg:stop_clear(),
    Pid = spawn_link(?MODULE, tracer_proc, [self()]),
    catch exit(whereis(?MODULE), kill),
    catch erlang:unregister(?MODULE),
    true = erlang:register(?MODULE, Pid),
    case dbg:tracer(process, {fun tracer/2, #tracer_cb_st{}}) of
        {ok, TracerPid} ->
            ?MODULE ! {tracer_pid, TracerPid},
            ok;
        Error -> Error
    end.

tp(M, MS)       -> dbg_apply(tp, [M,       MS]).
tp(M, F, MS)    -> dbg_apply(tp, [M, F,    MS]).
tp(M, F, A, MS) -> dbg_apply(tp, [M, F, A, MS]).

tpl(M, MS)       -> dbg_apply(tp, [M,       MS]).
tpl(M, F, MS)    -> dbg_apply(tp, [M, F,    MS]).
tpl(M, F, A, MS) -> dbg_apply(tp, [M, F, A, MS]).

p(I)    -> dbg_apply(p, [I]).
p(I, F) -> dbg_apply(p, [I, F]).

-record(state, {filters = #{}, reply}).
tracer_proc(ReplyPid) when is_pid(ReplyPid) ->
    tracer_proc(#state{reply = ReplyPid});
tracer_proc(#state{} = State) ->
    NewState =
        receive
            {tracer_pid, TracerPid} ->
                io:format("~p:~p:~p -- TracerPid ~p~n",
                    [?MODULE, ?FUNCTION_NAME, ?LINE, TracerPid]),
                true = erlang:link(TracerPid),
                State;
            {dbg_apply, From, Fun, Args} ->
                From ! apply(dbg, Fun, Args),
                State;
            {?MODULE, Other} ->
                process_trace(Other, State);
            Other ->
                io:format("~p:~p:~p got unexpected : ~p~n", [?MODULE, ?FUNCTION_NAME, ?LINE, Other]),
                State
        end,
    tracer_proc(NewState).

tracer(Trace, #tracer_cb_st{last_s = LastSec, count = Count} = St) ->
    {_,{_,_,Sec}} = calendar:now_to_datetime(erlang:timestamp()),
    NewCount =
        if LastSec < Sec -> 0;
            true -> Count + 1
        end,
    if NewCount > ?MAX_MSG_PER_SEC -> St; % dropped
        true ->
            send_to_tracer_proc(Trace, St#tracer_cb_st{count = NewCount, last_s = Sec})
    end.

dbg_apply(_, [?MODULE | _]) -> error({badarg, "Can't trace itself"});
dbg_apply(Fun, Args) ->
    case catch ?MODULE ! {dbg_apply, self(), Fun, Args} of
        {'EXIT', _} -> ?ClientErrorNoLogging("No trace running");
        _ ->
            receive Msg -> Msg
            after 1000 ->
                exit(whereis(?MODULE), kill),
                ?ClientErrorNoLogging("Trace apply timeout")
            end
    end.

process_trace({trace, From, call, {M,F,Args}, Extra},
              #state{filters = _F, reply = ReplyPid} = State) ->
    io:format("~p call ~p:~p/~p ~s~n", [From, M, F, length(Args),
              if is_binary(Extra) -> io_lib:format("extra binary ~p bytes", [byte_size(Extra)]);
                is_list(Extra) ->    io_lib:format("extra list ~p", [length(Extra)]);
                true ->              io_lib:format("extra unkown ~p", [Extra])
              end]),
    Row = #ddTrace{process = From, event_type = call, mod = M, func = F, args = Args,
                   extra = Extra},
    ReplyPid ! {mnesia_table_event,{write,ddTrace,Row,[],undefined}},
    State;
process_trace({trace,To,return_from,{M,F,Arity},Ret},
              #state{filters = _F, reply = ReplyPid} = State) ->
    io:format("~p:~p/~p returned to ~p : ~p~n", [M, F, Arity, To, Ret]),
    Row = #ddTrace{process = To, event_type = return_from, mod = M, func = F,
                   args = Arity, extra = Ret},
    ReplyPid ! {mnesia_table_event,{write,ddTrace,Row,[],undefined}},
    State.

send_to_tracer_proc(Trace, State) ->
    {messages, Messages} = process_info(whereis(?MODULE), messages),
    if length(Messages) < ?MAX_TRACER_PROC_MQ_LEN ->
        case catch ?MODULE ! {?MODULE, Trace} of
            {'EXIT', _} -> dbg:stop_clear();
            _ -> State
        end;
        true -> State
    end.

trigger(OldRec, NewRec, ddTrace, _User, _TrOpts) ->
    case {OldRec,NewRec} of
        {{},{}} ->
            ?Info("truncate trigger"),
            ok;          
        {{},NewRec} ->
            ?Info("insert trigger : ~p", [NewRec]),
            ?ClientErrorNoLogging("insert not supported");
        {OldRec,{}} ->
            ?Info("drop trigger : ~p", [OldRec]),
            ?ClientErrorNoLogging("delete not supported");
        {OldRec,NewRec} ->
            ?Info("update trigger : old ~p, new ~p", [OldRec, NewRec]),
            ?ClientErrorNoLogging("update not supported")
    end.