-module(imem_tracer).
-include("imem.hrl").

-export([start/0, tp/2, tp/3, tp/4, tpl/2, tpl/3, tpl/4, p/1, p/2]).

-define(KILL_WAIT, 1000).
-define(MAX_MSG_PER_SEC, 100).
-define(MAX_TRACER_PROC_MQ_LEN, 10000).

-safe([tp/2, tp/3, tp/4, tpl/2, tpl/3, tpl/4, p/1, p/2]).

-ifdef(CONSOLE).

    f().
    Match_spec = [{'_', [], [{message,{process_dump}}, {exception_trace}]}].
    Match_spec = [{'$1', [], ['$1']}].
    Trace_spec = {erlang, now, 0}.
    Trace_spec = {lists, last, 1}.
    imem_tracer:start().
    imem_tracer:tpl(Trace_spec, Match_spec).
    imem_tracer:p(all, [c]).

-endif.

-record(tracer_cb_st, {count = 0, last_s = 0}).

start() ->
    catch dbg:stop_clear(),
    Pid = spawn_link(fun tracer_proc/0),
    catch exit(whereis(?MODULE), kill),
    catch erlang:unregister(?MODULE),
    true = erlang:register(?MODULE, Pid),
    case dbg:tracer(process, {fun tracer/2, #tracer_cb_st{}}) of
        {ok, TracerPid} -> ?MODULE ! {tracer_pid, TracerPid};
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

-record(state, {filters = #{}}).
tracer_proc() -> tracer_proc(#state{}).
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
    ?MODULE ! {dbg_apply, self(), Fun, Args},
    receive Msg -> Msg after 1000 -> exit(whereis(?MODULE), kill)
    end.

process_trace({trace, From, call, {M,F,Args}, Extra}, #state{filters = _F} = State) ->
    io:format("~p call ~p:~p/~p ~s~n", [From, M, F, length(Args),
              if is_binary(Extra) -> io_lib:format("extra binary ~p bytes", [byte_size(Extra)]);
                is_list(Extra) ->    io_lib:format("extra list ~p", [length(Extra)]);
                true ->              io_lib:format("extra unkown ~p", [Extra])
              end]),
    State;
process_trace({trace,To,return_from,{M,F,A},Ret}, #state{filters = _F} = State) ->
    io:format("~p:~p/~p returned to ~p : ~p~n", [M, F, A, To, Ret]),
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
