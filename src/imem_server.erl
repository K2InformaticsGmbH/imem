-module(imem_server).
-behaviour(ranch_protocol).

-include("imem.hrl").

-export([ start_link/4
        , start_link/1
        , init/4
        , send_resp/2
        , mfa/2
        ]).
 
start_link(Params) ->
    {_, Interface} = lists:keyfind(tcp_ip,1,Params),
    {_, ListenPort} = lists:keyfind(tcp_port,1,Params),
    case inet:getaddr(Interface, inet) of
        {error, Reason} ->
            ?Log("~p [ERROR] not started ~p~n", [self(), Reason]),
            {error, Reason};
        {ok, ListenIf} when is_integer(ListenPort) ->
            ?Log("~p listening on ~p:~p~n", [self(), ListenIf, ListenPort]),
            ranch:start_listener(?MODULE, 1, ranch_tcp, [{ip, ListenIf}, {port, ListenPort}], ?MODULE, []);
        _ ->
            {stop, disabled}
    end.

start_link(ListenerPid, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [ListenerPid, Socket, Transport, Opts]),
    {ok, Pid}.
 
init(ListenerPid, Socket, Transport, _Opts = []) ->
    {ok, {Address, Port}} = inet:peername(Socket),
    ?Log("~p received connection from ~p:~p~n", [self(), Address, Port]),
    ok = ranch:accept_ack(ListenerPid),
    loop(Socket, Transport, <<>>).
 
loop(Socket, Transport, Buf) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            NewBuf = <<Buf/binary, Data/binary>>,
            case (catch binary_to_term(NewBuf)) of
                {'EXIT', _} ->
                    ?Log(" ~p received ~p bytes buffering...~n", [self(), byte_size(NewBuf)]),
                    loop(Socket, Transport, NewBuf);
                Term ->
                    mfa(Term, {Transport, Socket, element(1, Term)}),
                    TSize = byte_size(term_to_binary(Term)),
                    RestSize = byte_size(NewBuf)-TSize,
                    loop(Socket, Transport, binary_part(NewBuf, {TSize, RestSize}))
            end;
        close ->
            ?Log("closing socket...~n", [Socket]),
            Transport:close(Socket)
    end.

mfa({Ref, Mod, Fun, Args}, Transport) ->
    NewArgs = args(Ref,Fun,Args,Transport),
    ApplyRes = try
                   apply(Mod,Fun,NewArgs)
               catch 
                   _Class:Reason ->
                       {error, Reason}
               end,
    %?Log("~p MFA -> R ~n ~p:~p(~p) -> ~p~n", [Transport,Mod,Fun,NewArgs,ApplyRes]),
    %?Log("~p MF -> R ~n ~p:~p -> ~p~n", [Transport,Mod,Fun,ApplyRes]),
    send_resp(ApplyRes, Transport),
    ok.

args(R, fetch_recs_async, A, {_,_,R} = T) ->
    Args = lists:sublist(A, length(A)-1) ++ [T],
    %?Log("fetch_recs_async, Args for TCP~n ~p~n", [Args]),
    Args;
args(R, fetch_recs_async, A, {_,R} = T) ->
    Args = lists:sublist(A, length(A)-1) ++ [T],
    %?Log("fetch_recs_async, Args for direct~n ~p~n", [Args]),
    Args;
args(_, _F, A, _) ->
    %?Log("~p(~p)~n", [_F, A]),
    A.

send_resp(Resp, {Transport, Socket, Ref}) ->
    RespBin = term_to_binary({Ref, Resp}),
    %?Log("TX (~p)~n~p~n", [byte_size(RespBin), RespBin]),
    %%PayloadSize = byte_size(RespBin),
    %%Transport:send(Socket, << 0,0,0,0,1,1,1,1, PayloadSize:32, RespBin/binary >>);
    Transport:send(Socket, RespBin);
send_resp(Resp, {Pid, Ref}) when is_pid(Pid) ->
    Pid ! {Ref, Resp};
send_resp(Resp, Pid) when is_pid(Pid) ->
    Pid ! Resp.
