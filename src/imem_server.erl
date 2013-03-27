-module(imem_server).
-behaviour(ranch_protocol).

-include("imem.hrl").

-export([ start_link/4
        , start_link/1
        , init/4
        , send_resp/2
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
                    case Term of
                        [Mod,Fun|Args] ->
                            % replace penultimate pid wih socket (if present)
                            NewArgs = case Fun of
                                fetch_recs_async -> lists:sublist(Args, length(Args)-1) ++ [{Transport, Socket}];
                                _ -> Args
                            end,
                            ApplyRes = try
                                           apply(Mod,Fun,NewArgs)
                                       catch 
                                           _Class:Reason ->
                                               {error, Reason}
                                       end,
                            %?Log("MFA -> R -- ~p:~p(~p) -> ~p~n", [Mod,Fun,NewArgs,ApplyRes]),
                            %?Log("MF -> R -- ~p:~p -> ~p~n", [Mod,Fun,ApplyRes]),
                            send_resp(ApplyRes, {Transport, Socket})
                    end,
                    TSize = byte_size(term_to_binary(Term)),
                    RestSize = byte_size(NewBuf)-TSize,
                    loop(Socket, Transport, binary_part(NewBuf, {TSize, RestSize}))
            end;
        close ->
            Transport:close(Socket)
    end.

send_resp(Resp, {Transport, Socket}) ->
    %?Log("Async Tx ~p~n", [Resp]),
    RespBin = term_to_binary(Resp),
    Transport:send(Socket, RespBin);
send_resp(Resp, Pid) when is_pid(Pid) ->
    Pid ! Resp.
