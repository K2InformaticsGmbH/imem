-module(imem_server).

-behaviour(ranch_protocol).

-include("imem.hrl").
-include("imem_meta.hrl").

-export([start_link/4, start_link/1, start/0, stop/0, restart/0, init/4, send_resp/2, mfa/2]).

%% -- Certificate management APIs --

-export([get_cert_key/1]).

start_link(Params) ->
    Interface = proplists:get_value(tcp_ip, Params),
    ListenPort = proplists:get_value(tcp_port, Params),
    SSL = proplists:get_value(ssl, Params),
    Pwd = proplists:get_value(pwd, Params),
    {THandler, Opts} =
        if
            length(SSL) > 0 -> {ranch_ssl, SSL};
            true -> {ranch_tcp, []}
        end,
    case inet:getaddr(Interface, inet) of
        {error, Reason} ->
            ?Error("~p failed to start ~p~n", [?MODULE, Reason]),
            {error, Reason};
        {ok, ListenIf} when is_integer(ListenPort) ->
            NewOpts =
                if
                    THandler =:= ranch_ssl ->
                        case ?GET_CONFIG(imemSslOpts, [], '$no_ssl_conf', "SSL listen socket options") of
                            '$no_ssl_conf' ->
                                {ok, CertBin} =
                                    file:read_file(filename:join([Pwd, proplists:get_value(certfile, Opts)])),
                                {ok, KeyBin} = file:read_file(filename:join([Pwd, proplists:get_value(keyfile, Opts)])),
                                Cert = get_cert_key(CertBin),
                                Key = get_cert_key(KeyBin),
                                ImemSslDefault = Cert ++ Key,
                                ?Debug("Installing SSL ~p", [ImemSslDefault]),
                                ?PUT_CONFIG(
                                    imemSslOpts,
                                    [],
                                    #{cert => CertBin, key => KeyBin},
                                    list_to_binary(
                                        io_lib:format(
                                            "Installed at ~p on ~s",
                                            [node(), imem_datatype:timestamp_to_io(?TIMESTAMP)]
                                        )
                                    )
                                ),
                                ImemSslDefault ++ proplists:delete(keyfile, proplists:delete(certfile, Opts));
                            #{cert := CertBin, key := KeyBin} ->
                                CertFile = filename:join([Pwd, proplists:get_value(certfile, Opts)]),
                                case file:read_file(CertFile) of
                                    {ok, CertBin} -> nop;
                                    _ -> ok = file:write_file(CertFile, CertBin)
                                end,
                                KeyFile = filename:join([Pwd, proplists:get_value(keyfile, Opts)]),
                                case file:read_file(KeyFile) of
                                    {ok, KeyBin} -> nop;
                                    _ -> ok = file:write_file(KeyFile, KeyBin)
                                end,
                                Cert = get_cert_key(CertBin),
                                Key = get_cert_key(KeyBin),
                                Cert ++ Key ++ proplists:delete(keyfile, proplists:delete(certfile, Opts))
                        end;
                    true -> Opts
                end,
            ?Info("~p starting...~n", [?MODULE]),
            case ranch:start_listener(
                ?MODULE,
                1,
                THandler,
                [{ip, ListenIf}, {port, ListenPort} | NewOpts],
                ?MODULE,
                if
                    THandler =:= ranch_ssl -> [ssl];
                    true -> []
                end
            ) of
                {ok, _} = Success ->
                    ?Info(
                        "~p started, listening~s on ~s:~p~n",
                        [
                            ?MODULE,
                            if
                                THandler =:= ranch_ssl -> "(ssl)";
                                true -> ""
                            end,
                            inet:ntoa(ListenIf),
                            ListenPort
                        ]
                    ),
                    ?Debug("options ~p~n", [NewOpts]),
                    Success;
                Error ->
                    ?Error("~p failed to start~n~p~n", [?MODULE, Error]),
                    Error
            end;
        _ -> {stop, disabled}
    end.

start_link(ListenerPid, Socket, Transport, Opts) ->
    Pid = spawn_opt(?MODULE, init, [ListenerPid, Socket, Transport, Opts], [link, {fullsweep_after, 0}]),
    {ok, Pid}.

start() ->
    {ok, TcpIf} = application:get_env(imem, tcp_ip),
    {ok, TcpPort} = application:get_env(imem, tcp_port),
    {ok, SSL} = application:get_env(imem, ssl),
    Pwd =
        case code:lib_dir(imem) of
            {error, _} -> ".";
            Path -> Path
        end,
    start_link([{tcp_ip, TcpIf}, {tcp_port, TcpPort}, {pwd, Pwd}, {ssl, SSL}]).

stop() -> ranch:stop_listener(?MODULE).

restart() ->
    stop(),
    start().

init(ListenerPid, Socket, Transport, Opts) ->
    PeerNameMod =
        case lists:member(ssl, Opts) of
            true -> ssl;
            _ -> inet
        end,
    {ok, {Address, Port}} = PeerNameMod:peername(Socket),
    Peer = list_to_binary(io_lib:format("~s:~p", [inet_parse:ntoa(Address), Port])),
    ?Debug("~p received connection from ~s~n", [self(), Peer]),
    ok = Transport:setopts(Socket, [{packet, 4}, {active, true}]),
    ok = ranch:accept_ack(ListenerPid),
    % Linkinking TCP socket
    % for easy lookup
    erlang:link(
        case lists:member(ssl, Opts) of
            true ->
                {sslsocket, {gen_tcp, TcpSocket, tls_connection, _}, _} = Socket,
                TcpSocket;
            _ -> Socket
        end
    ),
    loop(Socket, Peer, Transport).

-define(TLog(__F, __A), ok).

%-define(TLog(__F, __A), ?Info(__F, __A)).
loop(Socket, Peer, Transport) ->
    {OK, Closed, Error} = Transport:messages(),
    receive
        {OK, Socket, Data} ->
            case (catch binary_to_term(Data)) of
                {'EXIT', Exception} -> ?Error("[MALFORMED] ~p bytes from ~s: ~p", [byte_size(Data), Peer, Exception]);
                Term ->
                    if
                        element(2, Term) =:= imem_sec ->
                            ?TLog("mfa ~p", [Term]),
                            mfa(Term, {Transport, Socket, element(1, Term)});
                        true ->
                            send_resp({error, {"security breach attempt", Term}}, {Transport, Socket, element(1, Term)})
                    end,
                    loop(Socket, Peer, Transport)
            end;
        {Closed, Socket} -> ?Debug("socket from ~s got closed!~n", [Peer]);
        {Error, Socket, Reason} -> ?Error("socket from ~s error: ~p", [Peer, Reason]);
        close ->
            ?Warn("closing socket from ~s...~n", [Peer]),
            Transport:close(Socket)
    end.

mfa({Ref, Mod, which_applications, Args}, Transport) when Mod =:= imem_sec; Mod =:= imem_meta ->
    mfa({Ref, application, which_applications, Args}, Transport);
mfa({_Ref, imem_sec, echo, [_, Term]}, Transport) ->
    send_resp({server_echo, Term}, Transport),
    ok;
mfa({Ref, Mod, Fun, Args}, Transport) ->
    NewArgs = args(Ref, Fun, Args, Transport),
    ApplyRes =
        try
            ?TLog("~p MFA -> R ~n ~p:~p(~p)~n", [Transport, Mod, Fun, NewArgs]),
            apply(Mod, Fun, NewArgs)
        catch
            _Class:Reason -> {error, {Reason, erlang:get_stacktrace()}}
        end,
    ?TLog("~p MFA -> R ~n ~p:~p(~p) -> ~p~n", [Transport, Mod, Fun, NewArgs, ApplyRes]),
    ?TLog("~p MF -> R ~n ~p:~p -> ~p~n", [Transport, Mod, Fun, ApplyRes]),
    send_resp(ApplyRes, Transport),
    ok.

% 'ok' returned for erlimem compatibility
args(R, fetch_recs_async, A, {_, _, R} = T) ->
    Args = lists:sublist(A, length(A) - 1) ++ [T],
    ?TLog("fetch_recs_async, Args for TCP~n ~p~n", [Args]),
    Args;
args(R, fetch_recs_async, A, {_, R} = T) ->
    Args = lists:sublist(A, length(A) - 1) ++ [T],
    ?TLog("fetch_recs_async, Args for direct~n ~p~n", [Args]),
    Args;
args(R, request_metric, A, {_, _, R} = T) ->
    Args = A ++ [T],
    ?TLog("request_metric, Args for TCP~n ~p~n", [Args]),
    Args;
args(_, _F, A, _) ->
    ?TLog("~p(~p)~n", [_F, A]),
    A.

send_resp(Resp, {Transport, Socket, Ref}) ->
    RespBin = term_to_binary({Ref, Resp}),
    ?TLog("TX (~p)~n~p~n", [byte_size(RespBin), RespBin]),
    Transport:send(Socket, RespBin);
send_resp(Resp, {Pid, Ref}) when is_pid(Pid) -> Pid ! {Ref, Resp}.

%%
%% -- Certificate management interface --
%%

-spec get_cert_key(binary()) -> [{cert | cacerts | key, any()}].
get_cert_key(Bin) when is_binary(Bin) ->
    case public_key:pem_decode(Bin) of
        [{'Certificate', Cert, not_encrypted} | Certs] ->
            CACerts = [C || {'Certificate', C, not_encrypted} <- Certs],
            [
                {cert, Cert} | if
                    length(CACerts) > 0 -> [{cacerts, CACerts}];
                    true -> []
                end
            ];
        [{KeyType, Key, _} | _] -> [{key, {KeyType, Key}}]
    end.
