-module(imem_server).
-behaviour(gen_server).

-record(state, {
        lsock = undefined
        , csock = undefined
        , buf = <<>>
        , native_if_mod
        , is_secure = false
    }).

-export([start_link/1
        , init/1
		, handle_call/3
		, handle_cast/2
		, handle_info/2
		, terminate/2
		, code_change/3
        , send_resp/2
		]).

start_link(Params) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

init([Sock, NativeIfMod, IsSec]) ->
    lager:info("~p tcp client ~p", [self(), Sock]),
    {ok, #state{csock=Sock, native_if_mod=NativeIfMod, is_secure=IsSec}};
init(Params) ->
    {_, Interface} = lists:keyfind(tcp_ip,1,Params),
    {_, ListenPort} = lists:keyfind(tcp_port,1,Params),
    {_, NativeIfMod} = lists:keyfind(if_mod,1,Params),
    {_, IsSec} = lists:keyfind(if_sec,1,Params),
    case inet:getaddr(Interface, inet) of
        {error, Reason} ->
            {stop, Reason};
        {ok, ListenIf} when is_integer(ListenPort) ->
            case gen_tcp:listen(ListenPort, [binary, {packet, 0}, {active, false}, {ip, ListenIf}]) of
                {ok, LSock} ->
                    lager:info("~p started imem_server ~p @ ~p", [self(), LSock, {ListenIf, ListenPort}]),
                    gen_server:cast(self(), accept),
                    {ok, #state{lsock=LSock, native_if_mod=NativeIfMod, is_secure=IsSec}};
                Reason ->
                    lager:error("~p imem_server not started ~p!", [self(), Reason]),
                    {ok, #state{}}
            end;
        _ ->
            {stop, disabled}
    end.

handle_call(_Request, _From, State) ->
    lager:debug("handle_call ~p", [_Request]),
    {reply, ok, State}.

handle_cast(accept, #state{lsock=LSock, native_if_mod=NativeIfMod, is_secure=IsSec}=State) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    lager:debug("accept conn ~p", [Sock]),
    {ok,Pid} = gen_server:start(?MODULE, [Sock, NativeIfMod, IsSec], []),
    ok = gen_tcp:controlling_process(Sock, Pid),
    gen_server:cast(Pid, activate),
    gen_server:cast(self(), accept),
    {noreply, State#state{csock=Sock}};
handle_cast(activate, #state{csock=Sock} = State) ->
    ok = inet:setopts(Sock, [{active, once}, binary, {packet, 0}, {nodelay, true}]),
    lager:debug("~p Socket activated ~p", [self(), Sock]),
    {noreply, State};
handle_cast(_Msg, State) ->
    lager:debug("handle_cast ~p", [_Msg]),
	{noreply, State}.

handle_info({tcp, Sock, Data}, #state{buf=Buf, native_if_mod=_Mod, is_secure=_IsSec}=State) ->
    ok = inet:setopts(Sock, [{active, once}]),
    NewBuf = <<Buf/binary, Data/binary>>,
    Res = (catch binary_to_term(NewBuf)),
    case Res  of
        {'EXIT', _} ->
            lager:debug("~p received ~p bytes buffering...", [self(), byte_size(NewBuf)]),
            {noreply, State#state{buf=NewBuf}};
        [Mod,Fun|Args] ->
            % replace penultimate pid wih socket (if present)
            lager:debug("call ~p:~p(~p)", [Mod,Fun,Args]),
            case Fun of
                fetch_recs_async ->
                    NewArgs = lists:sublist(Args, length(Args)-1) ++ [Sock],
                    catch apply(Mod,Fun,NewArgs);
                _ ->
                    ApplyRes = try apply(Mod,Fun,Args)
                    catch 
                        _Class:Reason -> {error, Reason}
                    end,
                    send_resp(ApplyRes, Sock)
            end,
            %send_resp(catch apply(Mod,Fun,NewArgs), Sock),
            {noreply, State#state{buf= <<>>}}
    end;
handle_info({tcp_closed, Sock}, State) ->
    lager:debug("handle_info closed ~p", [Sock]),
	{stop, sock_close, State};
handle_info(Info, State) ->
    lager:error("handle_info unknown ~p", [Info]),
	{noreply, State}.

terminate(_Reason, #state{csock=undefined}) -> ok;
terminate(_Reason, #state{csock=Sock}) ->
    lager:debug("~p closing tcp ~p", [self(), Sock]),
    gen_tcp:close(Sock).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

send_resp(Resp, Sock) ->
    RespBin = term_to_binary(Resp),
    gen_tcp:send(Sock, RespBin).
