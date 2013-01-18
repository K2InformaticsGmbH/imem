-module(imem_server).
-behaviour(gen_server).

-record(state, {
        lsock = undefined
        , csock = undefined
        , buf = <<>>
        , native_if_mod
    }).

-export([start_srv/2
        , stop_srv/0
		]).

-export([start_link/1
        , init/1
		, handle_call/3
		, handle_cast/2
		, handle_info/2
		, terminate/2
		, code_change/3
        , send_resp/2
		]).

start_srv(HostIf, Port) when is_integer(Port) ->
    {ok, IfIpAddr} = inet:getaddr(HostIf, inet),
    gen_server:call(?MODULE, {start_listen, IfIpAddr, Port}).

stop_srv() -> gen_server:call(?MODULE, {stop_listen}).

start_link(Params) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

init([Sock, true]) ->
    io:format("~p tcp client ~p~n", [self(), Sock]),
    {ok, #state{csock=Sock}};
init(Params) ->
    {_, Interface} = lists:keyfind(tcp_ip,1,Params),
    {_, ListenPort} = lists:keyfind(tcp_port,1,Params),
    case inet:getaddr(Interface, inet) of
        {error, Reason} ->
            {stop, Reason};
        {ok, ListenIf} when is_integer(ListenPort) ->
            case gen_tcp:listen(ListenPort, [binary, {packet, 0}, {active, false}, {ip, ListenIf}]) of
                {ok, LSock} ->
                    io:format("~p started imem_server ~p @ ~p~n", [self(), LSock, {ListenIf, ListenPort}]),
                    gen_server:cast(self(), accept),
                    {ok, #state{lsock=LSock}};
                Reason ->
                    io:format("~p [ERROR] imem_server not started ~p~n", [self(), Reason]),
                    {ok, #state{}}
            end;
        _ ->
            {stop, disabled}
    end.

handle_call({stop_listen}, _From, #state{lsock=LSock} = State) ->
    catch gen_tcp:close(LSock),
    io:format("~p imem tcp service stopped!~n", [self()]),
    {reply, ok, State};
handle_call({start_listen, ListenIf, ListenPort}, _From, #state{lsock=LSock} = State) ->
    catch gen_tcp:close(LSock),
    case gen_tcp:listen(ListenPort, [binary, {packet, 0}, {active, false}, {ip, ListenIf}]) of
        {ok, LSock} ->
            io:format("~p started imem_server ~p @ ~p~n", [self(), LSock, {ListenIf, ListenPort}]),
            gen_server:cast(self(), accept),
            {reply, ok, #state{lsock=LSock}};
        Reason ->
            io:format("~p [ERROR] imem_server not started ~p~n", [self(), Reason]),
            {reply, Reason, State}
    end;
handle_call(_Request, _From, State) ->
    % io:format("handle_call ~p~n", [_Request]),
    {reply, ok, State}.

handle_cast(accept, #state{lsock=LSock}=State) ->
io:format("handle_cast(accept conn ~p~n", [LSock]),
    {ok, Sock} = gen_tcp:accept(LSock),
    {ok,Pid} = gen_server:start(?MODULE, [Sock, true], []),
    ok = gen_tcp:controlling_process(Sock, Pid),
    gen_server:cast(Pid, activate),
    gen_server:cast(self(), accept),
    {noreply, State#state{csock=Sock}};
handle_cast(activate, #state{csock=Sock} = State) ->
    ok = inet:setopts(Sock, [{active, once}, binary, {packet, 0}, {nodelay, true}]),
    % io:format("~p Socket activated ~p~n", [self(), Sock]),
    {noreply, State};
handle_cast(_Msg, State) ->
    % io:format("handle_cast ~p~n", [_Msg]),
	{noreply, State}.

handle_info({tcp, Sock, Data}, #state{buf=Buf}=State) ->
    ok = inet:setopts(Sock, [{active, once}]),
    NewBuf = <<Buf/binary, Data/binary>>,
    Res = (catch binary_to_term(NewBuf)),
    case Res  of
        {'EXIT', _} ->
            %io:format(user, "~p received ~p bytes buffering...~n", [self(), byte_size(NewBuf)]),
            {noreply, State#state{buf=NewBuf}};
        [Mod,Fun|Args] ->
            % replace penultimate pid wih socket (if present)
            %io:format(user, "__SERVER__ call ~p:~p(~p)~n", [Mod,Fun,Args]),
            %io:format(user, "__SERVER__ call ~p:~p~n", [Mod,Fun]),
            case Fun of
                fetch_recs_async ->
                    NewArgs = lists:sublist(Args, length(Args)-1) ++ [Sock],
                    catch apply(Mod,Fun,NewArgs);
                _ ->
                    ApplyRes = try apply(Mod,Fun,Args)
                    catch 
                        _Class:Reason ->
                            {error, Reason}
                    end,
                    send_resp(ApplyRes, Sock)
            end,
            {noreply, State#state{buf= <<>>}}
    end;
handle_info({tcp_closed, _Sock}, State) ->
    % io:format("handle_info closed ~p~n", [_Sock]),
	{stop, sock_close, State};
handle_info(Info, State) ->
    io:format("handle_info unknown ~p~n", [Info]),
	{noreply, State}.

terminate(_Reason, #state{csock=undefined}) -> ok;
terminate(_Reason, #state{csock=Sock}) ->
    % io:format("~p closing tcp ~p~n", [self(), Sock]),
    gen_tcp:close(Sock).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

send_resp(Resp, SockOrPid) ->
%    io:format(user, "__SERVER__ resp ~p~n", [Resp]),
    RespBin = term_to_binary(Resp),
    case SockOrPid of
        Pid when is_pid(Pid)    -> Pid ! Resp;
        Sock                    -> gen_tcp:send(Sock, RespBin)
    end.
