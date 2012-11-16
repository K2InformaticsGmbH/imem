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
		]).

start_link(Params) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

init([Sock, NativeIfMod, IsSec]) ->
    io:format(user, "~p tcp client ~p~n", [self(), Sock]),
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
                    io:format(user, "~p started imem_server ~p @ ~p~n", [self(), LSock, {ListenIf, ListenPort}]),
                    gen_server:cast(self(), accept),
                    {ok, #state{lsock=LSock, native_if_mod=NativeIfMod, is_secure=IsSec}};
                Reason ->
                    io:format(user, "~p imem_server not started ~p!~n", [self(), Reason]),
                    {ok, #state{}}
            end;
        _ ->
            {stop, disabled}
    end.

handle_call(_Request, _From, State) ->
    io:format(user, "handle_call ~p~n", [_Request]),
    {reply, ok, State}.

handle_cast(accept, #state{lsock=LSock, native_if_mod=NativeIfMod, is_secure=IsSec}=State) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    io:format(user, "accept conn ~p~n", [Sock]),
    {ok,Pid} = gen_server:start(?MODULE, [Sock, NativeIfMod, IsSec], []),
    ok = gen_tcp:controlling_process(Sock, Pid),
    gen_server:cast(Pid, activate),
    gen_server:cast(self(), accept),
    {noreply, State#state{csock=Sock}};
handle_cast(activate, #state{csock=Sock} = State) ->
    ok = inet:setopts(Sock, [{active, once}, binary, {packet, 0}, {nodelay, true}]),
    io:format(user, "~p Socket activated ~p~n", [self(), Sock]),
    {noreply, State};
handle_cast(_Msg, State) ->
    io:format(user, "handle_cast ~p~n", [_Msg]),
	{noreply, State}.

handle_info({tcp, Sock, Data}, #state{buf=Buf, native_if_mod=Mod, is_secure=IsSec}=State) ->
    ok = inet:setopts(Sock, [{active, once}]),
    NewBuf = <<Buf/binary, Data/binary>>,
    case (catch binary_to_term(NewBuf)) of
        {'EXIT', _} ->
            io:format(user, "~p received ~p bytes buffering...~n", [self(), byte_size(NewBuf)]),
            {noreply, State#state{buf=NewBuf}};
        D ->
            process_cmd(D, Sock, Mod, IsSec),
            {noreply, State#state{buf= <<>>}}
    end;
handle_info({tcp_closed, Sock}, State) ->
    io:format(user, "handle_info closed ~p~n", [Sock]),
	{stop, sock_close, State};
handle_info(_Info, State) ->
    io:format(user, "handle_info ~p~n", [_Info]),
	{noreply, State}.

terminate(_Reason, #state{csock=undefined}) -> ok;
terminate(_Reason, #state{csock=Sock}) ->
    io:format(user, "~p closing tcp ~p~n", [self(), Sock]),
    gen_tcp:close(Sock).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

process_cmd(Cmd, Sock, Module, IsSec) when is_tuple(Cmd), is_atom(Module) ->
    Fun = element(1, Cmd),
    Args = lists:nthtail(1, tuple_to_list(Cmd)),
    Resp = exec_fun_in_module(Module, Fun, Args, Sock, IsSec),
    if Fun =/= read_block -> send_resp(Resp, Sock); true -> ok end.

exec_fun_in_module(_Module, Fun, Args, Sock, IsSec) when Fun =:= read_block ->
    apply(imem_statement, Fun, Args ++ [Sock, IsSec]);
exec_fun_in_module(_Module, Fun, Args, _Sock, IsSec) when Fun =:= exec ->
    apply(imem_statement, Fun, Args ++ [IsSec]);
exec_fun_in_module(Module, Fun, Args, _Sock, _IsSec) ->
    ArgsLen = length(Args),
    case code:ensure_loaded(Module) of
        {_,Module} ->
            case lists:keyfind(Fun, 1, Module:module_info(exports)) of
                {_, Arity} when ArgsLen >= Arity ->
                    if ArgsLen > Arity -> apply(Module, Fun, lists:nthtail(1, Args));
                        true ->           apply(Module, Fun, Args)
                    end;
                false -> {error, atom_to_list(Module)++":"++atom_to_list(Fun)++" doesn't exists or exported"};
                {_, Arity} -> {error, atom_to_list(Module)++":"++atom_to_list(Fun)++" wrong number of arguments", ArgsLen, Arity}
            end;
        _ -> {error, "Module "++ atom_to_list(Module) ++" doesn't exists"}
    end.

send_resp(Resp, Sock) ->
    RespBin = term_to_binary(Resp),
    gen_tcp:send(Sock, RespBin).
