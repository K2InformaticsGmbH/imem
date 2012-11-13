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
%%            gen_server:cast(self(), {stop, Reason}),
%%            {ok, #state{}};
        {ok, ListenIf} when is_integer(ListenPort) ->
            case gen_tcp:listen(ListenPort, [binary, {packet, 0}, {active, false}, {ip, ListenIf}]) of
                {ok, LSock} ->
                    io:format(user, "~p started imem_server ~p @ ~p~n", [self(), LSock, {ListenIf, ListenPort}]),
                    gen_server:cast(self(), accept),
                    {ok, #state{lsock=LSock, native_if_mod=NativeIfMod, is_secure=IsSec}};
                Reason ->
                    io:format(user, "~p imem_server not started ~p!~n", [self(), Reason]),
                    {ok, #state{}}
%%                    gen_server:cast(self(), {stop, Reason}),
%%                    {ok, #state{}}
            end;
        _ ->
            {stop, disabled}
%%            gen_server:cast(self(), {stop, disabled}),
%%            {ok, #state{}}
    end.

handle_call(_Request, _From, State) ->
    io:format(user, "handle_call ~p~n", [_Request]),
    {reply, ok, State}.

% handle_cast({stop, Reason}, State) ->
%     io:format(user, "~p imem_server not started : ~p~n", [self(), Reason]),
%     {stop,{shutdown,Reason},State};
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

handle_info({tcp, Sock, Data}, #state{buf=Buf, native_if_mod=Mod}=State) ->
    ok = inet:setopts(Sock, [{active, once}]),
    NewBuf = <<Buf/binary, Data/binary>>,
    case (catch binary_to_term(NewBuf, [safe])) of
        {'EXIT', _} ->
            io:format(user, "~p received ~p bytes buffering...~n", [self(), byte_size(Data)]),
            {noreply, State#state{buf=NewBuf}};
        D ->
            process_cmd(D, Sock, Mod),
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

process_cmd(Cmd, Sock, Module) when is_tuple(Cmd), is_atom(Module) ->
    Fun = element(1, Cmd),
    Args = lists:nthtail(1, tuple_to_list(Cmd)),
    Resp = exec_fun_in_module(Module, Fun, Args, Sock),
    if Fun =/= read_block -> send_resp(Resp, Sock); true -> ok end.

exec_fun_in_module(Module, read_block, Args, Sock) when Module =/= imem_statement ->
    exec_fun_in_module(imem_statement, read_block, Args ++ [Sock], Sock);
exec_fun_in_module(Module, Fun, Args, Sock) ->
    ArgsLen = length(Args),
    case code:ensure_loaded(Module) of
        {_,Module} ->
            case lists:keyfind(Fun, 1, Module:module_info(exports)) of
                {_, Arity} when ArgsLen >= Arity ->
                    if ArgsLen > Arity -> apply(Module, Fun, lists:nthtail(1, Args));
                        true ->           apply(Module, Fun, Args)
                    end;
                false ->
                    case Module of
                        imem_statement -> {error, atom_to_list(Module)++":"++atom_to_list(Fun)++" doesn't exists or exported"};
                        _ -> exec_fun_in_module(imem_statement, Fun, Args, Sock)
                    end;
                {_, Arity} -> {error, atom_to_list(Module)++":"++atom_to_list(Fun)++" wrong number of arguments", ArgsLen, Arity}
            end;
        _ -> {error, "Module "++ atom_to_list(Module) ++" doesn't exists"}
    end.

%% process_cmd({find_imem_nodes, Schema},                          Sock) -> send_resp(imem_if:find_imem_nodes(Schema), Sock);
%% process_cmd({all_tables},                                       Sock) -> send_resp(imem_if:all_tables(), Sock);
%% process_cmd({columns, TableName},                               Sock) -> send_resp(imem_if:columns(TableName), Sock);
%% process_cmd({read, TableName, Key},                             Sock) -> send_resp(imem_if:read(TableName, Key), Sock);
%% process_cmd({write, TableName, Row},                            Sock) -> send_resp(imem_if:write(TableName, Row), Sock);
%% process_cmd({delete, TableName, Key},                           Sock) -> send_resp(imem_if:delete(TableName, Key), Sock);
%% process_cmd({add_attribute, A, Opts},                           Sock) -> send_resp(imem_if:add_attribute(A, Opts), Sock);
%% process_cmd({delete_table, TableName},                          Sock) -> send_resp(imem_if:drop_table(TableName), Sock);
%% process_cmd({update_opts, Tuple, Opts},                         Sock) -> send_resp(imem_if:update_opts(Tuple, Opts), Sock);
%% process_cmd({read_all_rows, TableName},                         Sock) -> send_resp(imem_if:read_all(TableName), Sock);
%% process_cmd({create_table, TableName, Columns},                 Sock) -> send_resp(imem_if:create_table(TableName, Columns), Sock);
%% process_cmd({create_table, TableName, Columns, Opts},           Sock) -> send_resp(imem_if:create_table(TableName, Columns, Opts), Sock);
%% process_cmd({create_local_table, TableName, Columns, Opts},     Sock) -> send_resp(imem_if:create_local_table(TableName, Columns, Opts), Sock);
%% process_cmd({select, TableName, MatchSpec},                     Sock) -> send_resp(imem_if:select(TableName, MatchSpec), Sock);
%% process_cmd({insert, TableName, Row},                           Sock) -> send_resp(imem_if:insert(TableName, Row), Sock);
%% process_cmd({exec, Statement, BlockSize, Schema},               Sock) -> send_resp(imem_statement:exec(Statement, BlockSize, Schema), Sock);
%% process_cmd({read_block, StmtRef},                              Sock) -> imem_statement:read_block(StmtRef, Sock).

send_resp(Resp, Sock) ->
    RespBin = term_to_binary(Resp),
    gen_tcp:send(Sock, RespBin).

%% EXAMPLE1: create a table and add data to it
% rd(table1, {a,b,c}).
% Opts = imem_if:add_ram_copies(imem_if:find_imem_nodes(imem), []).
% Opts1 = imem_if:add_attribute(record_info(fields, table1), Opts).
% imem_if:create_table(table1, Opts1).
% mnesia:dirty_write(table1, #table1{a='change_count', b=0}).
% mnesia:dirty_write(table1, {table1, 'change_county', 3, undefined}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%    rr("D:/Work/Git/imem/include/imem_records.hrl").
%    Opts = imem_if:add_ram_copies(imem_if:find_imem_nodes(imem), []).
%    %imem_if:add_disc_copies(Ns, Opts).
%    Opts1 = imem_if:add_attribute(record_info(fields, sub_counter), Opts).
%    imem_if:create_table(sub_counter, Opts1).
%    imem_if:create_table(subscriber, Opts1).
%    imem_if:create_table(syncinfo, Opts1).
%	case mnesia:create_table(syncinfo, [{ram_copies, NodeList}, {attributes, record_info(fields, syncinfo)}]) of
%		{aborted, _} ->
%			io:format("copying 'syncinfo' table...~n", []),
%			mnesia:wait_for_tables([syncinfo], 30000),
%			mnesia:add_table_copy(syncinfo, node(), ram_copies);
%		_ ->
%			io:format("table syncinfo created...~n", []),
%			mnesia:clear_table(syncinfo),
%			mnesia:dirty_write(syncinfo, #syncinfo{key='change_count', val=0}),
%			mnesia:dirty_write(syncinfo, #syncinfo{key='sync_time', val=get_datetime_stamp()}),
%			mnesia:dirty_write(syncinfo, #syncinfo{key='update_time', val=get_datetime_stamp()}),
%			mnesia:dirty_write(syncinfo, #syncinfo{key='record_count', val=0})
%	end,
%	mnesia:wait_for_tables([subscriber, syncinfo], Timeout),
