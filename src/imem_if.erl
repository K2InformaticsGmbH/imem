-module(imem_if).
-behaviour(gen_server).

-compile(export_all).

-record(state, {
        lsock = undefined
        , csock = undefined
        , buf = <<>>
    }).

-export([start_link/0
        , init/1
		, handle_call/3
		, handle_cast/2
		, handle_info/2
		, terminate/2
		, code_change/3
		]).

add_ram_copies(Ns, Opts) -> update_opts({ram_copies,Ns}, Opts).
add_disc_copies(Ns, Opts) -> update_opts({disc_copies,Ns}, Opts).
add_attribute(A, Opts) -> update_opts({attributes,A}, Opts).

update_opts({K,_} = T, Opts) when is_atom(K) -> lists:keystore(K, 1, Opts, T).

build_table(TableName, Columns) when is_atom(TableName), is_list(Columns) ->
    Cols = [list_to_atom(lists:flatten(io_lib:format("~p", [X]))) || X <- Columns],
    Opts0 = add_disc_copies(find_imem_nodes(imem), []),
    Opts1 = add_attribute(Cols, Opts0),
    create_table(TableName, Opts1).

create_table(Table, Opts) when is_list(Table) ->
    create_table(list_to_atom(Table), Opts);    
create_table(Table, Opts) when is_atom(Table) ->
   	case mnesia:create_table(Table, Opts) of
		{aborted, _} ->
			io:format("copying '~p' table...~n", [Table]),
			mnesia:wait_for_tables([Table], 30000),
			mnesia:add_table_copy(Table, node(), ram_copies);
		_ ->
			io:format("table '~p' created...~n", [Table]),
			mnesia:clear_table(Table)
	end.

delete_table(Table) when is_atom(Table) ->
    mnesia:delete_table(Table).

insert_into_table(TableName, Row) when is_atom(TableName), is_tuple(Row) ->
    Row1 = case element(1, Row) of
        TableName ->
            [_|R] = tuple_to_list(Row),
            R;
        _ -> tuple_to_list(Row)
    end,
    insert_into_table(TableName, Row1);
insert_into_table(TableName, Row) when is_atom(TableName), is_list(Row) ->
    RowLen = length(Row),
    TableRowLen = length(mnesia:table_info(TableName, attributes)),
    if TableRowLen =:= RowLen ->
        mnesia:dirty_write(TableName, list_to_tuple([TableName|Row]));
        true -> {error, {"schema mismatch {table_row_len, insert_row_len} ", TableRowLen, RowLen}}
    end.

find_imem_nodes(App) ->
    [node() |
        lists:foldl(
            fun(N, Acc) ->
                    case lists:keyfind(App, 1, rpc:call(N, application, loaded_applications, [])) of
                        false -> Acc;
                        _ -> [N|Acc]
                    end
            end
            , []
            , nodes())].
find_imem_nodes() ->
    {ok, App} = application:get_application(),
    [node() |
        lists:foldl(
            fun(N, Acc) ->
                    case lists:keyfind(App, 1, rpc:call(N, application, loaded_applications, [])) of
                        false -> Acc;
                        _ -> [N|Acc]
                    end
            end
            , []
            , nodes())].


start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([Sock]) ->
    io:format(user, "~p tcp client ~p~n", [self(), Sock]),
    {ok, #state{csock=Sock}};
init([]) ->
    ListenIf = application:get_env(mgmt_if),
    {ok, ListenPort} = inet:getaddr(application:get_env(mgmt_port), inet),
    case gen_tcp:listen(ListenPort, [binary, {packet, 0}, {active, false}, {ip, ListenIf}]) of
        {ok, LSock} ->
            io:format(user, "~p started imem_if ~p~n", [self(), LSock]),
            gen_server:cast(self(), accept),
            {ok, #state{lsock=LSock}};
        Reason ->
            io:format(user, "~p imem_if not started : ~p~n", [self(), Reason]),
            {ok, #state{}}
    end.

handle_call(_Request, _From, State) ->
    io:format(user, "handle_call ~p~n", [_Request]),
    {reply, ok, State}.

handle_cast(accept, #state{lsock=LSock}=State) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    io:format(user, "accept conn ~p~n", [Sock]),
    {ok,Pid} = gen_server:start(?MODULE, [Sock], []),
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

handle_info({tcp, Sock, Data}, #state{buf=Buf}=State) ->
    ok = inet:setopts(Sock, [{active, once}]),
    NewBuf = <<Buf/binary, Data/binary>>,
    case (catch binary_to_term(NewBuf, [safe])) of
        {'EXIT', _} ->
            io:format(user, "~p received ~p bytes buffering...~n", [self(), byte_size(Data)]),
            {noreply, State#state{buf=NewBuf}};
        D ->
            io:format(user, "Cmd ~p~n", [D]),
            process_cmd(D, Sock),
            {noreply, State#state{buf= <<>>}}
    end;
handle_info({tcp_closed, Sock}, State) ->
    io:format(user, "handle_info closed ~p~n", [Sock]),
	{stop, sock_close, State};
handle_info(_Info, State) ->
    io:format(user, "handle_info ~p~n", [_Info]),
	{noreply, State}.

terminate(_Reason, #state{csock=Sock}) ->
    io:format(user, "~p terminating~n", [self()]),
    gen_tcp:close(Sock),
    shutdown.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

process_cmd({tables}, Sock) ->
    Tables = lists:delete(schema, mnesia:system_info(tables)),
    Tb = term_to_binary(Tables),
    io:format(user, "tables ~p size ~p~n", [Tables, byte_size(Tb)]),
    gen_tcp:send(Sock, Tb);
process_cmd({table, Tab}, Sock) ->
    Cols = mnesia:table_info(Tab, attributes),
    gen_tcp:send(Sock, term_to_binary(Cols));
process_cmd({row, Tab}, Sock) ->
    {_, Keys} = mnesia:transaction(fun() -> mnesia:all_keys(Tab) end),
    Data = [lists:nthtail(1, tuple_to_list(lists:nth(1, mnesia:dirty_read(Tab, X)))) || X <- Keys],
    gen_tcp:send(Sock, term_to_binary(Data));
process_cmd({build_table, TableName, Columns}, Sock) ->
    build_table(TableName, Columns),
    gen_tcp:send(Sock, term_to_binary(ok));
process_cmd({delete_table, TableName}, Sock) ->
    delete_table(TableName),
    gen_tcp:send(Sock, term_to_binary(ok));
process_cmd({insert_into_table, TableName, Row}, Sock) ->
    insert_into_table(TableName, Row),
    gen_tcp:send(Sock, term_to_binary(ok)).

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
