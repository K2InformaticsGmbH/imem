-module(imem_statement).

%% gen_server
-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([ exec/3
        , read_block/2
        ]).

-record(state, {
    statement
}).

-record(statement, {
        table = undefined
        , block_size = 0
        , key = '$start_of_table'
        , stmt_str = ""
        , stmt_parse = undefined
        , cols = []
    }).

exec(Statement, BlockSize, Schema) when is_list(Statement) ->
    Sql =
    case [lists:last(string:strip(Statement))] of
        ";" -> Statement;
        _ -> Statement ++ ";"
    end,
    case (catch sql_lex:string(Sql)) of
        {ok, Tokens, _} ->
            case (catch sql_parse:parse(Tokens)) of
                {ok, [ParseTree|_]} -> exec(ParseTree, #statement{stmt_str=Statement
                                                                , stmt_parse=ParseTree
                                                                , block_size=BlockSize}, Schema);
                {'EXIT', Error} -> {error, Error};
                Error -> {error, Error}
            end;
        {'EXIT', Error} -> {error, Error}
    end;

exec({create_table, TableName, Columns}, _Stmt, _Schema) ->
    Tab = binary_to_atom(TableName),
    Cols = [binary_to_atom(X) || {X, _} <- Columns],
    io:format(user,"create ~p columns ~p~n", [Tab, Cols]),
    imem_if:create_table(Tab,Cols,[local]);

exec({insert, TableName, {_, Columns}, {_, Values}}, _Stmt, _Schema) ->
    Tab = binary_to_atom(TableName),
    io:format(user,"insert ~p ~p in ~p~n", [Columns, Values, Tab]),
    Vs = [binary_to_list(V) || V <- Values],
    imem_if:insert(Tab, Vs);

exec({drop_table, {tables, TableNames}, _, _}, _Stmt, _Schema) ->
    Tabs = [binary_to_atom(T) || T <- TableNames],
    io:format(user,"drop_table ~p~n", [Tabs]),
    [imem_if:drop_table(Tab) || Tab <- Tabs],
    ok;

exec({select, Params}, Stmt, _Schema) ->
    Columns = case lists:keyfind(fields, 1, Params) of
        false -> [];
        {_, Cols} -> Cols
    end,
    TableName = case lists:keyfind(from, 1, Params) of
        {_, Tabs} when length(Tabs) == 1 -> binary_to_atom(lists:nth(1, Tabs));
        _ -> undefined
    end,
    io:format(user,"select params ~p in ~p~n", [Columns, TableName]),
    case TableName of
        undefined -> {error, "Only single valid names are supported"};
        _ ->
            Clms = case Columns of
                [<<"*">>] -> imem_if:table_columns(TableName);
                _ -> Columns
            end,
            Statement = Stmt#statement {
                table = TableName
                , cols = Clms
            },
            {ok, StmtRef} = create_stmt(Statement),
            {ok, Clms, StmtRef}
    end.

read_block(Pid, Sock) when is_pid(Pid) ->
    gen_server:cast(Pid, {read_block, gen_server:call(Pid, get_stmt), Sock}).

binary_to_atom(Bin) when is_binary(Bin) -> list_to_atom(binary_to_list(Bin)).

%% gen_server
create_stmt(Statement) ->
    gen_server:start(?MODULE, [Statement], []).

init([Statement]) ->
    {ok, #state{statement=Statement}}.

handle_call(get_stmt, _From, #state{statement=Statement}=State) ->
    {reply,Statement,State};
handle_call(_Msg, _From, State) ->
    {reply,ok,State}.

handle_cast({read_block, #statement{table=TableName,key=Key,block_size=BlockSize} = Stmt, Sock}, State) ->
    {NewKey, Rows} = imem_if:read_block(TableName, Key, BlockSize),
    gen_tcp:send(Sock, term_to_binary({ok, Rows})),
    {noreply,State#state{statement=Stmt#statement{key=NewKey}}};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
