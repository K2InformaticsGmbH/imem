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

-export([ exec/5
        , read_block/4
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

exec(SeCo, Statement, BlockSize, Schema, IsSec) when is_list(Statement) ->
    Sql =
    case [lists:last(string:strip(Statement))] of
        ";" -> Statement;
        _ -> Statement ++ ";"
    end,
    case (catch sql_lex:string(Sql)) of
        {ok, Tokens, _} ->
            case (catch sql_parse:parse(Tokens)) of
                {ok, [ParseTree|_]} -> exec(SeCo, ParseTree, #statement{stmt_str=Statement
                                                                , stmt_parse=ParseTree
                                                                , block_size=BlockSize}, Schema, IsSec);
                {'EXIT', Error} -> {error, Error};
                Error -> {error, Error}
            end;
        {'EXIT', Error} -> {error, Error}
    end;

exec(SeCo, {create_table, TableName, Columns}, _Stmt, _Schema, IsSec) ->
    Tab = binary_to_atom(TableName),
    Cols = [binary_to_atom(X) || {X, _} <- Columns],
    io:format(user,"create ~p columns ~p~n", [Tab, Cols]),
    call_mfa(IsSec,create_table,[SeCo,Tab,Cols,[local]]);

exec(SeCo, {insert, TableName, {_, Columns}, {_, Values}}, _Stmt, _Schema, IsSec) ->
    Tab = binary_to_atom(TableName),
    io:format(user,"insert ~p ~p in ~p~n", [Columns, Values, Tab]),
    Vs = [binary_to_list(V) || V <- Values],
    call_mfa(IsSec,insert,[SeCo,Tab, Vs]);

exec(SeCo, {drop_table, {tables, TableNames}, _, _}, _Stmt, _Schema, IsSec) ->
    Tabs = [binary_to_atom(T) || T <- TableNames],
    io:format(user,"drop_table ~p~n", [Tabs]),
    [call_mfa(IsSec,drop_table,[SeCo,Tab]) || Tab <- Tabs],
    ok;

exec(SeCo, {select, Params}, Stmt, _Schema, IsSec) ->
    Columns = case lists:keyfind(fields, 1, Params) of
        false -> [];
        {_, Cols} -> Cols
    end,
    TableName = case lists:keyfind(from, 1, Params) of
        {_, Tabs} when length(Tabs) == 1 -> binary_to_atom(lists:nth(1, Tabs));
        _ -> undefined
    end,
    case TableName of
        undefined -> {error, "Only single valid names are supported"};
        _ ->
            Clms = case Columns of
                [<<"*">>] -> call_mfa(IsSec,table_columns,[SeCo,TableName]);
                _ -> Columns
            end,
            Statement = Stmt#statement {
                table = TableName
                , cols = Clms
            },
            {ok, StmtRef} = create_stmt(Statement),
            io:format(user,"select params ~p in ~p~n", [{Columns, Clms}, TableName]),
            {ok, Clms, StmtRef}
    end.

read_block(SeCo, Pid, Sock, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {read_block, Sock, SeCo, IsSec}).

binary_to_atom(Bin) when is_binary(Bin) -> list_to_atom(binary_to_list(Bin)).

call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% gen_server
create_stmt(Statement) ->
    gen_server:start(?MODULE, [Statement], []).

init([Statement]) ->
    {ok, #state{statement=Statement}}.

handle_call(_Msg, _From, State) ->
    {reply,ok,State}.

handle_cast({read_block, Sock, SeCo, IsSec}, #state{statement=Stmt}=State) ->
    #statement{table=TableName,key=Key,block_size=BlockSize} = Stmt,
    case TableName of
    all_tables ->
        Rows = call_mfa(IsSec,select,[SeCo,TableName,[{{ddTable,'$1','_','_','_','_'},[],['$1']}]]),
        gen_tcp:send(Sock, term_to_binary({ok, Rows})),
        {noreply,State};
    TableName ->
        {NewKey, Rows} = call_mfa(IsSec,read_block,[SeCo,TableName, Key, BlockSize]),
        gen_tcp:send(Sock, term_to_binary({ok, Rows})),
        {noreply,State#state{statement=Stmt#statement{key=NewKey}}}
    end;
%handle_cast({read_block, Sock, SeCo, IsSec}, #state{statement=Stmt}=State) ->
%    #statement{table=TableName,key=Key,block_size=BlockSize} = Stmt,
%    {NewKey, Rows} = call_mfa(IsSec,read_block,[SeCo,TableName, Key, BlockSize]),
%    gen_tcp:send(Sock, term_to_binary({ok, Rows})),
%    {noreply,State#state{statement=Stmt#statement{key=NewKey}}};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.


% EUnit tests --

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    application:load(imem),
    {ok, Schema} = application:get_env(imem, mnesia_schema_name),
    {ok, Cwd} = file:get_cwd(),
    NewSchema = Cwd ++ "/../" ++ Schema,
    application:set_env(imem, mnesia_schema_name, NewSchema),
    application:set_env(imem, mnesia_node_type, disc),
    application:start(imem).

teardown(_) -> 
    catch imem_meta:drop_table(def),
    application:stop(imem).

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun test_with_sec/1
            , fun test_without_sec/1
        ]}
    }.
    
test_with_sec(_) ->
    SeCo = imem_seco:authenticate(adminSessionId, <<"admin">>, imem_seco:create_credentials(<<"change_on_install">>)),
    IsSec = false,
    io:format(user, "-------- create,insert,select (with security) --------~n", []),
    ?assertEqual(true, is_integer(SeCo)),
    ?assertEqual(SeCo, imem_seco:login(SeCo)),
    ?assertEqual(ok, exec(SeCo, "create table def (col1 int, col2 char);", 0, "Imem", IsSec)),
    ?assertEqual(ok, insert_range(SeCo, 10, "def", "Imem", IsSec)),
    {ok, _Clm, _StmtRef} = exec(SeCo, "select * from def;", 100, "Imem", IsSec),
    ?assertEqual(ok, exec(SeCo, "drop table def;", 0, "Imem", IsSec)).

test_without_sec(_) ->
    SeCo = {},
    IsSec = false,
    io:format(user, "-------- create,insert,select (without security) --------~n", []),
    ?assertEqual(ok, exec(SeCo, "create table def (col1 int, col2 char);", 0, "Imem", IsSec)),
    ?assertEqual(ok, insert_range(SeCo, 10, "def", "Imem", IsSec)),
    {ok, _Clm, StmtRef} = exec(SeCo, "select * from def;", 100, "Imem", IsSec),
    io:format(user, "select ~p~n", [StmtRef]),
    ?assertEqual(ok, exec(SeCo, "drop table def;", 0, "Imem", IsSec)).

insert_range(_SeCo, 0, _TableName, _Schema, _IsSec) -> ok;
insert_range(SeCo, N, TableName, Schema, IsSec) when is_integer(N), N > 0 ->
    exec(SeCo, "insert into " ++ TableName ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');", 0, Schema, IsSec),
    insert_range(SeCo, N-1, TableName, Schema, IsSec).
