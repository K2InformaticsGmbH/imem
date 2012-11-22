-module(imem_statement).

-include("imem_seco.hrl").

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

-record(state, { statement
               , seco
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

exec(SKey, {create_user, Name, {identified_by, Password}, Opts}, _Stmt, _Schema, IsSec) ->
    call_mfa(IsSec, admin_exec, [SKey, imem_account, create, [SKey, user, Name, Name, Password]]),
    case lists:member({account,lock}, Opts) of 
        true -> call_mfa(IsSec, admin_exec, [SKey, imem_account, lock, [SKey, Name]]);
        false -> ok
    end,
    case lists:member({password,expire}, Opts) of 
        true ->  call_mfa(IsSec, admin_exec, [SKey, imem_account, expire, [SKey, Name]]);
        false -> call_mfa(IsSec, admin_exec, [SKey, imem_account, renew, [SKey, Name]])
    end;

exec(SKey, {alter_user, Name, {spec, Specs}}, _Stmt, _Schema, IsSec) ->
    case lists:member({account,unlock}, Specs) of 
        true -> call_mfa(IsSec, admin_exec, [SKey, imem_account, unlock, [SKey, Name]]);
        false -> ok
    end,
    case lists:member({account,lock}, Specs) of 
        true -> call_mfa(IsSec, admin_exec, [SKey, imem_account, lock, [SKey, Name]]);
        false -> ok
    end,
    case lists:member({password,expire}, Specs) of 
        true ->  call_mfa(IsSec, admin_exec, [SKey, imem_account, expire, [SKey, Name]]);
        false -> ok
    end,
    case lists:keyfind(identified_by, 1, Specs) of 
        {identified_by, NewPassword} ->  
            call_mfa(IsSec, admin_exec, [SKey, imem_seco, change_credentials, [SKey, {pwdmd5,NewPassword}]]);
        false -> ok
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
            {ok, StmtRef} = create_stmt(Statement, SeCo, IsSec),
            io:format(user,"select params ~p in ~p~n", [{Columns, Clms}, TableName]),
            {ok, Clms, StmtRef}
    end.

% statement has its own SeCo
read_block(_SeCo, Pid, Sock, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {read_block, Sock, IsSec}).

binary_to_atom(Bin) when is_binary(Bin) -> list_to_atom(binary_to_list(Bin)).

call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% gen_server
create_stmt(Statement, SeCo, IsSec) ->
    case IsSec of
        false -> gen_server:start(?MODULE, [Statement], []);
        _ ->
            {ok,Pid} = gen_server:start(?MODULE, [Statement], []),            
            NewSeCo = imem_sec:clone_seco(SeCo, Pid),
            ok = gen_server:call(Pid, {set_seco, NewSeCo}),
            {ok,Pid}
    end.

init([Statement]) ->
    {ok, #state{statement=Statement}}.

handle_call({set_seco, SeCo}, _From, State) ->    
    {reply,ok,State#state{seco=SeCo}}.

handle_cast({read_block, Sock, IsSec}, #state{statement=Stmt,seco=SeCo}=State) ->
    #statement{table=TableName,key=Key,block_size=BlockSize} = Stmt,
    {Result, NewState} =
    case TableName of
    all_tables ->
        {Rows, true} = call_mfa(IsSec,select,[SeCo,TableName,?MatchAllKeys]),
        {term_to_binary({ok, Rows}),State};
    TableName ->
        {NewKey, Rows} = call_mfa(IsSec,read_block,[SeCo,TableName, Key, BlockSize]),
        {term_to_binary({ok, Rows}),State#state{statement=Stmt#statement{key=NewKey}}}
    end,
    case Sock of
        Pid when is_pid(Pid)    -> Pid ! Result;
        Sock                    -> gen_tcp:send(Sock, Result)
    end,
    {noreply,NewState};
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


if_select_account_by_name(_SeCo, Name) -> 
    MatchHead = #ddAccount{name='$1', _='_'},
    Guard = {'==', '$1', Name},
    Result = '$_',
    imem_meta:select(ddAccount, [{MatchHead, [Guard], [Result]}]).    


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
    try
        ClEr = 'ClientError',
        % SeEx = 'SecurityException',

        {[#ddAccount{credentials=[AdminCred|_]}],true} = if_select_account_by_name(none, <<"admin">>),
        SeCo = imem_seco:authenticate(adminSessionId, <<"admin">>, AdminCred),
        IsSec = true,
        io:format(user, "-------- create,insert,select (with security) --------~n", []),
        ?assertEqual(true, is_integer(SeCo)),
        ?assertEqual(SeCo, imem_seco:login(SeCo)),
        ?assertEqual(ok, exec(SeCo, "create table def (col1 int, col2 char);", 0, "Imem", IsSec)),
        ?assertEqual(ok, insert_range(SeCo, 10, "def", "Imem", IsSec)),
        {ok, _Clm, _StmtRef} = exec(SeCo, "select * from def;", 100, "Imem", IsSec),
        Result0 = call_mfa(IsSec,select,[SeCo,ddTable,?MatchAllKeys]),
        ?assertMatch({_,true}, Result0),
        io:format(user, "~n~p~n", [Result0]),
        Result1 = call_mfa(IsSec,select,[SeCo,all_tables,?MatchAllKeys]),
        ?assertMatch({_,true}, Result1),
        io:format(user, "~n~p~n", [Result1]),
        ?assertEqual(ok, exec(SeCo, "drop table def;", 0, "Imem", IsSec)),
        ?assertEqual(ok, exec(SeCo, "CREATE USER test_user_1 IDENTIFIED BY a_password;", 0, "Imem", IsSec)),
        ?assertException(throw, {ClEr,{"Account already exists", test_user_1}}, exec(SeCo, "CREATE USER test_user_1 IDENTIFIED BY a_password;", 0, "Imem", IsSec))
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

test_without_sec(_) ->
    try
        SeCo = {},
        IsSec = false,
        io:format(user, "-------- create,insert,select (without security) --------~n", []),
        ?assertEqual(ok, exec(SeCo, "create table def (col1 int, col2 char);", 0, "Imem", IsSec)),
        ?assertEqual(ok, insert_range(SeCo, 10, "def", "Imem", IsSec)),
        {ok, _Clm, StmtRef} = exec(SeCo, "select * from def;", 100, "Imem", IsSec),
        io:format(user, "select ~p~n", [StmtRef]),
        ?assertEqual(ok, exec(SeCo, "drop table def;", 0, "Imem", IsSec))
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 


insert_range(_SeCo, 0, _TableName, _Schema, _IsSec) -> ok;
insert_range(SeCo, N, TableName, Schema, IsSec) when is_integer(N), N > 0 ->
    exec(SeCo, "insert into " ++ TableName ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');", 0, Schema, IsSec),
    insert_range(SeCo, N-1, TableName, Schema, IsSec).
