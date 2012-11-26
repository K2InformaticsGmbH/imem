-module(imem_sql_insert).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

exec(SeCo, {insert, Table, {_, []}, {_, Values}} , _Stmt, _Schema, IsSec) ->
    Tab = ?binary_to_existing_atom(Table),
    io:format(user,"insert ~p ~p into ~p~n", [[], Values, Tab]),
    Vs = [binary_to_list(V) || V <- Values],    %% ToDo: convert to column type
    if_call_mfa(IsSec,insert,[SeCo, Tab, Vs]);  
exec(SeCo, {insert, Table, {_, Columns}, {_, Values}} , _Stmt, _Schema, IsSec) ->
    Tab = ?binary_to_existing_atom(Table),
    io:format(user,"insert ~p ~p into ~p~n", [Columns, Values, Tab]),
    Vs = [binary_to_list(V) || V <- Values],    %% ToDo: convert to column type
    if_call_mfa(IsSec,insert,[SeCo, Tab, Vs]).  %% ToDo: set default column values

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% TESTS ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) -> 
    catch imem_meta:drop_table(def),
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun test_without_sec/1
            , fun test_with_sec/1
        ]}
    }.
    
test_without_sec(_) -> 
    test_with_or_without_sec(false).

test_with_sec(_) ->
    test_with_or_without_sec(true).

test_with_or_without_sec(IsSec) ->
    try
        % ClEr = 'ClientError',
        % SeEx = 'SecurityException',
        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),
        SKey=?imem_test_admin_login(),
        ?assertEqual(ok, imem_sql:exec(SKey, "create table def (col1 int, col2 char);", 0, "Imem", IsSec)),
        ?assertEqual(ok, insert_range(SKey, 10, "def", "Imem", IsSec)),
        {ok, _Clm, _StmtRef} = imem_sql:exec(SKey, "select * from def;", 100, "Imem", IsSec),
        Result0 = if_call_mfa(IsSec,select,[SKey,ddTable,?MatchAllKeys]),
        ?assertMatch({_,true}, Result0),
        io:format(user, "~n~p~n", [Result0]),
        Result1 = if_call_mfa(IsSec,select,[SKey,all_tables,?MatchAllKeys]),
        ?assertMatch({_,true}, Result1),
        io:format(user, "~n~p~n", [Result1]),
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec))
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 


insert_range(_SKey, 0, _TableName, _Schema, _IsSec) -> ok;
insert_range(SKey, N, TableName, Schema, IsSec) when is_integer(N), N > 0 ->
    imem_sql:exec(SKey, "insert into " ++ TableName ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');", 0, Schema, IsSec),
    insert_range(SKey, N-1, TableName, Schema, IsSec).
