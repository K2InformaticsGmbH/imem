-module(imem_sql_insert).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

exec(SKey, {insert, TableName, {_, Columns}, {_, Values}} , _Stmt, _Schema, IsSec) ->
    Table = imem_sql:table_qname(TableName),
    % io:format(user,"insert ~p values ~p into ~p~n", [Columns, Values, Table]),
    ColMap = imem_sql:column_map([Table], Columns),
    Vs = [imem_sql:strip_quotes(binary_to_list(V)) || V <- Values],
    %% create a change list  similar to:  [1,ins,{},"99", 11, 12, undefined],                                      
    ChangeList = [[1,ins,{}|Vs]],
    % io:format(user, "~p - generated change list~n~p~n", [?MODULE, ChangeList]),
    UpdatePlan = if_call_mfa(IsSec,update_prepare,[SKey, [Table], ColMap, ChangeList]),
    if_call_mfa(IsSec,update_tables,[SKey, UpdatePlan, optimistic]).


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
        ClEr = 'ClientError',
        % SeEx = 'SecurityException',
        CoEx = 'ConcurrencyException',
        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        io:format(user, "schema ~p~n", [imem_meta:schema()]),
        io:format(user, "data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),
        ?assertEqual(ok, imem_sql:exec(SKey, "create table def (col1 varchar, col2 integer);", 0, "Imem", IsSec)),
        ?assertEqual(ok, insert_range(SKey, 3, "def", "Imem", IsSec)),
        {ok, _Clm, _RowFun, _StmtRef} = imem_sql:exec(SKey, "select * from def;", 100, "Imem", IsSec),         
        {List1, true} = if_call_mfa(IsSec,select,[SKey, def, ?MatchAllRecords]),
        io:format(user, "table def 1~n~p~n", [lists:sort(List1)]),
        ?assertEqual(3, length(List1)),
        ?assertEqual(ok, imem_sql:exec(SKey, "insert into def (col1) values ('A');", 0, "Imem", IsSec)),
        ?assertEqual(ok, imem_sql:exec(SKey, "insert into def (col1,col2) values ('B', 'undefined');", 0, "Imem", IsSec)),
        ?assertEqual(ok, imem_sql:exec(SKey, "insert into def (col1,col2) values ('C', 5);", 0, "Imem", IsSec)),
        {List2, true} = if_call_mfa(IsSec,select,[SKey, def, ?MatchAllRecords]),
        io:format(user, "table def 2~n~p~n", [lists:sort(List2)]),
        ?assertEqual(6, length(List2)),

        ?assertException(throw,{ClEr,{"Too many values",{1,["C","5"]}}}, imem_sql:exec(SKey, "insert into def (col1) values ('C', 5);", 0, "Imem", IsSec)),
        ?assertException(throw,{CoEx,{"Key violation",{1,{[{def,"C",5}],{def,"C",undefined}}}}}, imem_sql:exec(SKey, "insert into def (col1) values ('C');", 0, "Imem", IsSec)),
        ?assertException(throw,{ClEr,{"Missing key column",{1,[{3,8}]}}}, imem_sql:exec(SKey, "insert into def (col2) values (8);", 0, "Imem", IsSec)),

        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec))
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 


insert_range(_SKey, 0, _TableName, _Schema, _IsSec) -> ok;
insert_range(SKey, N, TableName, Schema, IsSec) when is_integer(N), N > 0 ->
    imem_sql:exec(SKey, "insert into " ++ TableName ++ " values ('" ++ integer_to_list(N) ++ "', " ++ integer_to_list(N) ++ ");", 0, Schema, IsSec),
    insert_range(SKey, N-1, TableName, Schema, IsSec).
