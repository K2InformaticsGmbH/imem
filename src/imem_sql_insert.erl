-module(imem_sql_insert).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

exec(SKey, {insert, TableName, {_, Columns}, {_, Values}}=_ParseTree , _Stmt, _Schema, IsSec) ->
    Table = imem_sql:table_qname(TableName),
    % ?Log("insert ~p values ~p into ~p~n", [Columns, Values, Table]),
    % ?Log("parse tree~n~p~n", [_ParseTree]),
    ColMap = imem_sql:column_map([Table], Columns),
    Vs = [imem_sql:un_escape_sql(binary_to_list(V)) || V <- Values],
    %% create a change list  similar to:  [1,ins,{},"99", 11, 12, undefined],                                      
    ChangeList = [[1,ins,{}|Vs]],
    % ?Log("~p - generated change list~n~p~n", [?MODULE, ChangeList]),
    UpdatePlan = if_call_mfa(IsSec,update_prepare,[SKey, [Table], ColMap, ChangeList]),
    if_call_mfa(IsSec,update_tables,[SKey, UpdatePlan, optimistic]),
    ok.


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
    catch imem_meta:drop_table(key_test),
    catch imem_meta:drop_table(not_null),
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
        ?Log("----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        ?Log("schema ~p~n", [imem_meta:schema()]),
        ?Log("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),

        Sql1 = "create table def (col1 varchar2, col2 integer, col3 term);",
        ?Log("Sql1: ~p~n", [Sql1]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql1, 0, 'Imem', IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, def])),

        [Meta1] = if_call_mfa(IsSec, read, [SKey, ddTable, {'Imem',def}]),
        ?Log("Meta table def:~n~p~n", [Meta1]),

        ?assertEqual(ok, insert_range(SKey, 3, "def", 'Imem', IsSec)),

        Sql1a = "insert into def (col1) values ('a');",
        ?Log("Sql1a: ~p~n", [Sql1a]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql1a, 0, 'Imem', IsSec)),

        Sql2 = "insert into def (col1) values ('{B}');",
        ?Log("Sql2: ~p~n", [Sql2]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql2, 0, 'Imem', IsSec)),

        Sql2b = "insert into def (col1,col2) values ('[]', 6);",     %% ToDo: 5+1 should work as well 
        ?Log("Sql2b: ~p~n", [Sql2b]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql2b, 0, 'Imem', IsSec)),

        Sql3 = "insert into def (col1,col2) values ('C', 7);",  
        ?Log("Sql3: ~p~n", [Sql3]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql3, 0, 'Imem', IsSec)),

        Sql3a = "insert into def (col1,col2) values ('D''s', undefined);",  
        ?Log("Sql3a: ~p~n", [Sql3a]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql3a, 0, 'Imem', IsSec)),

        Sql3b = "insert into def (col1,col2) values ('E', 'undefined');",  %% should crash
        ?Log("Sql3b: ~p~n", [Sql3b]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql3b, 0, 'Imem', IsSec)),

        Sql4 = "insert into def (col1,col3) values ('F', \"[1,2,3]\");",  
        ?Log("Sql3b: ~p~n", [Sql4]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql4, 0, 'Imem', IsSec)),

        Sql4a = "insert into def (col1,col3) values ('G', '[1,2,3]');",  
        ?Log("Sql3b: ~p~n", [Sql4a]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql4a, 0, 'Imem', IsSec)),

        Sql4b = "insert into def (col1,col3) values ('H', undefined);",  
        ?Log("Sql3b: ~p~n", [Sql4b]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql4b, 0, 'Imem', IsSec)),

        Sql4c = "insert into def (col1,col3) values ('I', \"an_atom\");",  
        ?Log("Sql3c: ~p~n", [Sql4c]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql4c, 0, 'Imem', IsSec)),

        Sql4d = "insert into def (col1,col3) values ('J', \"'AnAtom'\");",  
        ?Log("Sql3d: ~p~n", [Sql4d]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql4d, 0, 'Imem', IsSec)),

        Sql4e = "insert into def (col1,col3) values ('K', \"undefined\");",  
        ?Log("Sql3e: ~p~n", [Sql4e]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql4e, 0, 'Imem', IsSec)),

        Sql4f = "insert into def (col1,col3) values ('L', 'undefined');",  
        ?Log("Sql3f: ~p~n", [Sql4f]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql4f, 0, 'Imem', IsSec)),

        Sql4g = "insert into def (col1,col3) values ('M', 'und''efined');",  
        ?Log("Sql3g: ~p~n", [Sql4g]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql4g, 0, 'Imem', IsSec)),

        Sql4h = "insert into def (col1,col3) values ('N', 'not quite undefined');",  
        ?Log("Sql3h: ~p~n", [Sql4h]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql4h, 0, 'Imem', IsSec)),

        {List1, true} = if_call_mfa(IsSec,select,[SKey, def, ?MatchAllRecords]),
        ?Log("table def 1~n~p~n", [lists:sort(List1)]),
        ?assert(lists:member({def,"1",1,undefined},List1)),
        ?assert(lists:member({def,"2",2,undefined},List1)),
        ?assert(lists:member({def,"3",3,undefined},List1)),
        ?assert(lists:member({def,"a",undefined,undefined},List1)),
        ?assert(lists:member({def,"{B}",undefined,undefined},List1)),
        ?assert(lists:member({def,"[]",6,undefined},List1)),
        ?assert(lists:member({def,"C",7,undefined},List1)),
        ?assert(lists:member({def,"E",undefined,undefined},List1)),
        ?assert(lists:member({def,"F",undefined,[1,2,3]},List1)),
        ?assert(lists:member({def,"G",undefined,"[1,2,3]"},List1)),
        ?assert(lists:member({def,"H",undefined,undefined},List1)),
        ?assert(lists:member({def,"I",undefined,an_atom},List1)),
        ?assert(lists:member({def,"J",undefined,'AnAtom'},List1)),
        ?assert(lists:member({def,"K",undefined,undefined},List1)),
        ?assert(lists:member({def,"L",undefined,undefined},List1)),
        ?assert(lists:member({def,"M",undefined,"und'efined"},List1)),
        ?assert(lists:member({def,"D's",undefined,undefined},List1)),
        ?assert(lists:member({def,"N",undefined,"not quite undefined"},List1)),

        Sql5 = "insert into def (col1) values ('C', 5);",
        ?Log("Sql5: ~p~n", [Sql5]),
        ?assertException(throw,{ClEr,{"Too many values",{1,["'C'","5"]}}}, imem_sql:exec(SKey, Sql5, 0, 'Imem', IsSec)),

        Sql6 = "insert into def (col1) values ('C');",
        ?Log("Sql6: ~p~n", [Sql6]),
        ?assertException(throw,{CoEx,{"Key violation",{1,{[{def,"C",7,undefined}],{def,"C",undefined,undefined}}}}}, imem_sql:exec(SKey, Sql6, 0, 'Imem', IsSec)),

        Sql6a = "insert into def (col1) values ('E');",
        ?Log("Sql6a: ~p~n", [Sql6a]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql6a, 0, 'Imem', IsSec)),     %% identity update accepted

        Sql7 = "insert into def (col2) values (8);",
        ?Log("Sql7: ~p~n", [Sql7]),
        ?assertException(throw,{ClEr,{"Missing key column",{1,[{3,8}]}}}, imem_sql:exec(SKey, Sql7, 0, 'Imem', IsSec)),

        {List2, true} = if_call_mfa(IsSec,select,[SKey, def, ?MatchAllRecords]),
        ?Log("table def 2~n~p~n", [lists:sort(List2)]),


        Sql8 = "create table not_null (col1 varchar2 not null, col2 integer not null);",
        ?Log("Sql8: ~p~n", [Sql8]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql8, 0, 'Imem', IsSec)),

        [Meta2] = if_call_mfa(IsSec, read, [SKey, ddTable, {'Imem',not_null}]),
        ?Log("Meta table not_null:~n~p~n", [Meta2]),

        Sql9 = "insert into not_null (col1, col2) values ('A',5);",
        ?Log("Sql9: ~p~n", [Sql9]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql9, 0, 'Imem', IsSec)),
        ?assertEqual(1,  if_call_mfa(IsSec, table_size, [SKey, not_null])),

        Sql10 = "insert into not_null (col2) values (5);",
        ?Log("Sql10: ~p~n", [Sql10]),
        ?assertException(throw, {ClEr,{"Missing key column",{1,[{3,5}]}}}, imem_sql:exec(SKey, Sql10, 0, 'Imem', IsSec)),

        Sql11 = "insert into not_null (col1) values ('B');",
        ?Log("Sql11: ~p~n", [Sql11]),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {1,{not_null,_}}}}, imem_sql:exec(SKey, Sql11, 0, 'Imem', IsSec)),

        Sql30 = "create table key_test (col1 '{atom,integer}', col2 '{string,binstr}');",
        ?Log("Sql30: ~p~n", [Sql30]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql30, 0, 'Imem', IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, key_test])),
        TableDef = if_call_mfa(IsSec, read, [SKey, key_test, {imem_meta:schema(),key_test}]),
        ?Log("TableDef: ~p~n", [TableDef]),

        Sql97 = "drop table key_test;",
        ?Log("Sql97: ~p~n", [Sql97]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql97 , 0, 'Imem', IsSec)),

        Sql98 = "drop table not_null;",
        ?Log("Sql98: ~p~n", [Sql98]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql98 , 0, 'Imem', IsSec)),

        Sql99 = "drop table def;",
        ?Log("Sql99: ~p~n", [Sql99]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql99, 0, 'Imem', IsSec))

    catch
        Class:Reason ->  ?Log("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 


insert_range(_SKey, 0, _TableName, _Schema, _IsSec) -> ok;
insert_range(SKey, N, TableName, Schema, IsSec) when is_integer(N), N > 0 ->
    imem_sql:exec(SKey, "insert into " ++ TableName ++ " values ('" ++ integer_to_list(N) ++ "', " ++ integer_to_list(N) ++ ", undefined);", 0, Schema, IsSec),
    insert_range(SKey, N-1, TableName, Schema, IsSec).
