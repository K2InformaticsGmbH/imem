-module(imem_sql_insert).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-export([ exec/5
        ]).

exec(SKey, {insert, TableName, {_, Columns}, {_, Values}, _Returning}=ParseTree , _Stmt, Opts, IsSec) ->
    % ?LogDebug("insert ~p values ~p into ~p~n", [Columns, Values, TableName]),
    % ?LogDebug("parse tree~n~p~n", [ParseTree]),
    Params = imem_sql:params_from_opts(Opts,ParseTree),
    % ?LogDebug("Params: ~p~n", [Params]),
    MetaFields = imem_sql:prune_fields(imem_meta:meta_field_list(),ParseTree),       
    FullMap0 = imem_sql_expr:column_map_tables([TableName],MetaFields,Params),
    % ?LogDebug("FullMap0:~n~p~n", [?FP(FullMap0,"23678")]),
    [Tbin] = [{TS,TN} || #bind{tind=Ti,cind=Ci,schema=TS,table=TN} <- FullMap0,Ti/=?MetaIdx,Ci==?FirstIdx],
    Table = imem_meta:qualified_table_name(Tbin),
    % ?LogDebug("Table: ~p~n", [Table]),
    RecName = if_call_mfa(IsSec,table_record_name,[SKey, Table]),
    % ?LogDebug("RecName: ~p~n", [RecName]),
    DefRecList = [RecName|[D || #bind{tind=Ti,default=D} <- FullMap0, Ti==?MainIdx]],
    DefRec = list_to_tuple(DefRecList),
    % ?LogDebug("DefRec: ~p~n", [DefRec]),
    ?LogDebug("DefRecList: ~p~n", [DefRecList]),
    ColMap0 = imem_sql_expr:column_map_columns(Columns, FullMap0),
    % ?LogDebug("ColMap0:~n~p~n", [?FP(ColMap0,"23678")]),
    CCount = length(ColMap0), 
    VCount = length(Values),
    if 
        VCount==CCount -> ok;
        VCount>CCount ->  ?ClientError({"Too many values", Values});
        VCount<CCount ->  ?ClientError({"Too few values", Values})
    end,
    ColBTrees0 = [{imem_sql_expr:expr(V, FullMap0, CMap), CMap} || {V,CMap} <- lists:zip(Values,ColMap0)],
    % ?LogDebug("ColBTrees0:~n~p~n", [ColBTrees0]),
    % TODO: Rest could be repeated for array bound Params (in transaction)
    MR = imem_sql:meta_rec(IsSec,SKey,MetaFields,Params,undefined),
    % ?LogDebug("Meta Rec: ~p~n", [MR]),
    ColBTrees1 = [{imem_sql_expr:bind_tree(T,{MR}),CMap} || {T,CMap} <- ColBTrees0],
    % ?LogDebug("ColBTrees1:~n~p~n", [ColBTrees1]),
    ColBTrees2 = [{ case imem_sql_funs:expr_fun(T) of
                        F when is_function(F) -> F({MR});
                        V -> V 
                    end,CMap} || {T,CMap} <- ColBTrees1],
    % ?LogDebug("ColBTrees2:~n~p~n", [ColBTrees2]),
    NewRec0 = merge_values(ColBTrees2, DefRec),
    % ?LogDebug("NewRec:~n~p~n", [NewRec0]),
    % NewRec1 = evaluate_funs(NewRec0),
    % ?LogDebug("NewRec1:~n~p~n", [NewRec1]),
    % NewRec2 = imem_meta:apply_arity1_defaults(DefRecList,NewRec1),
    [if_call_mfa(IsSec,insert,[SKey, Table, NewRec0])].

merge_values([], Rec) -> Rec;
merge_values([{V,#bind{cind=Ci,type=Type,len=Len,prec=Prec,default=Def}}|Values], Rec) ->
    case imem_datatype:type_check(V,Type,Len,Prec,Def) of
        ok ->       
            merge_values(Values, erlang:setelement(Ci, Rec, V));
        {error,Reason} ->    
            ?ClientError(Reason)
    end.

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) -> 
    catch imem_meta:drop_table(fun_test),
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
        ?Info("----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),
        Schema = imem_meta:schema(),
        ?Info("schema ~p~n", [Schema]),
        ?Info("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(Schema)),
        ?assertEqual(true, lists:member({Schema,node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),

        ?assertEqual(ok, imem_sql:exec(SKey, "
            create table fun_test (
                col0 integer default 12
              , col1 integer default fun(Rec) -> element(2,Rec)*element(2,Rec) end.
            );"
            , 0, [{schema,imem}], IsSec)),

        Sql0a = "insert into fun_test (col0) values (12)",  
        ?Info("Sql0a:~n~s~n", [Sql0a]),
        ?assertEqual([{fun_test,12,144}], imem_sql:exec(SKey, Sql0a, 0, [{schema,imem}], IsSec)),

        TT0aRows = lists:sort(if_call_mfa(IsSec,read,[SKey, fun_test])),
        ?Info("fun_test~n~p~n", [TT0aRows]),
        [{fun_test,Col0a,Col1a}] = TT0aRows,
        ?assertEqual(Col0a*Col0a, Col1a),

        Sql1 = "create table def (col1 string, col2 integer, col3 term);",
        ?Info("Sql1: ~p~n", [Sql1]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql1, 0, [{schema,imem}], IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, def])),

        [Meta1] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,def}]),
        ?Info("Meta table def:~n~p~n", [Meta1]),


        Sql8 = "insert into def (col1,col3) values (:J, :A);",  
        ?Info("Sql8: ~p~n", [Sql8]),
        Pstring8 = {<<":J">>,<<"string">>,<<"0">>,[<<"\"J\"">>]},
        Patom8   = {<<":A">>,<<"atom">>,<<"0">>,[<<"another_atom">>]},
        ?assertEqual([{def,"J",undefined,another_atom}], imem_sql:exec(SKey, Sql8, 0, [{params,[Pstring8,Patom8]}], IsSec)),
        ?assertEqual([{def,"J",undefined,'another_atom'}], imem_meta:read({Schema,def},"J")),


        % ?assertEqual(ok, insert_range(SKey, 3, "def", imem, IsSec)),

        Sql1a = "insert into def (col1) values ('a');",
        ?Info("Sql1a: ~p~n", [Sql1a]),
        ?assertException(throw,{ClEr,{"Wrong data type for value, expecting type or default",{<<"a">>,string,undefined}}}, imem_sql:exec(SKey, Sql1a, 0, [{schema,imem}], IsSec)),

        Sql2 = "insert into def (col1) values ('\"{B}\"');",
        ?Info("Sql2: ~p~n", [Sql2]),
        ?assertEqual([{def,"{B}",undefined,undefined}], imem_sql:exec(SKey, Sql2, 0, [{schema,imem}], IsSec)),

        Sql2b = "insert into def (col1,col2) values ('\"[]\"', 6);",  
        ?Info("Sql2b: ~p~n", [Sql2b]),
        ?assertEqual([{def,"[]",6,undefined}], imem_sql:exec(SKey, Sql2b, 0, [{schema,imem}], IsSec)),

        Sql2c = "drop table def;",
        ?Info("Sql2c: ~p~n", [Sql2c]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql2c, 0, [{schema,imem}], IsSec)),

        Sql2d = "create table def (col1 varchar2(10), col2 integer, col3 term);",
        ?Info("Sql2d: ~p~n", [Sql2d]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql2d, 0, [{schema,imem}], IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, def])),

        Sql3 = "insert into def (col1,col2) values ('C', 7+1);",  
        ?Info("Sql3: ~p~n", [Sql3]),
        ?assertEqual([{def,<<"C">>,8,undefined}], imem_sql:exec(SKey, Sql3, 0, [{schema,imem}], IsSec)),

        Sql3a = "insert into def (col1,col2) values ('D''s', 'undefined');",  
        ?Info("Sql3a: ~p~n", [Sql3a]),
        ?assertEqual([{def,<<"D's">>,undefined,undefined}], imem_sql:exec(SKey, Sql3a, 0, [{schema,imem}], IsSec)),

        Sql3b = "insert into def (col1,col2) values ('E', 'undefined');", 
        ?Info("Sql3b: ~p~n", [Sql3b]),
        ?assertEqual([{def,<<"E">>,undefined,undefined}], imem_sql:exec(SKey, Sql3b, 0, [{schema,imem}], IsSec)),

        Sql4 = "insert into def (col1,col3) values ('F', \"COL\");",  
        ?Info("Sql3b: ~p~n", [Sql4]),
        ?assertException(throw,{ClEr,{"Unknown field or table name",<<"\"COL\"">>}}, imem_sql:exec(SKey, Sql4, 0, [{schema,imem}], IsSec)),

        Sql4a = "insert into def (col1,col3) values ('G', '[1,2,3]');",  
        ?Info("Sql3b: ~p~n", [Sql4a]),
        ?assertEqual([{def,<<"G">>,undefined,[1,2,3]}], imem_sql:exec(SKey, Sql4a, 0, [{schema,imem}], IsSec)),

        Sql4b = "insert into def (col1,col3) values ('H', undefined);",  
        ?Info("Sql3b: ~p~n", [Sql4b]),
        ?assertException(throw,{ClEr,{"Unknown field or table name",<<"undefined">>}}, imem_sql:exec(SKey, Sql4b, 0, [{schema,imem}], IsSec)),

        Sql4c = "insert into def (col1,col3) values ('I', to_atom('an_atom'));",  
        ?Info("Sql3c: ~p~n", [Sql4c]),
        ?assertEqual([{def,<<"I">>,undefined,'an_atom'}], imem_sql:exec(SKey, Sql4c, 0, [{schema,imem}], IsSec)),

        Sql4d = "insert into def (col1,col3) values ('J', sqrt(2));",  
        ?Info("Sql3d: ~p~n", [Sql4d]),
        ?assertEqual([{def,<<"J">>,undefined,math:sqrt(2)}], imem_sql:exec(SKey, Sql4d, 0, [{schema,imem}], IsSec)),

        Sql4e = "insert into def (col1,col3) values ('K', '\"undefined\"');",  
        ?Info("Sql3e: ~p~n", [Sql4e]),
        ?assertEqual([{def,<<"K">>,undefined,"undefined"}], imem_sql:exec(SKey, Sql4e, 0, [{schema,imem}], IsSec)),

        Sql4f = "insert into def (col1,col3) values ('L', 1+(2*3));",  
        ?Info("Sql3f: ~p~n", [Sql4f]),
        ?assertEqual([{def,<<"L">>,undefined,7}], imem_sql:exec(SKey, Sql4f, 0, [{schema,imem}], IsSec)),

        Sql4g = "insert into def (col1,col3) values ('M''s', 'undefined');",  
        ?Info("Sql3g: ~p~n", [Sql4g]),
        ?assertEqual([{def,<<"M's">>,undefined,undefined}], imem_sql:exec(SKey, Sql4g, 0, [{schema,imem}], IsSec)),

        Sql4h = "insert into def (col1,col3) values ('N', 'not quite undefined');",  
        ?Info("Sql3h: ~p~n", [Sql4h]),
        ?assertEqual([{def,<<"N">>,undefined,<<"not quite undefined">>}], imem_sql:exec(SKey, Sql4h, 0, [{schema,imem}], IsSec)),

        Sql5 = "insert into def (col1) values ('C', 5);",
        ?Info("Sql5: ~p~n", [Sql5]),
        ?assertException(throw,{ClEr,{"Too many values",_}}, imem_sql:exec(SKey, Sql5, 0, [{schema,imem}], IsSec)),

        Sql5a = "insert into def (col1,col2,col3) values ('C', 5);",
        ?Info("Sql5a: ~p~n", [Sql5a]),
        ?assertException(throw,{ClEr,{"Too few values",_}}, imem_sql:exec(SKey, Sql5a, 0, [{schema,imem}], IsSec)),

        Sql6 = "insert into def (col1) values ('C');",
        ?Info("Sql6: ~p~n", [Sql6]),
        ?assertException(throw,{CoEx,{"Insert failed, key already exists",_}}, imem_sql:exec(SKey, Sql6, 0, [{schema,imem}], IsSec)),

        Sql6a = "insert into def (col1,col2) values ( 'O', sqrt(2)+1);",
        ?Info("Sql6a: ~p~n", [Sql6a]),
        ?assertException(throw,{ClEr,{"Wrong data type for value, expecting type or default",_}}, imem_sql:exec(SKey, Sql6a, 0, [{schema,imem}], IsSec)),

        {List2, true} = if_call_mfa(IsSec,select,[SKey, def, ?MatchAllRecords]),
        ?Info("table def 2~n~p~n", [lists:sort(List2)]),

        Sql20 = "create table not_null (col1 varchar2 not null, col2 integer not null);",
        ?Info("Sql20: ~p~n", [Sql20]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql20, 0, [{schema,imem}], IsSec)),

        [Meta2] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,not_null}]),
        ?Info("Meta table not_null:~n~p~n", [Meta2]),

        Sql21 = "insert into not_null (col1, col2) values ('A',5);",
        ?Info("Sql21: ~p~n", [Sql21]),
        ?assertEqual([{not_null,<<"A">>,5}], imem_sql:exec(SKey, Sql21, 0, [{schema,imem}], IsSec)),
        ?assertEqual(1,  if_call_mfa(IsSec, table_size, [SKey, not_null])),

        Sql22 = "insert into not_null (col2) values (5);",
        ?Info("Sql22: ~p~n", [Sql22]),
        ?assertException(throw, {ClEr,{"Not null constraint violation",_}}, imem_sql:exec(SKey, Sql22, 0, [{schema,imem}], IsSec)),

        Sql23 = "insert into not_null (col1) values ('B');",
        ?Info("Sql23: ~p~n", [Sql23]),
        ?assertException(throw, {ClEr,{"Not null constraint violation", _}}, imem_sql:exec(SKey, Sql23, 0, [{schema,imem}], IsSec)),


        Sql30 = "create table key_test (col1 '{atom,integer}', col2 '{string,binstr}');",
        ?Info("Sql30: ~p~n", [Sql30]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql30, 0, [{schema,imem}], IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, key_test])),
        TableDef = if_call_mfa(IsSec, read, [SKey, key_test, {imem_meta:schema(),key_test}]),
        ?Info("TableDef: ~p~n", [TableDef]),

        Sql96 = "drop table fun_test;",
        ?Info("Sql96: ~p~n", [Sql96]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql96 , 0, [{schema,imem}], IsSec)),

        Sql97 = "drop table key_test;",
        ?Info("Sql97: ~p~n", [Sql97]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql97 , 0, [{schema,imem}], IsSec)),

        Sql98 = "drop table not_null;",
        ?Info("Sql98: ~p~n", [Sql98]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql98 , 0, [{schema,imem}], IsSec)),

        Sql99 = "drop table def;",
        ?Info("Sql99: ~p~n", [Sql99]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql99, 0, [{schema,imem}], IsSec))

    catch
        Class:Reason ->  ?Info("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
