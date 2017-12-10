%%%-------------------------------------------------------------------
%%% File        : imem_sql_insert_ct.erl
%%% Description : Common testing imem_sql_insert.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_sql_insert_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_group/1,
    test_with_sec/1,
    test_without_sec/1
]).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include("imem_seco.hrl").

%%--------------------------------------------------------------------
%% Group related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_group(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/1 - Start ===>~n", []),

    catch imem_meta:drop_table(fun_test),
    catch imem_meta:drop_table(key_test),
    catch imem_meta:drop_table(not_null),
    catch imem_meta:drop_table(def),

    ok.

%%====================================================================
%% Test Cases.
%%====================================================================

test_with_or_without_sec(IsSec) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_or_without_sec/1 - Start(~p) ===>~n", [IsSec]),

    ClEr = 'ClientError',
    CoEx = 'ConcurrencyException',
    Schema = imem_meta:schema(),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [Schema]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(Schema)),
    ?assertEqual(true, lists:member({Schema, node()}, imem_meta:data_nodes())),

    SKey = ?imem_test_admin_login(),

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "
            create table fun_test (
                col0 integer default 12
              , col1 integer default fun(_,Rec) -> element(2,Rec)*element(2,Rec) end.
            );"
        , 0, [{schema, imem}], IsSec)),

    Sql0a = "insert into fun_test (col0) values (5)",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql0a:~n~s~n", [Sql0a]),
    ?assertEqual([{fun_test, 5, 25}], imem_sql:exec(SKey, Sql0a, 0, [{schema, imem}], IsSec)),

    TT0aRows = lists:sort(imem_sql_insert:if_call_mfa(IsSec, read, [SKey, fun_test])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":fun_test~n~p~n", [TT0aRows]),
    [{fun_test, Col0a, Col1a}] = TT0aRows,
    ?assertEqual(Col0a * Col0a, Col1a),

    Sql0b = "insert into fun_test",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql0b:~n~s~n", [Sql0b]),
    ?assertEqual([{fun_test, 12, 144}], imem_sql:exec(SKey, Sql0b, 0, [{schema, imem}], IsSec)),

    Sql1 = "create table def (col1 string, col2 integer, col3 term);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql1: ~p~n", [Sql1]),
    ?assertMatch({ok, _}, imem_sql:exec(SKey, Sql1, 0, [{schema, imem}], IsSec)),
    ?assertEqual(0, imem_sql_insert:if_call_mfa(IsSec, table_size, [SKey, def])),

    [_Meta1] = imem_sql_insert:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, def}]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Meta table def:~n~p~n", [_Meta1]),


    Sql8 = "insert into def (col1,col3) values (:J, :A);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql8: ~p~n", [Sql8]),
    Pstring8 = {<<":J">>, <<"string">>, <<"0">>, [<<"\"J\"">>]},
    Patom8 = {<<":A">>, <<"atom">>, <<"0">>, [<<"another_atom">>]},
    ?assertEqual([{def, "J", undefined, another_atom}], imem_sql:exec(SKey, Sql8, 0, [{params, [Pstring8, Patom8]}], IsSec)),
    ?assertEqual([{def, "J", undefined, 'another_atom'}], imem_meta:read({Schema, def}, "J")),


    % ?assertEqual(ok, insert_range(SKey, 3, "def", imem, IsSec)),

    Sql1a = "insert into def (col1) values ('a');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql1a: ~p~n", [Sql1a]),
    ?assertException(throw, {ClEr, {"Wrong data type for value, expecting type or default", {<<"a">>, string, []}}}, imem_sql:exec(SKey, Sql1a, 0, [{schema, imem}], IsSec)),

    Sql2 = "insert into def (col1) values ('\"{B}\"');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql2: ~p~n", [Sql2]),
    ?assertEqual([{def, "{B}", undefined, undefined}], imem_sql:exec(SKey, Sql2, 0, [{schema, imem}], IsSec)),

    Sql2b = "insert into def (col1,col2) values ('\"[]\"', 6);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql2b: ~p~n", [Sql2b]),
    ?assertEqual([{def, "[]", 6, undefined}], imem_sql:exec(SKey, Sql2b, 0, [{schema, imem}], IsSec)),

    Sql2c = "drop table def;",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql2c: ~p~n", [Sql2c]),
    ?assertEqual(ok, imem_sql:exec(SKey, Sql2c, 0, [{schema, imem}], IsSec)),

    Sql2d = "create table def (col1 varchar2(10), col2 integer, col3 term);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql2d: ~p~n", [Sql2d]),
    ?assertMatch({ok, _}, imem_sql:exec(SKey, Sql2d, 0, [{schema, imem}], IsSec)),
    ?assertEqual(0, imem_sql_insert:if_call_mfa(IsSec, table_size, [SKey, def])),

    Sql3 = "insert into def (col1,col2) values ('C', 7+1);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3: ~p~n", [Sql3]),
    ?assertEqual([{def, <<"C">>, 8, undefined}], imem_sql:exec(SKey, Sql3, 0, [{schema, imem}], IsSec)),

    Sql3a = "insert into def (col1,col2) values ('D''s', 'undefined');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3a: ~p~n", [Sql3a]),
    ?assertEqual([{def, <<"D's">>, undefined, undefined}], imem_sql:exec(SKey, Sql3a, 0, [{schema, imem}], IsSec)),

    Sql3b = "insert into def (col1,col2) values ('E', 'undefined');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3b: ~p~n", [Sql3b]),
    ?assertEqual([{def, <<"E">>, undefined, undefined}], imem_sql:exec(SKey, Sql3b, 0, [{schema, imem}], IsSec)),

    Sql4 = "insert into def (col1,col3) values ('F', \"COL\");",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3b: ~p~n", [Sql4]),
    ?assertException(throw, {ClEr, {"Unknown field or table name", <<"COL">>}}, imem_sql:exec(SKey, Sql4, 0, [{schema, imem}], IsSec)),

    Sql4a = "insert into def (col1,col3) values ('G', '[1,2,3]');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3b: ~p~n", [Sql4a]),
    ?assertEqual([{def, <<"G">>, undefined, [1, 2, 3]}], imem_sql:exec(SKey, Sql4a, 0, [{schema, imem}], IsSec)),

    Sql4b = "insert into def (col1,col3) values ('H', undefined);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3b: ~p~n", [Sql4b]),
    ?assertException(throw, {ClEr, {"Unknown field or table name", <<"undefined">>}}, imem_sql:exec(SKey, Sql4b, 0, [{schema, imem}], IsSec)),

    Sql4c = "insert into def (col1,col3) values ('I', to_atom('an_atom'));",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3c: ~p~n", [Sql4c]),
    ?assertEqual([{def, <<"I">>, undefined, 'an_atom'}], imem_sql:exec(SKey, Sql4c, 0, [{schema, imem}], IsSec)),

    Sql4d = "insert into def (col1,col3) values ('J', sqrt(2));",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3d: ~p~n", [Sql4d]),
    ?assertEqual([{def, <<"J">>, undefined, math:sqrt(2)}], imem_sql:exec(SKey, Sql4d, 0, [{schema, imem}], IsSec)),

    Sql4e = "insert into def (col1,col3) values ('K', '\"undefined\"');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3e: ~p~n", [Sql4e]),
    ?assertEqual([{def, <<"K">>, undefined, "undefined"}], imem_sql:exec(SKey, Sql4e, 0, [{schema, imem}], IsSec)),

    Sql4f = "insert into def (col1,col3) values ('L', 1+(2*3));",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3f: ~p~n", [Sql4f]),
    ?assertEqual([{def, <<"L">>, undefined, 7}], imem_sql:exec(SKey, Sql4f, 0, [{schema, imem}], IsSec)),

    Sql4g = "insert into def (col1,col3) values ('M''s', 'undefined');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3g: ~p~n", [Sql4g]),
    ?assertEqual([{def, <<"M's">>, undefined, undefined}], imem_sql:exec(SKey, Sql4g, 0, [{schema, imem}], IsSec)),

    Sql4h = "insert into def (col1,col3) values ('N', 'not quite undefined');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql3h: ~p~n", [Sql4h]),
    ?assertEqual([{def, <<"N">>, undefined, <<"not quite undefined">>}], imem_sql:exec(SKey, Sql4h, 0, [{schema, imem}], IsSec)),

    Sql5 = "insert into def (col1) values ('C', 5);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql5: ~p~n", [Sql5]),
    ?assertException(throw, {ClEr, {"Too many values", _}}, imem_sql:exec(SKey, Sql5, 0, [{schema, imem}], IsSec)),

    Sql5a = "insert into def (col1,col2,col3) values ('C', 5);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql5a: ~p~n", [Sql5a]),
    ?assertException(throw, {ClEr, {"Too few values", _}}, imem_sql:exec(SKey, Sql5a, 0, [{schema, imem}], IsSec)),

    Sql6 = "insert into def (col1) values ('C');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql6: ~p~n", [Sql6]),
    ?assertException(throw, {CoEx, {"Insert failed, key already exists in", _}}, imem_sql:exec(SKey, Sql6, 0, [{schema, imem}], IsSec)),

    Sql6a = "insert into def (col1,col2) values ( 'O', sqrt(2)+1);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql6a: ~p~n", [Sql6a]),
    ?assertException(throw, {ClEr, {"Wrong data type for value, expecting type or default", _}}, imem_sql:exec(SKey, Sql6a, 0, [{schema, imem}], IsSec)),

    {_List2, true} = imem_sql_insert:if_call_mfa(IsSec, select, [SKey, def, ?MatchAllRecords]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":table def 2~n~p~n", [lists:sort(_List2)]),

    Sql20 = "create table not_null (col1 varchar2 not null, col2 integer not null);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql20: ~p~n", [Sql20]),
    ?assertMatch({ok, _}, imem_sql:exec(SKey, Sql20, 0, [{schema, imem}], IsSec)),

    [_Meta2] = imem_sql_insert:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, not_null}]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Meta table not_null:~n~p~n", [_Meta2]),

    Sql21 = "insert into not_null (col1, col2) values ('A',5);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql21: ~p~n", [Sql21]),
    ?assertEqual([{not_null, <<"A">>, 5}], imem_sql:exec(SKey, Sql21, 0, [{schema, imem}], IsSec)),
    ?assertEqual(1, imem_sql_insert:if_call_mfa(IsSec, table_size, [SKey, not_null])),

    Sql22 = "insert into not_null (col2) values (5);",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql22: ~p~n", [Sql22]),
    ?assertException(throw, {ClEr, {"Not null constraint violation", _}}, imem_sql:exec(SKey, Sql22, 0, [{schema, imem}], IsSec)),

    Sql23 = "insert into not_null (col1) values ('B');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql23: ~p~n", [Sql23]),
    ?assertException(throw, {ClEr, {"Not null constraint violation", _}}, imem_sql:exec(SKey, Sql23, 0, [{schema, imem}], IsSec)),


    Sql30 = "create table key_test (col1 '{atom,integer}', col2 '{string,binstr}');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql30: ~p~n", [Sql30]),
    ?assertMatch({ok, _}, imem_sql:exec(SKey, Sql30, 0, [{schema, imem}], IsSec)),
    ?assertEqual(0, imem_sql_insert:if_call_mfa(IsSec, table_size, [SKey, key_test])),
    _TableDef = imem_sql_insert:if_call_mfa(IsSec, read, [SKey, key_test, {imem_meta:schema(), key_test}]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":TableDef: ~p~n", [_TableDef]),

    Sql96 = "drop table fun_test;",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql96: ~p~n", [Sql96]),
    ?assertEqual(ok, imem_sql:exec(SKey, Sql96, 0, [{schema, imem}], IsSec)),

    Sql97 = "drop table key_test;",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql97: ~p~n", [Sql97]),
    ?assertEqual(ok, imem_sql:exec(SKey, Sql97, 0, [{schema, imem}], IsSec)),

    Sql98 = "drop table not_null;",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql98: ~p~n", [Sql98]),
    ?assertEqual(ok, imem_sql:exec(SKey, Sql98, 0, [{schema, imem}], IsSec)),

    Sql99 = "drop table def;",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql99: ~p~n", [Sql99]),
    ?assertEqual(ok, imem_sql:exec(SKey, Sql99, 0, [{schema, imem}], IsSec)),

    ok.

test_with_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_sec/1 - Start ===>~n", []),

    test_with_or_without_sec(true).

test_without_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_without_sec/1 - Start ===>~n", []),

    test_with_or_without_sec(false).
