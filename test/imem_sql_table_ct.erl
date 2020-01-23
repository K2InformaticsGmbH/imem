%%%-------------------------------------------------------------------
%%% File        : imem_sql_table_ct.erl
%%% Description : Common testing imem_sql_table.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_sql_table_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_testcase/2,
    test_with_sec/1,
    test_without_sec/1
]).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include("imem_seco.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_testcase(TestCase, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_testcase/2 - Start(~p) ===>~n", [TestCase]),

    catch imem_meta:drop_table(key_test),
    catch imem_meta:drop_table(truncate_test),
    catch imem_meta:drop_table(def),

    ok.

%%====================================================================
%% Test cases.
%%====================================================================

test_with_or_without_sec(IsSec) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_or_without_sec/1 - Start(~p) ===>~n", [IsSec]),

    ClEr = 'ClientError',
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

    SKey = ?imem_test_admin_login(),

    Sql0 = "create table def (col1 varchar2(10) not null, col2 integer default 12, col3 list default fun() -> [/] end.);",
    ?assertException(throw, {ClEr, {"Bad default fun", _}}, imem_sql:exec(SKey, Sql0, 0, [{schema, imem}], IsSec)),

    Sql1 = "create table def (col1 varchar2(10) not null, col2 integer default 12, col3 list default fun() -> [] end.);",
    Expected =
        [{ddColumn, col1, binstr, 10, undefined, ?nav, []},
            {ddColumn, col2, integer, undefined, undefined, 12, []},
            {ddColumn, col3, list, undefined, undefined, <<"fun() -> [] end.">>, []}
        ],
    ?assertMatch({ok, _}, imem_sql:exec(SKey, Sql1, 0, [{schema, imem}], IsSec)),
    [Meta] = imem_sql_table:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, def}]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Meta table~n~p~n", [Meta]),
    ?assertEqual(0, imem_sql_table:if_call_mfa(IsSec, table_size, [SKey, def])),
    ?assertEqual(Expected, element(3, Meta)),

    ?assertMatch({ok, _}, imem_sql:exec(SKey,
        "create cluster table truncate_test (col1 integer, col2 string);", 0, [{schema, imem}], IsSec)),
    imem_sql_table:if_call_mfa(IsSec, write, [SKey, truncate_test, {truncate_test, 1, ""}]),
    imem_sql_table:if_call_mfa(IsSec, write, [SKey, truncate_test, {truncate_test, 2, "abc"}]),
    imem_sql_table:if_call_mfa(IsSec, write, [SKey, truncate_test, {truncate_test, 3, "123"}]),
    imem_sql_table:if_call_mfa(IsSec, write, [SKey, truncate_test, {truncate_test, 4, undefined}]),
    imem_sql_table:if_call_mfa(IsSec, write, [SKey, truncate_test, {truncate_test, 5, []}]),
    ?assertEqual(5, imem_sql_table:if_call_mfa(IsSec, table_size, [SKey, truncate_test])),
    ?assertEqual(ok, imem_sql:exec(SKey,
        "truncate table truncate_test;", 0, [{schema, imem}], IsSec)),
    ?assertEqual(0, imem_sql_table:if_call_mfa(IsSec, table_size, [SKey, truncate_test])),
    ?assertEqual(ok, imem_sql:exec(SKey, "drop table truncate_test;", 0, [{schema, imem}], IsSec)),

    Sql30 = "create loCal SeT table key_test (col1 '{atom,integer}', col2 '{string,binstr}');",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql30: ~p~n", [Sql30]),
    ?assertMatch({ok, _}, imem_sql:exec(SKey, Sql30, 0, [{schema, imem}], IsSec)),
    ?assertEqual(0, imem_sql_table:if_call_mfa(IsSec, table_size, [SKey, key_test])),
    _TableDef = imem_sql_table:if_call_mfa(IsSec, read, [SKey, ddTable, {imem_meta:schema(), key_test}]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":TableDef: ~p~n", [_TableDef]),

    Sql40 = "create someType table def (col1 varchar2(10) not null, col2 integer);",
    ?assertException(throw, {ClEr, {"Unsupported option", {type, <<"someType">>}}}, imem_sql:exec(SKey, Sql40, 0, [{schema, imem}], IsSec)),
    Sql41 = "create imem_meta table skvhTEST();",
    ?assertException(throw, {ClEr, {"Invalid module name for table type", {type, imem_meta}}}, imem_sql:exec(SKey, Sql41, 0, [{schema, imem}], IsSec)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql41: ~p~n", [Sql41]),

    Sql97 = "drop table key_test;",
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Sql97: ~p~n", [Sql97]),
    ?assertEqual(ok, imem_sql:exec(SKey, Sql97, 0, [{schema, imem}], IsSec)),

    ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, [{schema, imem}], IsSec)),
    ?assertException(throw, {ClEr, {"Table does not exist", def}}, imem_sql_table:if_call_mfa(IsSec, table_size, [SKey, def])),
    ?assertException(throw, {ClEr, {"Table does not exist", def}}, imem_sql:exec(SKey, "drop table def;", 0, [{schema, imem}], IsSec)),

    ok.

test_with_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_sec/1 - Start ===>~n", []),

    test_with_or_without_sec(true).

test_without_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_without_sec/1 - Start ===>~n", []),

    test_with_or_without_sec(false).
