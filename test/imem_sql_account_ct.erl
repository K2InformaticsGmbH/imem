%%%-------------------------------------------------------------------
%%% File        : imem_sql_account_ct.erl
%%% Description : Common testing imem_sql_account_sql.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_sql_account_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([end_per_testcase/2, test_with_sec/1]).

-define(NODEBUG, true).

-include_lib("imem.hrl").

-include("imem_seco.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_testcase(TestCase, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_testcase/2 - Start(~p) ===>~n", [TestCase]),
    SKey = ?imem_test_admin_login(),
    catch imem_account:delete(SKey, <<"test_user_1">>, []),
    ok.

%%====================================================================
%% Test cases.
%%====================================================================

test_with_or_without_sec(IsSec) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_or_without_sec/1 - Start(~p) ===>~n", [IsSec]),
    ClEr = 'ClientError',
    UiEx = 'UnimplementedException',
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    SKey = ?imem_test_admin_login(),
    ?assertEqual(
        ok,
        imem_sql:exec(SKey, "CREATE USER test_user_1 IDENTIFIED BY a_password;", 0, [{schema, imem}], IsSec)
    ),
    UserId = imem_account:get_id_by_name(SKey, <<"test_user_1">>),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":UserId ~p~n", [UserId]),
    ?assertException(
        throw,
        {ClEr, {"Account already exists", <<"test_user_1">>}},
        imem_sql:exec(SKey, "CREATE USER test_user_1 IDENTIFIED BY a_password;", 0, [{schema, imem}], IsSec)
    ),
    ?assertException(
        throw,
        {UiEx, {"Unimplemented account delete option", [cascade]}},
        imem_sql:exec(SKey, "DROP USER test_user_1 CASCADE;", 0, [{schema, imem}], IsSec)
    ),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, manage_system)),
    ?assertEqual(
        ok,
        imem_sql:exec(SKey, "GRANT manage_system TO test_user_1 with admin option;", 0, [{schema, imem}], IsSec)
    ),
    ?assertEqual(true, imem_seco:has_permission(SKey, UserId, manage_system)),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {module, imem_test, execute})),
    ?assertEqual(ok, imem_sql:exec(SKey, "GRANT EXECUTE ON imem_test TO test_user_1;", 0, [{schema, imem}], IsSec)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":ddRole ~p~n", [imem_meta:read(ddRole)]),
    ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {module, imem_test, execute})),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table, ddTable, select})),
    ?assertEqual(ok, imem_sql:exec(SKey, "GRANT SELECT ON ddTable TO test_user_1;", 0, [{schema, imem}], IsSec)),
    ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {table, ddTable, select})),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table, ddTable, update})),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table, ddTable, insert})),
    ?assertEqual(ok, imem_sql:exec(SKey, "grant update, insert on ddTable to test_user_1;", 0, [{schema, imem}], IsSec)),
    ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {table, ddTable, update})),
    ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {table, ddTable, insert})),
    ?assertEqual(ok, imem_sql:exec(SKey, "revoke update on ddTable from test_user_1;", 0, [{schema, imem}], IsSec)),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table, ddTable, update})),
    ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {table, ddTable, insert})),
    ?assertEqual(
        ok,
        imem_sql:exec(SKey, "revoke update,insert on ddTable from test_user_1;", 0, [{schema, imem}], IsSec)
    ),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table, ddTable, update})),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table, ddTable, insert})),
    ?assertEqual(ok, imem_sql:exec(SKey, "revoke execute on imem_test from test_user_1;", 0, [{schema, imem}], IsSec)),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {module, imem_test, excecute})),
    ?assertEqual(ok, imem_sql:exec(SKey, "revoke manage_system from test_user_1;", 0, [{schema, imem}], IsSec)),
    ?assertEqual(false, imem_seco:has_permission(SKey, UserId, manage_system)),
    ?assertEqual(ok, imem_sql:exec(SKey, "DROP USER test_user_1;", 0, [{schema, imem}], IsSec)),
    ok.

test_with_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_sec/1 - Start ===>~n", []),
    test_with_or_without_sec(true).
