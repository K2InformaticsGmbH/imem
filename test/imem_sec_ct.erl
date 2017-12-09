%%%-------------------------------------------------------------------
%%% File        : imem_sec_ct.erl
%%% Description : Common testing imem_sec.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_sec_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_group/1,
    test/1
]).

-define(NODEBUG, true).
-define(CONFIG_TABLE_OPTS, [{record_name, ddConfig}, {type, ordered_set}]).

-include_lib("imem.hrl").
-include("imem_seco.hrl").

%%--------------------------------------------------------------------
%% Group related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_group(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/1 - Start ===>~n", []),

    SKey = ?imem_test_admin_login(),
    catch imem_account:delete(SKey, <<"test_user_123">>),
    catch imem_role:delete(SKey, table_creator),
    catch imem_role:delete(SKey, test_role),
    catch imem_seco:logout(SKey),
    catch imem_meta:drop_table(user_table_123),

    ok.

%%====================================================================
%% Test Cases.
%%====================================================================

test(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":config_operations/1 - Start ===>~n", []),

    ClEr = 'ClientError',
    SeEx = 'SecurityException',
    CoEx = 'ConcurrencyException',
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

    SeCoAdmin = ?imem_test_admin_login(),
    ?assert(1 =< imem_sec:table_size(SeCoAdmin, ddSeCo@)),
    AllTablesAdmin = imem_sec:all_tables(SeCoAdmin),
    ?assertEqual(true, lists:member(ddAccount, AllTablesAdmin)),
    ?assertEqual(true, lists:member(imem_meta:physical_table_name(ddPerm@), AllTablesAdmin)),
    ?assertEqual(true, lists:member(imem_meta:physical_table_name(ddQuota@), AllTablesAdmin)),
    ?assertEqual(true, lists:member(ddRole, AllTablesAdmin)),
    ?assertEqual(true, lists:member(imem_meta:physical_table_name(ddSeCo@), AllTablesAdmin)),
    ?assertEqual(true, lists:member(ddTable, AllTablesAdmin)),

    ?assertEqual(ok, imem_sec:admin_exec(SeCoAdmin, imem_account, create, [user, <<"test_user_123">>, <<"Test user 123">>, <<"PasswordMd5">>])),
    UserId = imem_sec:admin_exec(SeCoAdmin, imem_account, get_id_by_name, [<<"test_user_123">>]),
    ?assert(is_integer(UserId)),
    ?assertEqual(ok, imem_sec:admin_exec(SeCoAdmin, imem_role, grant_permission, [<<"test_user_123">>, create_table])),

    SeCoUser0 = imem_sec:authenticate(none, userSessionId, <<"test_user_123">>, {pwdmd5, <<"PasswordMd5">>}),
    ?assertEqual(true, is_integer(SeCoUser0)),
    ?assertException(throw, {SeEx, {?PasswordChangeNeeded, _}}, imem_sec:login(SeCoUser0)),
    SeCoUser = imem_sec:authenticate(none, someSessionId, <<"test_user_123">>, {pwdmd5, <<"PasswordMd5">>}),
    ?assertEqual(true, is_integer(SeCoUser)),
    ?assertEqual(SeCoUser, imem_sec:change_credentials(SeCoUser, {pwdmd5, <<"PasswordMd5">>}, {pwdmd5, <<"NewPasswordMd5">>})),
    Type123a = {[a, b, c], [term, term, term], {user_table_123, undefined, undefined, undefined}},
    Type123b = {[a, b, a], [term, term, term], {user_table_123, undefined, undefined, undefined}},
    Type123c = {[a, b, x], [term, term, term], {user_table_123, undefined, undefined, undefined}},
    ?assertMatch({ok, _}, imem_sec:create_table(SeCoUser, user_table_123, Type123a, [])),
    ?assertException(throw, {ClEr, {"Table already exists", user_table_123}}, imem_sec:create_table(SeCoUser, user_table_123, Type123b, [])),
    ?assertException(throw, {ClEr, {"Table already exists", user_table_123}}, imem_sec:create_table(SeCoUser, user_table_123, Type123c, [])),
    ?assertEqual(0, imem_sec:table_size(SeCoUser, user_table_123)),

    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, select)),
    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, insert)),
    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, delete)),
    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, update)),

    ?assertEqual(ok, imem_sec:admin_exec(SeCoAdmin, imem_role, revoke_role, [<<"test_user_123">>, create_table])),
    % ?LogDebug("success ~p~n", [role_revoke_role]),
    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, select)),
    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, insert)),
    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, delete)),
    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, update)),
    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, drop)),
    ?assertEqual(true, imem_sec:have_table_permission(SeCoUser, user_table_123, alter)),

    ?assertException(throw, {SeEx, {"Select unauthorized", {dba_tables, SeCoUser}}}, imem_sec:select(SeCoUser, dba_tables, ?MatchAllKeys)),
    {DbaTables, true} = imem_sec:select(SeCoAdmin, dba_tables, ?MatchAllKeys),
    ?assertEqual(true, lists:member({imem, ddAccount}, DbaTables)),
    ?assertEqual(true, lists:member({imem, imem_meta:physical_table_name(ddPerm@)}, DbaTables)),
    ?assertEqual(true, lists:member({imem, imem_meta:physical_table_name(ddQuota@)}, DbaTables)),
    ?assertEqual(true, lists:member({imem, ddRole}, DbaTables)),
    ?assertEqual(true, lists:member({imem, imem_meta:physical_table_name(ddSeCo@)}, DbaTables)),
    ?assertEqual(true, lists:member({imem, ddTable}, DbaTables)),
    ?assertEqual(true, lists:member({imem, user_table_123}, DbaTables)),

    {AdminTables, true} = imem_sec:select(SeCoAdmin, user_tables, ?MatchAllKeys),
    ?assertEqual(false, lists:member({imem, ddAccount}, AdminTables)),
    ?assertEqual(false, lists:member({imem, imem_meta:physical_table_name(ddPerm@)}, AdminTables)),
    ?assertEqual(false, lists:member({imem, imem_meta:physical_table_name(ddQuota@)}, AdminTables)),
    ?assertEqual(false, lists:member({imem, ddRole}, AdminTables)),
    ?assertEqual(false, lists:member({imem, imem_meta:physical_table_name(ddSeCo@)}, AdminTables)),
    ?assertEqual(false, lists:member({imem, ddTable}, AdminTables)),
    ?assertEqual(false, lists:member({imem, user_table_123}, AdminTables)),

    {UserTables, true} = imem_sec:select(SeCoUser, user_tables, ?MatchAllKeys),
    ?assertEqual(false, lists:member({imem, ddAccount}, UserTables)),
    ?assertEqual(false, lists:member({imem, ddPerm@}, UserTables)),
    ?assertEqual(false, lists:member({imem, ddQuota@}, UserTables)),
    ?assertEqual(false, lists:member({imem, ddRole}, UserTables)),
    ?assertEqual(false, lists:member({imem, ddSeCo@}, UserTables)),
    ?assertEqual(false, lists:member({imem, ddTable}, UserTables)),
    ?assertEqual(true, lists:member({imem, user_table_123}, UserTables)),

    LogTable = imem_sec:physical_table_name(SeCoAdmin, ?LOG_TABLE),
    ?assertException(throw, {SeEx, {"Select unauthorized", {_, SeCoUser}}}, imem_sec:physical_table_name(SeCoUser, ?LOG_TABLE)),
    LogTables = imem_sec:physical_table_names(SeCoAdmin, ?LOG_TABLE),
    ?assert(lists:member(LogTable, LogTables)),
    ?assertEqual(LogTables, imem_sec:physical_table_names(SeCoAdmin, atom_to_list(?LOG_TABLE))),
    ?assertEqual([], imem_sec:physical_table_names(SeCoUser, atom_to_list(?LOG_TABLE))),

    ?assertEqual(LogTables, imem_sec:tables_starting_with(SeCoAdmin, "ddLog_")),
    ?assertEqual([user_table_123], imem_sec:tables_starting_with(SeCoUser, "user_table_")),
    ?assertEqual([user_table_123], imem_sec:tables_starting_with(SeCoAdmin, user_table_)),
    ?assertEqual([ddTable], imem_sec:tables_starting_with(SeCoAdmin, ddTable)),
    ?assertEqual([], imem_sec:tables_starting_with(SeCoUser, ddTable)),
    ?assertEqual([], imem_sec:tables_starting_with(SeCoAdmin, "akkahadöl_")),

    ?assertEqual({user_table_123, "A", "B", "C"}, imem_sec:insert(SeCoUser, user_table_123, {user_table_123, "A", "B", "C"})),
    ?assertEqual(1, imem_sec:table_size(SeCoUser, user_table_123)),
    ?assertEqual({user_table_123, "AA", "BB", "CC"}, imem_sec:merge(SeCoUser, user_table_123, {user_table_123, "AA", "BB", "CC"})),
    ?assertEqual(2, imem_sec:table_size(SeCoUser, user_table_123)),
    ?assertEqual({user_table_123, "AA", "B0", "CC"}, imem_sec:update(SeCoUser, user_table_123, {{user_table_123, "AA", "BB", "CC"}, {user_table_123, "AA", "B0", "CC"}})),
    ?assertEqual(2, imem_sec:table_size(SeCoUser, user_table_123)),
    ?assertException(throw, {CoEx, {"Update failed, key does not exist", {user_table_123, "A0"}}}, imem_sec:update(SeCoUser, user_table_123, {{user_table_123, "A0", "B0", "C0"}, {user_table_123, "A0", "B0", "CC"}})),

    ?assertEqual(2, imem_sec:table_size(SeCoUser, user_table_123)),
    ?assertEqual(ok, imem_sec:drop_table(SeCoUser, user_table_123)),
    ?assertException(throw, {ClEr, {"Table does not exist", user_table_123}}, imem_sec:table_size(SeCoUser, user_table_123)),

    ?assertEqual(ok, imem_sec:admin_exec(SeCoAdmin, imem_account, delete, [<<"test_user_123">>])),

    ok.
