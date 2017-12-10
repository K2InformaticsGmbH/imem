%%%-------------------------------------------------------------------
%%% File        : imem_SUITE.erl
%%% Description : Common testing imem.
%%%
%%% Created     : 20.11.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    end_per_group/2,
    end_per_suite/1,
    groups/0,
    init_per_group/2,
    init_per_suite/1,
    suite/0
]).

-export([
    cache_test_without_sec/1,
    config_operations/1,
    dal_skvh_concurrency/1,
    dal_skvh_operations/1,
    if_csv_test_csv_1/1,
    if_mnesia_table_operations/1,
    import_test_with_sec/1,
    import_test_without_sec/1,
    meta_concurrency/1,
    meta_operations/1,
    meta_partitions/1,
    meta_preparations/1,
    monitor_operations/1,
    sec_test/1,
    seco_test/1,
    snap_test_snapshot/1,
    sql_account_test_with_sec/1,
    sql_expr_test_without_sec/1,
    sql_funs_test_with_sec/1,
    sql_funs_test_without_sec/1,
    sql_index_test_with_sec/1,
    sql_index_test_without_sec/1,
    sql_insert_test_with_sec/1,
    sql_insert_test_without_sec/1,
    sql_table_test_with_sec/1,
    sql_table_test_without_sec/1,
    sql_test_with_sec/1,
    sql_test_without_sec/1,
    statement_test_with_sec_part1/1,
    statement_test_with_sec_part2/1,
    statement_test_with_sec_part3/1,
    statement_test_without_sec_part1/1,
    statement_test_without_sec_part2/1,
    statement_test_without_sec_part3/1,
    test_test/1
]).

%%--------------------------------------------------------------------
%% Returns the list of all test cases and test case groups
%% in the test suite module to be executed.
%%--------------------------------------------------------------------

all() ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":all/0 - Start ===>~n", []),
    [
        {group, imem_cache},
        {group, imem_config},
        {group, imem_dal_skvh},
        {group, imem_if_csv},
        {group, imem_if_mnesia},
        {group, imem_import},
        {group, imem_meta},
        {group, imem_monitor},
        {group, imem_sec},
        {group, imem_seco},
        {group, imem_snap},
        {group, imem_sql},
        {group, imem_sql_account},
        {group, imem_sql_expr},
        {group, imem_sql_funs},
        {group, imem_sql_index},
        {group, imem_sql_insert},
        {group, imem_sql_table},
        {group, imem_statement},
        {group, imem_test}
    ].

%%--------------------------------------------------------------------
% The test suite information function.
%%--------------------------------------------------------------------

suite() ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":suite/0 - Start ===>~n", []),
    [
        {timetrap, {minutes, 10}}
    ].

%%--------------------------------------------------------------------
%% Suite related setup and teardown functions.
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":init_per_suite/1 - Start ===>~n", []),
    application:load(imem),
    application:set_env(imem, mnesia_node_type, disc),
    imem:start(),
    Config.

end_per_suite(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_suite/1 - Start ===>~n", []),
    imem:stop(),
    ok.

%%--------------------------------------------------------------------
% Defines test case groups.
%%--------------------------------------------------------------------

groups() ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":groups/0 - Start ===>~n", []),
    [
        {
            imem_cache, [],
            [
                cache_test_without_sec
            ]
        },
        {
            imem_config, [],
            [
                config_operations
            ]
        },
        {
            imem_dal_skvh, [],
            [
                dal_skvh_concurrency,
                dal_skvh_operations
            ]
        },
        {
            imem_if_csv, [],
            [
                if_csv_test_csv_1
            ]
        },
        {
            imem_if_mnesia, [],
            [
                if_mnesia_table_operations
            ]
        },
        {
            imem_import, [],
            [
                import_test_with_sec,
                import_test_without_sec
            ]
        },
        {
            imem_meta, [],
            [
                meta_operations,
                meta_preparations,
                meta_partitions,
                meta_concurrency
            ]
        },
        {
            imem_monitor, [],
            [
                monitor_operations
            ]
        },
        {
            imem_sec, [],
            [
                sec_test
            ]
        },
        {
            imem_seco, [],
            [
                seco_test
            ]
        },
        {
            imem_snap, [],
            [
                snap_test_snapshot
            ]
        },
        {
            imem_sql, [],
            [
                sql_test_with_sec,
                sql_test_without_sec
            ]
        },
        {
            imem_sql_account, [],
            [
                sql_account_test_with_sec
            ]
        },
        {
            imem_sql_expr, [],
            [
                sql_expr_test_without_sec
            ]
        },
        {
            imem_sql_funs, [],
            [
                sql_funs_test_with_sec,
                sql_funs_test_without_sec
            ]
        },
        {
            imem_sql_index, [],
            [
                sql_index_test_with_sec,
                sql_index_test_without_sec
            ]
        },
        {
            imem_sql_insert, [],
            [
                sql_insert_test_with_sec,
                sql_insert_test_without_sec
            ]
        },
        {
            imem_sql_table, [],
            [
                sql_table_test_with_sec,
                sql_table_test_without_sec
            ]
        },
        {
            imem_statement, [],
            [
                statement_test_without_sec_part1,
                statement_test_without_sec_part2,
                statement_test_without_sec_part3,
                statement_test_with_sec_part1,
                statement_test_with_sec_part2,
                statement_test_with_sec_part3
            ]
        },
        {
            imem_test, [],
            [
                test_test
            ]
        }
    ].

%%--------------------------------------------------------------------
%% Group related setup and teardown functions.
%%--------------------------------------------------------------------

init_per_group(imem_dal_skvh = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":init_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_dal_skvh_ct:init_per_group(Config);
init_per_group(_Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":init_per_group/2 - ~p - Start ===>~n", [_Group]),
    Config.

end_per_group(imem_config = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_config_ct:end_per_group(Config);
end_per_group(imem_dal_skvh = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_dal_skvh_ct:end_per_group(Config);
end_per_group(imem_if_mnesia = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_if_mnesia_ct:end_per_group(Config);
end_per_group(imem_import = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_import_ct:end_per_group(Config);
end_per_group(imem_meta = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_meta_ct:end_per_group(Config);
end_per_group(imem_sec = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_sec_ct:end_per_group(Config);
end_per_group(imem_seco = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_seco_ct:end_per_group(Config);
end_per_group(imem_sql_account = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_sql_account_ct:end_per_group(Config);
end_per_group(imem_sql_expr = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_sql_expr_ct:end_per_group(Config);
end_per_group(imem_sql_index = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_sql_index_ct:end_per_group(Config);
end_per_group(imem_sql_insert = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_sql_insert_ct:end_per_group(Config);
end_per_group(imem_sql_table = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_sql_table_ct:end_per_group(Config);
end_per_group(imem_test = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_test_ct:end_per_group(Config);
end_per_group(imem_statement = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    imem_statement_ct:end_per_group(Config);
end_per_group(_Group, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [_Group]),
    ok.

%%====================================================================
%% Test Cases: imem_config.
%%====================================================================

config_operations(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":config_operations/1 - Start ===>~n", []),
    imem_config_ct:config_operations(Config).

%%====================================================================
%% Test Cases: imem_cache.
%%====================================================================

cache_test_without_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":cache_test_without_sec/1 - Start ===>~n", []),
    imem_cache_ct:test_without_sec(Config).

%%====================================================================
%% Test Cases: imem_dal_skvh.
%%====================================================================

dal_skvh_concurrency(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":dal_skvh_concurrency/1 - Start ===>~n", []),
    imem_dal_skvh_ct:skvh_concurrency(Config).

dal_skvh_operations(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":dal_skvh_operations/1 - Start ===>~n", []),
    imem_dal_skvh_ct:skvh_operations(Config).

%%====================================================================
%% Test Cases: imem_if_csv.
%%====================================================================

if_csv_test_csv_1(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":if_csv_test_csv_1/1 - Start ===>~n", []),
    imem_if_csv_ct:test_csv_1(Config).

%%====================================================================
%% Test Cases: imem_if_mnesia.
%%====================================================================

if_mnesia_table_operations(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":if_mnesia_table_operations/1 - Start ===>~n", []),
    imem_if_mnesia_ct:table_operations(Config).

%%====================================================================
%% Test Cases: imem_import.
%%====================================================================

import_test_with_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":import_test_with_sec/1 - Start ===>~n", []),
    imem_import_ct:test_with_sec(Config).

import_test_without_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":import_test_without_sec/1 - Start ===>~n", []),
    imem_import_ct:test_without_sec(Config).

%%====================================================================
%% Test Cases: imem_meta.
%%====================================================================

meta_concurrency(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_concurrency/1 - Start ===>~n", []),
    imem_meta_ct:meta_concurrency(Config).

meta_operations(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_operations/1 - Start ===>~n", []),
    imem_meta_ct:meta_operations(Config).

meta_partitions(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_partitions/1 - Start ===>~n", []),
    imem_meta_ct:meta_partitions(Config).

meta_preparations(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_preparations/1 - Start ===>~n", []),
    imem_meta_ct:meta_preparations(Config).

%%====================================================================
%% Test Cases: imem_monitor.
%%====================================================================

monitor_operations(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":monitor_operations/1 - Start ===>~n", []),
    imem_monitor_ct:monitor_operations(Config).

%%====================================================================
%% Test Cases: imem_sec.
%%====================================================================

sec_test(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sec_test/1 - Start ===>~n", []),
    imem_sec_ct:test(Config).

%%====================================================================
%% Test Cases: imem_seco.
%%====================================================================

seco_test(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":seco_test/1 - Start ===>~n", []),
    imem_seco_ct:test(Config).

%%====================================================================
%% Test Cases: imem_snap.
%%====================================================================

snap_test_snapshot(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":snap_test_snapshot/1 - Start ===>~n", []),
    imem_snap_ct:test_snapshot(Config).

%%====================================================================
%% Test Cases: imem_sql.
%%====================================================================

sql_test_with_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_test_with_sec/1 - Start ===>~n", []),
    imem_sql_ct:test_with_sec(Config).

sql_test_without_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_test_without_sec/1 - Start ===>~n", []),
    imem_sql_ct:test_without_sec(Config).

%%====================================================================
%% Test Cases: imem_sql_account.
%%====================================================================

sql_account_test_with_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_account_test_with_sec/1 - Start ===>~n", []),
    imem_sql_account_ct:test_with_sec(Config).

%%====================================================================
%% Test Cases: imem_sql_expr.
%%====================================================================

sql_expr_test_without_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_expr_test_without_sec/1 - Start ===>~n", []),
    imem_sql_expr_ct:test_without_sec(Config).

%%====================================================================
%% Test Cases: imem_sql_funs.
%%====================================================================

sql_funs_test_with_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_funs_test_with_sec/1 - Start ===>~n", []),
    imem_sql_funs_ct:test_with_sec(Config).

sql_funs_test_without_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_funs_test_without_sec/1 - Start ===>~n", []),
    imem_sql_funs_ct:test_without_sec(Config).

%%====================================================================
%% Test Cases: imem_sql_index.
%%====================================================================

sql_index_test_with_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_index_test_with_sec/1 - Start ===>~n", []),
    imem_sql_index_ct:test_with_sec(Config).

sql_index_test_without_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_index_test_without_sec/1 - Start ===>~n", []),
    imem_sql_index_ct:test_without_sec(Config).

%%====================================================================
%% Test Cases: imem_sql_insert.
%%====================================================================

sql_insert_test_with_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_insert_test_with_sec/1 - Start ===>~n", []),
    imem_sql_insert_ct:test_with_sec(Config).

sql_insert_test_without_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_insert_test_without_sec/1 - Start ===>~n", []),
    imem_sql_insert_ct:test_without_sec(Config).

%%====================================================================
%% Test Cases: imem_sql_table.
%%====================================================================

sql_table_test_with_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_table_test_with_sec/1 - Start ===>~n", []),
    imem_sql_table_ct:test_with_sec(Config).

sql_table_test_without_sec(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":sql_table_test_without_sec/1 - Start ===>~n", []),
    imem_sql_table_ct:test_without_sec(Config).

%%====================================================================
%% Test Cases: imem_statement.
%%====================================================================

statement_test_with_sec_part1(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":statement_test_with_sec_part1/1 - Start ===>~n", []),
    imem_statement_ct:test_with_sec_part1(Config).

statement_test_with_sec_part2(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":statement_test_with_sec_part1/2 - Start ===>~n", []),
    imem_statement_ct:test_with_sec_part2(Config).

statement_test_with_sec_part3(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":statement_test_with_sec_part3/1 - Start ===>~n", []),
    imem_statement_ct:test_with_sec_part3(Config).

statement_test_without_sec_part1(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":statement_test_without_sec_part1/1 - Start ===>~n", []),
    imem_statement_ct:test_without_sec_part1(Config).

statement_test_without_sec_part2(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":statement_test_without_sec_part2/1 - Start ===>~n", []),
    imem_statement_ct:test_without_sec_part2(Config).

statement_test_without_sec_part3(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":statement_test_without_sec_part3/1 - Start ===>~n", []),
    imem_statement_ct:test_without_sec_part3(Config).

%%====================================================================
%% Test Cases: imem_test.
%%====================================================================

test_test(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_test/1 - Start ===>~n", []),
    imem_test_ct:test(Config).
