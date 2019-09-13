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
-include("imem_ct.hrl").

-export([
    all/0,
    end_per_suite/1,
    end_per_testcase/2,
    groups/0,
    init_per_suite/1,
    init_per_testcase/2,
    suite/0
]).

-export([
    cache_test_without_sec/1,
    partition_size_metric/1,
    config_operations/1,
    dal_skvh_concurrency/1,
    dal_skvh_operations/1,
    dal_skvh_purge_history/1,
    if_csv_test_csv_1/1,
    if_mnesia_table_operations/1,
    import_test_with_sec/1,
    import_test_without_sec/1,
    meta_concurrency/1,
    meta_operations/1,
    meta_partitions/1,
    meta_preparations/1,
    physical_table_names/1,
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
    sql_select_db1_with_sec/1,
    sql_select_db1_without_sec/1,
    sql_select_db2_with_sec/1,
    sql_select_db2_without_sec/1,
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
    [
        {group, imem_cache},
        {group, imem_config},
        {group, imem_dal_skvh},
        {group, imem_if_csv},
        {group, imem_if_mnesia},
        {group, imem_import},
        {group, imem_meta},
        {group, imem_metrics},
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
        {group, imem_sql_select},
        {group, imem_sql_table},
        {group, imem_statement},
        {group, imem_test}
    ].

%%--------------------------------------------------------------------
% The test suite information function.
%%--------------------------------------------------------------------

suite() ->
    [
        {timetrap, {minutes, 10}}
    ].

%%--------------------------------------------------------------------
%% Suite related setup and teardown functions.
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    ?CTPAL("Start imem"),
    application:load(imem),
    application:set_env(imem, mnesia_node_type, disc),
    imem:start(),
    Config.

end_per_suite(_Config) ->
    ?CTPAL("Stop imem"),
    imem:stop(),
    ok.

%%--------------------------------------------------------------------
% Defines test case groups.
%%--------------------------------------------------------------------

groups() ->
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
                dal_skvh_operations,
                dal_skvh_concurrency,
                dal_skvh_purge_history
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
                import_test_without_sec,
                import_test_with_sec
            ]
        },
        {
            imem_meta, [],
            [
                physical_table_names,
                meta_operations,
                meta_preparations,
                meta_partitions,
                meta_concurrency
            ]
        },
        {
            imem_metrics, [],
            [
                partition_size_metric
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
                sql_test_without_sec,
                sql_test_with_sec
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
                sql_funs_test_without_sec,
                sql_funs_test_with_sec
            ]
        },
        {
            imem_sql_index, [],
            [
                sql_index_test_without_sec,
                sql_index_test_with_sec
            ]
        },
        {
            imem_sql_insert, [],
            [
                sql_insert_test_without_sec,
                sql_insert_test_with_sec
            ]
        },
        {
            imem_sql_select, [],
            [
                sql_select_db1_without_sec,
                sql_select_db1_with_sec,
                sql_select_db2_without_sec,
                sql_select_db2_with_sec
            ]
        },
        {
            imem_sql_table, [],
            [
                sql_table_test_without_sec,
                sql_table_test_with_sec
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
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

init_per_testcase(dal_skvh_concurrency = TestCase, Config) ->
    imem_dal_skvh_ct:init_per_testcase(TestCase, Config);
init_per_testcase(dal_skvh_operations = TestCase, Config) ->
    imem_dal_skvh_ct:init_per_testcase(TestCase, Config);
init_per_testcase(dal_skvh_purge_history = TestCase, Config) ->
    imem_dal_skvh_ct:init_per_testcase(TestCase, Config);
init_per_testcase(sql_select_db1_with_sec = TestCase, Config) ->
    imem_sql_select_ct:init_per_testcase(TestCase, Config);
init_per_testcase(sql_select_db1_without_sec = TestCase, Config) ->
    imem_sql_select_ct:init_per_testcase(TestCase, Config);
init_per_testcase(sql_select_db2_with_sec = TestCase, Config) ->
    imem_sql_select_ct:init_per_testcase(TestCase, Config);
init_per_testcase(sql_select_db2_without_sec = TestCase, Config) ->
    imem_sql_select_ct:init_per_testcase(TestCase, Config);
init_per_testcase(TestCase, Config) ->
    ?CTPAL("Start ~p", [TestCase]),
    Config.

end_per_testcase(config_operations = TestCase, Config) ->
    imem_config_ct:end_per_testcase(TestCase, Config);
end_per_testcase(dal_skvh_concurrency = TestCase, Config) ->
    imem_dal_skvh_ct:end_per_testcase(TestCase, Config);
end_per_testcase(dal_skvh_operations = TestCase, Config) ->
    imem_dal_skvh_ct:end_per_testcase(TestCase, Config);
end_per_testcase(dal_skvh_purge_history = TestCase, Config) ->
    imem_dal_skvh_ct:end_per_testcase(TestCase, Config);
end_per_testcase(if_mnesia_table_operations = TestCase, Config) ->
    imem_if_mnesia_ct:end_per_testcase(TestCase, Config);
end_per_testcase(import_test_with_sec = TestCase, Config) ->
    imem_import_ct:end_per_testcase(TestCase, Config);
end_per_testcase(import_test_without_sec = TestCase, Config) ->
    imem_import_ct:end_per_testcase(TestCase, Config);
end_per_testcase(meta_concurrency = TestCase, Config) ->
    imem_meta_ct:end_per_testcase(TestCase, Config);
end_per_testcase(physical_table_names = TestCase, Config) ->
    imem_meta_ct:end_per_testcase(TestCase, Config);
end_per_testcase(meta_operations = TestCase, Config) ->
    imem_meta_ct:end_per_testcase(TestCase, Config);
end_per_testcase(meta_partitions = TestCase, Config) ->
    imem_meta_ct:end_per_testcase(TestCase, Config);
end_per_testcase(meta_preparations = TestCase, Config) ->
    imem_meta_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sec_test = TestCase, Config) ->
    imem_sec_ct:end_per_testcase(TestCase, Config);
end_per_testcase(seco_test = TestCase, Config) ->
    imem_seco_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_account_test_with_sec = TestCase, Config) ->
    imem_sql_account_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_expr_test_without_sec = TestCase, Config) ->
    imem_sql_expr_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_index_test_with_sec = TestCase, Config) ->
    imem_sql_index_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_index_test_without_sec = TestCase, Config) ->
    imem_sql_index_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_insert_test_with_sec = TestCase, Config) ->
    imem_sql_insert_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_insert_test_without_sec = TestCase, Config) ->
    imem_sql_insert_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_select_db1_with_sec = TestCase, Config) ->
    imem_sql_select_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_select_db1_without_sec = TestCase, Config) ->
    imem_sql_select_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_select_db2_with_sec = TestCase, Config) ->
    imem_sql_select_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_select_db2_without_sec = TestCase, Config) ->
    imem_sql_select_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_table_test_with_sec = TestCase, Config) ->
    imem_sql_table_ct:end_per_testcase(TestCase, Config);
end_per_testcase(sql_table_test_without_sec = TestCase, Config) ->
    imem_sql_table_ct:end_per_testcase(TestCase, Config);
end_per_testcase(statement_test_with_sec_part1 = TestCase, Config) ->
    imem_statement_ct:end_per_testcase(TestCase, Config);
end_per_testcase(statement_test_with_sec_part2 = TestCase, Config) ->
    imem_statement_ct:end_per_testcase(TestCase, Config);
end_per_testcase(statement_test_with_sec_part3 = TestCase, Config) ->
    imem_statement_ct:end_per_testcase(TestCase, Config);
end_per_testcase(statement_test_without_sec_part1 = TestCase, Config) ->
    imem_statement_ct:end_per_testcase(TestCase, Config);
end_per_testcase(statement_test_without_sec_part2 = TestCase, Config) ->
    imem_statement_ct:end_per_testcase(TestCase, Config);
end_per_testcase(statement_test_without_sec_part3 = TestCase, Config) ->
    imem_statement_ct:end_per_testcase(TestCase, Config);
end_per_testcase(test_test = TestCase, Config) ->
    imem_test_ct:end_per_testcase(TestCase, Config);
end_per_testcase(TestCase, Config) ->
    ?CTPAL("Start ~p", [TestCase]),
    Config.

%%====================================================================
%% Test cases: imem_config.
%%====================================================================

config_operations(Config) ->
    ?CTPAL("Start"),
    imem_config_ct:config_operations(Config).

%%====================================================================
%% Test cases: imem_cache.
%%====================================================================

cache_test_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_cache_ct:test_without_sec(Config).

%%====================================================================
%% Test cases: imem_metrics.
%%====================================================================

partition_size_metric(Config) ->
    ?CTPAL("Start"),
    imem_metrics_ct:test_partition_size(Config).

%%====================================================================
%% Test cases: imem_dal_skvh.
%%====================================================================

dal_skvh_concurrency(Config) ->
    ?CTPAL("Start"),
    imem_dal_skvh_ct:skvh_concurrency(Config).

dal_skvh_operations(Config) ->
    ?CTPAL("Start"),
    imem_dal_skvh_ct:skvh_operations(Config).

dal_skvh_purge_history(Config) ->
    ?CTPAL("Start"),
    imem_dal_skvh_ct:skvh_purge_history(Config).

%%====================================================================
%% Test cases: imem_if_csv.
%%====================================================================

if_csv_test_csv_1(Config) ->
    ?CTPAL("Start"),
    imem_if_csv_ct:test_csv_1(Config).

%%====================================================================
%% Test cases: imem_if_mnesia.
%%====================================================================

if_mnesia_table_operations(Config) ->
    ?CTPAL("Start"),
    imem_if_mnesia_ct:table_operations(Config).

%%====================================================================
%% Test cases: imem_import.
%%====================================================================

import_test_with_sec(Config) ->
    ?CTPAL("Start"),
    imem_import_ct:test_with_sec(Config).

import_test_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_import_ct:test_without_sec(Config).

%%====================================================================
%% Test cases: imem_meta.
%%====================================================================

physical_table_names(Config) ->
    ?CTPAL("Start"),
    imem_meta_ct:physical_table_names(Config).

meta_concurrency(Config) ->
    ?CTPAL("Start"),
    imem_meta_ct:meta_concurrency(Config).

meta_operations(Config) ->
    ?CTPAL("Start"),
    imem_meta_ct:meta_operations(Config).

meta_partitions(Config) ->
    ?CTPAL("Start"),
    imem_meta_ct:meta_partitions(Config).

meta_preparations(Config) ->
    ?CTPAL("Start"),
    imem_meta_ct:meta_preparations(Config).

%%====================================================================
%% Test cases: imem_monitor.
%%====================================================================

monitor_operations(Config) ->
    ?CTPAL("Start"),
    imem_monitor_ct:monitor_operations(Config).

%%====================================================================
%% Test cases: imem_sec.
%%====================================================================

sec_test(Config) ->
    ?CTPAL("Start"),
    imem_sec_ct:test(Config).

%%====================================================================
%% Test cases: imem_seco.
%%====================================================================

seco_test(Config) ->
    ?CTPAL("Start"),
    imem_seco_ct:test(Config).

%%====================================================================
%% Test cases: imem_snap.
%%====================================================================

snap_test_snapshot(Config) ->
    ?CTPAL("Start"),
    imem_snap_ct:test_snapshot(Config).

%%====================================================================
%% Test cases: imem_sql.
%%====================================================================

sql_test_with_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_ct:test_with_sec(Config).

sql_test_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_ct:test_without_sec(Config).

%%====================================================================
%% Test cases: imem_sql_account.
%%====================================================================

sql_account_test_with_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_account_ct:test_with_sec(Config).

%%====================================================================
%% Test cases: imem_sql_expr.
%%====================================================================

sql_expr_test_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_expr_ct:test_without_sec(Config).

%%====================================================================
%% Test cases: imem_sql_funs.
%%====================================================================

sql_funs_test_with_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_funs_ct:test_with_sec(Config).

sql_funs_test_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_funs_ct:test_without_sec(Config).

%%====================================================================
%% Test cases: imem_sql_index.
%%====================================================================

sql_index_test_with_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_index_ct:test_with_sec(Config).

sql_index_test_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_index_ct:test_without_sec(Config).

%%====================================================================
%% Test cases: imem_sql_insert.
%%====================================================================

sql_insert_test_with_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_insert_ct:test_with_sec(Config).

sql_insert_test_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_insert_ct:test_without_sec(Config).

%%====================================================================
%% Test cases: imem_sql_select.
%%====================================================================

sql_select_db1_with_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_select_ct:db1_with_sec(Config).

sql_select_db1_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_select_ct:db1_without_sec(Config).

sql_select_db2_with_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_select_ct:db2_with_sec(Config).

sql_select_db2_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_select_ct:db2_without_sec(Config).

%%====================================================================
%% Test cases: imem_sql_table.
%%====================================================================

sql_table_test_with_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_table_ct:test_with_sec(Config).

sql_table_test_without_sec(Config) ->
    ?CTPAL("Start"),
    imem_sql_table_ct:test_without_sec(Config).

%%====================================================================
%% Test cases: imem_statement.
%%====================================================================

statement_test_with_sec_part1(Config) ->
    ?CTPAL("Start"),
    imem_statement_ct:test_with_sec_part1(Config).

statement_test_with_sec_part2(Config) ->
    ?CTPAL("Start"),
    imem_statement_ct:test_with_sec_part2(Config).

statement_test_with_sec_part3(Config) ->
    ?CTPAL("Start"),
    imem_statement_ct:test_with_sec_part3(Config).

statement_test_without_sec_part1(Config) ->
    ?CTPAL("Start"),
    imem_statement_ct:test_without_sec_part1(Config).

statement_test_without_sec_part2(Config) ->
    ?CTPAL("Start"),
    imem_statement_ct:test_without_sec_part2(Config).

statement_test_without_sec_part3(Config) ->
    ?CTPAL("Start"),
    imem_statement_ct:test_without_sec_part3(Config).

%%====================================================================
%% Test cases: imem_test.
%%====================================================================

test_test(Config) ->
    ?CTPAL("Start"),
    imem_test_ct:test(Config).
