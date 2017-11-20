%%%-------------------------------------------------------------------
%%% File        : imem_meta_SUITE.erl
%%% Description : Testing imem_meta.
%%%
%%% Created     : 09.11.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_meta_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0,
    end_per_suite/1,
    init_per_suite/1,
    suite/0
]).


-export([
    meta_concurrency/1,
    meta_operations/1,
    meta_partitions/1,
    meta_preparations/1
]).

-define(NODEBUG, true).
-include_lib("imem.hrl").
-include_lib("imem_meta.hrl").

-define(TPTEST0, tpTest_1000@).
-define(TPTEST1, tpTest1_999999999@_).
-define(TPTEST2, tpTest_100@).

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS - TEST SUITE
%%--------------------------------------------------------------------

all() ->
    [
        meta_operations,
        meta_preparations,
        meta_partitions,
        meta_concurrency
    ].

suite() ->
    [
        {timetrap, {minutes, 10}}
    ].

init_per_suite(Config) ->
    ?debugFmt(?MODULE_STRING ++ "~n:init_per_suite - Start ===>~n", []),
    ?imem_test_setup,
    Config.

end_per_suite(_Config) ->
    ?debugFmt(?MODULE_STRING ++ "~n:end_per_suite - Start ===>~n", []),
    catch imem_meta:drop_table(meta_table_3),
    catch imem_meta:drop_table(meta_table_2),
    catch imem_meta:drop_table(meta_table_1),
    catch imem_meta:drop_table(?TPTEST0),
    catch imem_meta:drop_table(?TPTEST1),
    catch imem_meta:drop_table(?TPTEST2),
    catch imem_meta:drop_table(fakelog_1@),
    catch imem_meta:drop_table(imem_table_123),
    catch imem_meta:drop_table('"imem_table_123"'),
    % imem_meta:delete(ddAlias, {imem,tpTest_1492087000@nohost}),
    ?imem_test_teardown,
    ok.

%%====================================================================
%% Test Cases.
%%====================================================================

meta_concurrency(Config) -> imem_meta_ct:meta_concurrency(Config).

meta_operations(Config) -> imem_meta_ct:meta_operations(Config).

meta_partitions(Config) -> imem_meta_ct:meta_partitions(Config).

meta_preparations(Config) -> imem_meta_ct:meta_preparations(Config).
