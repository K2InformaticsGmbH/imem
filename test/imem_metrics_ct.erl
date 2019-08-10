%%%-------------------------------------------------------------------
%%% File        : imem_metrics_ct.erl
%%% Description : Common testing imem_metrics.
%%%
%%% Created     : 02.08.2019
%%%
%%% Copyright (C) 2019 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_metrics_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    test_partition_size/1
]).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include_lib("imem_meta.hrl").

%%====================================================================
%% Test cases.
%%====================================================================

test_partition_size(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ":test_partition_size/1 - Start ===>~n", []),
    PartitionTableAlias = test_5@,
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": non partition table ~n", []),
    ?assertEqual(#{size => 0}, imem_metrics:get_metric({partition_size, test, -10, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": non existing partition table ~n", []),
    ?assertEqual(#{size => 0}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -10, 0})),
    ?assertMatch({ok, _}, imem_meta:create_check_table(PartitionTableAlias, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": empty partition table ~n", []),
    ?assertEqual(0, imem_meta:table_size(PartitionTableAlias)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 0 partition size check ~n", []),
    ?assertEqual(#{size => 0}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -10, 0})),
    LogRec = #ddLog{logTime = ?TIME_UID, logLevel = info, pid = self()
        , module = ?MODULE, function = meta_partitions, node = node()
        , fields = [], message = <<"some log message">>},
    [PartitionTable1] = imem_meta:physical_table_names(PartitionTableAlias),
    ?assertEqual(ok, imem_meta:write(PartitionTable1, LogRec)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 1 partition size check with one partition~n", []),
    ?assertEqual(1, imem_meta:table_size(PartitionTableAlias)),
    ?assertEqual(#{size => 1}, imem_metrics:get_metric({partition_size, PartitionTableAlias, 0, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 1 partition size check with 3 partitions~n", []),
    ?assertEqual(#{size => 1}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -2, 0})),
    {Secs, Mics, Node, _} = ?TIME_UID,
    ?assertEqual(ok, imem_meta:write(PartitionTable1, LogRec#ddLog{logTime = {Secs + 1, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(ok, imem_meta:write(PartitionTable1, LogRec#ddLog{logTime = {Secs + 2, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(ok, imem_meta:write(PartitionTable1, LogRec#ddLog{logTime = {Secs + 3, Mics, Node, ?INTEGER_UID}})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 4 partition size check with one partition~n", []),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ [io_lib:format(" table : ~p size : ~p ", [T, imem_meta:table_size(T)]) || T <- imem_meta:physical_table_names(PartitionTableAlias)]),
    ?assertEqual(#{size => 4}, imem_metrics:get_metric({partition_size, PartitionTableAlias, 0, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 4 partition size check with 3 partitions~n", []),
    ?assertEqual(#{size => 4}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -2, 0})),
    ct:sleep({seconds, 5}),
    [PartitionTable1, PartitionTable2 | _] = imem_meta:physical_table_names(PartitionTableAlias),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 4 partition size check with one partition after rolling~n", []),
    ?assertEqual(#{size => 4}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -2, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 4 partition size check with 3 partitions after rolling~n", []),
    ?assertEqual(#{size => 4}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -1, 0})),
    ?assertEqual(ok, imem_meta:write(PartitionTable2, LogRec#ddLog{logTime = {Secs + 4, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(ok, imem_meta:write(PartitionTable2, LogRec#ddLog{logTime = {Secs + 5, Mics, Node, ?INTEGER_UID}})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ [io_lib:format(" table : ~p size : ~p ", [T, imem_meta:table_size(T)]) || T <- imem_meta:physical_table_names(PartitionTableAlias)]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 2 partition size check with one partition after rolling~n", []),
    ?assertEqual(#{size => 2}, imem_metrics:get_metric({partition_size, PartitionTableAlias, 0, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 6 partition size check with 2 partitions after rolling~n", []),
    ?assertEqual(#{size => 6}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -1, 0})),
    ct:sleep({seconds, 5}),
    [PartitionTable1, PartitionTable2, PartitionTable3 | _] = imem_meta:physical_table_names(PartitionTableAlias),
    ?assertEqual(ok, imem_meta:write(PartitionTable3, LogRec#ddLog{logTime = {Secs + 6, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(ok, imem_meta:write(PartitionTable3, LogRec#ddLog{logTime = {Secs + 7, Mics, Node, ?INTEGER_UID}})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ [io_lib:format(" table : ~p size : ~p ", [T, imem_meta:table_size(T)]) || T <- imem_meta:physical_table_names(PartitionTableAlias)]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 2 partition size check with one partition after rolling~n", []),
    ?assertEqual(#{size => 2}, imem_metrics:get_metric({partition_size, PartitionTableAlias, 0, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 4 partition size check with 2 partitions after rolling~n", []),
    ?assertEqual(#{size => 4}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -1, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 8 partition size check with 3 partitions after rolling~n", []),
    ?assertEqual(#{size => 8}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -2, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 8 partition size check with 4 partitions after rolling~n", []),
    ?assertEqual(#{size => 8}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -3, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ [io_lib:format(" table : ~p size : ~p ", [T, imem_meta:table_size(T)]) || T <- imem_meta:physical_table_names(PartitionTableAlias)]),
    imem_meta:drop_table(PartitionTable2),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 2 partition size check with one partition after dropping 2nd partition~n", []),
    ?assertEqual(#{size => 2}, imem_metrics:get_metric({partition_size, PartitionTableAlias, 0, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 2 partition size check with two partition after dropping 2nd partition~n", []),
    ?assertEqual(#{size => 2}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -1, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ": 6 partition size check with three partition after dropping 2nd partition~n", []),
    ?assertEqual(#{size => 6}, imem_metrics:get_metric({partition_size, PartitionTableAlias, -2, 0})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ":dropped ~p~n", [PartitionTable2]),
    [imem_meta:drop_table(T) || T <- imem_meta:physical_table_names(PartitionTableAlias)],
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ":~p:test_partition_size~n", [?MODULE]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ":success ~p~n", [partition_size]),
    ok.
