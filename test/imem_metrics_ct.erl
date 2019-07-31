%%%-------------------------------------------------------------------
%%% File        : imem_metrics_ct.erl
%%% Description : Common testing imem_metrics.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
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
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_partition_size/1 - Start ===>~n", []),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    PartitionTable = test_10@,
    ?assertEqual(#{size => 0}, imem_metrics:get_metric({partition_size, PartitionTable, -10, 0})),
    ?assertMatch({ok, _}, imem_meta:create_check_table(PartitionTable, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),
    ?assertEqual(0, imem_meta:table_size(PartitionTable)),
    ?assertEqual(#{size => 0}, imem_metrics:get_metric({partition_size, PartitionTable, -10, 0})),
    LogRec = #ddLog{logTime = ?TIME_UID, logLevel = info, pid = self()
        , module = ?MODULE, function = meta_partitions, node = node()
        , fields = [], message = <<"some log message">>},
    ?assertEqual(ok, imem_meta:write(PartitionTable, LogRec)),
    ?assertEqual(#{size => 1}, imem_metrics:get_metric({partition_size, PartitionTable, 0, 0})),
    ?assertEqual(#{size => 1}, imem_metrics:get_metric({partition_size, PartitionTable, -2, 0})),
    {Secs, Mics, Node, _} = ?TIME_UID,
    ?assertEqual(ok, imem_meta:write(PartitionTable, LogRec#ddLog{logTime = {Secs + 1, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(ok, imem_meta:write(PartitionTable, LogRec#ddLog{logTime = {Secs + 2, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(ok, imem_meta:write(PartitionTable, LogRec#ddLog{logTime = {Secs + 3, Mics, Node, ?INTEGER_UID}})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ [io_lib:format(" !!!!!!! table : ~p size : ~p ", [T, imem_meta:table_size(T)]) || T <- imem_meta:physical_table_names(PartitionTable)]),
    ?assertEqual(#{size => 4}, imem_metrics:get_metric({partition_size, PartitionTable, 0, 0})),
    ?assertEqual(#{size => 4}, imem_metrics:get_metric({partition_size, PartitionTable, -2, 0})),
    ?assertEqual(1, length(imem_meta:physical_table_names(PartitionTable))),
    ct:sleep({seconds, 10}),
    ?assertEqual(2, length(imem_meta:physical_table_names(PartitionTable))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ [io_lib:format(" !!!!!!! table : ~p size : ~p ", [T, imem_meta:table_size(T)]) || T <- imem_meta:physical_table_names(PartitionTable)]),
    ?assertEqual(#{size => 4}, imem_metrics:get_metric({partition_size, PartitionTable, -2, 0})),
    ?assertEqual(#{size => 4}, imem_metrics:get_metric({partition_size, PartitionTable, -1, 0})),
    ?assertEqual(ok, imem_meta:write(PartitionTable, LogRec#ddLog{logTime = {Secs + 4, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(ok, imem_meta:write(PartitionTable, LogRec#ddLog{logTime = {Secs + 5, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(#{size => 2}, imem_metrics:get_metric({partition_size, PartitionTable, 0, 0})),
    ?assertEqual(#{size => 6}, imem_metrics:get_metric({partition_size, PartitionTable, -2, 0})),
    ct:sleep({seconds, 10}),
    ?assertEqual(3, length(imem_meta:physical_table_names(PartitionTable))),
    ?assertEqual(ok, imem_meta:write(PartitionTable, LogRec#ddLog{logTime = {Secs + 6, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(ok, imem_meta:write(PartitionTable, LogRec#ddLog{logTime = {Secs + 7, Mics, Node, ?INTEGER_UID}})),
    ?assertEqual(#{size => 2}, imem_metrics:get_metric({partition_size, PartitionTable, 0, 0})),
    ?assertEqual(#{size => 6}, imem_metrics:get_metric({partition_size, PartitionTable, -2, 0})),
    ?assertEqual(#{size => 96}, imem_metrics:get_metric({partition_size, PartitionTable, -3, 0})),
    
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [schema]),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [data_nodes]),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:test_partition_size~n", [?MODULE]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [partition_size]),

    ok.
