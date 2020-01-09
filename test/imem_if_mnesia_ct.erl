%%%-------------------------------------------------------------------
%%% File        : imem_if_mnesia_ct.erl
%%% Description : Common testing imem_if_mnesia.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_if_mnesia_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([end_per_testcase/2, table_operations/1]).

-define(NODEBUG, true).

-include_lib("imem.hrl").

-include("imem_if.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_testcase(TestCase, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_testcase/2 - Start(~p) ===>~n", [TestCase]),
    catch imem_if_mnesia:drop_table(imem_table_bag),
    catch imem_if_mnesia:drop_table(imem_table_123),
    ok.

%%====================================================================
%% Test cases.
%%====================================================================

table_operations(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":table_operations/1 - Start ===>~n", []),
    ClEr = 'ClientError',
    CoEx = 'ConcurrencyException',
    Self = self(),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    ?assertEqual([{c, b, a}], imem_if_mnesia:field_pick([{a, b, c}], "321")),
    ?assertEqual([{c, a}], ?FP([{a, b, c}], "31")),
    ?assertEqual([{b}], ?FP([{a, b, c}], "2")),
    ?assertEqual([{a, j}], ?FP([{a, b, c, d, e, f, g, h, i, j, k}], "1(10)")),
    ?assertEqual([{a, k}], ?FP([{a, b, c, d, e, f, g, h, i, j, k}], "1(11)")),
    ?assertEqual([{a, c}], ?FP([{a, b, c, d, e, f, g, h, i}], "13(10)")),
    %% TODO: should be [{a,'N/A',c}]
    ?assertEqual([{a, c}], ?FP([{a, b, c, d, e, f, g, h, i}], "1(10)3")),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:test_database_operations~n", [?MODULE]),
    ?assertException(
        throw,
        {ClEr, {"Table does not exist", non_existing_table}},
        imem_if_mnesia:table_size(non_existing_table)
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [table_size_no_exists]),
    ?assertException(
        throw,
        {ClEr, {"Table does not exist", non_existing_table}},
        imem_if_mnesia:table_memory(non_existing_table)
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [table_memory_no_exists]),
    ?assertException(
        throw,
        {ClEr, {"Table does not exist", non_existing_table}},
        imem_if_mnesia:read(non_existing_table)
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [table_read_no_exists]),
    ?assertException(
        throw,
        {ClEr, {"Table does not exist", non_existing_table}},
        imem_if_mnesia:read(non_existing_table, no_key)
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [row_read_no_exists]),
    ?assertException(
        throw,
        {ClEr, {"Table does not exist", non_existing_table}},
        imem_if_mnesia:write(non_existing_table, {non_existing_table, "AAA", "BB", "CC"})
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [row_write_no_exists]),
    ?assertException(
        throw,
        {ClEr, {"Table does not exist", non_existing_table}},
        imem_if_mnesia:dirty_write(non_existing_table, {non_existing_table, "AAA", "BB", "CC"})
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [row_dirty_write_no_exists]),
    %        ?assertException(throw, {SyEx, {aborted,{bad_type,non_existing_table,{},imem_if_mnesia:write}}}, imem_if_mnesia:write(non_existing_table, {})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [row_write_bad_type]),
    ?assertEqual(ok, imem_if_mnesia:create_table(imem_table_123, [a, b, c], [])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [create_set_table]),
    ?assertEqual(0, imem_if_mnesia:table_size(imem_table_123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [table_size_empty]),
    BaseMemory = imem_if_mnesia:table_memory(imem_table_123),
    %% got value of 303 words x 8 bytes on 10.05.2013
    ?assert(BaseMemory < 4000),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p ~p~n", [table_memory_empty, BaseMemory]),
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, "A", "B", "C"})),
    ?assertEqual(1, imem_if_mnesia:table_size(imem_table_123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [write_table]),
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, "AA", "BB", "CC"})),
    ?assertEqual(2, imem_if_mnesia:table_size(imem_table_123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [write_table]),
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, "AA", "BB", "cc"})),
    ?assertEqual(2, imem_if_mnesia:table_size(imem_table_123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [write_table]),
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, "AAA", "BB", "CC"})),
    ?assertEqual(3, imem_if_mnesia:table_size(imem_table_123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [write_table]),
    ?assertEqual(ok, imem_if_mnesia:dirty_write(imem_table_123, {imem_table_123, "AAA", "BB", "CC"})),
    ?assertEqual(3, imem_if_mnesia:table_size(imem_table_123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [write_table]),
    FullMemory = imem_if_mnesia:table_memory(imem_table_123),
    ?assert(FullMemory > BaseMemory),
    %% got 362 words on 10.5.2013
    ?assert(FullMemory < BaseMemory + 800),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p ~p~n", [table_memory_full, FullMemory]),
    ?assertEqual([{imem_table_123, "A", "B", "C"}], imem_if_mnesia:read(imem_table_123, "A")),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [read_table_1]),
    ?assertEqual([{imem_table_123, "AA", "BB", "cc"}], imem_if_mnesia:read(imem_table_123, "AA")),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [read_table_2]),
    ?assertEqual([], imem_if_mnesia:read(imem_table_123, "XX")),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [read_table_3]),
    AllRecords =
        lists:sort(
            [{imem_table_123, "A", "B", "C"}, {imem_table_123, "AA", "BB", "cc"}, {imem_table_123, "AAA", "BB", "CC"}]
        ),
    AllKeys = ["A", "AA", "AAA"],
    ?assertEqual(AllRecords, lists:sort(imem_if_mnesia:read(imem_table_123))),
    ?assertEqual(AllRecords, lists:sort(imem_if_mnesia:dirty_read(imem_table_123))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [read_table_4]),
    ?assertEqual({AllRecords, true}, imem_if_mnesia:select_sort(imem_table_123, ?MatchAllRecords)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [select_all_records]),
    ?assertEqual({AllKeys, true}, imem_if_mnesia:select_sort(imem_table_123, ?MatchAllKeys)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [select_all_keys]),
    ?assertException(
        throw,
        {ClEr, {"Table does not exist", non_existing_table}},
        imem_if_mnesia:select(non_existing_table, ?MatchAllRecords)
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [select_table_no_exists]),
    MatchHead = {'$1', '$2', '$3', '$4'},
    Guard = {'==', '$3', "BB"},
    Result = {{'$3', '$4'}},
    DTupResult = lists:sort([{"BB", "cc"}, {"BB", "CC"}]),
    ?assertEqual({DTupResult, true}, imem_if_mnesia:select_sort(imem_table_123, [{MatchHead, [Guard], [Result]}])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [select_some_data1]),
    STupResult = lists:sort(["cc", "CC"]),
    ?assertEqual({STupResult, true}, imem_if_mnesia:select_sort(imem_table_123, [{MatchHead, [Guard], ['$4']}])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [select_some_data]),
    NTupResult = lists:sort([{"cc"}, {"CC"}]),
    ?assertEqual({NTupResult, true}, imem_if_mnesia:select_sort(imem_table_123, [{MatchHead, [Guard], [{{'$4'}}]}])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [select_some_data2]),
    Limit = 10,
    SelRes = imem_if_mnesia:select_sort(imem_table_123, [{MatchHead, [Guard], [{{'$4'}}]}], Limit),
    ?assertMatch({[_ | _], true}, SelRes),
    {SelList, true} = SelRes,
    ?assertEqual(NTupResult, SelList),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [select_some_data3]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:test_transactions~n", [?MODULE]),
    ct:pal(
        info,
        ?MAX_IMPORTANCE,
        ?MODULE_STRING ++ ":data in table ~p~n~p~n",
        [imem_table_123, lists:sort(imem_if_mnesia:read(imem_table_123))]
    ),
    Trig =
        fun
            (O, N, T, U, TO) ->
                imem_meta:log_to_db(
                    debug,
                    ?MODULE,
                    trigger,
                    [{table, T}, {old, O}, {new, N}, {user, U}, {tropts, TO}],
                    "trigger"
                )
        end,
    U = unknown,
    Update1 =
        fun
            (X) ->
                imem_if_mnesia:update_xt(
                    {imem_table_123, set},
                    1,
                    optimistic,
                    {imem_table_123, "AAA", "BB", "CC"},
                    {imem_table_123, "AAA", "11", X},
                    Trig,
                    U,
                    []
                ),
                imem_if_mnesia:update_xt(
                    {imem_table_123, set},
                    2,
                    optimistic,
                    {},
                    {imem_table_123, "XXX", "11", "22"},
                    Trig,
                    U,
                    []
                ),
                imem_if_mnesia:update_xt(
                    {imem_table_123, set},
                    3,
                    optimistic,
                    {imem_table_123, "AA", "BB", "cc"},
                    {},
                    Trig,
                    U,
                    []
                ),
                lists:sort(imem_if_mnesia:read(imem_table_123))
        end,
    UR1 = imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update1, ["99"])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":updated data in table ~p~n~p~n", [imem_table_123, UR1]),
    ?assertEqual(
        UR1,
        [{imem_table_123, "A", "B", "C"}, {imem_table_123, "AAA", "11", "99"}, {imem_table_123, "XXX", "11", "22"}]
    ),
    Update1a =
        fun
            (X) ->
                imem_if_mnesia:update_xt(
                    {imem_table_123, set},
                    1,
                    optimistic,
                    {imem_table_123, "AAA", "11", "99"},
                    {imem_table_123, "AAA", "BB", X},
                    Trig,
                    U,
                    []
                )
        end,
    UR1a = imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update1a, ["xx"])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":updated key ~p~n", [UR1a]),
    ?assertEqual({1, {imem_table_123, "AAA", "BB", "xx"}}, UR1a),
    ?assertEqual(ok, imem_if_mnesia:truncate_table(imem_table_123)),
    ?assertEqual(0, imem_if_mnesia:table_size(imem_table_123)),
    ?assertEqual(BaseMemory, imem_if_mnesia:table_memory(imem_table_123)),
    ?assertEqual(ok, imem_if_mnesia:drop_table(imem_table_123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_table]),
    ?assertEqual(ok, imem_if_mnesia:create_table(imem_table_bag, [a, b, c], [{type, bag}])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [create_bag_table]),
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_bag, {imem_table_bag, "A", "B", "C"})),
    ?assertEqual(1, imem_if_mnesia:table_size(imem_table_bag)),
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_bag, {imem_table_bag, "AA", "BB", "CC"})),
    ?assertEqual(2, imem_if_mnesia:table_size(imem_table_bag)),
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_bag, {imem_table_bag, "AA", "BB", "cc"})),
    ?assertEqual(3, imem_if_mnesia:table_size(imem_table_bag)),
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_bag, {imem_table_bag, "AAA", "BB", "CC"})),
    ?assertEqual(4, imem_if_mnesia:table_size(imem_table_bag)),
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_bag, {imem_table_bag, "AAA", "BB", "CC"})),
    ?assertEqual(bag, imem_if_mnesia:table_info(imem_table_bag, type)),
    ?assertEqual(4, imem_if_mnesia:table_size(imem_table_bag)),
    ct:pal(
        info,
        ?MAX_IMPORTANCE,
        ?MODULE_STRING ++ ":data in table ~p~n~p~n",
        [imem_table_bag, lists:sort(imem_if_mnesia:read(imem_table_bag))]
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [write_table]),
    Update2 =
        fun
            (X) ->
                imem_if_mnesia:update_xt(
                    {imem_table_bag, bag},
                    1,
                    optimistic,
                    {imem_table_bag, "AA", "BB", "cc"},
                    {imem_table_bag, "AA", "11", X},
                    Trig,
                    U,
                    []
                ),
                imem_if_mnesia:update_xt(
                    {imem_table_bag, bag},
                    2,
                    optimistic,
                    {},
                    {imem_table_bag, "XXX", "11", "22"},
                    Trig,
                    U,
                    []
                ),
                imem_if_mnesia:update_xt(
                    {imem_table_bag, bag},
                    3,
                    optimistic,
                    {imem_table_bag, "A", "B", "C"},
                    {},
                    Trig,
                    U,
                    []
                ),
                lists:sort(imem_if_mnesia:read(imem_table_bag))
        end,
    UR2 = imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update2, ["99"])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":updated data in table ~p~n~p~n", [imem_table_bag, UR2]),
    ?assertEqual(
        [
            {imem_table_bag, "AA", "11", "99"},
            {imem_table_bag, "AA", "BB", "CC"},
            {imem_table_bag, "AAA", "BB", "CC"},
            {imem_table_bag, "XXX", "11", "22"}
        ],
        UR2
    ),
    Update3 =
        fun
            () ->
                imem_if_mnesia:update_xt(
                    {imem_table_bag, bag},
                    1,
                    optimistic,
                    {imem_table_bag, "AA", "BB", "cc"},
                    {imem_table_bag, "AA", "11", "11"},
                    Trig,
                    U,
                    []
                )
        end,
    ?assertException(
        throw,
        {CoEx, {"Data is modified by someone else", {1, {imem_table_bag, "AA", "BB", "cc"}}}},
        imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update3))
    ),
    Update4 =
        fun
            () ->
                imem_if_mnesia:update_xt(
                    {imem_table_bag, bag},
                    1,
                    optimistic,
                    {imem_table_bag, "AA", "11", "99"},
                    {imem_table_bag, "AB", "11", "11"},
                    Trig,
                    U,
                    []
                )
        end,
    ?assertEqual(
        {1, {imem_table_bag, "AB", "11", "11"}},
        imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update4))
    ),
    ?assertEqual(ok, imem_if_mnesia:drop_table(imem_table_bag)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_table]),
    ?assertEqual(ok, imem_if_mnesia:create_table(imem_table_123, [hlk, val], [])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [imem_table_123]),
    ?assertEqual([], imem_if_mnesia:read_hlk(imem_table_123, [some_key])),
    HlkR1 = {imem_table_123, [some_key], some_value},
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, HlkR1)),
    ?assertEqual([HlkR1], imem_if_mnesia:read_hlk(imem_table_123, [some_key])),
    ?assertEqual([HlkR1], imem_if_mnesia:read_hlk(imem_table_123, [some_key, some_context])),
    ?assertEqual([], imem_if_mnesia:read_hlk(imem_table_123, [some_wrong_key])),
    HlkR2 = {imem_table_123, [some_key], some_value},
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, HlkR2)),
    ?assertEqual([HlkR2], imem_if_mnesia:read_hlk(imem_table_123, [some_key, over_context])),
    ?assertEqual([], imem_if_mnesia:read_hlk(imem_table_123, [])),
    ?assertException(
        throw,
        {ClEr, {"Table does not exist", non_existing_table}},
        imem_if_mnesia:read_hlk(non_existing_table, [some_key, over_context])
    ),
    Key = [sum],
    ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, Key, 0})),
    [spawn(fun () -> Self ! {N, read_write_test(imem_table_123, Key, N)} end) || N <- lists:seq(1, 10)],
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p", [read_write_spawned]),
    ReadWriteResult = receive_results(10, []),
    ?assertEqual(10, length(ReadWriteResult)),
    ?assertEqual([{imem_table_123, Key, 55}], imem_if_mnesia:read(imem_table_123, Key)),
    ?assertEqual([{atomic, ok}], lists:usort([R || {_, R} <- ReadWriteResult])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [bulk_read_write]),
    ?assertEqual(ok, imem_if_mnesia:drop_table(imem_table_123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_table]),
    ok.

%%====================================================================
%% Helper functions.
%%====================================================================

read_write_test(Tab, Key, N) ->
    Upd =
        fun
            () ->
                [{Tab, Key, Val}] = imem_if_mnesia:read(Tab, Key),
                imem_if_mnesia:write(Tab, {Tab, Key, Val + N})
        end,
    imem_if_mnesia:transaction(Upd).

receive_results(N, Acc) ->
    receive
        Result ->
            case N of
                1 ->
                    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result ~p", [Result]),
                    [Result | Acc];
                _ ->
                    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result ~p", [Result]),
                    receive_results(N - 1, [Result | Acc])
            end
    after
        4000 ->
            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result timeout ~p", [4000]),
            Acc
    end.
