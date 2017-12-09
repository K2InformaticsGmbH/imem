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

-export([
    end_per_group/1,
    table_operations/1
]).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include("imem_if.hrl").

%%--------------------------------------------------------------------
%% Group related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_group(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/1 - Start ===>~n", []),
    catch imem_if_mnesia:drop_table(imem_table_bag),
    catch imem_if_mnesia:drop_table(imem_table_123),
    ok.

%%====================================================================
%% Test Cases.
%%====================================================================

table_operations(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":table_operations/1 - Start ===>~n", []),
    try
        ClEr = 'ClientError',
        CoEx = 'ConcurrencyException',
        Self = self(),

        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

        ?assertEqual([{c, b, a}], imem_if_mnesia:field_pick([{a, b, c}], "321")),
        ?assertEqual([{c, a}], ?FP([{a, b, c}], "31")),
        ?assertEqual([{b}], ?FP([{a, b, c}], "2")),
        ?assertEqual([{a, j}], ?FP([{a, b, c, d, e, f, g, h, i, j, k}], "1(10)")),
        ?assertEqual([{a, k}], ?FP([{a, b, c, d, e, f, g, h, i, j, k}], "1(11)")),
        ?assertEqual([{a, c}], ?FP([{a, b, c, d, e, f, g, h, i}], "13(10)")),
        ?assertEqual([{a, c}], ?FP([{a, b, c, d, e, f, g, h, i}], "1(10)3")), %% TODO: should be [{a,'N/A',c}]

        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, imem_if_mnesia:table_size(non_existing_table)),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, imem_if_mnesia:table_memory(non_existing_table)),

        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, imem_if_mnesia:read(non_existing_table)),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, imem_if_mnesia:read(non_existing_table, no_key)),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, imem_if_mnesia:write(non_existing_table, {non_existing_table, "AAA", "BB", "CC"})),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, imem_if_mnesia:dirty_write(non_existing_table, {non_existing_table, "AAA", "BB", "CC"})),
%        ?assertException(throw, {SyEx, {aborted,{bad_type,non_existing_table,{},imem_if_mnesia:write}}}, imem_if_mnesia:write(non_existing_table, {})),
        ?assertEqual(ok, imem_if_mnesia:create_table(imem_table_123, [a, b, c], [])),
        ?assertEqual(0, imem_if_mnesia:table_size(imem_table_123)),
        BaseMemory = imem_if_mnesia:table_memory(imem_table_123),
        ?assert(BaseMemory < 4000),    %% got value of 303 words x 8 bytes on 10.05.2013
        ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, "A", "B", "C"})),
        ?assertEqual(1, imem_if_mnesia:table_size(imem_table_123)),
        ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, "AA", "BB", "CC"})),
        ?assertEqual(2, imem_if_mnesia:table_size(imem_table_123)),
        ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, "AA", "BB", "cc"})),
        ?assertEqual(2, imem_if_mnesia:table_size(imem_table_123)),
        ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, "AAA", "BB", "CC"})),
        ?assertEqual(3, imem_if_mnesia:table_size(imem_table_123)),
        ?assertEqual(ok, imem_if_mnesia:dirty_write(imem_table_123, {imem_table_123, "AAA", "BB", "CC"})),
        ?assertEqual(3, imem_if_mnesia:table_size(imem_table_123)),
        FullMemory = imem_if_mnesia:table_memory(imem_table_123),
        ?assert(FullMemory > BaseMemory),
        ?assert(FullMemory < BaseMemory + 800),  %% got 362 words on 10.5.2013
        ?assertEqual([{imem_table_123, "A", "B", "C"}], imem_if_mnesia:read(imem_table_123, "A")),
        ?assertEqual([{imem_table_123, "AA", "BB", "cc"}], imem_if_mnesia:read(imem_table_123, "AA")),
        ?assertEqual([], imem_if_mnesia:read(imem_table_123, "XX")),
        AllRecords = lists:sort([{imem_table_123, "A", "B", "C"}, {imem_table_123, "AA", "BB", "cc"}, {imem_table_123, "AAA", "BB", "CC"}]),
        AllKeys = ["A", "AA", "AAA"],
        ?assertEqual(AllRecords, lists:sort(imem_if_mnesia:read(imem_table_123))),
        ?assertEqual(AllRecords, lists:sort(imem_if_mnesia:dirty_read(imem_table_123))),
        ?assertEqual({AllRecords, true}, imem_if_mnesia:select_sort(imem_table_123, ?MatchAllRecords)),
        ?assertEqual({AllKeys, true}, imem_if_mnesia:select_sort(imem_table_123, ?MatchAllKeys)),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, imem_if_mnesia:select(non_existing_table, ?MatchAllRecords)),
        MatchHead = {'$1', '$2', '$3', '$4'},
        Guard = {'==', '$3', "BB"},
        Result = {{'$3', '$4'}},
        DTupResult = lists:sort([{"BB", "cc"}, {"BB", "CC"}]),
        ?assertEqual({DTupResult, true}, imem_if_mnesia:select_sort(imem_table_123, [{MatchHead, [Guard], [Result]}])),
        STupResult = lists:sort(["cc", "CC"]),
        ?assertEqual({STupResult, true}, imem_if_mnesia:select_sort(imem_table_123, [{MatchHead, [Guard], ['$4']}])),
        NTupResult = lists:sort([{"cc"}, {"CC"}]),
        ?assertEqual({NTupResult, true}, imem_if_mnesia:select_sort(imem_table_123, [{MatchHead, [Guard], [{{'$4'}}]}])),
        Limit = 10,
        SelRes = imem_if_mnesia:select_sort(imem_table_123, [{MatchHead, [Guard], [{{'$4'}}]}], Limit),
        ?assertMatch({[_ | _], true}, SelRes),
        {SelList, true} = SelRes,
        ?assertEqual(NTupResult, SelList),

        Trig = fun(O, N, T, U, TO) ->
            imem_meta:log_to_db(debug, ?MODULE, trigger, [{table, T}, {old, O}, {new, N}, {user, U}, {tropts, TO}], "trigger") end,
        U = unknown,
        Update1 = fun(X) ->
            imem_if_mnesia:update_xt({imem_table_123, set}, 1, optimistic, {imem_table_123, "AAA", "BB", "CC"}, {imem_table_123, "AAA", "11", X}, Trig, U, []),
            imem_if_mnesia:update_xt({imem_table_123, set}, 2, optimistic, {}, {imem_table_123, "XXX", "11", "22"}, Trig, U, []),
            imem_if_mnesia:update_xt({imem_table_123, set}, 3, optimistic, {imem_table_123, "AA", "BB", "cc"}, {}, Trig, U, []),
            lists:sort(imem_if_mnesia:read(imem_table_123))
                  end,
        UR1 = imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update1, ["99"])),
        ?assertEqual(UR1, [{imem_table_123, "A", "B", "C"}, {imem_table_123, "AAA", "11", "99"}, {imem_table_123, "XXX", "11", "22"}]),

        Update1a = fun(X) ->
            imem_if_mnesia:update_xt({imem_table_123, set}, 1, optimistic, {imem_table_123, "AAA", "11", "99"}, {imem_table_123, "AAA", "BB", X}, Trig, U, [])
                   end,
        UR1a = imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update1a, ["xx"])),
        ?assertEqual({1, {imem_table_123, "AAA", "BB", "xx"}}, UR1a),


        ?assertEqual(ok, imem_if_mnesia:truncate_table(imem_table_123)),
        ?assertEqual(0, imem_if_mnesia:table_size(imem_table_123)),
        ?assertEqual(BaseMemory, imem_if_mnesia:table_memory(imem_table_123)),

        ?assertEqual(ok, imem_if_mnesia:drop_table(imem_table_123)),

        ?assertEqual(ok, imem_if_mnesia:create_table(imem_table_bag, [a, b, c], [{type, bag}])),

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

        Update2 = fun(X) ->
            imem_if_mnesia:update_xt({imem_table_bag, bag}, 1, optimistic, {imem_table_bag, "AA", "BB", "cc"}, {imem_table_bag, "AA", "11", X}, Trig, U, []),
            imem_if_mnesia:update_xt({imem_table_bag, bag}, 2, optimistic, {}, {imem_table_bag, "XXX", "11", "22"}, Trig, U, []),
            imem_if_mnesia:update_xt({imem_table_bag, bag}, 3, optimistic, {imem_table_bag, "A", "B", "C"}, {}, Trig, U, []),
            lists:sort(imem_if_mnesia:read(imem_table_bag))
                  end,
        UR2 = imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update2, ["99"])),
        ?assertEqual([{imem_table_bag, "AA", "11", "99"}, {imem_table_bag, "AA", "BB", "CC"}, {imem_table_bag, "AAA", "BB", "CC"}, {imem_table_bag, "XXX", "11", "22"}], UR2),

        Update3 = fun() ->
            imem_if_mnesia:update_xt({imem_table_bag, bag}, 1, optimistic, {imem_table_bag, "AA", "BB", "cc"}, {imem_table_bag, "AA", "11", "11"}, Trig, U, [])
                  end,
        ?assertException(throw, {CoEx, {"Data is modified by someone else", {1, {imem_table_bag, "AA", "BB", "cc"}}}}, imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update3))),

        Update4 = fun() ->
            imem_if_mnesia:update_xt({imem_table_bag, bag}, 1, optimistic, {imem_table_bag, "AA", "11", "99"}, {imem_table_bag, "AB", "11", "11"}, Trig, U, [])
                  end,
        ?assertEqual({1, {imem_table_bag, "AB", "11", "11"}}, imem_if_mnesia:return_atomic(imem_if_mnesia:transaction(Update4))),

        ?assertEqual(ok, imem_if_mnesia:drop_table(imem_table_bag)),

        ?assertEqual(ok, imem_if_mnesia:create_table(imem_table_123, [hlk, val], [])),
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

        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, imem_if_mnesia:read_hlk(non_existing_table, [some_key, over_context])),

        Key = [sum],
        ?assertEqual(ok, imem_if_mnesia:write(imem_table_123, {imem_table_123, Key, 0})),
        [spawn(fun() ->
            Self ! {N, read_write_test(imem_table_123, Key, N)} end) || N <- lists:seq(1, 10)],
        ReadWriteResult = receive_results(10, []),
        ?assertEqual(10, length(ReadWriteResult)),
        ?assertEqual([{imem_table_123, Key, 55}], imem_if_mnesia:read(imem_table_123, Key)),
        ?assertEqual([{atomic, ok}], lists:usort([R || {_, R} <- ReadWriteResult])),

        ?assertEqual(ok, imem_if_mnesia:drop_table(imem_table_123)),

        ok
    catch
        Class:Reason ->
            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
            throw({Class, Reason})
    end,
    ok.

%%====================================================================
%% Helper functions.
%%====================================================================

read_write_test(Tab, Key, N) ->
    Upd = fun() ->
        [{Tab, Key, Val}] = imem_if_mnesia:read(Tab, Key),
        imem_if_mnesia:write(Tab, {Tab, Key, Val + N})
          end,
    imem_if_mnesia:transaction(Upd).

receive_results(N, Acc) ->
    receive
        Result ->
            case N of
                1 ->
                    % ?LogDebug("Result ~p", [Result]),
                    [Result | Acc];
                _ ->
                    % ?LogDebug("Result ~p", [Result]),
                    receive_results(N - 1, [Result | Acc])
            end
    after 4000 ->
        ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "Result timeout ~p", [4000]),
        Acc
    end.
