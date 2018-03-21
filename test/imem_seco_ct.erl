%%%-------------------------------------------------------------------
%%% File        : imem_seco_ct.erl
%%% Description : Common testing imem_seco.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_seco_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_testcase/2,
    test/1
]).

-define(NODEBUG, true).
-define(CONFIG_TABLE_OPTS, [{record_name, ddConfig}, {type, ordered_set}]).

-include_lib("imem.hrl").
-include("imem_seco.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_testcase(TestCase, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_testcase/2 - Start(~p) ===>~n", [TestCase]),

    SKey = ?imem_test_admin_login(),
    catch imem_account:delete(SKey, <<"test">>),
    catch imem_account:delete(SKey, <<"test_admin">>),
    catch imem_role:delete(SKey, table_creator),
    catch imem_role:delete(SKey, test_role),
    catch imem_seco:logout(SKey),
    catch imem_meta:drop_table(user_table_123),

    ok.

%%====================================================================
%% Test cases.
%%====================================================================

test(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test/1 - Start ===>~n", []),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:test_database~n", [?MODULE]),

    Seco0 = imem_meta:table_size(ddSeCo@),
    Perm0 = imem_meta:table_size(ddPerm@),
    ?assert(0 =< imem_meta:table_size(ddSeCo@)),
    ?assert(0 =< imem_meta:table_size(ddPerm@)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [minimum_table_sizes]),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:test_admin_login~n", [?MODULE]),

    SeCoAdmin0 = ?imem_test_admin_login(),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [test_admin_login]),

    Seco1 = imem_meta:table_size(ddSeCo@),
    Perm1 = imem_meta:table_size(ddPerm@),
    ?assertEqual(Seco0 + 1, Seco1),
    ?assertEqual(Perm0, Perm1),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [status1]),
    Seco2 = imem_sec:table_size(SeCoAdmin0, ddSeCo@),
    Perm2 = imem_sec:table_size(SeCoAdmin0, ddPerm@),
    ?assertEqual(Seco0 + 1, Seco2),
    ?assertEqual(Perm0 + 2, Perm2),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [status1]),

    imem_seco ! {'DOWN', simulated_reference, process, self(), simulated_exit},
    timer:sleep(2000),
    Seco3 = imem_meta:table_size(ddSeCo@),
    Perm3 = imem_meta:table_size(ddPerm@),
    ?assertEqual(Seco0, Seco3),
    ?assertEqual(Perm0, Perm3),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [status2]),

    ?assertEqual(<<"+41794321750">>, imem_seco:normalized_msisdn(<<"+41794321750">>)),
    ?assertEqual(<<"+41794321750">>, imem_seco:normalized_msisdn(<<"+41 (0)79 432 17 50">>)),
    ?assertEqual(<<"+41794321750">>, imem_seco:normalized_msisdn(<<"4179-432-17-50">>)),
    ?assertEqual(<<"+41794321750">>, imem_seco:normalized_msisdn(<<"079 432 17 50">>)),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [normalized_msisdn]),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:test_imem_seco~n", [?MODULE]),

    ok.
