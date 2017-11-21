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
    meta_concurrency/1,
    meta_operations/1,
    meta_partitions/1,
    meta_preparations/1,
    skvh_concurrency/1,
    skvh_operations/1
]).

-define(NODEBUG, true).
-include_lib("imem.hrl").

-define(AUDIT_SUFFIX, "Audit_86400@_").
-define(AUDIT(__Channel), binary_to_list(__Channel) ++ ?AUDIT_SUFFIX).
-define(Channels, [<<"skvhTest", N>> || N <- lists:seq($0, $9)]).
-define(HIST_SUFFIX, "Hist").
-define(HIST(__Channel), binary_to_list(__Channel) ++ ?HIST_SUFFIX).
-define(TPTEST0, tpTest_1000@).
-define(TPTEST1, tpTest1_999999999@_).
-define(TPTEST2, tpTest_100@).

%%--------------------------------------------------------------------
%% Returns the list of all test cases and test case groups
%% in the test suite module to be executed.
%%--------------------------------------------------------------------

all() ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":all/0 - Start ===>~n", []),
    [
        {group, imem_dal_skvh},
        {group, imem_meta}
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
    Config.

end_per_suite(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_suite/1 - Start ===>~n", []),
    ok.

%%--------------------------------------------------------------------
% Defines test case groups.
%%--------------------------------------------------------------------

groups() ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":groups/0 - Start ===>~n", []),
    [
        {
            imem_dal_skvh, [],
            [
                skvh_concurrency,
                skvh_operations
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
        }
    ].

%%--------------------------------------------------------------------
%% Group related setup and teardown functions.
%%--------------------------------------------------------------------

init_per_group(imem_dal_skvh = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":init_per_group/2 - ~p - Start ===>~n", [Group]),
    ?imem_test_setup,
    catch imem_meta:drop_table(mapChannel),
    catch imem_meta:drop_table(lstChannel),
    catch imem_meta:drop_table(binChannel),
    catch imem_meta:drop_table(noOptsChannel),
    catch imem_meta:drop_table(noHistoryHChannel),
    catch imem_meta:drop_table(skvhTest),
    catch imem_meta:drop_table(skvhTestAudit_86400@_),
    catch imem_meta:drop_table(skvhTestHist),
    [begin
         catch imem_meta:drop_table(binary_to_atom(imem_dal_skvh:table_name(Ch), utf8)),
         catch imem_meta:drop_table(list_to_atom(?AUDIT(Ch))),
         catch imem_meta:drop_table(list_to_atom(?HIST(Ch)))
     end
        || Ch <- ?Channels
    ],
    timer:sleep(50),
    Config;
init_per_group(imem_meta = Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":init_per_group/2 - ~p - Start ===>~n", [Group]),
    ?imem_test_setup,
    Config;
init_per_group(_Group, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":init_per_group/2 - ~p - Start ===>~n", [_Group]),
    Config.

end_per_group(imem_dal_skvh = Group, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
    catch imem_meta:drop_table(mapChannel),
    catch imem_meta:drop_table(lstChannel),
    catch imem_meta:drop_table(binChannel),
    catch imem_meta:drop_table(noOptsChannel),
    catch imem_meta:drop_table(noHistoryHChannel),
    catch imem_meta:drop_table(skvhTest),
    catch imem_meta:drop_table(skvhTestAudit_86400@_),
    catch imem_meta:drop_table(skvhTestHist),
    ?imem_test_teardown,
    ok;
end_per_group(imem_meta = Group, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [Group]),
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
    ok;
end_per_group(_Group, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/2 - ~p - Start ===>~n", [_Group]),
    ok.

%%====================================================================
%% Test Cases.
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

skvh_concurrency(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":skvh_concurrency/1 - Start ===>~n", []),
    imem_dal_skvh_ct:skvh_concurrency(Config).

skvh_operations(Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":skvh_operations/1 - Start ===>~n", []),
    imem_dal_skvh_ct:skvh_operations(Config).
