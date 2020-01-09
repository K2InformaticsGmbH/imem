%%%-------------------------------------------------------------------
%%% File        : imem_import_ct.erl
%%% Description : Common testing imem_import.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_import_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([end_per_testcase/2, test_with_sec/1, test_without_sec/1]).

-define(NODEBUG, true).

-include_lib("imem.hrl").

-include("imem_seco.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_testcase(TestCase, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_testcase/2 - Start(~p) ===>~n", [TestCase]),
    % catch imem_meta:drop_table(import_test_3),
    % catch imem_meta:drop_table(import_test_2),
    % catch imem_meta:drop_table(import_test_1),
    ok.

%%====================================================================
%% Test cases.
%%====================================================================

test_with_or_without_sec(IsSec) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_or_without_sec/1 - Start(~p) ===>~n", [IsSec]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    SKey =
        case IsSec of
            true -> ?imem_test_admin_login();
            _ -> ok
        end,
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:test_mnesia~n", [?MODULE]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [schema]),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [data_nodes]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:import from string~n", [?MODULE]),
    Imp1 =
        "\n        table_name\n\n        import_test_1\n\n        field_types\n\n        integer,string,tuple\n\n        field_defaults\n\n        $not_a_value,[],{}\n\n        field_names\n\n        int1,   str2,       tup3\n\n        1,      \"text1\",  {1}\n\n        2,      [],         \n\n        3,      \"äöü\",    {a}\n\n        4,      ,           \n\n        ",
    ?assertEqual(ok, imem_import:create_from_string(SKey, Imp1, [], "imem", IsSec)),
    % ?assertEqual(4,  if_call_mfa(IsSec, table_size, [SKey, import_test_1])),
    % ?assertEqual(ok, imem_meta:drop_table(import_test_3)),
    % ?assertEqual(ok, imem_meta:drop_table(import_test_2)),
    % ?assertEqual(ok, imem_meta:drop_table(import_test_1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_tables]),
    case IsSec of
        true -> ?imem_logout(SKey);
        _ -> ok
    end,
    ok.

test_with_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_sec/1 - Start ===>~n", []),
    test_with_or_without_sec(true).

test_without_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_without_sec/1 - Start ===>~n", []),
    test_with_or_without_sec(false).
