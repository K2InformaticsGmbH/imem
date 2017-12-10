%%%-------------------------------------------------------------------
%%% File        : imem_config_ct.erl
%%% Description : Common testing imem_config.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_import_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_group/1,
    test_with_sec/1,
    test_without_sec/1
]).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include("imem_seco.hrl").

%%--------------------------------------------------------------------
%% Group related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_group(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/1 - Start ===>~n", []),

    % catch imem_meta:drop_table(import_test_3),
    % catch imem_meta:drop_table(import_test_2),
    % catch imem_meta:drop_table(import_test_1),

    ok.

%%====================================================================
%% Test Cases.
%%====================================================================

test_with_or_without_sec(IsSec) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_or_without_sec/1 - Start ===>~n", []),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

    SKey = case IsSec of
               true -> ?imem_test_admin_login();
               _ -> ok
           end,

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "~p:test_mnesia~n", [?MODULE]),

    ?assertEqual(true, is_atom(imem_meta:schema())),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "success ~p~n", [schema]),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "success ~p~n", [data_nodes]),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "~p:import from string~n", [?MODULE]),

    Imp1 = "
        table_name\n
        import_test_1\n
        field_types\n
        integer,string,tuple\n
        field_defaults\n
        $not_a_value,[],{}\n
        field_names\n
        int1,   str2,       tup3\n
        1,      \"text1\",  {1}\n
        2,      [],         \n
        3,      \"äöü\",    {a}\n
        4,      ,           \n
        ",

    ?assertEqual(ok, imem_import:create_from_string(SKey, Imp1, [], "imem", IsSec)),
    % ?assertEqual(4,  if_call_mfa(IsSec, table_size, [SKey, import_test_1])),

    % ?assertEqual(ok, imem_meta:drop_table(import_test_3)),
    % ?assertEqual(ok, imem_meta:drop_table(import_test_2)),
    % ?assertEqual(ok, imem_meta:drop_table(import_test_1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "success ~p~n", [drop_tables]),

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

