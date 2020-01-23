%%%-------------------------------------------------------------------
%%% File        : imem_sql_ct.erl
%%% Description : Common testing imem_sql.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_sql_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    test_with_sec/1,
    test_without_sec/1
]).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include("imem_seco.hrl").
-include("imem_ct.hrl").

%%====================================================================
%% Test cases.
%%====================================================================

test_with_or_without_sec(IsSec) ->
    ?CTPAL("Start ~p", [IsSec]),

    ?CTPAL("schema ~p", [imem_meta:schema()]),
    ?CTPAL("data_nodes ~p", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

    SKey = case IsSec of
               true -> ?imem_test_admin_login();
               _ -> ok
           end,

    ?assertEqual("", imem_sql:escape_sql("")),
    ?assertEqual(<<"">>, imem_sql:escape_sql(<<"">>)),
    ?assertEqual("abc", imem_sql:escape_sql("abc")),
    ?assertEqual(<<"abc">>, imem_sql:escape_sql(<<"abc">>)),
    ?assertEqual("''abc", imem_sql:escape_sql("'abc")),
    ?assertEqual(<<"''abc">>, imem_sql:escape_sql(<<"'abc">>)),
    ?assertEqual("ab''c", imem_sql:escape_sql("ab'c")),
    ?assertEqual(<<"ab''c">>, imem_sql:escape_sql(<<"ab'c">>)),
    ?assertEqual(<<"''ab''''c''">>, imem_sql:escape_sql(<<"'ab''c'">>)),

    ?assertEqual("", imem_sql:un_escape_sql("")),
    ?assertEqual(<<"">>, imem_sql:un_escape_sql(<<"">>)),
    ?assertEqual("abc", imem_sql:un_escape_sql("abc")),
    ?assertEqual(<<"abc">>, imem_sql:un_escape_sql(<<"abc">>)),
    ?assertEqual("'abc", imem_sql:un_escape_sql("'abc")),
    ?assertEqual(<<"'abc">>, imem_sql:un_escape_sql(<<"'abc">>)),
    ?assertEqual("ab'c", imem_sql:un_escape_sql("ab''c")),
    ?assertEqual(<<"ab'c">>, imem_sql:un_escape_sql(<<"ab''c">>)),
    ?assertEqual(<<"'ab'c'">>, imem_sql:un_escape_sql(<<"'ab''c'">>)),

    ?CTPAL("~p:test_mnesia", [?MODULE]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?CTPAL("success ~p", [schema]),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    ?CTPAL("success ~p", [data_nodes]),

    ?CTPAL("~p:test_database_operations~n", [?MODULE]),
    _Types1 = [#ddColumn{name = a, type = char, len = 1}     %% key
        , #ddColumn{name = b1, type = char, len = 1}    %% value 1
        , #ddColumn{name = c1, type = char, len = 1}    %% value 2
    ],
    _Types2 = [#ddColumn{name = a, type = integer, len = 10}    %% key
        , #ddColumn{name = b2, type = float, len = 8, prec = 3}   %% value
    ],

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "create table meta_table_1 (a char, b1 char, c1 char);", 0, [{schema, imem}], IsSec)),
    ?assertEqual(0, if_call_mfa(IsSec, table_size, [SKey, meta_table_1])),

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "create table meta_table_2 (a integer, b2 float);", 0, [{schema, imem}], IsSec)),
    ?assertEqual(0, if_call_mfa(IsSec, table_size, [SKey, meta_table_2])),

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "create table meta_table_3 (a char, b3 integer, c1 char);", 0, [{schema, imem}], IsSec)),
    ?assertEqual(0, if_call_mfa(IsSec, table_size, [SKey, meta_table_1])),
    ?CTPAL("success ~p", [create_tables]),

    ?assertEqual(ok, imem_meta:drop_table(meta_table_3)),
    ?assertEqual(ok, imem_meta:drop_table(meta_table_2)),
    ?assertEqual(ok, imem_meta:drop_table(meta_table_1)),
    ?CTPAL("success ~p", [drop_tables]),

    case IsSec of
        true -> ?imem_logout(SKey);
        _ -> ok
    end,

    ok.

test_with_sec(_Config) ->
    ?CTPAL("Start"),

    test_with_or_without_sec(true).

test_without_sec(_Config) ->
    ?CTPAL("Start"),

    test_with_or_without_sec(false).


%%====================================================================
%% Helper functions.
%%====================================================================

if_call_mfa(IsSec, Fun, Args) ->
    case IsSec of
        true -> apply(imem_sec, Fun, Args);
        _ -> apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.
