%%%-------------------------------------------------------------------
%%% File        : imem_sql_expr_ct.erl
%%% Description : Common testing imem_sql_expr.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_sql_expr_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_testcase/2,
    test_with_sec/1,
    test_without_sec/1
]).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include("imem_seco.hrl").
-include("imem_sql.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_testcase(TestCase, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_testcase/2 - Start(~p) ===>~n", [TestCase]),

    catch imem_meta:drop_table(meta_table_3),
    catch imem_meta:drop_table(meta_table_2),
    catch imem_meta:drop_table(meta_table_1),

    ok.

%%====================================================================
%% Test cases.
%%====================================================================

test_with_or_without_sec(IsSec) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_or_without_sec/1 - Start(~p) ===>~n", [IsSec]),

    ClEr = 'ClientError',
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

    % field names
    ?assertEqual({undefined, undefined, <<"field">>}, imem_sql_expr:binstr_to_qname3(<<"field">>)),
    ?assertEqual({undefined, <<"table">>, <<"field">>}, imem_sql_expr:binstr_to_qname3(<<"table.field">>)),
    ?assertEqual({<<"schema">>, <<"table">>, <<"field">>}, imem_sql_expr:binstr_to_qname3(<<"schema.table.field">>)),

    ?assertEqual(<<"field">>, imem_sql_expr:qname3_to_binstr(imem_sql_expr:binstr_to_qname3(<<"field">>))),
    ?assertEqual(<<"table.field">>, imem_sql_expr:qname3_to_binstr(imem_sql_expr:binstr_to_qname3(<<"table.field">>))),
    ?assertEqual(<<"schema.table.field">>, imem_sql_expr:qname3_to_binstr(imem_sql_expr:binstr_to_qname3(<<"schema.table.field">>))),

    ?assertEqual(true, is_atom(imem_meta:schema())),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [schema]),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [data_nodes]),

    %% uses_filter
    ?assertEqual(true, imem_sql_expr:uses_filter({'is_member', {'+', '$2', 1}, '$3'})),
    ?assertEqual(false, imem_sql_expr:uses_filter({'==', {'+', '$2', 1}, '$3'})),
    ?assertEqual(true, imem_sql_expr:uses_filter({'==', {'safe_integer', {'+', '$2', 1}}, '$3'})),
    ?assertEqual(false, imem_sql_expr:uses_filter({'or', {'==', '$2', 1}, {'==', '$3', 1}})),
    ?assertEqual(true, imem_sql_expr:uses_filter({'and', {'==', '$2', 1}, {'is_member', 1, '$3'}})),

    % BTreeSample = 
    %     {'>',{ bind,2,7,<<"imem">>,<<"ddAccount">>,<<"ddAccount">>,<<"lastLoginTime">>,
    %            datetime,undefined,undefined,undefined,false,undefined,undefined,undefined,'$27'}
    %         ,{ bind,0,0,undefined,undefined,undefined,undefined,datetime,0,0,undefined,false,undefined,undefined
    %             , {add_dt, {bind,1,4,<<"imem">>,<<"meta">>,<<"meta">>,<<"sysdate">>,
    %                         datetime,20,0,undefined,true,undefined,undefined,undefined,'$14'}
    %                      , {'-', {bind,0,0,undefined,undefined,undefined,undefined,
    %                               float,0,0,undefined,true,undefined,undefined,1.1574074074074073e-5,[]}
    %                        }
    %               }
    %             ,[]
    %         }
    %     },
    % ?assertEqual(true, uses_bind(2,7,BTreeSample)),
    % ?assertEqual(false, uses_bind(2,6,BTreeSample)),
    % ?assertEqual(true, uses_bind(1,4,BTreeSample)),
    % ?assertEqual(true, uses_bind(0,0,BTreeSample)),

    ColMapSample = {bind, 0, 0, undefined, undefined, <<"'a' || 'b123'">>, undefined, binstr, 0, 0, undefined, false, undefined
        , {'||', <<"'a'">>, <<"'b123'">>}
        , {concat, {bind, 0, 0, undefined, undefined, undefined, undefined, binstr, 0, 0, <<>>, true, undefined, undefined, <<"a">>, []}
            , {bind, 0, 0, undefined, undefined, undefined, undefined, binstr, 0, 0, <<>>, true, undefined, undefined, <<"b123">>, []}
        }
        , 1
    },
    ColMapExpected = {bind, 0, 0, undefined, undefined, <<"'a' || 'b123'">>, undefined, binstr, 0, 0, undefined, false, undefined
        , {'||', <<"'a'">>, <<"'b123'">>}
        , <<"ab123">>
        , 1
    },
    ?assertEqual(ColMapExpected, imem_sql_expr:bind_subtree_const(ColMapSample)),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:test_database_operations~n", [?MODULE]),
    _Types1 = [#ddColumn{name = a, type = char, len = 1}     %% key
        , #ddColumn{name = b1, type = char, len = 1}    %% value 1
        , #ddColumn{name = c1, type = char, len = 1}    %% value 2
    ],
    _Types2 = [#ddColumn{name = a, type = integer, len = 10}    %% key
        , #ddColumn{name = b2, type = float, len = 8, prec = 3}   %% value
    ],

    ?assertMatch({ok, _}, imem_sql:exec(anySKey, "create table meta_table_1 (a char, b1 char, c1 char);", 0, "imem", IsSec)),
    ?assertEqual(0, if_call_mfa(IsSec, table_size, [anySKey, meta_table_1])),

    ?assertMatch({ok, _}, imem_sql:exec(anySKey, "create table meta_table_2 (a integer, b2 float);", 0, "imem", IsSec)),
    ?assertEqual(0, if_call_mfa(IsSec, table_size, [anySKey, meta_table_2])),

    ?assertMatch({ok, _}, imem_sql:exec(anySKey, "create table meta_table_3 (a char, b3 integer, c1 char);", 0, "imem", IsSec)),
    ?assertEqual(0, if_call_mfa(IsSec, table_size, [anySKey, meta_table_1])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [create_tables]),

    Table1 = <<"imem.meta_table_1">>,
    Table2 = <<"meta_table_2">>,
    Table3 = <<"meta_table_3">>,
    TableX = {as, <<"meta_table_x">>, <<"meta_table_1">>},

    Alias1 = {as, <<"meta_table_1">>, <<"alias1">>},
    Alias2 = {as, <<"imem.meta_table_1">>, <<"alias2">>},

    ?assertException(throw, {ClEr, {"Table does not exist", {imem, meta_table_x}}}, imem_sql_expr:column_map_tables([Table1, TableX, Table3], [], [])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [table_no_exists]),

    FullMap0 = imem_sql_expr:column_map_tables([], imem_meta:meta_field_list(), []),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":FullMap0~n~p~n", [FullMap0]),
    MetaFieldCount = length(imem_meta:meta_field_list()),
    ?assertEqual(MetaFieldCount, length(FullMap0)),

    FullMap1 = imem_sql_expr:column_map_tables([Table1], imem_meta:meta_field_list(), []),
    ?assertEqual(MetaFieldCount + 3, length(FullMap1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [full_map_1]),

    FullMap13 = imem_sql_expr:column_map_tables([Table1, Table3], imem_meta:meta_field_list(), []),
    ?assertEqual(MetaFieldCount + 6, length(FullMap13)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [full_map_13]),

    FullMap123 = imem_sql_expr:column_map_tables([Table1, Table2, Table3], imem_meta:meta_field_list(), []),
    ?assertEqual(MetaFieldCount + 8, length(FullMap123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [full_map_123]),

    AliasMap1 = imem_sql_expr:column_map_tables([Alias1], imem_meta:meta_field_list(), []),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":AliasMap1~n~p~n", [AliasMap1]),
    ?assertEqual(MetaFieldCount + 3, length(AliasMap1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [alias_map_1]),

    AliasMap123 = imem_sql_expr:column_map_tables([Alias1, Alias2, Table3], imem_meta:meta_field_list(), []),
    %% select from 
    %%            meta_table_1 as alias1        (a char, b1 char    , c1 char)
    %%          , imem.meta_table1 as alias2    (a char, b1 char    , c1 char)
    %%          , meta_table_3                  (a char, b3 integer , c1 char)
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":AliasMap123~n~p~n", [AliasMap123]),
    ?assertEqual(MetaFieldCount + 9, length(AliasMap123)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [alias_map_123]),

    % ColsE1=     [ #bind{tag="A1", schema= <<"imem">>, table= <<"meta_table_1">>, name= <<"a">>}
    %             , #bind{tag="A2", name= <<"x">>}
    %             , #bind{tag="A3", name= <<"c1">>}
    %             ],
    ColsE1 = [<<"imem.meta_table_1.a">>
        , <<"x">>
        , <<"c1">>
    ],

    ?assertException(throw, {ClEr, {"Unknown field or table name", <<"x">>}}, imem_sql_expr:column_map_columns(ColsE1, FullMap1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [unknown_column_name_1]),

    % ColsE2=     [ #bind{tag="A1", schema= <<"imem">>, table= <<"meta_table_1">>, name= <<"a">>}
    %             , #bind{tag="A2", table= <<"meta_table_x">>, name= <<"b1">>}
    %             , #bind{tag="A3", name= <<"c1">>}
    %             ],
    ColsE2 = [<<"imem.meta_table_1.a">>
        , <<"meta_table_x.b1">>
        , <<"c1">>
    ],

    ?assertException(throw, {ClEr, {"Unknown field or table name", <<"meta_table_x.b1">>}}, imem_sql_expr:column_map_columns(ColsE2, FullMap1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [unknown_column_name_2]),

    % ColsF =     [ {as, <<"imem.meta_table_1.a">>, <<"a">>}
    %             , {as, <<"meta_table_1.b1">>, <<"b1">>}
    %             , {as, <<"c1">>, <<"c1">>}
    %             ],

    ColsA = [{as, <<"imem.meta_table_1.a">>, <<"a">>}
        , {as, <<"meta_table_1.b1">>, <<"b1">>}
        , {as, <<"c1">>, <<"c1">>}
    ],

    ?assertException(throw, {ClEr, {"Ambiguous field or table name", <<"a">>}}, imem_sql_expr:column_map_columns([<<"a">>], FullMap13)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [columns_ambiguous_a]),

    ?assertException(throw, {ClEr, {"Ambiguous field or table name", <<"c1">>}}, imem_sql_expr:column_map_columns(ColsA, FullMap13)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [columns_ambiguous_c1]),

    ?assertEqual(3, length(imem_sql_expr:column_map_columns(ColsA, FullMap1))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [columns_A]),

    ?assertEqual(6, length(imem_sql_expr:column_map_columns([<<"*">>], FullMap13))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [columns_13_join]),

    Cmap3 = imem_sql_expr:column_map_columns([<<"*">>], FullMap123),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":ColMap3 ~p~n", [Cmap3]),
    ?assertEqual(8, length(Cmap3)),
    ?assertEqual(lists:sort(Cmap3), Cmap3),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [columns_123_join]),


    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":AliasMap1~n~p~n", [AliasMap1]),

    Abind1 = imem_sql_expr:column_map_columns([<<"*">>], AliasMap1),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":AliasBind1~n~p~n", [Abind1]),

    Abind2 = imem_sql_expr:column_map_columns([<<"alias1.*">>], AliasMap1),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":AliasBind2~n~p~n", [Abind2]),
    ?assertEqual(Abind1, Abind2),

    Abind3 = imem_sql_expr:column_map_columns([<<"imem.alias1.*">>], AliasMap1),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":AliasBind3~n~p~n", [Abind3]),
    ?assertEqual(Abind1, Abind3),

    ?assertEqual(3, length(Abind1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [alias_1]),

    ?assertEqual(9, length(imem_sql_expr:column_map_columns([<<"*">>], AliasMap123))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [alias_113_join]),

    ?assertEqual(3, length(imem_sql_expr:column_map_columns([<<"meta_table_3.*">>], AliasMap123))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [columns_113_star1]),

    ?assertEqual(4, length(imem_sql_expr:column_map_columns([<<"alias1.*">>, <<"meta_table_3.a">>], AliasMap123))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [columns_alias_1]),

    ?assertEqual(2, length(imem_sql_expr:column_map_columns([<<"alias1.a">>, <<"alias2.a">>], AliasMap123))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [columns_alias_2]),

    ?assertEqual(2, length(imem_sql_expr:column_map_columns([<<"alias1.a">>, <<"sysdate">>], AliasMap1))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [sysdate]),

    ?assertException(throw, {ClEr, {"Unknown field or table name", <<"any.sysdate">>}}, imem_sql_expr:column_map_columns([<<"alias1.a">>, <<"any.sysdate">>], AliasMap1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [sysdate_reject]),

    ColsFS = [#bind{tag = "A", tind = 1, cind = 1, schema = <<"imem">>, table = <<"meta_table_1">>, name = <<"a">>, type = integer, alias = <<"a">>}
        , #bind{tag = "B", tind = 1, cind = 2, table = <<"meta_table_1">>, name = <<"b1">>, type = string, alias = <<"b1">>}
        , #bind{tag = "C", tind = 1, cind = 3, name = <<"c1">>, type = ipaddr, alias = <<"c1">>}
    ],

    ?assertEqual([], imem_sql_expr:filter_spec_where(?NoFilter, ColsFS, [])),
    ?assertEqual({wt}, imem_sql_expr:filter_spec_where(?NoFilter, ColsFS, {wt})),
    FA1 = {1, [<<"$in$">>, <<"111">>]},
    CA1 = {'=', <<"imem.meta_table_1.a">>, <<"111">>},
    ?assertEqual({'and', CA1, {wt}}, imem_sql_expr:filter_spec_where({'or', [FA1]}, ColsFS, {wt})),
    FB2 = {2, [<<"$in$">>, <<"222">>]},
    CB2 = {'=', <<"meta_table_1.b1">>, {'fun', <<"to_string">>, [<<"'222'">>]}},
    ?assertEqual({'and', {'and', CA1, CB2}, {wt}}, imem_sql_expr:filter_spec_where({'and', [FA1, FB2]}, ColsFS, {wt})),
    FC3 = {3, [<<"$in$">>, <<"3.1.2.3">>, <<"3.3.2.1">>]},
    CC3 = {'in', <<"c1">>, {'list', [{'fun', <<"to_ipaddr">>, [<<"'3.1.2.3'">>]}, {'fun', <<"to_ipaddr">>, [<<"'3.3.2.1'">>]}]}},
    ?assertEqual({'and', {'or', {'or', CA1, CB2}, CC3}, {wt}}, imem_sql_expr:filter_spec_where({'or', [FA1, FB2, FC3]}, ColsFS, {wt})),
    ?assertEqual({'and', {'and', {'and', CA1, CB2}, CC3}, {wt}}, imem_sql_expr:filter_spec_where({'and', [FA1, FB2, FC3]}, ColsFS, {wt})),

    FB2a = {2, [<<"$in$">>, <<"22'2">>]},
    CB2a = {'=', <<"meta_table_1.b1">>, {'fun', <<"to_string">>, [<<"'22''2'">>]}},
    ?assertEqual({'and', {'and', CA1, CB2a}, {wt}}, imem_sql_expr:filter_spec_where({'and', [FA1, FB2a]}, ColsFS, {wt})),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [filter_spec_where]),

    ?assertEqual([], imem_sql_expr:sort_spec_order([], ColsFS, ColsFS)),
    SA = {1, 1, <<"desc">>},
    OA = {<<"a.a">>, <<"desc">>}, %% bad test setup FullMap alias
    ?assertEqual([OA], imem_sql_expr:sort_spec_order([SA], ColsFS, ColsFS)),
    SB = {1, 2, <<"asc">>},
    OB = {<<"b1.b1">>, <<"asc">>}, %% bad test setup FullMap alias
    ?assertEqual([OB], imem_sql_expr:sort_spec_order([SB], ColsFS, ColsFS)),
    SC = {1, 3, <<"desc">>},
    OC = {<<"c1.c1">>, <<"desc">>}, %% bad test setup FullMap alias
    ?assertEqual([OC], imem_sql_expr:sort_spec_order([SC], ColsFS, ColsFS)),
    ?assertEqual([OC, OA], imem_sql_expr:sort_spec_order([SC, SA], ColsFS, ColsFS)),
    ?assertEqual([OB, OC, OA], imem_sql_expr:sort_spec_order([SB, SC, SA], ColsFS, ColsFS)),

    ?assertEqual([OC], imem_sql_expr:sort_spec_order([OC], ColsFS, ColsFS)),
    ?assertEqual([OC, OA], imem_sql_expr:sort_spec_order([OC, SA], ColsFS, ColsFS)),
    ?assertEqual([OC, OA], imem_sql_expr:sort_spec_order([SC, OA], ColsFS, ColsFS)),
    ?assertEqual([OB, OC, OA], imem_sql_expr:sort_spec_order([OB, OC, OA], ColsFS, ColsFS)),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [sort_spec_order]),


    ?assertEqual(ok, imem_meta:drop_table(meta_table_3)),
    ?assertEqual(ok, imem_meta:drop_table(meta_table_2)),
    ?assertEqual(ok, imem_meta:drop_table(meta_table_1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_tables]),

    case IsSec of
        true -> ?imem_logout(anySKey);
        _ -> ok
    end,

    ok.

test_with_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_sec/1 - Start ===>~n", []),

    test_with_or_without_sec(true).

test_without_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_without_sec/1 - Start ===>~n", []),

    test_with_or_without_sec(false).

%%====================================================================
%% Helper functions.
%%====================================================================

if_call_mfa(IsSec, Fun, Args) ->
    case IsSec of
        true -> apply(imem_sec, Fun, Args);
        _ -> apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.
