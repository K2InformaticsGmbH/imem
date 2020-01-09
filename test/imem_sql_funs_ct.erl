%%%-------------------------------------------------------------------
%%% File        : imem_sql_funs_ct.erl
%%% Description : Common testing imem_sql_funs.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_sql_funs_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([test_with_sec/1, test_without_sec/1]).

-define(NODEBUG, true).

-include_lib("imem.hrl").

-include("imem_seco.hrl").
-include("imem_sql.hrl").

%%====================================================================
%% Test cases.
%%====================================================================

test_with_or_without_sec(IsSec) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_or_without_sec/1 - Start(~p) ===>~n", [IsSec]),
    ?assertEqual(<<"Imem.ddTable">>, imem_sql_funs:to_name({'Imem', ddTable})),
    ?assertEqual(<<"imem.ddTable">>, imem_sql_funs:to_name({imem, ddTable})),
    ?assertEqual(<<"undefined.ddTable">>, imem_sql_funs:to_name({undefined, ddTable})),
    ?assertEqual(<<"ddTable">>, imem_sql_funs:to_name(<<"ddTable">>)),
    % ?assertEqual(<<"ddTable">>, imem_sql_funs:to_name("ddTable")),
    ?assertEqual(<<"imem.ddäöü"/utf8>>, imem_sql_funs:to_name({<<"imem">>, <<"ddäöü">>})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":to_name success~n", []),
    ?assertEqual(<<"">>, imem_sql_funs:to_text([])),
    ?assertEqual(<<"SomeText1234">>, imem_sql_funs:to_text("SomeText1234")),
    ?assertEqual(<<"SomeText1234">>, imem_sql_funs:to_text(<<"SomeText1234">>)),
    ?assertEqual(<<".SomeText1234.">>, imem_sql_funs:to_text([2 | "SomeText1234"] ++ [3])),
    ?assertEqual(<<"ddäöü"/utf8>>, imem_sql_funs:to_text(<<"ddäöü">>)),
    ?assertEqual(<<".ddäöü."/utf8>>, imem_sql_funs:to_text(<<2, "ddäöü", 3>>)),
    ?assertEqual(<<"{'Imem',ddTable}">>, imem_sql_funs:to_text({'Imem', ddTable})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":to_text success~n", []),
    %% Like strig to Regex string
    ?assertEqual(<<"^Sm.th$">>, imem_sql_funs:transform_like(<<"Sm_th">>, <<>>)),
    ?assertEqual(<<"^.*Sm.th.*$">>, imem_sql_funs:transform_like(<<"%Sm_th%">>, <<>>)),
    ?assertEqual(<<"^.A.*Sm.th.*$">>, imem_sql_funs:transform_like(<<"_A%Sm_th%">>, <<>>)),
    ?assertEqual(<<"^.A.*S\\$m.t\\*\\[h.*$">>, imem_sql_funs:transform_like(<<"_A%S$m_t*[h%">>, <<>>)),
    ?assertEqual(
        <<"^.A.*S\\^\\$\\.\\[\\]\\|\\(\\)\\?\\*\\+\\-\\{\\}m.th.*$">>,
        imem_sql_funs:transform_like(<<"_A%S^$.[]|()?*+-{}m_th%">>, <<>>)
    ),
    ?assertEqual(<<"^Sm_th.$">>, imem_sql_funs:transform_like(<<"Sm@_th_">>, <<"@">>)),
    ?assertEqual(<<"^Sm%th.*$">>, imem_sql_funs:transform_like(<<"Sm@%th%">>, <<"@">>)),
    ?assertEqual(<<"^.m_th.$">>, imem_sql_funs:transform_like(<<"_m@_th_">>, <<"@">>)),
    ?assertEqual(<<"^.*m%th.*$">>, imem_sql_funs:transform_like(<<"%m@%th%">>, <<"@">>)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [transform_like]),
    %% Regular Expressions
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":testing regular expressions: ~p~n", ["like_compile"]),
    RE1 = imem_sql_funs:like_compile("abc_123%@@"),
    ?assertEqual(true, imem_sql_funs:re_match(RE1, <<"abc_123jhhsdhjhj@@">>)),
    ?assertEqual(true, imem_sql_funs:re_match(RE1, <<"abc_123@@">>)),
    ?assertEqual(true, imem_sql_funs:re_match(RE1, <<"abc_123%@@">>)),
    ?assertEqual(true, imem_sql_funs:re_match(RE1, <<"abc_123%%@@">>)),
    ?assertEqual(true, imem_sql_funs:re_match(RE1, <<"abc0123@@">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE1, <<"abc_123%@">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE1, <<"">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE1, <<"abc_@@">>)),
    %% string is expanded using ~p before comparison
    ?assertEqual(false, imem_sql_funs:re_match(RE1, "abc_123%@@")),
    %% string is expanded using ~p before comparison
    ?assertEqual(false, imem_sql_funs:re_match(RE1, "abc_123%%@@@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE1, "abc0123@@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE1, "abc_123%@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE1, "")),
    RE1a = imem_sql_funs:like_compile("\"abc_123%@@\""),
    ?assertEqual(true, imem_sql_funs:re_match(RE1a, "abc_123%@@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE1a, <<"abc_123jhhsdhjhj@@">>)),
    ?assertEqual(true, imem_sql_funs:re_match(RE1a, "abc_123%%@@@")),
    ?assertEqual(true, imem_sql_funs:re_match(RE1a, "abc0123@@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE1a, "abc_123%@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE1a, "abc_123%@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE1a, "")),
    ?assertEqual(false, imem_sql_funs:re_match(RE1a, <<"">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE1a, <<"abc_@@">>)),
    RE2 = imem_sql_funs:like_compile(<<"%@@">>, <<>>),
    ?assertEqual(false, imem_sql_funs:re_match(RE2, "abc_123%@@")),
    ?assertEqual(true, imem_sql_funs:re_match(RE2, <<"abc_123%@@">>)),
    ?assertEqual(true, imem_sql_funs:re_match(RE2, <<"123%@@">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE2, "@@")),
    ?assertEqual(true, imem_sql_funs:re_match(RE2, <<"@@">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE2, "@@@")),
    ?assertEqual(true, imem_sql_funs:re_match(RE2, <<"@@@">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE2, "abc_123%@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE2, <<"abc_123%@">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE2, "@.@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE2, <<"@.@">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE2, "@_@")),
    ?assertEqual(false, imem_sql_funs:re_match(RE2, <<"@_@">>)),
    RE3 = imem_sql_funs:like_compile(<<"text_in%">>),
    ?assertEqual(false, imem_sql_funs:re_match(RE3, "text_in_text")),
    ?assertEqual(true, imem_sql_funs:re_match(RE3, <<"text_in_text">>)),
    ?assertEqual(true, imem_sql_funs:re_match(RE3, <<"text_in_quotes\"">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE3, <<"\"text_in_quotes">>)),
    ?assertEqual(false, imem_sql_funs:re_match(RE3, "\"text_in_quotes\"")),
    ?assertEqual(false, imem_sql_funs:re_match(RE3, <<"\"text_in_quotes\"">>)),
    RE4 = imem_sql_funs:like_compile(<<"%12">>),
    ?assertEqual(true, imem_sql_funs:re_match(RE4, 12)),
    ?assertEqual(true, imem_sql_funs:re_match(RE4, 112)),
    ?assertEqual(true, imem_sql_funs:re_match(RE4, 12)),
    ?assertEqual(false, imem_sql_funs:re_match(RE4, 122)),
    ?assertEqual(false, imem_sql_funs:re_match(RE4, 1)),
    ?assertEqual(false, imem_sql_funs:re_match(RE4, 11)),
    RE5 = imem_sql_funs:like_compile(<<"12.5%">>),
    ?assertEqual(true, imem_sql_funs:re_match(RE5, 12.51)),
    ?assertEqual(true, imem_sql_funs:re_match(RE5, 12.55)),
    ?assertEqual(true, imem_sql_funs:re_match(RE5, 12.5)),
    ?assertEqual(false, imem_sql_funs:re_match(RE5, 12)),
    ?assertEqual(false, imem_sql_funs:re_match(RE5, 12.4)),
    ?assertEqual(false, imem_sql_funs:re_match(RE5, 12.49999)),
    %% ToDo: implement and test patterns involving regexp reserved characters
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [replace_match]),
    % L = {like,'$6',"%5%"},
    % NL = {not_like,'$7',"1%"},
    % ?assertEqual( true, replace_match(L,L)),
    % ?assertEqual( NL, replace_match(NL,L)),
    % ?assertEqual( {'and',true,NL}, replace_match({'and',L,NL},L)),
    % ?assertEqual( {'and',L,true}, replace_match({'and',L,NL},NL)),
    % ?assertEqual( {'and',{'and',true,NL},{a,b,c}}, replace_match({'and',{'and',L,NL},{a,b,c}},L)),
    % ?assertEqual( {'and',{'and',L,{a,b,c}},true}, replace_match({'and',{'and',L,{a,b,c}},NL},NL)),
    % ?assertEqual( {'and',{'and',true,NL},{'and',L,NL}}, replace_match({'and',{'and',L,NL},{'and',L,NL}},L)),
    % ?assertEqual( {'and',NL,{'and',true,NL}}, replace_match({'and',NL,{'and',L,NL}},L)),
    % ?assertEqual( {'and',NL,{'and',NL,NL}}, replace_match({'and',NL,{'and',NL,NL}},L)),
    % ?assertEqual( {'and',NL,{'and',NL,true}}, replace_match({'and',NL,{'and',NL,L}},L)),
    % ?assertEqual( {'and',{'and',{'and',{'=<',5,'$1'},L},true},{'==','$1','$6'}}, replace_match({'and',{'and',{'and',{'=<',5,'$1'},L},NL},{'==','$1','$6'}},NL)),
    % ?assertEqual( {'and',{'and',{'and',{'=<',5,'$1'},true},NL},{'==','$1','$6'}}, replace_match({'and',{'and',{'and',{'=<',5,'$1'},L},NL},{'==','$1','$6'}},L)),
    %% expr_fun
    ?assertEqual(true, imem_sql_funs:expr_fun(true)),
    ?assertEqual(false, imem_sql_funs:expr_fun(false)),
    ?assertEqual(true, imem_sql_funs:expr_fun({'not', false})),
    ?assertEqual(false, imem_sql_funs:expr_fun({'not', true})),
    ?assertEqual(12, imem_sql_funs:expr_fun(12)),
    ?assertEqual(a, imem_sql_funs:expr_fun(a)),
    ?assertEqual({a, b}, imem_sql_funs:expr_fun({const, {a, b}})),
    ?assertEqual(true, imem_sql_funs:expr_fun({'==', 10, 10})),
    ?assertEqual(true, imem_sql_funs:expr_fun({'==', {const, {a, b}}, {const, {a, b}}})),
    ?assertEqual(false, imem_sql_funs:expr_fun({'==', {const, {a, b}}, {const, {a, 1}}})),
    ?assertEqual(true, imem_sql_funs:expr_fun({is_member, a, [a, b]})),
    ?assertEqual(true, imem_sql_funs:expr_fun({is_member, 1, [a, b, 1]})),
    ?assertEqual(false, imem_sql_funs:expr_fun({is_member, 1, [a, b, c]})),
    ?assertEqual(true, imem_sql_funs:expr_fun({is_member, 1, {const, {a, b, 1}}})),
    ?assertEqual(false, imem_sql_funs:expr_fun({is_member, 1, {const, {a, b, c}}})),
    ?assertEqual(true, imem_sql_funs:expr_fun({is_member, {const, {a, 1}}, #{b => 2, a => 1}})),
    ?assertEqual(false, imem_sql_funs:expr_fun({is_member, {const, {a, 2}}, #{b => 2, a => 1}})),
    ?assertEqual(false, imem_sql_funs:expr_fun({is_member, {const, {c, 3}}, #{b => 2, a => 1}})),
    ?assertEqual(true, imem_sql_funs:expr_fun({is_like, "12345", "%3%"})),
    ?assertEqual(true, imem_sql_funs:expr_fun({is_like, <<"12345">>, "%3__"})),
    ?assertEqual(true, imem_sql_funs:expr_fun({is_like, <<"12345">>, <<"1%">>})),
    ?assertEqual(false, imem_sql_funs:expr_fun({is_like, "12345", <<"1%">>})),
    ?assertEqual(true, imem_sql_funs:expr_fun({is_like, {'+', 12300, 45}, <<"%45">>})),
    ?assertEqual(true, imem_sql_funs:expr_fun({is_like, "12345", <<"\"1%\"">>})),
    ?assertEqual(false, imem_sql_funs:expr_fun({is_like, "12345", "%6%"})),
    ?assertEqual(false, imem_sql_funs:expr_fun({is_like, <<"12345">>, "%6%"})),
    ?assertEqual(false, imem_sql_funs:expr_fun({is_like, <<"12345">>, "%7__"})),
    ?assertEqual(33, imem_sql_funs:expr_fun({'*', {'+', 10, 1}, 3})),
    ?assertEqual(10, imem_sql_funs:expr_fun({abs, {'-', 10, 20}})),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", ["expr_fun constants"]),
    X1 = {{1, 2, 3}, {2, 2, 2}},
    % = 2
    B1a = #bind{tag = '$1', tind = 1, cind = 2},
    % = 2
    B1b = #bind{tag = '$2', tind = 1, cind = 2},
    F1 = imem_sql_funs:expr_fun({'==', B1a, B1b}),
    ?assertEqual(true, F1(X1)),
    % = 2
    B1c = #bind{tag = '$2', tind = 2, cind = 2},
    F1a = imem_sql_funs:expr_fun({'==', B1a, B1c}),
    ?assertEqual(true, F1a(X1)),
    % = 3
    B2 = #bind{tag = '$3', tind = 1, cind = 3},
    F2 = imem_sql_funs:expr_fun({is_member, a, B2}),
    ?assertEqual(false, F2({{1, 2, [3, 4, 5]}, {2, 2, 2}})),
    ?assertEqual(true, F2({{1, 2, [c, a, d]}, {2, 2, 2}})),
    F3 = imem_sql_funs:expr_fun({is_member, B1c, B2}),
    ?assertEqual(false, F3({{1, d, [c, a, d]}, {2, 2, 2}})),
    ?assertEqual(true, F3({{1, c, [2, a, d]}, {2, 2, 2}})),
    ?assertEqual(true, F3({{1, a, [c, 2, 2]}, {2, 2, 2}})),
    ?assertEqual(true, F3({{1, 3, {3, 4, 2}}, {2, 2, 2}})),
    ?assertEqual(false, F3({{1, 2, {3, 4, 5}}, {2, 2, 2}})),
    ?assertEqual(false, F3({{1, [a], [3, 4, 5]}, {2, 2, 2}})),
    ?assertEqual(false, F3({{1, 3, []}, {2, 2, 2}})),
    F4 = imem_sql_funs:expr_fun({is_member, {'+', B1c, 1}, B2}),
    ?assertEqual(true, F4({{1, 2, [3, 4, 5]}, {2, 2, 2}})),
    ?assertEqual(false, F4({{1, 2, [c, 4, d]}, {2, 2, 2}})),
    ?assertEqual([], imem_sql_funs:json_arr_proj([a1, a2, a3, a4, a5], [])),
    ?assertEqual([a3, a5], imem_sql_funs:json_arr_proj([a1, a2, a3, a4, a5], [3, 5])),
    ?assertEqual([a1, ?nav], imem_sql_funs:json_arr_proj([a1, a2, a3, a4, a5], [1, 6])),
    ?assertEqual([a1, ?nav], imem_sql_funs:json_arr_proj([a1, a2, a3, a4, a5], [1, ?nav])),
    ?assertEqual(?nav, imem_sql_funs:json_arr_proj([a1, a2, a3, a4, a5], [6])),
    ?assertEqual([3, 5], imem_sql_funs:json_arr_proj(<<"[1,2,3,4,5]">>, [3, 5])),
    ?assert(true),
    ok.

test_with_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_with_sec/1 - Start ===>~n", []),
    test_with_or_without_sec(true).

test_without_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_without_sec/1 - Start ===>~n", []),
    test_with_or_without_sec(false).
