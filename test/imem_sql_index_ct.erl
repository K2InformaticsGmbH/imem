%%%-------------------------------------------------------------------
%%% File        : imem_sql_index_ct.erl
%%% Description : Common testing imem_sql_index.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_sql_index_ct).

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
    catch imem_meta:drop_table(idx_index_test),
    catch imem_meta:drop_table(index_test),
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
    SKey = ?imem_test_admin_login(),
    % Creating and loading some data into index_test table
    catch imem_meta:drop_table(idx_index_test),
    catch imem_meta:drop_table(index_test),
    ?assertMatch(
        {ok, _},
        imem_sql:exec(SKey, "create table index_test (col1 integer, col2 binstr not null);", 0, imem, IsSec)
    ),
    [_] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    ?assertEqual(0, imem_sql_index:if_call_mfa(IsSec, table_size, [SKey, index_test])),
    TableData =
        [
            <<"{\"NAME\":\"john0\", \"SURNAME\":\"doe0\", \"AGE\":24}">>,
            <<"{\"NAME\":\"john1\", \"SURNAME\":\"doe1\", \"AGE\":25}">>,
            <<"{\"NAME\":\"john2\", \"SURNAME\":\"doe2\", \"AGE\":26}">>,
            <<"{\"NAME\":\"john3\", \"SURNAME\":\"doe3\", \"AGE\":27}">>,
            <<"{\"NAME\":\"john4\", \"SURNAME\":\"doe4\", \"AGE\":28}">>
        ],
    [
        imem_sql_index:if_call_mfa(IsSec, write, [SKey, index_test, {index_test, Id, Data}])
        || {Id, Data} <- lists:zip(lists:seq(1, length(TableData)), TableData)
    ],
    ?assertEqual(length(TableData), imem_sql_index:if_call_mfa(IsSec, table_size, [SKey, index_test])),
    % Creating index on col1
    ?assertEqual(ok, imem_sql:exec(SKey, "create index i_col1 on index_test (col1);", 0, imem, IsSec)),
    [Meta1] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    {value, {index, [DdIdx]}} = lists:keysearch(index, 1, Meta1#ddTable.opts),
    ?assertEqual(1, DdIdx#ddIdxDef.id),
    ?assertEqual(<<"i_col1">>, DdIdx#ddIdxDef.name),
    % Creating index on col2
    ?assertEqual(
        ok,
        imem_sql:exec(
            SKey,
            "create index i_col2 on index_test (col2)" " norm_with fun(X) -> imem_index:vnf_lcase_ascii_ne(X) end.;",
            0,
            imem,
            IsSec
        )
    ),
    [Meta2] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    {value, {index, [DdIdx1, DdIdx]}} = lists:keysearch(index, 1, Meta2#ddTable.opts),
    ?assertEqual(2, DdIdx1#ddIdxDef.id),
    ?assertEqual(<<"i_col2">>, DdIdx1#ddIdxDef.name),
    % Creating index on col2:NAME
    ?assertEqual(ok, imem_sql:exec(SKey, "create index i_col2_name on index_test (col2|:NAME|);", 0, imem, IsSec)),
    [Meta3] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    {value, {index, [DdIdx2, DdIdx1, DdIdx]}} = lists:keysearch(index, 1, Meta3#ddTable.opts),
    ?assertEqual(3, DdIdx2#ddIdxDef.id),
    ?assertEqual(<<"i_col2_name">>, DdIdx2#ddIdxDef.name),
    % Creating index on col2:SURNAME
    ?assertEqual(
        ok,
        imem_sql:exec(
            SKey,
            "create index i_col2_surname on index_test (col2|:SURNAME|)"
            " filter_with fun(X) -> imem_index:iff_true(X) end.;",
            0,
            imem,
            IsSec
        )
    ),
    [Meta4] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    {value, {index, [DdIdx3, DdIdx2, DdIdx1, DdIdx]}} = lists:keysearch(index, 1, Meta4#ddTable.opts),
    ?assertEqual(4, DdIdx3#ddIdxDef.id),
    ?assertEqual(<<"i_col2_surname">>, DdIdx3#ddIdxDef.name),
    % Creating index on col2:AGE
    ?assertEqual(ok, imem_sql:exec(SKey, "create index i_col2_age on index_test (col2|:AGE|);", 0, imem, IsSec)),
    [Meta5] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    {value, {index, [DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}} = lists:keysearch(index, 1, Meta5#ddTable.opts),
    ?assertEqual(5, DdIdx4#ddIdxDef.id),
    ?assertEqual(<<"i_col2_age">>, DdIdx4#ddIdxDef.name),
    % Creating index on col2:NAME and col2:AGE
    ?assertEqual(
        ok,
        imem_sql:exec(
            SKey,
            "create index i_col2_name_age on index_test (col2|:NAME|, col2|:AGE|)"
            " norm_with fun(X) -> imem_index:vnf_lcase_ascii_ne(X) end."
            " filter_with fun(X) -> imem_index:iff_true(X) end."
            ";",
            0,
            imem,
            IsSec
        )
    ),
    [Meta6] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    {value, {index, [DdIdx5, DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}} = lists:keysearch(index, 1, Meta6#ddTable.opts),
    ?assertEqual(6, DdIdx5#ddIdxDef.id),
    ?assertEqual(<<"i_col2_name_age">>, DdIdx5#ddIdxDef.name),
    % Creating index on col2:SURNAME and col1
    ?assertEqual(
        ok,
        imem_sql:exec(SKey, "create index i_col2_surname_col1 on index_test (col2|:SURNAME|, col1);", 0, imem, IsSec)
    ),
    [Meta7] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    {value, {index, [DdIdx6, DdIdx5, DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}} =
        lists:keysearch(index, 1, Meta7#ddTable.opts),
    ?assertEqual(7, DdIdx6#ddIdxDef.id),
    ?assertEqual(<<"i_col2_surname_col1">>, DdIdx6#ddIdxDef.name),
    % Creating index on all fields
    ?assertEqual(
        ok,
        imem_sql:exec(
            SKey,
            "create index i_all on index_test" " (col1, col2, col2|:NAME|, col2|:SURNAME|, col2|:AGE|);",
            0,
            imem,
            IsSec
        )
    ),
    [Meta8] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    {value, {index, [DdIdx7, DdIdx6, DdIdx5, DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}} =
        lists:keysearch(index, 1, Meta8#ddTable.opts),
    ?assertEqual(8, DdIdx7#ddIdxDef.id),
    ?assertEqual(<<"i_all">>, DdIdx7#ddIdxDef.name),
    print_indices(IsSec, SKey, imem, index_test),
    % Creating a duplicate index (negative test)
    ?assertException(
        throw,
        {'ClientError', {"Index already exists for table", {<<"i_col2_age">>, <<"index_test">>}}},
        imem_sql:exec(SKey, "create index i_col2_age on index_test (col2|:AGE|);", 0, imem, IsSec)
    ),
    %
    % Dropping indexes in random order
    %
    % Drop index i_col2_name_age
    ?assertEqual(ok, imem_sql:exec(SKey, "drop index i_col2_name_age from index_test;", 0, imem, IsSec)),
    [Meta9] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    ?assertEqual(
        {value, {index, [DdIdx7, DdIdx6, DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}},
        lists:keysearch(index, 1, Meta9#ddTable.opts)
    ),
    % Dropping non-exixtant index (negative test)
    ?assertException(
        throw,
        {'ClientError', {"Index does not exist for", index_test, <<"i_not_exists">>}},
        imem_sql:exec(SKey, "drop index i_not_exists from index_test;", 0, imem, IsSec)
    ),
    % Drop index i_col1
    ?assertEqual(ok, imem_sql:exec(SKey, "drop index i_col1 from index_test;", 0, imem, IsSec)),
    [Meta10] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    ?assertEqual(
        {value, {index, [DdIdx7, DdIdx6, DdIdx4, DdIdx3, DdIdx2, DdIdx1]}},
        lists:keysearch(index, 1, Meta10#ddTable.opts)
    ),
    % Drop index i_col2_surname
    ?assertEqual(ok, imem_sql:exec(SKey, "drop index i_col2_surname from index_test;", 0, imem, IsSec)),
    [Meta11] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    ?assertEqual(
        {value, {index, [DdIdx7, DdIdx6, DdIdx4, DdIdx2, DdIdx1]}},
        lists:keysearch(index, 1, Meta11#ddTable.opts)
    ),
    print_indices(IsSec, SKey, imem, index_test),
    % Drop index i_col2_surname_col1
    ?assertEqual(ok, imem_sql:exec(SKey, "drop index i_col2_surname_col1 from index_test;", 0, imem, IsSec)),
    [Meta12] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    ?assertEqual({value, {index, [DdIdx7, DdIdx4, DdIdx2, DdIdx1]}}, lists:keysearch(index, 1, Meta12#ddTable.opts)),
    % Drop index i_col2_name
    ?assertEqual(ok, imem_sql:exec(SKey, "drop index i_col2_name from index_test;", 0, imem, IsSec)),
    [Meta13] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    ?assertEqual({value, {index, [DdIdx7, DdIdx4, DdIdx1]}}, lists:keysearch(index, 1, Meta13#ddTable.opts)),
    % Drop index i_all
    ?assertEqual(ok, imem_sql:exec(SKey, "drop index i_all from index_test;", 0, imem, IsSec)),
    [Meta14] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    ?assertEqual({value, {index, [DdIdx4, DdIdx1]}}, lists:keysearch(index, 1, Meta14#ddTable.opts)),
    % Dropping previously dropped i_col2_name index (negative test)
    ?assertException(
        throw,
        {'ClientError', {"Index does not exist for", index_test, <<"i_col2_name">>}},
        imem_sql:exec(SKey, "drop index i_col2_name from index_test;", 0, imem, IsSec)
    ),
    print_indices(IsSec, SKey, imem, index_test),
    % Drop index i_col2_age
    ?assertEqual(ok, imem_sql:exec(SKey, "drop index i_col2_age from index_test;", 0, imem, IsSec)),
    [Meta15] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    ?assertEqual({value, {index, [DdIdx1]}}, lists:keysearch(index, 1, Meta15#ddTable.opts)),
    % Drop index i_col2 (last index)
    ?assertEqual(ok, imem_sql:exec(SKey, "drop index i_col2 from index_test;", 0, imem, IsSec)),
    [Meta16] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, index_test}]),
    ?assertEqual(false, lists:keysearch(index, 1, Meta16#ddTable.opts)),
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

print_indices(IsSec, SKey, Schema, Table) ->
    [Meta] = imem_sql_index:if_call_mfa(IsSec, read, [SKey, ddTable, {Schema, Table}]),
    {value, {index, Indices}} = lists:keysearch(index, 1, Meta#ddTable.opts),
    _Indices =
        lists:flatten(
            [
                [
                    " ",
                    binary_to_list(I#ddIdxDef.name),
                    " -> ",
                    string:join([binary_to_list(element(2, jpparse_fold:string(Pl))) || Pl <- I#ddIdxDef.pl], " | "),
                    "\n"
                ]
                || I <- Indices
            ]
        ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~nIndices :~n ~s", [_Indices]),
    ok.
