%%%-------------------------------------------------------------------
%%% File        : imem_if_csv_ct.erl
%%% Description : Common testing imem_if_csv.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_if_csv_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([test_csv_1/1]).

-define(MATCH_ALL_1_COL, [{{'_', '$22', '$23', '$24', '$25'}, [true], ['$_']}]).
-define(MATCH_ALL_2_COLS, [{{'_', '$22', '$23', '$24', '$25', '$26'}, [true], ['$_']}]).
-define(MATCH_ALL_3_COLS, [{{'_', '$22', '$23', '$24', '$25', '$26', '$27'}, [true], ['$_']}]).
-define(NODEBUG, true).

-include_lib("imem.hrl").

-include("imem_if_csv.hrl").

%%====================================================================
%% Test cases.
%%====================================================================

test_csv_1(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_csv_1/1 - Start ===>~n", []),
    ?assertEqual(<<>>, imem_if_csv:first_binary_bytes(<<>>, 4)),
    ?assertEqual(<<>>, imem_if_csv:last_binary_bytes(<<>>, 4)),
    ?assertEqual(<<"123">>, imem_if_csv:first_binary_bytes(<<"123">>, 4)),
    ?assertEqual(<<"123">>, imem_if_csv:last_binary_bytes(<<"123">>, 4)),
    ?assertEqual(<<"1234">>, imem_if_csv:first_binary_bytes(<<"1234">>, 4)),
    ?assertEqual(<<"1234">>, imem_if_csv:last_binary_bytes(<<"1234">>, 4)),
    ?assertEqual(<<"The ">>, imem_if_csv:first_binary_bytes(<<"The quick brown Fox jumps over the lazy Dog.">>, 4)),
    ?assertEqual(<<"Dog.">>, imem_if_csv:last_binary_bytes(<<"The quick brown Fox jumps over the lazy Dog.">>, 4)),
    ?assertEqual(0, imem_if_csv:count_char_seq(<<10, 13>>, <<13, 10>>)),
    ?assertEqual(1, imem_if_csv:count_char_seq(<<10, 13, 10, 13>>, <<13, 10>>)),
    ?assertEqual(0, imem_if_csv:count_char_seq(<<34>>, <<92, 34>>)),
    ?assertEqual(0, imem_if_csv:count_delimiter(<<>>, <<$">>)),
    ?assertEqual(0, imem_if_csv:count_delimiter(<<"123">>, <<$">>)),
    ?assertEqual(1, imem_if_csv:count_char_seq(<<$">>, <<$">>)),
    ?assertEqual(0, imem_if_csv:count_char_seq(<<$">>, <<$", $">>)),
    ?assertEqual(0, imem_if_csv:count_char_seq(<<$">>, <<$\\, $">>)),
    ?assertEqual(1, imem_if_csv:count_delimiter(<<$">>, <<$">>)),
    ?assertEqual(1, imem_if_csv:count_delimiter(<<"123\"abc">>, <<$">>)),
    ?assertEqual(2, imem_if_csv:count_delimiter(<<$", $a, $">>, <<$">>)),
    ?assertEqual(2, imem_if_csv:count_delimiter(<<"123\"abc\"567">>, <<$">>)),
    ?assertEqual(0, imem_if_csv:count_delimiter(<<$", $">>, <<$">>)),
    ?assertEqual(2, imem_if_csv:count_delimiter(<<"123", $", 13, 10, $", "abc">>, <<$">>)),
    ?assertEqual(2, imem_if_csv:count_delimiter(<<"123\"", 13, 10, "\"abc">>, <<$">>)),
    ?assertEqual([<<>>], imem_if_csv:delimited_split(<<"">>, <<10>>, <<$">>)),
    ?assertEqual([<<"abc", 13>>, <<"ABC">>], imem_if_csv:delimited_split(<<"abc", 13, 10, "ABC">>, <<10>>, <<$">>)),
    ?assertEqual(
        [<<"ab\"c", 13, 10, "AB\"C">>],
        imem_if_csv:delimited_split(<<"ab\"c", 13, 10, "AB\"C">>, <<13, 10>>, <<$">>)
    ),
    ?assertEqual(
        [<<"ab\"c", 13, 10, "AB\"C">>],
        imem_if_csv:delimited_split(<<"ab\"c", 13, 10, "AB\"C">>, <<13, 10>>, <<$">>)
    ),
    % ?LogDebug("schema ~p", [imem_meta:schema()]),
    ?assertEqual([], imem_statement:receive_raw()),
    Bin1 = <<"Col1\tCol2\r\nA1\t1\r\nA2\t2\r\n">>,
    ?assertEqual(3, imem_if_csv:count_char_seq(Bin1, <<"\r\n">>)),
    ?assertEqual(3, imem_if_csv:count_char_seq(Bin1, <<"\t">>)),
    ?assertEqual(3, imem_if_csv:count_char_seq(Bin1, <<"\t">>)),
    ?assertEqual(2, imem_if_csv:count_char_seq(Bin1, <<"A">>)),
    ?assertEqual(1, imem_if_csv:count_char_seq(Bin1, <<"A1">>)),
    ?assertEqual(2, imem_if_csv:column_count([[1, 2]])),
    ?assertEqual(2, imem_if_csv:column_count([[a], [1, 2]])),
    ?assertEqual(2, imem_if_csv:column_count([[a], [1, 2], [<<>>]])),
    Rows1 = imem_if_csv:delimited_split(Bin1, <<"\r\n">>),
    RowsSplitBySep1 = imem_if_csv:split_cols(Rows1, <<"\t">>),
    ?assertEqual([[<<"Col1">>, <<"Col2">>], [<<"A1">>, <<"1">>], [<<"A2">>, <<"2">>], [<<>>]], RowsSplitBySep1),
    ?assertEqual(2, imem_if_csv:column_count(RowsSplitBySep1)),
    SepCounts1 = [{imem_if_csv:count_char_seq(Bin1, Sep), Sep} || Sep <- [<<"\t">>, <<";">>, <<",">>]],
    ?assertEqual([{3, <<"\t">>}, {0, <<";">>}, {0, <<",">>}], SepCounts1),
    CsvFileName = <<"CsvTestFileName123abc.txt">>,
    file:write_file(CsvFileName, Bin1),
    ?assertMatch(
        #{
            columnCount := 2,
            columnSeparator := <<"\t">>,
            columns := [<<"col1">>, <<"col2">>],
            lineSeparator := <<"\r\n">>,
            encoding := utf8
        },
        imem_if_csv:file_info(CsvFileName)
    ),
    ?assertMatch(
        #{
            columnCount := 2,
            columnSeparator := <<"\t">>,
            columns := [<<"Col1">>, <<"Col2">>],
            lineSeparator := <<"\r\n">>
        },
        imem_if_csv:file_info(CsvFileName, #{header => true})
    ),
    ?assertMatch(
        #{
            columnCount := 2,
            columnSeparator := <<"\t">>,
            columns := [<<"col1">>, <<"col2">>],
            lineSeparator := <<"\r\n">>
        },
        imem_if_csv:file_info(CsvFileName, #{header => false})
    ),
    ?assertMatch(
        #{columnCount := 1, columnSeparator := <<"\t">>, columns := [<<"col1">>], lineSeparator := <<"\r\n">>},
        imem_if_csv:file_info(CsvFileName, #{columnCount => 1, columnSeparator => <<"\t">>})
    ),
    ?assertMatch(
        #{
            columnCount := 3,
            columnSeparator := <<"\t">>,
            columns := [<<"col1">>, <<"col2">>, <<"col3">>],
            lineSeparator := <<"\r\n">>
        },
        imem_if_csv:file_info(CsvFileName, #{columnCount => 3, columnSeparator => <<"\t">>})
    ),
    Bin2 = <<"A\r\nCol1\tCol2\r\nA1\t1\r\nA2\t2">>,
    Rows2 = imem_if_csv:delimited_split(Bin2, <<"\r\n">>),
    RowsSplitBySep2 = imem_if_csv:split_cols(Rows2, <<"\t">>),
    ?assertEqual([[<<"A">>], [<<"Col1">>, <<"Col2">>], [<<"A1">>, <<"1">>], [<<"A2">>, <<"2">>]], RowsSplitBySep2),
    ?assertEqual(2, imem_if_csv:column_count(RowsSplitBySep2)),
    SepCounts2 = [{imem_if_csv:count_char_seq(Bin2, Sep), Sep} || Sep <- [<<"\t">>, <<";">>, <<",">>]],
    ?assertEqual([{3, <<"\t">>}, {0, <<";">>}, {0, <<",">>}], SepCounts2),
    file:write_file(CsvFileName, Bin2),
    ?assertMatch(
        #{
            columnCount := 2,
            columnSeparator := <<"\t">>,
            columns := [<<"Col1">>, <<"Col2">>],
            lineSeparator := <<"\r\n">>,
            encoding := utf8
        },
        imem_if_csv:file_info(CsvFileName, #{header => true})
    ),
    file:write_file(CsvFileName, <<"\r\nCol1\tCol2\r\nA1\t1\r\nA2\t2">>),
    ?assertMatch(
        #{
            columnCount := 2,
            columnSeparator := <<"\t">>,
            columns := [<<"Col1">>, <<"Col2">>],
            lineSeparator := <<"\r\n">>
        },
        imem_if_csv:file_info(CsvFileName, #{header => true})
    ),
    file:write_file(CsvFileName, <<"1\t2\r\nCol1\tCol2\r\nA1\t1\r\nA2\t2">>),
    ?assertMatch(
        #{
            columnCount := 2,
            columnSeparator := <<"\t">>,
            columns := [<<"col1">>, <<"col2">>],
            lineSeparator := <<"\r\n">>
        },
        imem_if_csv:file_info(CsvFileName, #{header => true})
    ),
    file:write_file(CsvFileName, <<"1\t2\nCol1\tCol2\nCol1\n">>),
    ?assertMatch(
        #{columnCount := 1, columnSeparator := <<>>, columns := [<<"col1">>], lineSeparator := <<"\n">>},
        imem_if_csv:file_info(CsvFileName, #{header => true})
    ),
    file:write_file(CsvFileName, <<"Col1\tCol2\r\nA1\t1\r\nA2\t2\r\n">>),
    ?assertEqual(
        {
            [
                {csv_rec, CsvFileName, 0, 11, <<"Col1">>, <<"Col2">>},
                {csv_rec, CsvFileName, 11, 6, <<"A1">>, <<"1">>},
                {csv_rec, CsvFileName, 17, 6, <<"A2">>, <<"2">>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({?CSV_SCHEMA_DEFAULT, CsvFileName}, ?MATCH_ALL_2_COLS, 100, read)
    ),
    ?assertEqual(
        {
            [
                {csv_rec, CsvFileName, 0, 11, <<"Col1">>},
                {csv_rec, CsvFileName, 11, 6, <<"A1">>},
                {csv_rec, CsvFileName, 17, 6, <<"A2">>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({<<"csv$tab$1">>, CsvFileName}, ?MATCH_ALL_1_COL, 100, read)
    ),
    file:write_file(CsvFileName, <<"Col1\tCol2\r\nA1\t1\r\nA2\t2\r\n">>),
    ?assertEqual(
        {
            [
                {csv_rec, CsvFileName, 0, 11, <<"Col1">>, <<"Col2">>, <<>>},
                {csv_rec, CsvFileName, 11, 6, <<"A1">>, <<"1">>, <<>>},
                {csv_rec, CsvFileName, 17, 6, <<"A2">>, <<"2">>, <<>>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({<<"csv$tab$3">>, CsvFileName}, ?MATCH_ALL_3_COLS, 100, read)
    ),
    file:write_file(CsvFileName, <<"Col1\tCol2\r\nA1\t1\r\nA2\t2\r\n">>),
    ?assertEqual(
        {
            [
                {csv_rec, CsvFileName, 0, 11, <<"Col1">>, <<"Col2">>},
                {csv_rec, CsvFileName, 11, 6, <<"A1">>, <<"1">>},
                {csv_rec, CsvFileName, 17, 6, <<"A2">>, <<"2">>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({?CSV_SCHEMA_DEFAULT, CsvFileName}, ?MATCH_ALL_2_COLS, 100, read)
    ),
    file:write_file(CsvFileName, <<"A\t\r\nCol1\tCol2\r\nA1\t1\r\nA2\t2">>),
    ?assertEqual(
        {
            [
                {csv_rec, CsvFileName, 0, 4, <<"A">>, <<>>},
                {csv_rec, CsvFileName, 4, 11, <<"Col1">>, <<"Col2">>},
                {csv_rec, CsvFileName, 15, 6, <<"A1">>, <<"1">>},
                {csv_rec, CsvFileName, 21, 6, <<"A2">>, <<"2">>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({?CSV_SCHEMA_DEFAULT, CsvFileName}, ?MATCH_ALL_2_COLS, 100, read)
    ),
    file:write_file(CsvFileName, <<"A|\r\n\r\nCol1|Col2\r\nA1|1\r\nA2|2\r\n">>),
    ?assertMatch(
        {
            [
                {csv_rec, CsvFileName, 0, 4, <<"A">>, <<>>},
                {csv_rec, CsvFileName, _, 2, <<>>, <<>>},
                {csv_rec, CsvFileName, _, 11, <<"Col1">>, <<"Col2">>},
                {csv_rec, CsvFileName, _, 6, <<"A1">>, <<"1">>},
                {csv_rec, CsvFileName, _, 6, <<"A2">>, <<"2">>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({?CSV_SCHEMA_DEFAULT, CsvFileName}, ?MATCH_ALL_2_COLS, 100, read)
    ),
    file:write_file(CsvFileName, <<"A;\r\n\r\nCol1;Col2\r\nA1;1\r\nA2;2\r\n\r\n">>),
    ?assertMatch(
        {
            [
                {csv_rec, CsvFileName, 0, 4, <<"A">>, <<>>},
                {csv_rec, CsvFileName, _, 2, <<>>, <<>>},
                {csv_rec, CsvFileName, _, 11, <<"Col1">>, <<"Col2">>},
                {csv_rec, CsvFileName, _, 6, <<"A1">>, <<"1">>},
                {csv_rec, CsvFileName, _, 6, <<"A2">>, <<"2">>},
                {csv_rec, CsvFileName, _, 2, <<>>, <<>>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({?CSV_SCHEMA_DEFAULT, CsvFileName}, ?MATCH_ALL_2_COLS, 100, read)
    ),
    file:write_file(CsvFileName, <<"A;\n\r\nCol1;Col2\r\nA1;1\r\nA2;2">>),
    ?assertMatch(
        {
            [
                {csv_rec, CsvFileName, 0, 3, <<"A;">>},
                {csv_rec, CsvFileName, _, 2, <<"\r">>},
                {csv_rec, CsvFileName, _, 11, <<"Col1;Col2\r">>},
                {csv_rec, CsvFileName, _, 6, <<"A1;1\r">>},
                {csv_rec, CsvFileName, _, 5, <<"A2;2">>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({?CSV_SCHEMA_DEFAULT, CsvFileName}, ?MATCH_ALL_1_COL, 100, read)
    ),
    file:write_file(CsvFileName, <<"1\n2\n3\n4\n5">>),
    ?assertMatch(
        {
            [
                {csv_rec, CsvFileName, 0, 2, <<"1">>},
                {csv_rec, CsvFileName, _, 2, <<"2">>},
                {csv_rec, CsvFileName, _, 2, <<"3">>},
                {csv_rec, CsvFileName, _, 2, <<"4">>},
                {csv_rec, CsvFileName, _, 2, <<"5">>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({?CSV_SCHEMA_DEFAULT, CsvFileName}, ?MATCH_ALL_1_COL, 100, read)
    ),
    ?assertMatch(
        {
            [
                {csv_rec, CsvFileName, _, 2, <<"3">>},
                {csv_rec, CsvFileName, _, 2, <<"4">>},
                {csv_rec, CsvFileName, _, 2, <<"5">>}
            ],
            {'$end_of_table'}
        },
        imem_if_csv:select({<<"csv$skip2">>, CsvFileName}, ?MATCH_ALL_1_COL, 100, read)
    ),
    ?assertMatch(
        {[], {'$end_of_table'}},
        imem_if_csv:select({<<"csv$skip5">>, CsvFileName}, ?MATCH_ALL_1_COL, 100, read)
    ),
    ?assertMatch(
        {[], {'$end_of_table'}},
        imem_if_csv:select({<<"csv$skip6">>, CsvFileName}, ?MATCH_ALL_1_COL, 100, read)
    ),
    ok.
