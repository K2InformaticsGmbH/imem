%%%-------------------------------------------------------------------
%%% File        : imem_statement_ct.erl
%%% Description : Common testing imem_statement.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_statement_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_testcase/2,
    test_with_sec_part1/1,
    test_with_sec_part2/1,
    test_with_sec_part3/1,
    test_without_sec_part1/1,
    test_without_sec_part2/1,
    test_without_sec_part3/1
]).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include("imem_seco.hrl").
-include("imem_sql.hrl").
-include("imem_ct.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_testcase(TestCase, _Config) ->
    ?CTPAL("start ~p",[TestCase]),

    catch imem_meta:drop_table(def),
    catch imem_meta:drop_table(tuple_test),
    catch imem_meta:drop_table(fun_test),

    ok.

%%====================================================================
%% Test cases.
%%====================================================================

test_with_or_without_sec_part1(IsSec) ->
    ?CTPAL("IsSec ~p",[IsSec]),
    ?CTPAL("Schema ~p",[imem_meta:schema()]),
    ?CTPAL("Data Nodes ~p",[imem_meta:data_nodes()]),

    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

    ?assertEqual([], imem_statement:receive_raw()),

    SKey = case IsSec of
               true -> ?imem_test_admin_login();
               false -> none
           end,

    catch imem_meta:drop_table(def),
    catch imem_meta:drop_table(tuple_test),
    catch imem_meta:drop_table(fun_test),

    %% test table tuple_test

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "
            create table fun_test (
                col0 integer
              , col1 integer default fun(_,Rec) -> element(2,Rec)*element(2,Rec) end.
            );"
        , 0, [{schema, imem}], IsSec)),

    Sql0a = "insert into fun_test (col0) values (12)",
    ?CTPAL("Sql0a"),
    ?assertEqual([{fun_test, 12, 144}], imem_sql:exec(SKey, Sql0a, 0, [{schema, imem}], IsSec)),

    TT0aRows = lists:sort(imem_statement:if_call_mfa(IsSec, read, [SKey, fun_test])),
    ?CTPAL("fun_test ~p",[TT0aRows]),
    [{fun_test, Col0a, Col1a}] = TT0aRows,
    ?assertEqual(Col0a * Col0a, Col1a),

    SR00 = exec(SKey, query00, 15, IsSec, "select col0, col1 from fun_test;"),
    ?CTPAL("SR00"),
    ?assertEqual(ok, fetch_async(SKey, SR00, [], IsSec)),
    [{<<"12">>, <<"144">>}] = receive_tuples(SR00, true),

    ChangeList00 = [
        [1, upd, {{1,node()}, {fun_test, 12, 144}}, <<"13">>, <<"0">>]
    ],
    ?CTPAL("ChangeList00"),
    ?assertEqual(ok, imem_statement:update_cursor_prepare(SKey, SR00, IsSec, ChangeList00)),
    ?assertEqual([{1, {{1,node()}, {fun_test, 13, 169}}}], imem_statement:update_cursor_execute(SKey, SR00, IsSec, optimistic)),

    ChangeList01 = [
        [2, ins, {{2,node()}, {}}, <<"15">>, <<"1">>]
    ],
    ?CTPAL("ChangeList01"),
    ?assertEqual(ok, imem_statement:update_cursor_prepare(SKey, SR00, IsSec, ChangeList01)),
    ?assertEqual([{2, {{1,node()}, {fun_test, 15, 225}}}], imem_statement:update_cursor_execute(SKey, SR00, IsSec, optimistic)),

    ?assertEqual(ok, imem_statement:fetch_close(SKey, SR00, IsSec)),
    ?assertEqual(ok, fetch_async(SKey, SR00, [], IsSec)),
    [{<<"13">>, <<"169">>}, {<<"15">>, <<"225">>}] = lists:sort(receive_tuples(SR00, true)),
    ?assertEqual(ok, imem_statement:close(SKey, SR00)),

    ?assertEqual(ok, imem_sql:exec(SKey, "drop table fun_test;", 0, [{schema, imem}], IsSec)),

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "
            create table tuple_test (
            col1 tuple, 
            col2 list,
            col3 tuple(2),
            col4 integer
            );"
        , 0, [{schema, imem}], IsSec)),

    Sql1a = "
            insert into tuple_test (
                col1,col2,col3,col4
            ) values (
                 '{key1,nonode@nohost}'
                ,'[key1a,key1b,key1c]'
                ,'{key1,{key1a,key1b}}'
                ,1 
            );",
    ?CTPAL("Sql1a"),
    ?assertEqual([{tuple_test, {key1, nonode@nohost}, [key1a, key1b, key1c], {key1, {key1a, key1b}}, 1}]
        , imem_sql:exec(SKey, Sql1a, 0, [{schema, imem}], IsSec)
    ),

    Sql1b = "
            insert into tuple_test (
                col1,col2,col3,col4
            ) values (
                 '{key2,somenode@somehost}'
                ,'[key2a,key2b,3,4]'
                ,'{a,''B2''}'
                ,2 
            );",
    ?CTPAL("Sql1b"),
    ?assertEqual([{tuple_test, {key2, somenode@somehost}, [key2a, key2b, 3, 4], {a, 'B2'}, 2}]
        , imem_sql:exec(SKey, Sql1b, 0, [{schema, imem}], IsSec)
    ),

    Sql1c = "
            insert into tuple_test (
                col1,col2,col3,col4
            ) values (
                 '{key3,''nonode@nohost''}'
                ,'[key3a,key3b]'
                ,'{1,2}'
                ,3 
            );",
    ?CTPAL("Sql1c"),
    ?assertEqual([{tuple_test, {key3, nonode@nohost}, [key3a, key3b], {1, 2}, 3}]
        , imem_sql:exec(SKey, Sql1c, 0, [{schema, imem}], IsSec)
    ),

    TT1Rows = lists:sort(imem_statement:if_call_mfa(IsSec, read, [SKey, tuple_test])),
    TT1RowsExpected =
        [{tuple_test, {key1, nonode@nohost}, [key1a, key1b, key1c], {key1, {key1a, key1b}}, 1}
            , {tuple_test, {key2, somenode@somehost}, [key2a, key2b, 3, 4], {a, 'B2'}, 2}
            , {tuple_test, {key3, nonode@nohost}, [key3a, key3b], {1, 2}, 3}
        ],
    ?CTPAL("TT1RowsExpected"),
    ?assertEqual(TT1RowsExpected, TT1Rows),

    TT1a = exec(SKey, tt1a, 10, IsSec, "
            select col4 from tuple_test"
    ),
    ?assertEqual(ok, fetch_async(SKey, TT1a, [], IsSec)),
    ListTT1a = receive_tuples(TT1a, true),
    ?assertEqual(3, length(ListTT1a)),

    TT1b = exec(SKey, tt1b, 4, IsSec, "
            select
              element(1,col1)
            , element(1,col3)
            , element(2,col3)
            , col4 
            from tuple_test where col4=2"),
    ?CTPAL("TT1b"),
    ?assertEqual(ok, fetch_async(SKey, TT1b, [], IsSec)),
    ListTT1b = receive_tuples(TT1b, true),
    ?assertEqual(1, length(ListTT1b)),

    O1 = {tuple_test, {key1, nonode@nohost}, [key1a, key1b, key1c], {key1, {key1a, key1b}}, 1},
    O1X = {tuple_test, {keyX, nonode@nohost}, [key1a, key1b, key1c], {key1, {key1a, key1b}}, 1},
    O2 = {tuple_test, {key2, somenode@somehost}, [key2a, key2b, 3, 4], {a, 'B2'}, 2},
    O2X = {tuple_test, {key2, somenode@somehost}, [key2a, key2b, 3, 4], {a, b}, undefined},
    O3 = {tuple_test, {key3, nonode@nohost}, [key3a, key3b], {1, 2}, 3},

    TT1aChange = [
          [1, upd, {{1,node()}, O1}, <<"keyX">>, <<"key1">>, <<"{key1a,key1b}">>, <<"1">>]
        , [2, upd, {{2,node()}, O2}, <<"key2">>, <<"a">>, <<"b">>, <<"">>]
        , [3, upd, {{3,node()}, O3}, <<"key3">>, <<"1">>, <<"2">>, <<"3">>]
    ],
    ?CTPAL("TT1aChange"),
    ?assertEqual(ok, imem_statement:update_cursor_prepare(SKey, TT1b, IsSec, TT1aChange)),
    imem_statement:update_cursor_execute(SKey, TT1b, IsSec, optimistic),
    TT1aRows = lists:sort(imem_statement:if_call_mfa(IsSec, read, [SKey, tuple_test])),
    ?CTPAL("TT1aRows"),
    ?assertNotEqual(TT1Rows, TT1aRows),
    ?assert(lists:member(O1X, TT1aRows)),
    ?assert(lists:member(O2X, TT1aRows)),
    ?assert(lists:member(O3, TT1aRows)),

    O4X = {tuple_test, {key4}, [], {<<"">>, <<"">>}, 4},

    TT1bChange = [[4, ins, {}, <<"key4">>, <<"">>, <<"">>, <<"4">>]],
    ?CTPAL("TT1bChange"),
    ?assertEqual(ok, imem_statement:update_cursor_prepare(SKey, TT1b, IsSec, TT1bChange)),
    imem_statement:update_cursor_execute(SKey, TT1b, IsSec, optimistic),
    TT1bRows = lists:sort(imem_statement:if_call_mfa(IsSec, read, [SKey, tuple_test])),
    ?CTPAL("TT1bRows"),
    ?assert(lists:member(O4X, TT1bRows)),

    TT2a = exec(SKey, tt2, 4, IsSec, "
            select 
              col1
            , hd(col2), nth(2,col2)
            , col4
            from tuple_test
            where col4=1"),
    ?CTPAL("TT2a"),
    ?assertEqual(ok, fetch_async(SKey, TT2a, [], IsSec)),
    ListTT2a = receive_tuples(TT2a, true),
    ?assertEqual(1, length(ListTT2a)),

    TT2aChange = [
        [5, ins, {}, <<"{key5,nonode@nohost}">>, <<"a5">>, <<"b5">>, <<"5">>]
        , [6, ins, {}, <<"{key6,somenode@somehost}">>, <<"">>, <<"b6">>, <<"">>]
    ],
    O5X = {tuple_test, {key5, nonode@nohost}, [a5, b5], {}, 5},
    O6X = {tuple_test, {key6, somenode@somehost}, [<<"">>, b6], {}, undefined},
    ?CTPAL("TT2aChange"),
    ?assertEqual(ok, imem_statement:update_cursor_prepare(SKey, TT2a, IsSec, TT2aChange)),
    imem_statement:update_cursor_execute(SKey, TT2a, IsSec, optimistic),
    TT2aRows1 = lists:sort(imem_statement:if_call_mfa(IsSec, read, [SKey, tuple_test])),
    ?CTPAL("TT2aRows1"),
    ?assert(lists:member(O5X, TT2aRows1)),
    ?assert(lists:member(O6X, TT2aRows1)),

    % ?assertEqual(ok,imem_statement:close(SKey, TT1b)),
    % ?assertEqual(ok,imem_statement:close(SKey, TT2a)),

    ?assertEqual(ok, imem_sql:exec(SKey, "drop table tuple_test;", 0, [{schema, imem}], IsSec)),

    %% test table def

    ?CTPAL("create table def"),
    ?assertMatch({ok, _}, imem_sql:exec(SKey, "
            create table def (
                col1 varchar2(10), 
                col2 integer
            );"
        , 0, [{schema, imem}], IsSec)),

    ?assertEqual(ok, insert_range(SKey, 15, def, imem, IsSec)),

    TableRows1 = lists:sort(imem_statement:if_call_mfa(IsSec, read, [SKey, def])),
    ?CTPAL("TableRows1"),
    [_Meta] = imem_statement:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, def}]),

    ?CTPAL("SR0"),
    SR0 = exec(SKey, query0, 15, IsSec, "select * from def;"),
    try
        ?assertEqual(ok, fetch_async(SKey, SR0, [], IsSec)),
        List0 = receive_tuples(SR0, false),  % was true, MNESIA in erlang 20 does not allow to preview the end
        ?assertEqual(15, length(List0)),
        ?assertEqual([], imem_statement:receive_raw())
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR0))
    end,

    ?CTPAL("SR0a"),
    SR0a = exec(SKey, query0a, 4, IsSec, "select rownum from def;"),
    ?assertEqual(ok, fetch_async(SKey, SR0a, [], IsSec)),
    ?assertEqual([{<<"1">>}, {<<"2">>}, {<<"3">>}, {<<"4">>}], receive_tuples(SR0a, false)),
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual(ok, fetch_async(SKey, SR0a, [], IsSec)),
    ?assertEqual([{<<"5">>}, {<<"6">>}, {<<"7">>}, {<<"8">>}], receive_tuples(SR0a, false)),
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual(ok, fetch_async(SKey, SR0a, [], IsSec)),
    ?assertEqual([{<<"9">>}, {<<"10">>}, {<<"11">>}, {<<"12">>}], receive_tuples(SR0a, false)),
    ?assertEqual(ok, fetch_async(SKey, SR0a, [], IsSec)),
    ?assertEqual([{<<"13">>}, {<<"14">>}, {<<"15">>}], receive_tuples(SR0a, true)),
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual(ok, imem_statement:close(SKey, SR0a)),

    ?CTPAL("SR1"),
    SR1 = exec(SKey, query1, 4, IsSec, "select col1, col2 from def;"),
    ?assertEqual(ok, fetch_async(SKey, SR1, [], IsSec)),
    List1a = receive_tuples(SR1, false),
    ?assertEqual(4, length(List1a)),
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual(ok, fetch_async(SKey, SR1, [], IsSec)),
    List1b = receive_tuples(SR1, false),
    ?assertEqual(4, length(List1b)),
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual(ok, fetch_async(SKey, SR1, [], IsSec)),
    List1c = receive_tuples(SR1, false),
    ?assertEqual(4, length(List1c)),
    ?assertEqual(ok, fetch_async(SKey, SR1, [], IsSec)),
    List1d = receive_tuples(SR1, true),
    ?assertEqual(3, length(List1d)),
    ?assertEqual([], imem_statement:receive_raw()),

    %% ChangeList2 = [[OP,ID] ++ L || {OP,ID,L} <- lists:zip3([nop, ins, del, upd], [1,2,3,4], lists:map(RowFun2,List2a))],
    ChangeList2 = [
        [4, upd, {{1,node()}, {def, <<"12">>, 12}}, <<"112">>, <<"12">>]
    ],
    ?CTPAL("ChangeList2"),
    ?assertEqual(ok, imem_statement:update_cursor_prepare(SKey, SR1, IsSec, ChangeList2)),
    imem_statement:update_cursor_execute(SKey, SR1, IsSec, optimistic),
    TableRows2 = lists:sort(imem_statement:if_call_mfa(IsSec, read, [SKey, def])),
    ?CTPAL("TableRows2"),
    ?assert(TableRows1 /= TableRows2),
    ?assertEqual(length(TableRows1), length(TableRows2)),
    ?assertEqual(true, lists:member({def, <<"112">>, 12}, TableRows2)),
    ?assertEqual(false, lists:member({def, "12", 12}, TableRows2)),

    ChangeList3 = [
        [1, nop, {{1,node()}, {def, <<"2">>, 2}}, <<"2">>, <<"2">>],         %% no operation on this line
        [5, ins, {}, <<"99">>, <<"undefined">>],             %% insert {def,"99", undefined}
        [3, del, {{3,node()}, {def, <<"5">>, 5}}, <<"5">>, <<"5">>],         %% delete {def,"5",5}
        [4, upd, {{4,node()}, {def, <<"112">>, 12}}, <<"112">>, <<"12">>],   %% nop update {def,"112",12}
        [6, upd, {{6,node()}, {def, <<"10">>, 10}}, <<"10">>, <<"110">>]     %% update {def,"10",10} to {def,"10",110}
    ],
    ExpectedRows3 = [
        {def, <<"2">>, 2},                            %% no operation on this line
        {def, <<"99">>, undefined},                   %% insert {def,"99", undefined}
        {def, <<"10">>, 110},                         %% update {def,"10",10} to {def,"10",110}
        {def, <<"112">>, 12}                          %% nop update {def,"112",12}
    ],
    RemovedRows3 = [
        {def, <<"5">>, 5}                             %% delete {def,"5",5}
    ],
    ExpectedKeys3 = [
        {1, {{1,node()}, {def, <<"2">>, 2}}},
        {3, {{3,node()}, {}}},
        {4, {{4,node()}, {def, <<"112">>, 12}}},
        {5, {{5,node()}, {def, <<"99">>, undefined}}},
        {6, {{6,node()}, {def, <<"10">>, 110}}}
    ],
    ?CTPAL("ChangeList3"),
    ?assertEqual(ok, imem_statement:update_cursor_prepare(SKey, SR1, IsSec, ChangeList3)),
    ChangedKeys3 = imem_statement:update_cursor_execute(SKey, SR1, IsSec, optimistic),
    TableRows3 = lists:sort(imem_statement:if_call_mfa(IsSec, read, [SKey, def])),
    ?CTPAL("TableRows3"),
    [?assert(lists:member(R, TableRows3)) || R <- ExpectedRows3],
    [?assertNot(lists:member(R, TableRows3)) || R <- RemovedRows3],
    ?assertEqual(ExpectedKeys3, lists:sort([{I, setelement(?MetaIdx, C, {1,node()})} || {I, C} <- ChangedKeys3])),

    ?assertEqual(ok, imem_statement:if_call_mfa(IsSec, truncate_table, [SKey, def])),
    ?assertEqual(0, imem_meta:table_size(def)),
    ?assertEqual(ok, insert_range(SKey, 5, def, imem, IsSec)),
    ?assertEqual(5, imem_meta:table_size(def)),

    ?assertEqual(ok, imem_statement:close(SKey, SR1)),

    ?CTPAL("SR2"),    
    SR2 = exec(SKey, query2, 100, IsSec, "
            select rownum + 1  as RowNumPlus
            from def 
            where col2 <= 5;"
    ),
    try
        ?assertEqual(ok, fetch_async(SKey, SR2, [{tail_mode, true}], IsSec)),
        ?CTPAL("List2a"),
        List2a = receive_tuples(SR2, true),
        ?assertEqual(5, length(List2a)),
        ?assertEqual([{<<"2">>}, {<<"3">>}, {<<"4">>}, {<<"5">>}, {<<"6">>}], List2a),
        ?assertEqual([], imem_statement:receive_raw()),
        ?assertEqual(ok, insert_range(SKey, 10, def, imem, IsSec)),
        ?assertEqual(10, imem_meta:table_size(def)),  %% unchanged, all updates
        ?CTPAL("List2b"),
        List2b = receive_tuples(SR2, tail),
        ?assertEqual(5, length(List2b)),             %% 10 updates, 5 filtered with TailFun()           
        ?assertEqual([{<<"7">>}, {<<"8">>}, {<<"9">>}, {<<"10">>}, {<<"11">>}], List2b),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR2, IsSec)),
        ?assertEqual(ok, insert_range(SKey, 5, def, imem, IsSec)),
        ?assertEqual([], imem_statement:receive_raw())
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR2))
    end,

    ?assertEqual(ok, imem_statement:if_call_mfa(IsSec, truncate_table, [SKey, def])),
    ?assertEqual(0, imem_meta:table_size(def)),
    ?assertEqual(ok, insert_range(SKey, 5, def, imem, IsSec)),
    ?assertEqual(5, imem_meta:table_size(def)),

    ?CTPAL("SR3"),
    SR3 = exec(SKey, query3, 2, IsSec, "
            select rownum 
            from def t1, def t2 
            where t2.col1 = t1.col1 
            and t2.col2 <= 5;"
    ),
    try
        ?assertEqual(ok, fetch_async(SKey, SR3, [{tail_mode, true}], IsSec)),
        ?CTPAL("List3a"),
        List3a = receive_tuples(SR3, false),
        ?assertEqual(2, length(List3a)),
        ?assertEqual([{<<"1">>}, {<<"2">>}], List3a),
        ?assertEqual(ok, fetch_async(SKey, SR3, [{fetch_mode, push}, {tail_mode, true}], IsSec)),
        ?CTPAL("List3b"),
        List3b = receive_tuples(SR3, true),
        ?assertEqual(3, length(List3b)),    %% TODO: Should this come split into 2 + 1 rows ?
        ?assertEqual([{<<"3">>}, {<<"4">>}, {<<"5">>}], List3b),
        ?assertEqual(ok, insert_range(SKey, 10, def, imem, IsSec)),
        ?assertEqual(10, imem_meta:table_size(def)),
        ?CTPAL("List3c"),
        List3c = receive_tuples(SR3, tail),
        ?assertEqual(5, length(List3c)),
        ?assertEqual([{<<"6">>}, {<<"7">>}, {<<"8">>}, {<<"9">>}, {<<"10">>}], List3c),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR3, IsSec)),
        ?assertEqual(ok, insert_range(SKey, 5, def, imem, IsSec)),
        ?assertEqual([], imem_statement:receive_raw())
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR3))
    end,

    ok.

test_with_sec_part1(_Config) ->
    ?CTPAL("Start"),
    test_with_or_without_sec_part1(true).

test_without_sec_part1(_Config) ->
    ?CTPAL("Start"),
    test_with_or_without_sec_part1(false).

test_with_or_without_sec_part2(IsSec) ->

    ClEr = 'ClientError',

    ?assertEqual([], imem_statement:receive_raw()),

    SKey = case IsSec of
               true -> ?imem_test_admin_login();
               false -> none
           end,

    catch imem_meta:drop_table(def),

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "
            create table def (
                col1 varchar2(10), 
                col2 integer
            );"
        , 0, [{schema, imem}], IsSec)),

    ?assertEqual(ok, imem_statement:if_call_mfa(IsSec, truncate_table, [SKey, def])),
    ?assertEqual(0, imem_meta:table_size(def)),
    ?assertEqual(ok, insert_range(SKey, 10, def, imem, IsSec)),
    ?assertEqual(10, imem_meta:table_size(def)),

    SR3a = exec(SKey, query3a, 10, IsSec, "
            select rownum, t1.col2, t2.col2, t3.col2 
            from def t1, def t2, def t3 
            where t1.col2 < t2.col2 
            and t2.col2 < t3.col2
            and t3.col2 < 5"
    ),
    ?assertEqual(ok, fetch_async(SKey, SR3a, [], IsSec)),
    Cube3a = receive_tuples(SR3a, false),  % was true, MNESIA in Erlang 20 does not allow any more to detect the end
    ?assertEqual(4, length(Cube3a)),
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual(ok, imem_statement:close(SKey, SR3a)),

    SR3b = exec(SKey, query3b, 10, IsSec, "
            select rownum, t1.col2, t2.col2, t3.col2 
            from def t1, def t2, def t3 
            where t1.col2 < t2.col2 
            and t2.col2 < t3.col2
            and t3.col2 < 6"
    ),
    ?assertEqual(ok, fetch_async(SKey, SR3b, [], IsSec)),
    Cube3b = receive_tuples(SR3b, false),  % was true, MNESIA in Erlang 20 does not allow any more to detect the end
    ?assertEqual(10, length(Cube3b)),
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual(ok, imem_statement:close(SKey, SR3b)),

    SR3c = exec(SKey, query3c, 10, IsSec, "
            select rownum, t1.col2, t2.col2, t3.col2 
            from def t1, def t2, def t3 
            where t1.col2 < t2.col2 
            and t2.col2 < t3.col2
            and t3.col2 < 7"
    ),
    ?assertEqual(ok, fetch_async(SKey, SR3c, [], IsSec)),
    Cube3c1 = receive_tuples(SR3c, false),  % was true, MNESIA in Erlang 20 does not allow any more to detect the end
    ?assertEqual(20, length(Cube3c1)),      %% TODO: streaming join evaluation needed to keep this down at 10
    ?assertEqual(lists:seq(1, 20), lists:sort([list_to_integer(binary_to_list(element(1, Res))) || Res <- Cube3c1])),
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual(ok, imem_statement:close(SKey, SR3c)),

    SR3d = exec(SKey, query3d, 10, IsSec, "
            select rownum 
            from def t1, def t2, def t3"
    ),
    ?assertEqual(ok, fetch_async(SKey, SR3d, [], IsSec)),
    % [?assertEqual(100, length(receive_tuples(SR3d,false))) || _ <- lists:seq(1,9)],   %% TODO: should come in chunks
    ?assertEqual(1000, length(receive_tuples(SR3d, false))),            % was true, cuanhed for MNESIA in Erlang 20
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual(ok, imem_statement:close(SKey, SR3d)),

    SR3e = exec(SKey, query3e, 10, IsSec, "
            select rownum 
            from def, def, def, def"
    ),
    ?assertEqual(ok, fetch_async(SKey, SR3e, [], IsSec)),
    % ?assertEqual(10000, length(receive_tuples(SR3e,true))),           %% 50 ms is not enough to fetch 10'000 rows
    % ?assertEqual(10000, length(receive_tuples(SR3e,true,1500,[]))),   %% works but times out the test
    ?assertEqual(ok, imem_statement:close(SKey, SR3e)),                                %% test for stmt teardown while joining big result

    ?assertEqual(10, imem_meta:table_size(def)),

    SR4 = exec(SKey, query4, 5, IsSec, "
            select col1 from def;"
    ),
    try
        ?assertEqual(ok, fetch_async(SKey, SR4, [], IsSec)),
        ?CTPAL("List4a"),
        List4a = receive_tuples(SR4, false),
        ?assertEqual(5, length(List4a)),                                % first 5 rows
        ?CTPAL("trying to insert one row before fetch complete"),
        ?assertEqual(ok, insert_range(SKey, 1, def, imem, IsSec)),      % overwrite, still 10 rows in table, 5 fetched so far
        ?CTPAL("completed insert one row before fetch complete"),
        ?assertEqual(ok, fetch_async(SKey, SR4, [{tail_mode, true}], IsSec)),
        ?CTPAL("List4b"),
        List4b = receive_tuples(SR4, false),                            % was true, changed for MNESIA in Erlang 20
        ?assertEqual(5, length(List4b)),                                % 10 rows now in table, 10 fetched so far
        ?assertEqual(ok, fetch_async(SKey, SR4, [{tail_mode, true}], IsSec)),
        [{_, {[], true}}] = imem_statement:receive_raw(),               % forced cursor into tail mode now (needed with erlang 20)
        ?assertEqual(ok, insert_range(SKey, 1, def, imem, IsSec)),      % overwrite, still 10 rows now in table, 11 fetched so far
        ?CTPAL("List4c"),
        List4c = receive_tuples(SR4, tail),
        ?assertEqual(1, length(List4c)),                                % 10 rows in table, 12 fetched so far
        ?assertEqual(ok, insert_range(SKey, 1, def, imem, IsSec)),      % 1 overwrite, still 10 rows in table, 12 fetched so far
        ?assertEqual(ok, insert_range(SKey, 11, def, imem, IsSec)),     % 10 overwrites + 1 insert, 11 rows now in table, 12 fetched so far
        ?assertEqual(11, imem_meta:table_size(def)),                    % 11 rows in table
        ?CTPAL("List4d"),
        List4d = receive_tuples(SR4, tail),
        ?assertEqual(12, length(List4d)),
        ?CTPAL("12 tail rows received in single packets"),
        ?assertEqual(ok, fetch_async(SKey, SR4, [], IsSec)),
        ?CTPAL("Result4e"),
        Result4e = imem_statement:receive_raw(),
        [{StmtRef4, {error, {ClEr, Reason4e}}}] = Result4e,
        ?assertEqual("Fetching in tail mode, execute fetch_close before fetching from start again", Reason4e),
        ?assertEqual([StmtRef4], SR4#stmtResults.stmtRefs),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR4, IsSec)),
        ?assertEqual(ok, insert_range(SKey, 5, def, imem, IsSec)),
        ?assertEqual([], imem_statement:receive_raw())
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR4))
    end,

    ?CTPAL("SR5"),
    SR5 = exec(SKey, query5, 100, IsSec, "
        select to_name(qname) from all_tables 
        where element(1,qname) = to_atom('imem')
        and element(2,qname) like 'd%'
    "
    ),
    try
        ?assertEqual(ok, fetch_async(SKey, SR5, [], IsSec)),
        List5a = receive_tuples(SR5, true),
        ?assert(lists:member({<<"imem.def">>}, List5a)),
        ?assert(lists:member({<<"imem.ddTable">>}, List5a)),
        ?CTPAL("first read success (async)"),
        ?assertEqual(ok, fetch_async(SKey, SR5, [], IsSec)),
        [{StmtRef5, {error, Reason5a}}] = imem_statement:receive_raw(),
        ?assertEqual({'ClientError',
            "Fetch is completed, execute fetch_close before fetching from start again"},
            Reason5a),
        ?assertEqual([StmtRef5], SR5#stmtResults.stmtRefs),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR5, IsSec)),
        ?assertEqual(ok, fetch_async(SKey, SR5, [], IsSec)),
        List5b = receive_tuples(SR5, true),
        ?CTPAL("List5b ~p",[List5b]),
        D12 = List5a--List5b,
        D21 = List5b--List5a,
        ?assert((D12 == []) orelse (lists:usort([imem_meta:is_local_time_partitioned_table(N) || {N} <- D12]) == [true])),
        ?assert((D21 == []) orelse (lists:usort([imem_meta:is_local_time_partitioned_table(N) || {N} <- D21]) == [true])),
        ?CTPAL("second read success (async)"),
        ?assertException(throw,
            {'ClientError', "Fetch is completed, execute fetch_close before fetching from start again"},
            imem_statement:fetch_recs_sort(SKey, SR5, {self(), make_ref()}, 1000, IsSec)
        ),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR5, IsSec)), % actually not needed here, fetch_recs does it
        List5c = imem_statement:fetch_recs_sort(SKey, SR5, {self(), make_ref()}, 1000, IsSec),
        ?CTPAL("List5c ~p",[List5c]),
        ?assertEqual(length(List5b), length(List5c)),
        ?assertEqual(lists:sort(List5b), lists:sort(imem_statement:result_tuples(List5c, SR5#stmtResults.rowFun))),
        ?CTPAL("third read success (sync)"),
        ok
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR4))
    end,

    RowCount6 = imem_meta:table_size(def),
    SR6 = exec(SKey, query6, 3, IsSec, "
            select col1 from def;"
    ),
    try
        ?assertEqual(ok, fetch_async(SKey, SR6, [{fetch_mode, push}], IsSec)),
        List6a = receive_tuples(SR6, true),
        ?assertEqual(RowCount6, length(List6a)),
        ?assertEqual([], imem_statement:receive_raw()),
        ?assertEqual(ok, insert_range(SKey, 5, def, imem, IsSec)),
        ?assertEqual([], imem_statement:receive_raw())
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR6))
    end,

    SR7 = exec(SKey, query7, 3, IsSec, "
            select col1 from def;"
    ),
    try
        ?assertEqual(ok, fetch_async(SKey, SR7, [{fetch_mode, skip}, {tail_mode, true}], IsSec)),
        ?assertEqual([], imem_statement:receive_raw()),
        ?assertEqual(ok, insert_range(SKey, 5, def, imem, IsSec)),
        List7 = receive_tuples(SR7, tail),
        ?assertEqual(5, length(List7)),
        ?assertEqual([], imem_statement:receive_raw()),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR7, IsSec)),
        ?assertEqual(ok, insert_range(SKey, 5, def, imem, IsSec)),
        ?assertEqual([], imem_statement:receive_raw())
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR7))
    end,

    SR7a = exec(SKey, query7a, 3, IsSec, [{params, [{<<":key">>, <<"integer">>, <<"0">>, [<<"3">>]}]}], "
            select col2 from def where col2 = :key ;"
    ),
    try
        ?assertEqual(ok, fetch_async(SKey, SR7a, [], IsSec)),
        List7a = receive_tuples(SR7a, true),
        ?assertEqual([{<<"3">>}], List7a),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR7a, IsSec)),
        ?assertEqual(ok, fetch_async(SKey, SR7a, [{params, [{<<":key">>, <<"integer">>, <<"0">>, [<<"5">>]}]}], IsSec)),
        List7a1 = receive_tuples(SR7a, true),
        ?assertEqual([{<<"5">>}], List7a1),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR7a, IsSec))
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR7a))
    end,

    SR7b = exec(SKey, query7b, 3, IsSec, [{params, [{<<":key">>, <<"integer">>, <<"0">>, [<<"3">>]}]}], "
            select systimestamp, :key from def where col2 = :key ;"
    ),
    try
        ?assertEqual(ok, fetch_async(SKey, SR7b, [{params, [{<<":key">>, <<"integer">>, <<"0">>, [<<"4">>]}]}], IsSec)),
        List7b = receive_tuples(SR7b, true),
        ?assertMatch([{<<_:16, $., _:16, $., _:32, 32, _:16, $:, _:16, $:, _:16, $., _:48>>, <<"4">>}], List7b),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR7b, IsSec)),
        ?assertEqual(ok, fetch_async(SKey, SR7b, [{params, [{<<":key">>, <<"integer">>, <<"0">>, [<<"6">>]}]}], IsSec)),
        List7b1 = receive_tuples(SR7b, true),
        ?assertMatch([{<<_:16, $., _:16, $., _:32, 32, _:16, $:, _:16, $:, _:16, $., _:48>>, <<"6">>}], List7b1),
        ?assertNotEqual(List7b, List7b1),
        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR7b, IsSec))
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR7b))
    end,

    ok.

test_with_sec_part2(_Config) ->
    ?CTPAL("Start"),
    test_with_or_without_sec_part2(true).

test_without_sec_part2(_Config) ->
    ?CTPAL("Start"),
    test_with_or_without_sec_part2(false).

test_with_or_without_sec_part3(IsSec) ->
    ?CTPAL("IsSec ~p",[IsSec]),
    ?CTPAL("Schema ~p",[imem_meta:schema()]),
    ?CTPAL("Data Nodes ~p",[imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

    ?assertEqual([], imem_statement:receive_raw()),

    SKey = case IsSec of
               true -> ?imem_test_admin_login();
               false -> none
           end,

    catch imem_meta:drop_table(def),

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "
            create table def (
                col1 varchar2(10), 
                col2 integer
            );"
        , 0, [{schema, imem}], IsSec)),

    ?assertEqual(ok, insert_range(SKey, 11, def, imem, IsSec)),

    ?CTPAL("_TableRows1"),
    _TableRows1 = lists:sort(imem_statement:if_call_mfa(IsSec, read, [SKey, def])),
    [_Meta] = imem_statement:if_call_mfa(IsSec, read, [SKey, ddTable, {imem, def}]),
    
    ?CTPAL("SR8"),
    SR8 = exec(SKey, query8, 100, IsSec, "
            select 
                  col1 as c1
                , col2 
            from def 
            where col1 < '4' 
            order by col2 desc;"
    ),
    ?CTPAL("StmtCols8 ~p",[SR8#stmtResults.rowCols]),
    ?CTPAL("SortSpec8 ~p",[SR8#stmtResults.sortSpec]),
    ?assertEqual([{2, <<"desc">>}], SR8#stmtResults.sortSpec),

    try
        ?assertEqual(ok, fetch_async(SKey, SR8, [], IsSec)),
        ?CTPAL("List8a"),
        List8a = imem_statement:receive_recs(SR8, true),
        ?assertEqual([{<<"11">>, <<"11">>}, {<<"10">>, <<"10">>}, {<<"3">>, <<"3">>}, {<<"2">>, <<"2">>}, {<<"1">>, <<"1">>}], result_tuples_sort(List8a, SR8#stmtResults.rowFun, SR8#stmtResults.sortFun)),
        Result8a = imem_statement:filter_and_sort(SKey, SR8, {'and', []}, [{2, 2, <<"asc">>}], [], IsSec),
        ?CTPAL("Result8a"),
        {ok, Sql8b, SF8b} = Result8a,
        Sorted8b = [{<<"1">>, <<"1">>}, {<<"10">>, <<"10">>}, {<<"11">>, <<"11">>}, {<<"2">>, <<"2">>}, {<<"3">>, <<"3">>}],
        ?assertEqual(Sorted8b, result_tuples_sort(List8a, SR8#stmtResults.rowFun, SF8b)),
        Expected8b = "select col1 c1, col2 from def where col1 < '4' order by col1 asc",
        ?CTPAL("Expected8b"),
        ?assertEqual(Expected8b, string:strip(binary_to_list(Sql8b))),

        {ok, Sql8c, SF8c} = imem_statement:filter_and_sort(SKey, SR8, {'and', [{1, [<<"$in$">>, <<"1">>, <<"2">>, <<"3">>]}]}, [{?MainIdx, 2, <<"asc">>}], [1], IsSec),
        ?assertEqual(Sorted8b, result_tuples_sort(List8a, SR8#stmtResults.rowFun, SF8c)),
        Expected8c = "select col1 c1 from def where imem.def.col1 in ('1', '2', '3') and col1 < '4' order by col1 asc",
        ?CTPAL("Expected8c"),
        ?assertEqual(Expected8c, string:strip(binary_to_list(Sql8c))),

        {ok, Sql8d, SF8d} = imem_statement:filter_and_sort(SKey, SR8, {'or', [{1, [<<"$in$">>, <<"3">>]}]}, [{?MainIdx, 2, <<"asc">>}, {?MainIdx, 3, <<"desc">>}], [2], IsSec),
        ?assertEqual(Sorted8b, result_tuples_sort(List8a, SR8#stmtResults.rowFun, SF8d)),
        Expected8d = "select col2 from def where imem.def.col1 = '3' and col1 < '4' order by col1 asc, col2 desc",
        ?CTPAL("Expected8d"),
        ?assertEqual(Expected8d, string:strip(binary_to_list(Sql8d))),

        {ok, Sql8e, SF8e} = imem_statement:filter_and_sort(SKey, SR8, {'or', [{1, [<<"$in$">>, <<"3">>]}, {2, [<<"$in$">>, <<"3">>]}]}, [{?MainIdx, 2, <<"asc">>}, {?MainIdx, 3, <<"desc">>}], [2, 1], IsSec),
        ?assertEqual(Sorted8b, result_tuples_sort(List8a, SR8#stmtResults.rowFun, SF8e)),
        Expected8e = "select col2, col1 c1 from def where (imem.def.col1 = '3' or imem.def.col2 = 3) and col1 < '4' order by col1 asc, col2 desc",
        ?CTPAL("Expected8e"),
        ?assertEqual(Expected8e, string:strip(binary_to_list(Sql8e))),

        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR8, IsSec))
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR8))
    end,

    ?CTPAL("SR9"),
    SR9 = exec(SKey, query9, 100, IsSec, "
            select * from ddTable;"
    ),
    ?CTPAL("StmtCols9 ~p", [SR9#stmtResults.rowCols]),
    ?CTPAL("SortSpec9 ~p", [SR9#stmtResults.sortSpec]),
    try
        Result9 = imem_statement:filter_and_sort(SKey, SR9, {undefined, []}, [], [1, 3, 2], IsSec),
        ?CTPAL("Result9"),
        {ok, Sql9, _SF9} = Result9,
        Expected9 = "select qname, opts, columns from ddTable",
        ?assertEqual(Expected9, string:strip(binary_to_list(Sql9))),

        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR9, IsSec))
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR9))
    end,

    ?CTPAL("SR9a"),
    SR9a = exec(SKey, query9a, 100, IsSec, "
            select * 
            from def a, def b 
            where a.col1 = b.col1;"
    ),
    ?CTPAL("StmtCols9a ~p", [SR9a#stmtResults.rowCols]),
    try
        Result9a = imem_statement:filter_and_sort(SKey, SR9a, {undefined, []}, [{1, <<"asc">>}, {3, <<"desc">>}], [1, 3, 2], IsSec),
        ?CTPAL("Result9a"),
        {ok, Sql9a, _SF9a} = Result9a,
        ?CTPAL("Sql9a ~p", [Sql9a]),
        Expected9a = "select a.col1, b.col1, a.col2 from def a, def b where a.col1 = b.col1 order by 1 asc, 3 desc",
        ?assertEqual(Expected9a, string:strip(binary_to_list(Sql9a))),

        Result9b = imem_statement:filter_and_sort(SKey, SR9a, {undefined, []}, [{3, 2, <<"asc">>}, {2, 3, <<"desc">>}], [1, 3, 2], IsSec),
        ?CTPAL("Result9b"),
        {ok, Sql9b, _SF9b} = Result9b,
        ?CTPAL("Sql9b ~p", [Sql9b]),
        Expected9b = "select a.col1, b.col1, a.col2 from def a, def b where a.col1 = b.col1 order by b.col1 asc, a.col2 desc",
        ?assertEqual(Expected9b, string:strip(binary_to_list(Sql9b))),

        ?assertEqual(ok, imem_statement:fetch_close(SKey, SR9a, IsSec))
    after
        ?assertEqual(ok, imem_statement:close(SKey, SR9))
    end,


    SR10 = exec(SKey, query10, 100, IsSec, "
            select 
                 a.col1
                ,b.col1 
            from def a, def b 
            where a.col1=b.col1;"
    ),
    ?assertEqual(ok, fetch_async(SKey, SR10, [], IsSec)),
    List10a = receive_tuples(SR10, true),
    ?CTPAL("List10a"),
    ?assertEqual(11, length(List10a)),

    ChangeList10 = [
        [1, upd, {{1,node()}, {def, <<"5">>, 5}, {def, <<"5">>, 5}}, <<"X">>, <<"Y">>]
    ],
    ?assertEqual(ok, imem_statement:update_cursor_prepare(SKey, SR10, IsSec, ChangeList10)),
    ?CTPAL("Result10b"),
    Result10b = imem_statement:update_cursor_execute(SKey, SR10, IsSec, optimistic),
    ?assertMatch([{1, {_, {def, <<"X">>, 5}, {def, <<"X">>, 5}}}], Result10b),

    ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, [{schema, imem}], IsSec)),

    case IsSec of
        true -> ?imem_logout(SKey);
        false -> ok
    end,

    ok.

test_with_sec_part3(_Config) ->
    ?CTPAL("Start"),
    test_with_or_without_sec_part3(true).

test_without_sec_part3(_Config) ->
    ?CTPAL("Start"),
    test_with_or_without_sec_part3(false).

%%====================================================================
%% Helper functions.
%%====================================================================

exec(SKey, Id, BS, IsSec, Sql) ->
    exec(SKey, Id, BS, IsSec, [], Sql).

exec(SKey, _Id, BS, IsSec, Opts, Sql) ->
    ?CTPAL("exec ~p ~p",[_Id, lists:flatten(Sql)]),
    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, Opts, IsSec),
    ?assertEqual(ok, RetCode),
    #stmtResults{rowCols=RowCols} = StmtResult,
    %?CTPAL("Statement Cols ~p",[RowCols]),
    [?assert(is_binary(SC#rowCol.alias)) || SC <- RowCols],
    StmtResult.

fetch_async(SKey, StmtResult, Opts, IsSec) ->
    ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtResult, {self(), make_ref()}, Opts, IsSec)).

insert_range(_SKey, 0, _Table, _Schema, _IsSec) -> ok;
insert_range(SKey, N, Table, Schema, IsSec) when is_integer(N), N > 0 ->
    imem_statement:if_call_mfa(IsSec, write, [SKey, Table, {Table, list_to_binary(integer_to_list(N)), N}]),
    insert_range(SKey, N - 1, Table, Schema, IsSec).

receive_tuples(StmtResult, Complete) ->
    receive_tuples(StmtResult, Complete, 120, []).

% receive_tuples(StmtResult, Complete, Timeout) ->
%     receive_tuples(StmtResult, Complete, Timeout,[]).

receive_tuples(#stmtResults{stmtRefs=[StmtRef|_], rowFun=RowFun} = StmtResult, Complete, Timeout, Acc) ->
    case receive
             R ->    % ?Debug("~p got:~n~p~n", [?TIMESTAMP,R]),
                 R
         after Timeout ->
            stop
         end of
        stop ->
            Unchecked = case Acc of
                            [] ->
                                throw({no_response, {expecting, Complete}});
                            [{StmtRef, {_, Complete}} | _] ->
                                lists:reverse(Acc);
                            [{StmtRef, {L1, C1}} | _] ->
                                throw({bad_complete, {StmtRef, {L1, C1}}});
                            Res ->
                                throw({bad_receive, lists:reverse(Res)})
                        end,
            case lists:usort([element(1, SR) || SR <- Unchecked]) of
                [StmtRef] ->
                    %?CTPAL("Unchecked receive result : ~p",[Unchecked]),
                    List = lists:flatten([element(1, element(2, T)) || T <- Unchecked]),
                    RT = imem_statement:result_tuples(List, RowFun),
                    if
                        length(RT) =< 10 ->
                            %?CTPAL("Received : ~p",[RT]),
                            ok;
                        true ->
                            %?CTPAL("Received : ~p items:~n~p~n~p~n~p",[length(RT), hd(RT), '...', lists:last(RT)]),
                            ok
                    end,
                    RT;
                StmtRefs ->
                    throw({bad_stmtref, lists:delete(StmtRef, StmtRefs)})
            end;
        {_, Result} ->
            receive_tuples(StmtResult, Complete, Timeout, [Result | Acc])
    end.

% result_lists(List,RowFun) when is_list(List), is_function(RowFun) ->  
%     lists:map(RowFun,List).

% result_lists_sort(List,RowFun,SortFun) when is_list(List), is_function(RowFun), is_function(SortFun) ->  
%     lists:map(RowFun,imem_statement:recs_sort(List,SortFun)).

result_tuples_sort(List, RowFun, SortFun) when is_list(List), is_function(RowFun), is_function(SortFun) ->
    [list_to_tuple(R) || R <- lists:map(RowFun, imem_statement:recs_sort(List, SortFun))].
