%%%-------------------------------------------------------------------
%%% File        : imem_meta_ct.erl
%%% Description : Common testing imem_meta.
%%%
%%% Created     : 09.11.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_meta_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_testcase/2,
    meta_concurrency/1,
    meta_operations/1,
    meta_partitions/1,
    meta_preparations/1
]).

-define(LOG_TABLE_OPTS, [{record_name, ddLog}
    , {type, ordered_set}
    , {purge_delay, 430000}        %% 430000 = 5 Days - 2000 sec
]).
-define(TPTEST0, tpTest_1000@).
-define(TPTEST1, tpTest1_999999999@_).
-define(TPTEST2, tpTest_100@).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include_lib("imem_meta.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_testcase(TestCase, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_testcase/2 - Start(~p) ===>~n", [TestCase]),

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

    ok.

%%====================================================================
%% Test cases.
%%====================================================================

meta_concurrency(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_concurrency/1 - Start ===>~n", []),

    T_m_c = 'imem_table_123',

    ?assertMatch({ok, _}, imem_meta:create_table(T_m_c, [hlk, val], [])),
    Self = self(),
    Key = [sum],
    ?assertEqual(ok, imem_meta:write(T_m_c, {T_m_c, Key, 0})),
    [spawn(fun() ->
        Self ! {N, read_write_test(T_m_c, Key, N)} end) || N <- lists:seq(1, 10)],
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p", [read_write_spawned]),
    ReadWriteResult = receive_results(10, []),
    ?assertEqual(10, length(ReadWriteResult)),
    ?assertEqual([{T_m_c, Key, 55}], imem_meta:read(T_m_c, Key)),
    ?assertEqual([{atomic, ok}], lists:usort([R || {_, R} <- ReadWriteResult])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [bulk_read_write]),
    ?assertEqual(ok, imem_meta:drop_table(T_m_c)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_table]),

    ok.

meta_operations(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_operations/1 - Start ===>~n", []),

    ClEr = 'ClientError',
    SyEx = 'SystemException',
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    ?assertEqual([imem_meta:node_shard()], imem_meta:node_shards()),

    ?assertEqual(ok, imem_meta:check_table_meta(ddTable, record_info(fields, ddTable))),

    ?assertMatch({ok, _}, imem_meta:create_check_table(?LOG_TABLE, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system)),
    ?assertException(throw, {SyEx, {"Wrong table owner", {_, [system, admin]}}}, imem_meta:create_check_table(?LOG_TABLE, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], admin)),
    ?assertException(throw, {SyEx, {"Wrong table options", {_, _}}}, imem_meta:create_check_table(?LOG_TABLE, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog1}, {type, ordered_set}], system)),
    ?assertEqual(ok, imem_meta:check_table(?LOG_TABLE)),

    ?assertEqual(ok, imem_meta:check_table(?CACHE_TABLE)),

    Now = ?TIME_UID,
    LogCount1 = imem_meta:table_size(?LOG_TABLE),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":ddLog@ count ~p~n", [LogCount1]),
    Fields = [{test_criterium_1, value1}, {test_criterium_2, value2}],
    LogRec0 = #ddLog{logTime = Now, logLevel = info, pid = self()
        , module = ?MODULE, function = meta_operations, node = node()
        , fields = Fields, message = <<"some log message">>},
    ?assertEqual(ok, imem_meta:write(?LOG_TABLE, LogRec0)),
    LogCount2 = imem_meta:table_size(?LOG_TABLE),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":ddLog@ count ~p~n", [LogCount2]),
    ?assert(LogCount2 > LogCount1),
    _Log1 = imem_meta:read(?LOG_TABLE, Now),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":ddLog@ content ~p~n", [_Log1]),
    ?assertEqual(ok, imem_meta:log_to_db(info, ?MODULE, test, [{test_3, value3}, {test_4, value4}], "Message")),
    ?assertEqual(ok, imem_meta:log_to_db(info, ?MODULE, test, [{test_3, value3}, {test_4, value4}], [])),
    ?assertEqual(ok, imem_meta:log_to_db(info, ?MODULE, test, [{test_3, value3}, {test_4, value4}], [stupid_error_message, 1])),
    ?assertEqual(ok, imem_meta:log_to_db(info, ?MODULE, test, [{test_3, value3}, {test_4, value4}], {stupid_error_message, 2})),
    LogCount2a = imem_meta:table_size(?LOG_TABLE),
    ?assert(LogCount2a >= LogCount2 + 4),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p:test_database_operations~n", [?MODULE]),
    Types1 = [#ddColumn{name = a, type = string, len = 10}     %% key
        , #ddColumn{name = b1, type = binstr, len = 20}    %% value 1
        , #ddColumn{name = c1, type = string, len = 30}    %% value 2
    ],
    Types2 = [#ddColumn{name = a, type = integer, len = 10}    %% key
        , #ddColumn{name = b2, type = float, len = 8, prec = 3}   %% value
    ],

    BadTypes0 = [#ddColumn{name = 'a', type = integer, len = 10}
    ],
    BadTypes1 = [#ddColumn{name = 'a', type = integer, len = 10}
        , #ddColumn{name = 'a:b', type = integer, len = 10}
    ],
    BadTypes2 = [#ddColumn{name = 'a', type = integer, len = 10}
        , #ddColumn{name = current, type = integer, len = 10}
    ],
    BadTypes3 = [#ddColumn{name = 'a', type = integer, len = 10}
        , #ddColumn{name = b, type = iinteger, len = 10}
    ],
    BadNames1 = [#ddColumn{name = 'a', type = integer, len = 10}
        , #ddColumn{name = a, type = integer, len = 10}
    ],

    ?assertMatch({ok, _}, imem_meta:create_table(meta_table_1, Types1, [])),
    ?assertEqual(ok, imem_meta:create_index(meta_table_1, [])),
    ?assertMatch(ok, imem_meta:check_table(meta_table_1Idx)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":ddTable for meta_table_1~n~p~n", [imem_meta:read(ddTable, {imem_meta:schema(), meta_table_1})]),
    ?assertEqual(ok, imem_meta:drop_index(meta_table_1)),
    ?assertException(throw, {'ClientError', {"Table does not exist", meta_table_1Idx}}, imem_meta:check_table(meta_table_1Idx)),
    ?assertEqual(ok, imem_meta:create_index(meta_table_1, [])),
    ?assertException(throw, {'ClientError', {"Index already exists", {meta_table_1}}}, imem_meta:create_index(meta_table_1, [])),
    ?assertEqual([], imem_meta:read(meta_table_1Idx)),
    ?assertEqual(ok, imem_meta:write(meta_table_1Idx, #ddIndex{stu = {1, 2, 3}})),
    ?assertEqual([#ddIndex{stu = {1, 2, 3}}], imem_meta:read(meta_table_1Idx)),

    Idx1Def = #ddIdxDef{id = 1, name = <<"string index on b1">>, type = ivk, pl = [<<"b1">>]},
    ?assertEqual(ok, imem_meta:create_or_replace_index(meta_table_1, [Idx1Def])),
    ?assertEqual([], imem_meta:read(meta_table_1Idx)),
    ?assertEqual([<<"table">>], imem_index:vnf_lcase_ascii_ne(<<"täble"/utf8>>)),
    ?assertEqual({meta_table_1, "meta", <<"täble"/utf8>>, "1"}, imem_meta:insert(meta_table_1, {meta_table_1, "meta", <<"täble"/utf8>>, "1"})),
    ?assertEqual([{meta_table_1, "meta", <<"täble"/utf8>>, "1"}], imem_meta:read(meta_table_1)),
    ?assertEqual([#ddIndex{stu = {1, <<"table">>, "meta"}}], imem_meta:read(meta_table_1Idx)),
    ?assertEqual([<<"tuble">>], imem_index:vnf_lcase_ascii_ne(<<"tüble"/utf8>>)),
    ?assertException(throw, {'ConcurrencyException', {"Data is modified by someone else", _}}, imem_meta:update(meta_table_1, {{meta_table_1, "meta", <<"tible"/utf8>>, "1"}, {meta_table_1, "meta", <<"tüble"/utf8>>, "1"}})),
    ?assertEqual({meta_table_1, "meta", <<"tüble"/utf8>>, "1"}, imem_meta:update(meta_table_1, {{meta_table_1, "meta", <<"täble"/utf8>>, "1"}, {meta_table_1, "meta", <<"tüble"/utf8>>, "1"}})),
    ?assertEqual([{meta_table_1, "meta", <<"tüble"/utf8>>, "1"}], imem_meta:read(meta_table_1)),
    ?assertEqual([#ddIndex{stu = {1, <<"tuble">>, "meta"}}], imem_meta:read(meta_table_1Idx)),
    ?assertEqual(ok, imem_meta:drop_index(meta_table_1)),
    ?assertEqual(ok, imem_meta:create_index(meta_table_1, [Idx1Def])),
    ?assertEqual([#ddIndex{stu = {1, <<"tuble">>, "meta"}}], imem_meta:read(meta_table_1Idx)),
    ?assertException(throw, {'ConcurrencyException', {"Data is modified by someone else", _}}, imem_meta:remove(meta_table_1, {meta_table_1, "meta", <<"tible"/utf8>>, "1"})),
    ?assertEqual({meta_table_1, "meta", <<"tüble"/utf8>>, "1"}, imem_meta:remove(meta_table_1, {meta_table_1, "meta", <<"tüble"/utf8>>, "1"})),
    ?assertEqual([], imem_meta:read(meta_table_1)),
    ?assertEqual([], imem_meta:read(meta_table_1Idx)),

    Idx2Def = #ddIdxDef{id = 2, name = <<"unique string index on b1">>, type = iv_k, pl = [<<"b1">>]},
    ?assertEqual(ok, imem_meta:create_or_replace_index(meta_table_1, [Idx2Def])),
    ?assertEqual({meta_table_1, "meta", <<"täble"/utf8>>, "1"}, imem_meta:insert(meta_table_1, {meta_table_1, "meta", <<"täble"/utf8>>, "1"})),
    ?assertEqual(1, length(imem_meta:read(meta_table_1))),
    ?assertEqual(1, length(imem_meta:read(meta_table_1Idx))),
    ?assertEqual({meta_table_1, "meta1", <<"tüble"/utf8>>, "2"}, imem_meta:insert(meta_table_1, {meta_table_1, "meta1", <<"tüble"/utf8>>, "2"})),
    ?assertEqual(2, length(imem_meta:read(meta_table_1))),
    ?assertEqual(2, length(imem_meta:read(meta_table_1Idx))),
    ?assertException(throw, {'ClientError', {"Unique index violation", {meta_table_1Idx, 2, <<"table">>, "meta"}}}, imem_meta:insert(meta_table_1, {meta_table_1, "meta2", <<"table"/utf8>>, "2"})),
    ?assertEqual(2, length(imem_meta:read(meta_table_1))),
    ?assertEqual(2, length(imem_meta:read(meta_table_1Idx))),

    Idx3Def = #ddIdxDef{id = 3, name = <<"json index on b1:b">>, type = ivk, pl = [<<"b1:b">>, <<"b1:c:a">>]},
    ?assertEqual(ok, imem_meta:create_or_replace_index(meta_table_1, [Idx3Def])),
    ?assertEqual(2, length(imem_meta:read(meta_table_1))),
    ?assertEqual(0, length(imem_meta:read(meta_table_1Idx))),
    JSON1 = <<
        "{"
        "\"a\":\"Value-a\","
        "\"b\":\"Value-b\","
        "\"c\":{"
        "\"a\":\"Value-ca\","
        "\"b\":\"Value-cb\""
        "}"
        "}"
    >>,
    % {
    %     "a": "Value-a",
    %     "b": "Value-b",
    %     "c": {
    %         "a": "Value-ca",
    %         "b": "Value-cb"
    %     }
    % }
    PROP1 = [{<<"a">>, <<"Value-a">>}
        , {<<"b">>, <<"Value-b">>}
        , {<<"c">>, [
            {<<"a">>, <<"Value-ca">>}
            , {<<"b">>, <<"Value-cb">>}
        ]
        }
    ],
    ?assertEqual(PROP1, imem_json:decode(JSON1)),
    ?assertEqual({meta_table_1, "json1", JSON1, "3"}, imem_meta:insert(meta_table_1, {meta_table_1, "json1", JSON1, "3"})),
    ?assertEqual([#ddIndex{stu = {3, <<"value-b">>, "json1"}}
        , #ddIndex{stu = {3, <<"value-ca">>, "json1"}}
    ], imem_meta:read(meta_table_1Idx)),

    % Drop individual indices
    ?assertEqual(ok, imem_meta:drop_index(meta_table_1)),
    ?assertEqual(ok, imem_meta:create_index(meta_table_1, [Idx1Def, Idx2Def, Idx3Def])),
    ?assertEqual(ok, imem_meta:drop_index(meta_table_1, <<"json index on b1:b">>)),
    ?assertException(throw, {'ClientError', {"Index does not exist for"
        , meta_table_1
        , <<"non existent index">>}}
        , imem_meta:drop_index(meta_table_1, <<"non existent index">>)),
    ?assertException(throw, {'ClientError', {"Index does not exist for"
        , meta_table_1
        , 7}}
        , imem_meta:drop_index(meta_table_1, 7)),
    ?assertEqual(ok, imem_meta:drop_index(meta_table_1, 2)),
    ?assertEqual(ok, imem_meta:drop_index(meta_table_1, 1)),
    ?assertEqual({'ClientError', {"Table does not exist", meta_table_1Idx}}
        , imem_meta:drop_index(meta_table_1)),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_table_1 ~n~p", [imem_meta:read(meta_table_1)]),

    Idx4Def = #ddIdxDef{id = 4, name = <<"integer index on b1">>, type = ivk, pl = [<<"c1">>], vnf = <<"fun imem_index:vnf_integer/1">>},
    ?assertEqual(ok, imem_meta:create_or_replace_index(meta_table_1, [Idx4Def])),
    ?assertEqual(3, length(imem_meta:read(meta_table_1Idx))),
    imem_meta:insert(meta_table_1, {meta_table_1, "11", <<"11">>, "11"}),
    ?assertEqual(4, length(imem_meta:read(meta_table_1Idx))),
    imem_meta:insert(meta_table_1, {meta_table_1, "12", <<"12">>, "c112"}),
    IdxExpect4 = [{ddIndex, {4, 1, "meta"}, 0}
        , {ddIndex, {4, 2, "meta1"}, 0}
        , {ddIndex, {4, 3, "json1"}, 0}
        , {ddIndex, {4, 11, "11"}, 0}
    ],
    ?assertEqual(IdxExpect4, imem_meta:read(meta_table_1Idx)),

    Vnf5 = <<"fun(__X) -> case imem_index:vnf_integer(__X) of ['$not_a_value'] -> ['$not_a_value']; [__V] -> [2*__V] end end">>,
    Idx5Def = #ddIdxDef{id = 5, name = <<"integer times 2 on b1">>, type = ivk, pl = [<<"c1">>], vnf = Vnf5},
    ?assertEqual(ok, imem_meta:create_or_replace_index(meta_table_1, [Idx5Def])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_table_1Idx ~n~p", [imem_meta:read(meta_table_1Idx)]),
    IdxExpect5 = [{ddIndex, {5, 2, "meta"}, 0}
        , {ddIndex, {5, 4, "meta1"}, 0}
        , {ddIndex, {5, 6, "json1"}, 0}
        , {ddIndex, {5, 22, "11"}, 0}
    ],
    ?assertEqual(IdxExpect5, imem_meta:read(meta_table_1Idx)),

    ?assertMatch({ok, _}, imem_meta:create_table(meta_table_2, Types2, [])),

    ?assertMatch({ok, _}, imem_meta:create_table(meta_table_3, {[a, ?nav], [datetime, term], {meta_table_3, ?nav, undefined}}, [])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [create_table_not_null]),
    Trig = <<"fun(O,N,T,U,TO) -> imem_meta:log_to_db(debug,imem_meta,trigger,[{table,T},{old,O},{new,N},{user,U},{tropts,TO}],\"trigger\") end.">>,
    ?assertEqual(ok, imem_meta:create_or_replace_trigger(meta_table_3, Trig)),
    ?assertEqual(Trig, imem_meta:get_trigger(meta_table_3)),

    ?assertException(throw, {ClEr, {"No columns given in create table", bad_table_0}}, imem_meta:create_table('bad_table_0', [], [])),
    ?assertException(throw, {ClEr, {"No value column given in create table, add dummy value column", bad_table_0}}, imem_meta:create_table('bad_table_0', BadTypes0, [])),

    ?assertException(throw, {ClEr, {"Invalid character(s) in table name", 'bad_?table_1'}}, imem_meta:create_table('bad_?table_1', BadTypes1, [])),
    ?assertEqual({ok,{imem,select}}, imem_meta:create_table(select, BadTypes2, [])),

    ?assertException(throw, {ClEr, {"Invalid character(s) in column name", 'a:b'}}, imem_meta:create_table(bad_table_1, BadTypes1, [])),
    ?assertEqual({ok,{imem,bad_table_1}}, imem_meta:create_table(bad_table_1, BadTypes2, [])),
    ?assertException(throw, {ClEr, {"Invalid data type", iinteger}}, imem_meta:create_table(bad_table_1, BadTypes3, [])),
    ?assertException(throw, {ClEr, {"Duplicate column name",a}}, imem_meta:create_table(bad_table_1, BadNames1, [])),

    LogCount3 = imem_meta:table_size(?LOG_TABLE),
    ?assertEqual({meta_table_3, {{2000, 1, 1}, {12, 45, 55}}, undefined}, imem_meta:insert(meta_table_3, {meta_table_3, {{2000, 01, 01}, {12, 45, 55}}, ?nav})),
    ?assertEqual(1, imem_meta:table_size(meta_table_3)),
    ?assertEqual(LogCount3 + 1, imem_meta:table_size(?LOG_TABLE)),  %% trigger inserted one line
    ?assertException(throw, {ClEr, {"Not null constraint violation", {meta_table_3, _}}}, imem_meta:insert(meta_table_3, {meta_table_3, ?nav, undefined})),
    ?assertEqual(LogCount3 + 2, imem_meta:table_size(?LOG_TABLE)),  %% error inserted one line
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [not_null_constraint]),
    ?assertEqual({meta_table_3, {{2000, 1, 1}, {12, 45, 55}}, undefined}, imem_meta:update(meta_table_3, {{meta_table_3, {{2000, 1, 1}, {12, 45, 55}}, undefined}, {meta_table_3, {{2000, 01, 01}, {12, 45, 55}}, ?nav}})),
    ?assertEqual(1, imem_meta:table_size(meta_table_3)),
    ?assertEqual(LogCount3 + 3, imem_meta:table_size(?LOG_TABLE)),  %% trigger inserted one line
    ?assertEqual({meta_table_3, {{2000, 1, 1}, {12, 45, 56}}, undefined}, imem_meta:merge(meta_table_3, {meta_table_3, {{2000, 01, 01}, {12, 45, 56}}, ?nav})),
    ?assertEqual(2, imem_meta:table_size(meta_table_3)),
    ?assertEqual(LogCount3 + 4, imem_meta:table_size(?LOG_TABLE)),  %% trigger inserted one line
    ?assertEqual({meta_table_3, {{2000, 1, 1}, {12, 45, 56}}, undefined}, imem_meta:remove(meta_table_3, {meta_table_3, {{2000, 01, 01}, {12, 45, 56}}, undefined})),
    ?assertEqual(1, imem_meta:table_size(meta_table_3)),
    ?assertEqual(LogCount3 + 5, imem_meta:table_size(?LOG_TABLE)),  %% trigger inserted one line
    ?assertEqual(ok, imem_meta:drop_trigger(meta_table_3)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_table_3 before update~n~p", [imem_meta:read(meta_table_3)]),
    Trans3 = fun() ->
        %% key update
        imem_meta:update(meta_table_3, {{meta_table_3, {{2000, 01, 01}, {12, 45, 55}}, undefined}, {meta_table_3, {{2000, 01, 01}, {12, 45, 56}}, "alternative"}}),
        imem_meta:insert(meta_table_3, {meta_table_3, {{2000, 01, 01}, {12, 45, 57}}, ?nav})         %% return last result only
             end,
    ?assertEqual({meta_table_3, {{2000, 1, 1}, {12, 45, 57}}, undefined}, imem_meta:return_atomic(imem_meta:transaction(Trans3))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_table_3 after update~n~p", [imem_meta:read(meta_table_3)]),
    ?assertEqual(2, imem_meta:table_size(meta_table_3)),
    ?assertEqual(LogCount3 + 5, imem_meta:table_size(?LOG_TABLE)),  %% no trigger, no more log

    Keys4 = [
        {1, {meta_table_3, {{2000, 1, 1}, {12, 45, 59}}, undefined}}
    ],
    U = unknown,
    {_, _DefRec, TrigFun} = imem_meta:trigger_infos(meta_table_3),
    ?assertEqual(Keys4, imem_meta:update_tables([[{imem, meta_table_3, set}, 1, {}, {meta_table_3, {{2000, 01, 01}, {12, 45, 59}}, undefined}, TrigFun, U, []]], optimistic)),
    ?assertException(throw, {ClEr, {"Not null constraint violation", {1, {meta_table_3, _}}}}, imem_meta:update_tables([[{imem, meta_table_3, set}, 1, {}, {meta_table_3, ?nav, undefined}, TrigFun, U, []]], optimistic)),
    ?assertException(throw, {ClEr, {"Not null constraint violation", {1, {meta_table_3, _}}}}, imem_meta:update_tables([[{imem, meta_table_3, set}, 1, {}, {meta_table_3, {{2000, 01, 01}, {12, 45, 59}}, ?nav}, TrigFun, U, []]], optimistic)),

    ?assertEqual([meta_table_1, meta_table_1Idx, meta_table_2, meta_table_3], lists:sort(imem_meta:tables_starting_with("meta_table_"))),
    ?assertEqual([meta_table_1, meta_table_1Idx, meta_table_2, meta_table_3], lists:sort(imem_meta:tables_starting_with(meta_table_))),

    _DdNode0 = imem_meta:read(ddNode),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":ddNode0 ~p~n", [_DdNode0]),
    _DdNode1 = imem_meta:read(ddNode, node()),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":ddNode1 ~p~n", [_DdNode1]),
    _DdNode2 = imem_meta:select(ddNode, ?MatchAllRecords),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":ddNode2 ~p~n", [_DdNode2]),

    Schema0 = [{ddSchema, {imem_meta:schema(), node()}, []}],
    ?assertEqual(Schema0, imem_meta:read(ddSchema)),
    ?assertEqual({Schema0, true}, imem_meta:select(ddSchema, ?MatchAllRecords, 1000)),

    ?assertEqual(ok, imem_meta:drop_table(meta_table_3)),
    ?assertEqual(ok, imem_meta:drop_table(meta_table_2)),
    ?assertEqual(ok, imem_meta:drop_table(meta_table_1)),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_tables]),

    ok.

meta_partitions(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_partitions/1 - Start ===>~n", []),

    ClEr = 'ClientError',
    UiEx = 'UnimplementedException',

    LogTable = imem_meta:physical_table_name(?LOG_TABLE),
    ?assert(lists:member(LogTable, imem_meta:physical_table_names(?LOG_TABLE))),
    ?assertEqual(LogTable, imem_meta:physical_table_name(atom_to_list(?LOG_TABLE) ++ imem_meta:node_shard())),

    ?assertEqual([], imem_meta:physical_table_names(?TPTEST0)),

    ?assertException(throw, {ClEr, {"Table to be purged does not exist", ?TPTEST0}}, imem_meta:purge_table(?TPTEST0)),
    ?assertException(throw, {UiEx, {"Purge not supported on this table type", not_existing_table}}, imem_meta:purge_table(not_existing_table)),
    ?assert(imem_meta:purge_table(?LOG_TABLE) >= 0),
    ?assertException(throw, {UiEx, {"Purge not supported on this table type", ddTable}}, imem_meta:purge_table(ddTable)),

    ?assertNot(lists:member({imem_meta:schema(), ?TPTEST0}, [element(2, A) || A <- imem_meta:read(ddAlias)])),
    TimePartTable0 = imem_meta:physical_table_name(?TPTEST0),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":TimePartTable ~p~n", [TimePartTable0]),
    ?assertEqual(TimePartTable0, imem_meta:physical_table_name(?TPTEST0, ?TIMESTAMP)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(?TPTEST0, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Alias0 ~p~n", [[element(2, A) || A <- imem_meta:read(ddAlias)]]),
    ?assertEqual(ok, imem_meta:check_table(TimePartTable0)),
    ?assertEqual(0, imem_meta:table_size(TimePartTable0)),
    ?assertEqual([TimePartTable0], imem_meta:physical_table_names(?TPTEST0)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(?TPTEST0, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),

    ?assert(lists:member({imem_meta:schema(), ?TPTEST0}, [element(2, A) || A <- imem_meta:read(ddAlias)])),

    ?assert(lists:member({imem_meta:schema(), ?TPTEST0}, [element(2, A) || A <- imem_meta:read(ddAlias)])),
    ?assertNot(lists:member({imem_meta:schema(), ?TPTEST2}, [element(2, A) || A <- imem_meta:read(ddAlias)])),

    %% ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":parsed table names ~p", [[imem_meta:parse_table_name(TA) || #ddAlias{qname = {S, TA}} <- imem_if_mnesia:read(ddAlias), S == imem_meta:schema()]]),
    ?LogDebug("Parsed table names ~p", [[imem_meta:parse_table_name(TA) || #ddAlias{qname = {S, TA}} <- imem_if_mnesia:read(ddAlias), S == imem_meta:schema()]]),
    ?assertException(throw
        , {'ClientError', {"Name conflict (different rolling period) in ddAlias", ?TPTEST2}}
        , imem_meta:create_check_table(?TPTEST2
            , {record_info(fields, ddLog), ?ddLog, #ddLog{}}
            , [{record_name, ddLog}, {type, ordered_set}]
            , system
        )
    ),

    LogRec = #ddLog{logTime = ?TIME_UID, logLevel = info, pid = self()
        , module = ?MODULE, function = meta_partitions, node = node()
        , fields = [], message = <<"some log message">>
    },
    ?assertEqual(ok, imem_meta:write(?TPTEST0, LogRec)),
    ?assertEqual(1, imem_meta:table_size(TimePartTable0)),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST0)),
    {Secs, Mics, Node, _} = ?TIME_UID,
    LogRecF = LogRec#ddLog{logTime = {Secs + 2000, Mics, Node, ?INTEGER_UID}},
    ?assertEqual(ok, imem_meta:write(?TPTEST0, LogRecF)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":physical_table_names ~p~n", [imem_meta:physical_table_names(?TPTEST0)]),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST0, [{purge_delay, 10000}])),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST0)),
    PurgeResult = imem_meta:purge_table(?TPTEST0, [{purge_delay, -3000}]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":PurgeResult ~p~n", [PurgeResult]),
    ?assert(PurgeResult > 0),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST0)),
    ?assertEqual(ok, imem_meta:drop_table(?TPTEST0)),
    ?assertEqual([], imem_meta:physical_table_names(?TPTEST0)),
    Alias0a = imem_meta:read(ddAlias),
    ?assertEqual(false, lists:member({imem_meta:schema(), ?TPTEST0}, [element(2, A) || A <- Alias0a])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [?TPTEST0]),

    TimePartTable1 = imem_meta:physical_table_name(?TPTEST1),
    ?assertEqual(tpTest1_1999999998@_, TimePartTable1),
    ?assertEqual(TimePartTable1, imem_meta:physical_table_name(?TPTEST1, ?TIMESTAMP)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(?TPTEST1, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),
    ?assertEqual(ok, imem_meta:check_table(TimePartTable1)),
    ?assertEqual([TimePartTable1], imem_meta:physical_table_names(?TPTEST1)),
    ?assertEqual(0, imem_meta:table_size(TimePartTable1)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(?TPTEST1, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),

    Alias1 = imem_meta:read(ddAlias),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Alias1 ~p~n", [[element(2, A) || A <- Alias1]]),
    ?assert(lists:member({imem_meta:schema(), ?TPTEST1}, [element(2, A) || A <- Alias1])),

    ?assertEqual(ok, imem_meta:write(?TPTEST1, LogRec)),
    ?assertEqual(1, imem_meta:table_size(TimePartTable1)),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST1)),
    LogRecP = LogRec#ddLog{logTime = {900000000, 0, node(), ?INTEGER_UID}},  % ?TIME_UID format
    ?assertEqual(ok, imem_meta:write(?TPTEST1, LogRecP)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Big Partition Tables after back-insert~n~p", [imem_meta:physical_table_names(?TPTEST1)]),
    LogRecFF = LogRec#ddLog{logTime = {2900000000, 0}},    % using 2-tuple ?TIMESTAMP format here for backward compatibility test
    ?assertEqual(ok, imem_meta:write(?TPTEST1, LogRecFF)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Big Partition Tables after forward-insert~n~p", [imem_meta:physical_table_names(?TPTEST1)]),
    ?assertEqual(3, length(imem_meta:physical_table_names(?TPTEST1))),     % another partition created
    ?assert(imem_meta:purge_table(?TPTEST1) > 0),
    ?assertEqual(ok, imem_meta:drop_table(?TPTEST1)),
    Alias1a = imem_meta:read(ddAlias),
    ?assertEqual(false, lists:member({imem_meta:schema(), ?TPTEST1}, [element(2, A) || A <- Alias1a])),
    ?assertEqual([], imem_meta:physical_table_names(?TPTEST1)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [?TPTEST1]),

    ?assertEqual({error, {"Table template not found in ddAlias", dummy_table_name}}, imem_meta:create_partitioned_table_sync(dummy_table_name, dummy_table_name)),

    ?assertEqual([], imem_meta:physical_table_names(fakelog_1@)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(fakelog_1@, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system)),
    ?assertEqual(1, length(imem_meta:physical_table_names(fakelog_1@))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p created", [fakelog_1@]),
    LogRec3 = #ddLog{logTime = ?TIMESTAMP, logLevel = debug, pid = self()
        , module = ?MODULE, function = test, node = node()
        , fields = [], message = <<>>, stacktrace = []
    },  % using 2-tuple ?TIMESTAMP format here for backward compatibility test
    ?assertEqual(ok, imem_meta:dirty_write(fakelog_1@, LogRec3)),     % can write to first partition (maybe second)
    ?assert(length(imem_meta:physical_table_names(fakelog_1@)) >= 1),
    timer:sleep(999),
    ?assert(length(imem_meta:physical_table_names(fakelog_1@)) >= 2), % created by partition rolling
    ?assertEqual(ok, imem_meta:dirty_write(fakelog_1@, LogRec3#ddLog{logTime = ?TIMESTAMP})), % can write to second partition (maybe third)
    FL3Tables = imem_meta:physical_table_names(fakelog_1@),
    ?LogDebug("Tables written ~p~n~p", [fakelog_1@, FL3Tables]),
    ?assert(length(FL3Tables) >= 3),
    timer:sleep(999),
    ?assert(length(imem_meta:physical_table_names(fakelog_1@)) >= 4),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [roll_partitioned_table]),
    _ = {timeout, 5, fun() ->
        ?assertEqual(ok, imem_meta:drop_table(fakelog_1@)) end},
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_table]),

    ok.

meta_preparations(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":meta_preparations/1 - Start ===>~n", []),

    ?assertEqual(["Schema", ".", "BaseName", "_", "01234", "@", "Node"], imem_meta:parse_table_name("Schema.BaseName_01234@Node")),
    ?assertEqual(["Schema", ".", "BaseName", "", "", "", ""], imem_meta:parse_table_name("Schema.BaseName")),
    ?assertEqual(["", "", "BaseName", "_", "01234", "@", "Node"], imem_meta:parse_table_name("BaseName_01234@Node")),
    ?assertEqual(["", "", "BaseName", "", "", "@", "Node"], imem_meta:parse_table_name("BaseName@Node")),
    ?assertEqual(["", "", "BaseName", "", "", "@", ""], imem_meta:parse_table_name("BaseName@")),
    ?assertEqual(["", "", "BaseName", "", "", "", ""], imem_meta:parse_table_name("BaseName")),
    ?assertEqual(["Schema", ".", "Name_Period", "", "", "@", "Node"], imem_meta:parse_table_name('Schema.Name_Period@Node')),
    ?assertEqual(["Schema", ".", "Name", "_", "12345", "@", "Node"], imem_meta:parse_table_name('Schema.Name_12345@Node')),
    ?assertEqual(["Schema", ".", "Name_Period", "", "", "@", "_"], imem_meta:parse_table_name('Schema.Name_Period@_')),
    ?assertEqual(["Schema", ".", "Name", "_", "12345", "@", "_"], imem_meta:parse_table_name('Schema.Name_12345@_')),
    ?assertEqual(["Schema", ".", "Name_Period", "", "", "@", ""], imem_meta:parse_table_name('Schema.Name_Period@')),
    ?assertEqual(["Sch_01", ".", "Name", "_", "12345", "@", ""], imem_meta:parse_table_name('Sch_01.Name_12345@')),
    ?assertEqual(["Sch_99", ".", "Name_Period", "", "", "", ""], imem_meta:parse_table_name('Sch_99.Name_Period')),
    ?assertEqual(["Sch_ma", ".", "Name_12345", "", "", "", ""], imem_meta:parse_table_name('Sch_ma.Name_12345')),
    ?assertEqual(["", "", "Name_Period", "", "", "@", "Node"], imem_meta:parse_table_name('Name_Period@Node')),
    ?assertEqual(["", "", "Name", "_", "12345", "@", "Node"], imem_meta:parse_table_name('Name_12345@Node')),
    ?assertEqual(["", "", "Name_Period", "", "", "@", "_"], imem_meta:parse_table_name('Name_Period@_')),
    ?assertEqual(["", "", "Name", "_", "12345", "@", "_"], imem_meta:parse_table_name('Name_12345@_')),
    ?assertEqual(["", "", "Name_Period", "", "", "@", ""], imem_meta:parse_table_name('Name_Period@')),
    ?assertEqual(["", "", "Name", "_", "12345", "@", ""], imem_meta:parse_table_name('Name_12345@')),
    ?assertEqual(["", "", "Name_Period", "", "", "", ""], imem_meta:parse_table_name('Name_Period')),
    ?assertEqual(["", "", "Name_12345", "", "", "", ""], imem_meta:parse_table_name('Name_12345')),

    ?assertEqual(true, imem_meta:is_time_partitioned_alias(?TPTEST1)),
    ?assertEqual(true, imem_meta:is_time_partitioned_alias(?TPTEST0)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest@)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest@_)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest1234@_)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest_10A0@)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest)),
    ?assertEqual(true, imem_meta:is_node_sharded_alias(?TPTEST0)),
    ?assertEqual(false, imem_meta:is_node_sharded_alias(tpTest_1000)),
    ?assertEqual(false, imem_meta:is_node_sharded_alias(tpTest1000)),
    ?assertEqual(false, imem_meta:is_node_sharded_alias(?TPTEST1)),
    ?assertEqual(false, imem_meta:is_node_sharded_alias(?TPTEST1)),

    ok.

%%====================================================================
%% Helper functions.
%%====================================================================

read_write_test(Tab, Key, N) ->
    Upd = fun() ->
        [{Tab, Key, Val}] = imem_meta:read(Tab, Key),
        imem_meta:write(Tab, {Tab, Key, Val + N})
          end,
    imem_meta:transaction(Upd).

receive_results(N, Acc) ->
    receive
        Result ->
            case N of
                1 ->
                    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result ~p", [Result]),
                    [Result | Acc];
                _ ->
                    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result ~p", [Result]),
                    receive_results(N - 1, [Result | Acc])
            end
    after 1000 ->
        ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result timeout ~p", [1000]),
        Acc
    end.
