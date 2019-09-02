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

-export([ end_per_testcase/2
        , meta_concurrency/1
        , meta_operations/1
        , meta_partitions/1
        , meta_preparations/1
        , physical_table_names/1
        ]).

-define(LOG_TABLE_OPTS, [{record_name, ddLog}
    , {type, ordered_set}
    , {purge_delay, 430000}        %% 430000 = 5 Days - 2000 sec
]).

-define(TPTEST, tpTest).
-define(TPTEST_1, tpTest_1).
-define(TPTEST_2, tpTest_2).
-define(TPTEST_3, tpTest_3).
-define(TPTEST_1@, tpTest_1@).
-define(TPTEST_100@, tpTest_100@).
-define(TPTEST_1000@, tpTest_1000@).
-define(TPTEST_999999999@_, tpTest_999999999@_).
-define(TPTEST_1999999998@_, tpTest_1999999998@_).
-define(TPTEST_1IDX, tpTest_1Idx).

-define(NODEBUG, true).

-define(TEST_SLAVE_IMEM_NODE_NAME, "metaslave").

-include_lib("imem.hrl").
-include_lib("imem_meta.hrl").
-include("imem_ct.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_testcase(TestCase, _Config) ->
    ?CTPAL("End ~p",[TestCase]),

    catch imem_meta:drop_table(?TPTEST_1),
    catch imem_meta:drop_table(?TPTEST_2),
    catch imem_meta:drop_table(?TPTEST_3),
    catch imem_meta:drop_table(?TPTEST_1000@),
    catch imem_meta:drop_table(?TPTEST_999999999@_),
    catch imem_meta:drop_table(?TPTEST_100@),
    catch imem_meta:drop_table(?TPTEST_1@),
    stop_slaves(nodes()),
    ok.

%%====================================================================
%% Test cases.
%%====================================================================

physical_table_names(_Config) ->
    ?CTPAL("Start test single node"),

    LocalCacheStr = "ddCache@"++imem_meta:node_shard(),
    LocalCacheName = list_to_atom(LocalCacheStr), % ddCache@WKS018

    ?assertEqual([ddTable], imem_meta:physical_table_names(ddTable)),
    ?assertEqual([ddTable], imem_meta:physical_table_names("ddTable")),
    ?assertEqual([ddTable], imem_meta:physical_table_names(<<"ddTable">>)),
    ?assertEqual([ddTable], imem_meta:physical_table_names({imem_meta:schema(), ddTable})),

    [ ?assertEqual([Type], imem_meta:physical_table_names(atom_to_list(Type)))
      || Type <- ?DataTypes
    ],

    ?assertEqual([LocalCacheName], imem_meta:physical_table_names(ddCache@local)),
    ?assertEqual([LocalCacheName], imem_meta:physical_table_names(LocalCacheStr)),
    ?assertEqual([LocalCacheName], imem_meta:physical_table_names(LocalCacheName)),
    ?assertEqual([LocalCacheName], imem_meta:physical_table_names({imem_meta:schema(), LocalCacheName})),
    ?assertEqual([], imem_meta:physical_table_names("ddCache@123")),
    ?assertEqual([LocalCacheName], imem_meta:physical_table_names("ddCache@")),

    Result1 = [{node(),imem_meta:schema(),ddTable}],
    ?assertEqual(Result1, imem_meta:cluster_table_names(ddTable)),
    ?assertEqual(Result1, imem_meta:cluster_table_names("ddTable")),
    ?assertEqual(Result1, imem_meta:cluster_table_names(<<"ddTable">>)),
    
    [ ?assertEqual([{node(),imem_meta:schema(),Type}], imem_meta:cluster_table_names(atom_to_list(Type)))
      || Type <- ?DataTypes
    ],

    ?assertEqual([{node(),imem_meta:schema(),LocalCacheName}], imem_meta:cluster_table_names(ddCache@local)),
    ?assertEqual([{node(),imem_meta:schema(),LocalCacheName}], imem_meta:cluster_table_names(LocalCacheStr)),
    ?assertEqual([{node(),imem_meta:schema(),LocalCacheName}], imem_meta:cluster_table_names(LocalCacheName)),
    ?assertEqual([], imem_meta:cluster_table_names("ddCache@123")),
    ?assertEqual([{node(),imem_meta:schema(),LocalCacheName}], imem_meta:cluster_table_names("ddCache@")),
    ?assertEqual([{node(),<<"csv$">>,<<"\"TestCsvFile.csv\"">>}], imem_meta:cluster_table_names(<<"csv$.\"TestCsvFile.csv\"">>)),

    ?CTPAL("Start test slave node"),
    ?assertEqual([], imem_meta:nodes()),
    [Slave] = start_slaves([?TEST_SLAVE_IMEM_NODE_NAME]),
    ?CTPAL("Slaves ~p", [Slave]),
    ct:sleep(5000),

    ?CTPAL("slave nodes ~p", [imem_meta:nodes()]),
    ?assert(lists:member(Slave, imem_meta:nodes())),
    ?CTPAL("data_nodes ~p", [imem_meta:data_nodes()]),
    ?CTPAL("node_shards ~p", [imem_meta:node_shards()]),
    ?assert(lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
    ?assert(lists:member(imem_meta:node_shard(), imem_meta:node_shards())),
    ?assert(lists:member({imem_meta:schema(), Slave}, imem_meta:data_nodes())),
    ?assert(lists:member(?TEST_SLAVE_IMEM_NODE_NAME, imem_meta:node_shards())),

    DDCacheNames = imem_meta:physical_table_names("ddCache@"),
    SlaveCacheName = list_to_atom("ddCache@" ++ ?TEST_SLAVE_IMEM_NODE_NAME),
    ?CTPAL("ddCache@ -> ~p", [DDCacheNames]),
    ?assert(lists:member(LocalCacheName, DDCacheNames)),
    ?assert(lists:member(SlaveCacheName, DDCacheNames)),
    ?assertEqual([SlaveCacheName], imem_meta:physical_table_names("ddCache@" ++ ?TEST_SLAVE_IMEM_NODE_NAME)),
    LogNames = imem_meta:physical_table_names("ddLog_86400@"),
    LocalLogName = imem_meta:physical_table_name("ddLog_86400@"),
    [BaseName,_] = string:tokens(atom_to_list(LocalLogName),"@"),
    SlaveLogName = list_to_atom(BaseName ++ "@" ++ ?TEST_SLAVE_IMEM_NODE_NAME),
    ?CTPAL("ddLog_86400@ -> ~p", [LogNames]),
    ?assert(lists:member(LocalLogName, LogNames)),
    ?assert(lists:member(SlaveLogName, LogNames)),
    ?assertEqual([SlaveLogName], imem_meta:physical_table_names("ddLog_86400@" ++ ?TEST_SLAVE_IMEM_NODE_NAME)),
    ?assertEqual([], imem_meta:physical_table_names("ddAccount@" ++ ?TEST_SLAVE_IMEM_NODE_NAME)),
    ?assertEqual([{Slave,<<"csv$">>,<<"\"TestCsvFile.csv\"">>}], imem_meta:cluster_table_names("csv$.\"TestCsvFile.csv\"@"++?TEST_SLAVE_IMEM_NODE_NAME)),

    DDCacheClusterNames = imem_meta:cluster_table_names("ddCache@"),
    ?CTPAL("ddCache@ -> ~p", [DDCacheClusterNames]),
    ?assert(lists:member({node(),imem_meta:schema(),LocalCacheName}, DDCacheClusterNames)),
    ?assert(lists:member({Slave,imem_meta:schema(),SlaveCacheName}, DDCacheClusterNames)),
    ?assertEqual([{Slave,imem_meta:schema(),SlaveCacheName}], imem_meta:cluster_table_names("ddCache@" ++ ?TEST_SLAVE_IMEM_NODE_NAME)),
    LogClusterNames = imem_meta:cluster_table_names("ddLog_86400@"),
    ?CTPAL("ddLog_86400@ -> ~p", [LogClusterNames]),
    ?assert(lists:member({node(),imem_meta:schema(),LocalLogName}, LogClusterNames)),
    ?assert(lists:member({Slave,imem_meta:schema(),SlaveLogName}, LogClusterNames)),
    ?assertEqual([{Slave,imem_meta:schema(),SlaveLogName}], imem_meta:cluster_table_names("ddLog_86400@" ++ ?TEST_SLAVE_IMEM_NODE_NAME)),
    ?assertEqual([{Slave,imem_meta:schema(),ddAccount}], imem_meta:cluster_table_names("ddAccount@" ++ ?TEST_SLAVE_IMEM_NODE_NAME)),
    ?assertEqual([{Slave,imem_meta:schema(),integer}], imem_meta:cluster_table_names("integer@" ++ ?TEST_SLAVE_IMEM_NODE_NAME)),

    stop_slaves([Slave]),
    ct:sleep(1000),
    ?assertEqual([], imem_meta:nodes()),
    ok.

start_slaves(Slaves0) when is_list(Slaves0), length(Slaves0) > 0 ->
    Slaves = [S || S <- lists:usort(Slaves0), is_list(S)],
    if length(Slaves) == 0 -> error(no_slaves); true -> ok end,
    [NodeName, Host] = string:tokens(atom_to_list(node()), "@"),
    StartArgFmt = lists:concat([
        " -setcookie ", erlang:get_cookie(),
        " -pa ", string:join(code:get_path(), " "),
        " -kernel"
            " inet_dist_listen_min 7000"
            " inet_dist_listen_max 7020",
        " -imem"
            " mnesia_node_type ram"
            " mnesia_schema_name imem"
            " node_shard ~p"
            " tcp_server false"
            " cold_start_recover false"
    ]),
    SlaveNodes = start_slaves(NodeName, Host, StartArgFmt, Slaves),
    lists:foreach(
        fun(Node) ->
            ok = rpc:call(Node, application, load, [imem]),
            CMs = [node() | SlaveNodes] -- [Node],
            ok = rpc:call(
                Node, application, set_env, [imem, erl_cluster_mgrs, CMs]
            ),
            {ok, _} = rpc:call(Node, application, ensure_all_started, [imem])
        end,
        SlaveNodes
    ),
    SlaveNodes.

start_slaves(_NodeName, _Host, _StartArgFmt, []) -> [];
start_slaves(NodeName, Host, StartArgFmt, [Slave | Slaves]) ->
    SA = lists:flatten(io_lib:format(StartArgFmt, [Slave])),
    {ok, Node} = slave:start(Host, Slave, SA),
    [Node | start_slaves(NodeName, Host, StartArgFmt, Slaves)].

stop_slaves([]) -> ok;
stop_slaves([Slave | Slaves]) ->
    ok = slave:stop(Slave),
    stop_slaves(Slaves).

meta_concurrency(_Config) ->
    ?CTPAL("create_table"),
    ?assertMatch({ok, _}, imem_meta:create_table(?TPTEST_1, [hlk, val], [])),
    Self = self(),
    Key = [sum],
    ?CTPAL("write"),
    ?assertEqual(ok, imem_meta:write(?TPTEST_1, {?TPTEST_1, Key, 0})),
    [spawn(fun() ->
        Self ! {N, read_write_test(?TPTEST_1, Key, N)} end) || N <- lists:seq(1, 10)],
    ?CTPAL("ReadWriteResult"),
    ReadWriteResult = receive_results(10, []),
    ?assertEqual(10, length(ReadWriteResult)),
    ?assertEqual([{?TPTEST_1, Key, 55}], imem_meta:read(?TPTEST_1, Key)),
    ?assertEqual([{atomic, ok}], lists:usort([R || {_, R} <- ReadWriteResult])),
    ?assertEqual(ok, imem_meta:drop_table(?TPTEST_1)),

    ok.

meta_operations(_Config) ->
    ?CTPAL("Start"),

    ClEr = 'ClientError',
    SyEx = 'SystemException',
    ?CTPAL("schema ~p",[imem_meta:schema()]),
    ?CTPAL("data nodes ~p", [imem_meta:data_nodes()]),
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
    ?CTPAL("ddLog@ count ~p", [LogCount1]),
    Fields = [{test_criterium_1, value1}, {test_criterium_2, value2}],
    LogRec0 = #ddLog{logTime = Now, logLevel = info, pid = self()
        , module = ?MODULE, function = meta_operations, node = node()
        , fields = Fields, message = <<"some log message">>},
    ?assertEqual(ok, imem_meta:write(?LOG_TABLE, LogRec0)),
    LogCount2 = imem_meta:table_size(?LOG_TABLE),
    ?CTPAL("ddLog@ count ~p", [LogCount2]),
    ?assert(LogCount2 > LogCount1),
    _Log1 = imem_meta:read(?LOG_TABLE, Now),
    ?CTPAL("ddLog@ count ~p", [_Log1]),
    ?assertEqual(ok, imem_meta:log_to_db(info, ?MODULE, test, [{test_3, value3}, {test_4, value4}], "Message")),
    ?assertEqual(ok, imem_meta:log_to_db(info, ?MODULE, test, [{test_3, value3}, {test_4, value4}], [])),
    ?assertEqual(ok, imem_meta:log_to_db(info, ?MODULE, test, [{test_3, value3}, {test_4, value4}], [stupid_error_message, 1])),
    ?assertEqual(ok, imem_meta:log_to_db(info, ?MODULE, test, [{test_3, value3}, {test_4, value4}], {stupid_error_message, 2})),
    LogCount2a = imem_meta:table_size(?LOG_TABLE),
    ?assert(LogCount2a >= LogCount2 + 4),

    ?CTPAL("test_database_operations"),
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

    ?assertMatch({ok, _}, imem_meta:create_table(?TPTEST_1, Types1, [])),
    ?assertEqual(ok, imem_meta:create_index(?TPTEST_1, [])),
    ?assertMatch(ok, imem_meta:check_table(?TPTEST_1IDX)),
    ?CTPAL("ddTable for ?TPTEST_1 ~p", [imem_meta:read(ddTable, {imem_meta:schema(), ?TPTEST_1})]),
    ?assertEqual(ok, imem_meta:drop_index(?TPTEST_1)),
    ?assertException(throw, {'ClientError', {"Table does not exist", ?TPTEST_1IDX}}, imem_meta:check_table(?TPTEST_1IDX)),
    ?assertEqual(ok, imem_meta:create_index(?TPTEST_1, [])),
    ?assertException(throw, {'ClientError', {"Index already exists", {?TPTEST_1}}}, imem_meta:create_index(?TPTEST_1, [])),
    ?assertEqual([], imem_meta:read(?TPTEST_1IDX)),
    ?assertEqual(ok, imem_meta:write(?TPTEST_1IDX, #ddIndex{stu = {1, 2, 3}})),
    ?assertEqual([#ddIndex{stu = {1, 2, 3}}], imem_meta:read(?TPTEST_1IDX)),

    Idx1Def = #ddIdxDef{id = 1, name = <<"string index on b1">>, type = ivk, pl = [<<"b1">>]},
    ?assertEqual(ok, imem_meta:create_or_replace_index(?TPTEST_1, [Idx1Def])),
    ?assertEqual([], imem_meta:read(?TPTEST_1IDX)),
    ?assertEqual([<<"table">>], imem_index:vnf_lcase_ascii_ne(<<"täble"/utf8>>)),
    ?assertEqual({?TPTEST_1, "meta", <<"täble"/utf8>>, "1"}, imem_meta:insert(?TPTEST_1, {?TPTEST_1, "meta", <<"täble"/utf8>>, "1"})),
    ?assertEqual([{?TPTEST_1, "meta", <<"täble"/utf8>>, "1"}], imem_meta:read(?TPTEST_1)),
    ?assertEqual([#ddIndex{stu = {1, <<"table">>, "meta"}}], imem_meta:read(?TPTEST_1IDX)),
    ?assertEqual([<<"tuble">>], imem_index:vnf_lcase_ascii_ne(<<"tüble"/utf8>>)),
    ?assertException(throw, {'ConcurrencyException', {"Data is modified by someone else", _}}, imem_meta:update(?TPTEST_1, {{?TPTEST_1, "meta", <<"tible"/utf8>>, "1"}, {?TPTEST_1, "meta", <<"tüble"/utf8>>, "1"}})),
    ?assertEqual({?TPTEST_1, "meta", <<"tüble"/utf8>>, "1"}, imem_meta:update(?TPTEST_1, {{?TPTEST_1, "meta", <<"täble"/utf8>>, "1"}, {?TPTEST_1, "meta", <<"tüble"/utf8>>, "1"}})),
    ?assertEqual([{?TPTEST_1, "meta", <<"tüble"/utf8>>, "1"}], imem_meta:read(?TPTEST_1)),
    ?assertEqual([#ddIndex{stu = {1, <<"tuble">>, "meta"}}], imem_meta:read(?TPTEST_1IDX)),
    ?assertEqual(ok, imem_meta:drop_index(?TPTEST_1)),
    ?assertEqual(ok, imem_meta:create_index(?TPTEST_1, [Idx1Def])),
    ?assertEqual([#ddIndex{stu = {1, <<"tuble">>, "meta"}}], imem_meta:read(?TPTEST_1IDX)),
    ?assertException(throw, {'ConcurrencyException', {"Data is modified by someone else", _}}, imem_meta:remove(?TPTEST_1, {?TPTEST_1, "meta", <<"tible"/utf8>>, "1"})),
    ?assertEqual({?TPTEST_1, "meta", <<"tüble"/utf8>>, "1"}, imem_meta:remove(?TPTEST_1, {?TPTEST_1, "meta", <<"tüble"/utf8>>, "1"})),
    ?assertEqual([], imem_meta:read(?TPTEST_1)),
    ?assertEqual([], imem_meta:read(?TPTEST_1IDX)),

    Idx2Def = #ddIdxDef{id = 2, name = <<"unique string index on b1">>, type = iv_k, pl = [<<"b1">>]},
    ?assertEqual(ok, imem_meta:create_or_replace_index(?TPTEST_1, [Idx2Def])),
    ?assertEqual({?TPTEST_1, "meta", <<"täble"/utf8>>, "1"}, imem_meta:insert(?TPTEST_1, {?TPTEST_1, "meta", <<"täble"/utf8>>, "1"})),
    ?assertEqual(1, length(imem_meta:read(?TPTEST_1))),
    ?assertEqual(1, length(imem_meta:read(?TPTEST_1IDX))),
    ?assertEqual({?TPTEST_1, "meta1", <<"tüble"/utf8>>, "2"}, imem_meta:insert(?TPTEST_1, {?TPTEST_1, "meta1", <<"tüble"/utf8>>, "2"})),
    ?assertEqual(2, length(imem_meta:read(?TPTEST_1))),
    ?assertEqual(2, length(imem_meta:read(?TPTEST_1IDX))),
    ?assertException(throw, {'ClientError', {"Unique index violation", {?TPTEST_1IDX, 2, <<"table">>, "meta"}}}, imem_meta:insert(?TPTEST_1, {?TPTEST_1, "meta2", <<"table"/utf8>>, "2"})),
    ?assertEqual(2, length(imem_meta:read(?TPTEST_1))),
    ?assertEqual(2, length(imem_meta:read(?TPTEST_1IDX))),

    Idx3Def = #ddIdxDef{id = 3, name = <<"json index on b1:b">>, type = ivk, pl = [<<"b1:b">>, <<"b1:c:a">>]},
    ?assertEqual(ok, imem_meta:create_or_replace_index(?TPTEST_1, [Idx3Def])),
    ?assertEqual(2, length(imem_meta:read(?TPTEST_1))),
    ?assertEqual(0, length(imem_meta:read(?TPTEST_1IDX))),
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
    ?assertEqual({?TPTEST_1, "json1", JSON1, "3"}, imem_meta:insert(?TPTEST_1, {?TPTEST_1, "json1", JSON1, "3"})),
    ?assertEqual([#ddIndex{stu = {3, <<"value-b">>, "json1"}}
        , #ddIndex{stu = {3, <<"value-ca">>, "json1"}}
    ], imem_meta:read(?TPTEST_1IDX)),

    % Drop individual indices
    ?assertEqual(ok, imem_meta:drop_index(?TPTEST_1)),
    ?assertEqual(ok, imem_meta:create_index(?TPTEST_1, [Idx1Def, Idx2Def, Idx3Def])),
    ?assertEqual(ok, imem_meta:drop_index(?TPTEST_1, <<"json index on b1:b">>)),
    ?assertException(throw, {'ClientError', {"Index does not exist for"
        , ?TPTEST_1
        , <<"non existent index">>}}
        , imem_meta:drop_index(?TPTEST_1, <<"non existent index">>)),
    ?assertException(throw, {'ClientError', {"Index does not exist for"
        , ?TPTEST_1
        , 7}}
        , imem_meta:drop_index(?TPTEST_1, 7)),
    ?assertEqual(ok, imem_meta:drop_index(?TPTEST_1, 2)),
    ?assertEqual(ok, imem_meta:drop_index(?TPTEST_1, 1)),
    ?assertEqual({'ClientError', {"Table does not exist", ?TPTEST_1IDX}}
        , imem_meta:drop_index(?TPTEST_1)),

    ?CTPAL("?TPTEST_1 ~p", [imem_meta:read(?TPTEST_1)]),

    Idx4Def = #ddIdxDef{id = 4, name = <<"integer index on b1">>, type = ivk, pl = [<<"c1">>], vnf = <<"fun imem_index:vnf_integer/1">>},
    ?assertEqual(ok, imem_meta:create_or_replace_index(?TPTEST_1, [Idx4Def])),
    ?assertEqual(3, length(imem_meta:read(?TPTEST_1IDX))),
    imem_meta:insert(?TPTEST_1, {?TPTEST_1, "11", <<"11">>, "11"}),
    ?assertEqual(4, length(imem_meta:read(?TPTEST_1IDX))),
    imem_meta:insert(?TPTEST_1, {?TPTEST_1, "12", <<"12">>, "c112"}),
    IdxExpect4 = [{ddIndex, {4, 1, "meta"}, 0}
        , {ddIndex, {4, 2, "meta1"}, 0}
        , {ddIndex, {4, 3, "json1"}, 0}
        , {ddIndex, {4, 11, "11"}, 0}
    ],
    ?assertEqual(IdxExpect4, imem_meta:read(?TPTEST_1IDX)),

    Vnf5 = <<"fun(__X) -> case imem_index:vnf_integer(__X) of ['$not_a_value'] -> ['$not_a_value']; [__V] -> [2*__V] end end">>,
    Idx5Def = #ddIdxDef{id = 5, name = <<"integer times 2 on b1">>, type = ivk, pl = [<<"c1">>], vnf = Vnf5},
    ?assertEqual(ok, imem_meta:create_or_replace_index(?TPTEST_1, [Idx5Def])),
    ?CTPAL("?TPTEST_1IDX ~p", [imem_meta:read(?TPTEST_1IDX)]),
    IdxExpect5 = [{ddIndex, {5, 2, "meta"}, 0}
        , {ddIndex, {5, 4, "meta1"}, 0}
        , {ddIndex, {5, 6, "json1"}, 0}
        , {ddIndex, {5, 22, "11"}, 0}
    ],
    ?assertEqual(IdxExpect5, imem_meta:read(?TPTEST_1IDX)),

    ?assertMatch({ok, _}, imem_meta:create_table(?TPTEST_2, Types2, [])),

    ?assertMatch({ok, _}, imem_meta:create_table(?TPTEST_3, {[a, ?nav], [datetime, term], {?TPTEST_3, ?nav, undefined}}, [])),
    ?CTPAL("create_or_replace_trigger"),
    Trig = <<"fun(O,N,T,U,TO) -> imem_meta:log_to_db(debug,imem_meta,trigger,[{table,T},{old,O},{new,N},{user,U},{tropts,TO}],\"trigger\") end.">>,
    ?assertEqual(ok, imem_meta:create_or_replace_trigger(?TPTEST_3, Trig)),
    ?assertEqual(Trig, imem_meta:get_trigger(?TPTEST_3)),

    ?assertException(throw, {ClEr, {"No columns given in create table", bad_table_0}}, imem_meta:create_table('bad_table_0', [], [])),
    ?assertException(throw, {ClEr, {"No value column given in create table, add dummy value column", bad_table_0}}, imem_meta:create_table('bad_table_0', BadTypes0, [])),

    ?assertException(throw, {ClEr, {"Invalid character(s) in table name", 'bad_?table_1'}}, imem_meta:create_table('bad_?table_1', BadTypes1, [])),
    ?assertEqual({ok,{imem,select}}, imem_meta:create_table(select, BadTypes2, [])),

    ?assertException(throw, {ClEr, {"Invalid character(s) in column name", 'a:b'}}, imem_meta:create_table(bad_table_1, BadTypes1, [])),
    ?assertEqual({ok,{imem,bad_table_1}}, imem_meta:create_table(bad_table_1, BadTypes2, [])),
    ?assertException(throw, {ClEr, {"Invalid data type", iinteger}}, imem_meta:create_table(bad_table_1, BadTypes3, [])),
    ?assertException(throw, {ClEr, {"Duplicate column name",a}}, imem_meta:create_table(bad_table_1, BadNames1, [])),

    ?CTPAL("?TPTEST_3"),
    LogCount3 = imem_meta:table_size(?LOG_TABLE),
    ?assertEqual({?TPTEST_3, {{2000, 1, 1}, {12, 45, 55}}, undefined}, imem_meta:insert(?TPTEST_3, {?TPTEST_3, {{2000, 01, 01}, {12, 45, 55}}, ?nav})),
    ?assertEqual(1, imem_meta:table_size(?TPTEST_3)),
    ?assertEqual(LogCount3 + 1, imem_meta:table_size(?LOG_TABLE)),  %% trigger inserted one line
    ?assertException(throw, {ClEr, {"Not null constraint violation", {?TPTEST_3, _}}}, imem_meta:insert(?TPTEST_3, {?TPTEST_3, ?nav, undefined})),
    ?assertEqual(LogCount3 + 2, imem_meta:table_size(?LOG_TABLE)),  %% error inserted one line
    ?assertEqual({?TPTEST_3, {{2000, 1, 1}, {12, 45, 55}}, undefined}, imem_meta:update(?TPTEST_3, {{?TPTEST_3, {{2000, 1, 1}, {12, 45, 55}}, undefined}, {?TPTEST_3, {{2000, 01, 01}, {12, 45, 55}}, ?nav}})),
    ?assertEqual(1, imem_meta:table_size(?TPTEST_3)),
    ?assertEqual(LogCount3 + 3, imem_meta:table_size(?LOG_TABLE)),  %% trigger inserted one line
    ?assertEqual({?TPTEST_3, {{2000, 1, 1}, {12, 45, 56}}, undefined}, imem_meta:merge(?TPTEST_3, {?TPTEST_3, {{2000, 01, 01}, {12, 45, 56}}, ?nav})),
    ?assertEqual(2, imem_meta:table_size(?TPTEST_3)),
    ?assertEqual(LogCount3 + 4, imem_meta:table_size(?LOG_TABLE)),  %% trigger inserted one line
    ?assertEqual({?TPTEST_3, {{2000, 1, 1}, {12, 45, 56}}, undefined}, imem_meta:remove(?TPTEST_3, {?TPTEST_3, {{2000, 01, 01}, {12, 45, 56}}, undefined})),
    ?assertEqual(1, imem_meta:table_size(?TPTEST_3)),
    ?assertEqual(LogCount3 + 5, imem_meta:table_size(?LOG_TABLE)),  %% trigger inserted one line
    ?assertEqual(ok, imem_meta:drop_trigger(?TPTEST_3)),
    ?CTPAL("?TPTEST_3 before update ~p", [imem_meta:read(?TPTEST_3)]),
    Trans3 = fun() ->
        %% key update
        imem_meta:update(?TPTEST_3, {{?TPTEST_3, {{2000, 01, 01}, {12, 45, 55}}, undefined}, {?TPTEST_3, {{2000, 01, 01}, {12, 45, 56}}, "alternative"}}),
        imem_meta:insert(?TPTEST_3, {?TPTEST_3, {{2000, 01, 01}, {12, 45, 57}}, ?nav})         %% return last result only
             end,
    ?assertEqual({?TPTEST_3, {{2000, 1, 1}, {12, 45, 57}}, undefined}, imem_meta:return_atomic(imem_meta:transaction(Trans3))),
    ?CTPAL("?TPTEST_3 after update ~p", [imem_meta:read(?TPTEST_3)]),
    ?assertEqual(2, imem_meta:table_size(?TPTEST_3)),
    ?assertEqual(LogCount3 + 5, imem_meta:table_size(?LOG_TABLE)),  %% no trigger, no more log

    Keys4 = [
        {1, {?TPTEST_3, {{2000, 1, 1}, {12, 45, 59}}, undefined}}
    ],
    U = unknown,
    {_, _DefRec, TrigFun} = imem_meta:trigger_infos(?TPTEST_3),
    ?assertEqual(Keys4, imem_meta:update_tables([[{imem, ?TPTEST_3, set}, 1, {}, {?TPTEST_3, {{2000, 01, 01}, {12, 45, 59}}, undefined}, TrigFun, U, []]], optimistic)),
    ?assertException(throw, {ClEr, {"Not null constraint violation", {1, {?TPTEST_3, _}}}}, imem_meta:update_tables([[{imem, ?TPTEST_3, set}, 1, {}, {?TPTEST_3, ?nav, undefined}, TrigFun, U, []]], optimistic)),
    ?assertException(throw, {ClEr, {"Not null constraint violation", {1, {?TPTEST_3, _}}}}, imem_meta:update_tables([[{imem, ?TPTEST_3, set}, 1, {}, {?TPTEST_3, {{2000, 01, 01}, {12, 45, 59}}, ?nav}, TrigFun, U, []]], optimistic)),

    ?assertEqual([?TPTEST_1, ?TPTEST_1IDX, ?TPTEST_2, ?TPTEST_3], lists:sort(imem_meta:tables_starting_with(?TPTEST))),
    ?assertEqual([?TPTEST_1, ?TPTEST_1IDX, ?TPTEST_2, ?TPTEST_3], lists:sort(imem_meta:tables_starting_with(?TPTEST))),

    _DdNode0 = imem_meta:read(ddNode),
    ?CTPAL("ddNode0 ~p", [_DdNode0]),
    _DdNode1 = imem_meta:read(ddNode, node()),
    ?CTPAL("ddNode1 ~p", [_DdNode1]),
    _DdNode2 = imem_meta:select(ddNode, ?MatchAllRecords),
    ?CTPAL("ddNode2 ~p", [_DdNode2]),

    Schema0 = [{ddSchema, {imem_meta:schema(), node()}, []}],
    ?assertEqual(Schema0, imem_meta:read(ddSchema)),
    ?assertEqual({Schema0, true}, imem_meta:select(ddSchema, ?MatchAllRecords, 1000)),

    ?assertEqual(ok, imem_meta:drop_table(?TPTEST_3)),
    ?assertEqual(ok, imem_meta:drop_table(?TPTEST_2)),
    ?assertEqual(ok, imem_meta:drop_table(?TPTEST_1)),


    ok.

meta_partitions(_Config) ->
    ?CTPAL("Start"),

    ClEr = 'ClientError',
    UiEx = 'UnimplementedException',

    LogTable = imem_meta:physical_table_name(?LOG_TABLE),
    ?assert(lists:member(LogTable, imem_meta:physical_table_names(?LOG_TABLE))),
    ?assertEqual(LogTable, imem_meta:physical_table_name(atom_to_list(?LOG_TABLE) ++ imem_meta:node_shard())),

    ?assertEqual([], imem_meta:physical_table_names(?TPTEST_1000@)),

    ?assertException(throw, {ClEr, {"Table to be purged does not exist", ?TPTEST_1000@}}, imem_meta:purge_table(?TPTEST_1000@)),
    ?assertException(throw, {UiEx, {"Purge not supported on this table type", not_existing_table}}, imem_meta:purge_table(not_existing_table)),
    ?assert(imem_meta:purge_table(?LOG_TABLE) >= 0),
    ?assertException(throw, {UiEx, {"Purge not supported on this table type", ddTable}}, imem_meta:purge_table(ddTable)),

    ?assertNot(lists:member({imem_meta:schema(), ?TPTEST_1000@}, [element(2, A) || A <- imem_meta:read(ddAlias)])),
    TimePartTable0 = imem_meta:physical_table_name(?TPTEST_1000@),
    ?CTPAL("TimePartTable ~p", [TimePartTable0]),
    ?assertEqual(TimePartTable0, imem_meta:physical_table_name(?TPTEST_1000@, ?TIMESTAMP)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(?TPTEST_1000@, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),
    ?CTPAL("Alias0 ~p", [[element(2, A) || A <- imem_meta:read(ddAlias)]]),
    ?assertEqual(ok, imem_meta:check_table(TimePartTable0)),
    ?assertEqual(0, imem_meta:table_size(TimePartTable0)),
    ?assertEqual([TimePartTable0], imem_meta:physical_table_names(?TPTEST_1000@)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(?TPTEST_1000@, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),

    ?assert(lists:member({imem_meta:schema(), ?TPTEST_1000@}, [element(2, A) || A <- imem_meta:read(ddAlias)])),

    ?assert(lists:member({imem_meta:schema(), ?TPTEST_1000@}, [element(2, A) || A <- imem_meta:read(ddAlias)])),
    ?assertNot(lists:member({imem_meta:schema(), ?TPTEST_100@}, [element(2, A) || A <- imem_meta:read(ddAlias)])),

    ?CTPAL("parsed table names ~p", [[imem_meta:parse_table_name(TA) || #ddAlias{qname = {S, TA}} <- imem_if_mnesia:read(ddAlias), S == imem_meta:schema()]]),
    ?assertException(throw
        , {'ClientError', {"Name conflict (different rolling period) in ddAlias", ?TPTEST_100@}}
        , imem_meta:create_check_table(?TPTEST_100@
            , {record_info(fields, ddLog), ?ddLog, #ddLog{}}
            , [{record_name, ddLog}, {type, ordered_set}]
            , system
        )
    ),

    LogRec = #ddLog{logTime = ?TIME_UID, logLevel = info, pid = self()
        , module = ?MODULE, function = meta_partitions, node = node()
        , fields = [], message = <<"some log message">>
    },
    ?assertEqual(ok, imem_meta:write(?TPTEST_1000@, LogRec)),
    ?assertEqual(1, imem_meta:table_size(TimePartTable0)),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST_1000@)),
    {Secs, Mics, Node, _} = ?TIME_UID,
    LogRecF = LogRec#ddLog{logTime = {Secs + 2000, Mics, Node, ?INTEGER_UID}},
    ?assertEqual(ok, imem_meta:write(?TPTEST_1000@, LogRecF)),
    ?CTPAL("physical_table_names ~p", [imem_meta:physical_table_names(?TPTEST_1000@)]),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST_1000@, [{purge_delay, 10000}])),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST_1000@)),
    PurgeResult = imem_meta:purge_table(?TPTEST_1000@, [{purge_delay, -3000}]),
    ?CTPAL("PurgeResult ~p", [PurgeResult]),
    ?assert(PurgeResult > 0),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST_1000@)),
    ?assertEqual(ok, imem_meta:drop_table(?TPTEST_1000@)),
    ?assertEqual([], imem_meta:physical_table_names(?TPTEST_1000@)),
    Alias0a = imem_meta:read(ddAlias),
    ?assertEqual(false, lists:member({imem_meta:schema(), ?TPTEST_1000@}, [element(2, A) || A <- Alias0a])),

    TimePartTable1 = imem_meta:physical_table_name(?TPTEST_999999999@_),
    ?assertEqual(?TPTEST_1999999998@_, TimePartTable1),
    ?assertEqual(TimePartTable1, imem_meta:physical_table_name(?TPTEST_999999999@_, ?TIMESTAMP)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(?TPTEST_999999999@_, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),
    ?assertEqual(ok, imem_meta:check_table(TimePartTable1)),
    ?assertEqual([TimePartTable1], imem_meta:physical_table_names(?TPTEST_999999999@_)),
    ?assertEqual(0, imem_meta:table_size(TimePartTable1)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(?TPTEST_999999999@_, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, [{record_name, ddLog}, {type, ordered_set}], system)),

    Alias1 = imem_meta:read(ddAlias),
    ?CTPAL("Alias1 ~p", [[element(2, A) || A <- Alias1]]),
    ?assert(lists:member({imem_meta:schema(), ?TPTEST_999999999@_}, [element(2, A) || A <- Alias1])),

    ?assertEqual(ok, imem_meta:write(?TPTEST_999999999@_, LogRec)),
    ?assertEqual(1, imem_meta:table_size(TimePartTable1)),
    ?assertEqual(0, imem_meta:purge_table(?TPTEST_999999999@_)),
    LogRecP = LogRec#ddLog{logTime = {900000000, 0, node(), ?INTEGER_UID}},  % ?TIME_UID format
    ?assertEqual(ok, imem_meta:write(?TPTEST_999999999@_, LogRecP)),
    ?CTPAL("Big Partition Tables after back-insert ~p", [imem_meta:physical_table_names(?TPTEST_999999999@_)]),
    LogRecFF = LogRec#ddLog{logTime = {2900000000, 0}},    % using 2-tuple ?TIMESTAMP format here for backward compatibility test
    ?assertEqual(ok, imem_meta:write(?TPTEST_999999999@_, LogRecFF)),
    ?CTPAL("Big Partition Tables after forward-insert ~p", [imem_meta:physical_table_names(?TPTEST_999999999@_)]),
    ?assertEqual(3, length(imem_meta:physical_table_names(?TPTEST_999999999@_))),     % another partition created
    ?assert(imem_meta:purge_table(?TPTEST_999999999@_) > 0),
    ?assertEqual(ok, imem_meta:drop_table(?TPTEST_999999999@_)),
    Alias1a = imem_meta:read(ddAlias),
    ?assertEqual(false, lists:member({imem_meta:schema(), ?TPTEST_999999999@_}, [element(2, A) || A <- Alias1a])),
    ?assertEqual([], imem_meta:physical_table_names(?TPTEST_999999999@_)),

    ?CTPAL("dummy_table_name"),
    ?assertEqual({error, {"Table template not found in ddAlias", dummy_table_name}}, imem_meta:create_partitioned_table_sync(dummy_table_name, dummy_table_name)),
    ?CTPAL("physical_table_names"),
    ?assertEqual([], imem_meta:physical_table_names(?TPTEST_1@)),
    ?assertMatch({ok, _}, imem_meta:create_check_table(?TPTEST_1@, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system)),
    ?assertEqual(1, length(imem_meta:physical_table_names(?TPTEST_1@))),
    ?CTPAL("LogRec3"),
    LogRec3 = #ddLog{logTime = ?TIMESTAMP, logLevel = debug, pid = self()
        , module = ?MODULE, function = test, node = node()
        , fields = [], message = <<>>, stacktrace = []
    },  % using 2-tuple ?TIMESTAMP format here for backward compatibility test
    ?assertEqual(ok, imem_meta:dirty_write(?TPTEST_1@, LogRec3)),     % can write to first partition (maybe second)
    ?assert(length(imem_meta:physical_table_names(?TPTEST_1@)) >= 1),
    timer:sleep(999),
    ?assert(length(imem_meta:physical_table_names(?TPTEST_1@)) >= 2), % created by partition rolling
    ?assertEqual(ok, imem_meta:dirty_write(?TPTEST_1@, LogRec3#ddLog{logTime = ?TIMESTAMP})), % can write to second partition (maybe third)
    FL3Tables = imem_meta:physical_table_names(?TPTEST_1@),
    ?CTPAL("Tables written ~p ~p", [?TPTEST_1@, FL3Tables]),
    ?assert(length(FL3Tables) >= 3),
    timer:sleep(999),
    ?assert(length(imem_meta:physical_table_names(?TPTEST_1@)) >= 4),
    _ = {timeout, 5, fun() ->
        ?assertEqual(ok, imem_meta:drop_table(?TPTEST_1@)) end},

    ok.

meta_preparations(_Config) ->
    ?CTPAL("Start"),

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

    ?assertEqual(true, imem_meta:is_time_partitioned_alias(?TPTEST_999999999@_)),
    ?assertEqual(true, imem_meta:is_time_partitioned_alias(?TPTEST_1000@)),
    ?assertEqual(true, imem_meta:is_time_partitioned_alias(tpTest_123@)),
    ?assertEqual(true, imem_meta:is_time_partitioned_alias(tpTest_123@_)),
    ?assertEqual(true, imem_meta:is_time_partitioned_alias(tpTest_123@local)),
    ?assertEqual(true, imem_meta:is_time_partitioned_alias(tpTest_123@testnode)),

    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest@)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest@_)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest123@_)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest_12A@)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest@local)),
    ?assertEqual(false, imem_meta:is_time_partitioned_alias(tpTest@testnode)),

    ?assertEqual(true, imem_meta:is_node_sharded_alias(?TPTEST_1000@)),
    ?assertEqual(true, imem_meta:is_node_sharded_alias(tpTest@)),
    ?assertEqual(true, imem_meta:is_node_sharded_alias(tpTest123@)),
    ?assertEqual(true, imem_meta:is_node_sharded_alias(tpTest_123@)),

    ?assertEqual(false, imem_meta:is_node_sharded_alias(?TPTEST_999999999@_)),
    ?assertEqual(false, imem_meta:is_node_sharded_alias(tpTest_1000)),
    ?assertEqual(false, imem_meta:is_node_sharded_alias(tpTest1000)),
    ?assertEqual(false, imem_meta:is_node_sharded_alias(tpTest123@_)),
    ?assertEqual(false, imem_meta:is_node_sharded_alias(tpTest_123@_)),

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
                    ?CTPAL("Result ~p", [Result]),
                    [Result | Acc];
                _ ->
                    ?CTPAL("Result ~p", [Result]),
                    receive_results(N - 1, [Result | Acc])
            end
    after 1000 ->
        ?CTPAL("Result timeout"),
        Acc
    end.
