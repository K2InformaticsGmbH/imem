%%%-------------------------------------------------------------------
%%% File        : imem_sql_select_ct.erl
%%% Description : Common testing imem_sql_select.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_sql_select_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    db1_with_sec/1,
    db1_without_sec/1,
    db2_with_sec/1,
    db2_without_sec/1,
    end_per_testcase/2,
    init_per_testcase/2

]).

-define(BINSTR(__M), list_to_binary(integer_to_list(__M))).
-define(TEST_JSON(__N), begin __B = ?BINSTR(__N), <<"{\"name\":\"John", __B/binary, "\",\"age\":", __B/binary, ",\"empty\":null}">> end).
-define(TEST_JSON_LIST(__E), list_to_binary(lists:flatten([$[, [case NN of __E ->
    integer_to_list(NN); _ ->
    integer_to_list(NN) ++ [$,] end || NN <- lists:seq(1, __E)], $]]))).
-define(TEST_JSON_STR_LIST(__E), list_to_binary(lists:flatten([$[, [case NN of __E ->
    [34, $a] ++ integer_to_list(NN) ++ [34]; _ ->
    [34, $a] ++ integer_to_list(NN) ++ [34, $,] end || NN <- lists:seq(1, __E)], $]]))).
-define(DQFN(__BinFN), [$" | binary_to_list(__BinFN)] ++ [$"]). % wrap binary filename into double quotes as list

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include("imem_seco.hrl").
-include("imem_sql.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":init_per_testcase/2 - Start(~p) ===>~n", [TestCase]),

    catch imem_meta:drop_table(member_test),
    catch imem_meta:drop_table(def),
    catch imem_meta:drop_table(ddViewTest),
    catch imem_meta:drop_table(ddCmdTest),
    catch imem_meta:drop_table(skvhSqlTest),
    catch imem_meta:drop_table(skvhSqlTestAudit_86400@_),
    catch imem_meta:drop_table(skvhSqlTestHist),

    Config.

end_per_testcase(TestCase, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_testcase/2 - Start(~p) ===>~n", [TestCase]),

    catch imem_meta:drop_table(member_test),
    catch imem_meta:drop_table(def),
    catch imem_meta:drop_table(ddViewTest),
    catch imem_meta:drop_table(ddCmdTest),
    catch imem_meta:drop_table(skvhSqlTest),
    catch imem_meta:drop_table(skvhSqlTestAudit_86400@_),
    catch imem_meta:drop_table(skvhSqlTestHist),

    ok.

%%====================================================================
%% Test cases.
%%====================================================================

db1_with_or_without_sec(IsSec) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":db1_with_or_without_sec/1 - Start(~p) ===>~n", [IsSec]),

    SeEx = 'SecurityException',
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),
    ?assertEqual([], imem_statement:receive_raw()),
    ?assertEqual([imem], imem_datatype:field_value(tag, list, 0, 0, [], <<"[imem]">>)),

    timer:sleep(500),
    _LoginTime = calendar:local_time(),

    SKey = case IsSec of
               true -> ?imem_test_admin_login();
               false -> none
           end,

    exec_fetch_sort_equal(SKey, query3w, 100, IsSec, "
            select item
            from tuple
            where is_member(item,to_list('[{a,b},{c,d}]'))"
        ,
        [{<<"{a,b}">>}, {<<"{c,d}">>}]
    ),

    exec_fetch_sort_equal(SKey, query3v, 100, IsSec, "
            select item
            from tuple
            where item = to_tuple('{a,b}')"
        ,
        [{<<"{a,b}">>}]
    ),

    ?assertEqual("\"abc\"", ?DQFN(<<"abc">>)),

    ?assertEqual(ok, imem_dal_skvh:create_check_channel(<<"skvhSqlTest">>)),
    imem_dal_skvh:write(system, <<"skvhSqlTest">>, [123, 100], <<"100">>),
    imem_dal_skvh:write(system, <<"skvhSqlTest">>, [123, 200], <<"200">>),
    imem_dal_skvh:write(system, <<"skvhSqlTest">>, [123, 300], <<"300">>),
    imem_dal_skvh:write(system, <<"skvhSqlTest">>, [123, 400], <<"400">>),

    Sql3q1 = "select cvalue
                    from integer, skvhSqlTest
                    where item = 300
                    and ckey = list(123,item)",
    exec_fetch_sort_equal(SKey, query3q1, 100, IsSec, Sql3q1
        ,
        [{<<"300">>}]
    ),

    Sql3p1 = "select item
                    from dual,atom
                    where is_member(item, mfa('imem_sql_funs','filter_funs','[]'))
                    and item like 'list%'",
    case IsSec of
        true ->
            ?imem_test_admin_grant({eval_mfa, imem_seco, has_permission}),
            ?assert(imem_seco:have_permission(SKey, {eval_mfa, imem_seco, has_permission})),
            exec_fetch_sort_equal(SKey, query3p0, 100, IsSec, "
                    select mfa('imem_seco', 'has_permission', list(r.item, p.item)) as perm
                    from atom r, atom p
                    where is_member(r.item, '[system]')
                    and is_member(p.item, '[manage_system,noex]')"
                ,
                [{<<"true">>}, {<<"false">>}]
            ),
            ?imem_test_admin_grant({eval_mfa, imem_sql_funs, filter_funs}),
            ?assert(imem_seco:have_permission(SKey, {eval_mfa, imem_sql_funs, filter_funs})),
            exec_fetch_sort_equal(SKey, query3p1, 100, IsSec, Sql3p1
                ,
                [{<<"list">>}, {<<"list_to_binstr">>}, {<<"list_to_tuple">>}]
            ),
            exec_fetch_sort_equal(SKey, query3p2, 100, IsSec, "
                    select item, hd(mfa('imem_sql_funs', 'filter_funs', '[]'))
                    from atom where item = to_atom('filter_funs')"
                ,
                [{<<"filter_funs">>, <<"list">>}]
            ),
            exec_fetch_sort_equal(SKey, query3p3, 100, IsSec, "
                    select item, hd(mfa('imem_sql_funs', item, '[]'))
                    from atom where item = to_atom('filter_funs')"
                ,
                [{<<"filter_funs">>, <<"list">>}]
            ),
            % ?assertException(throw,{'UnimplementedException',{"Unsupported filter function", {mfa,imem_sql_funs,_,_}}},
            %     exec_fetch_sort(SKey, query3p3, 100, IsSec, "
            %         select item, hd(mfa('imem_sql_funs', item, '[]'))
            %         from atom where item = to_atom('filter_funs')" )
            % ),
            Sql3p4 = "select item
                    from dual,atom
                    where is_member(item, mfa('imem_meta','schema','[]'))
                    and item like 'list%'",
            ?assert(false == imem_seco:have_permission(SKey, {eval_mfa, imem_meta, schema})),
            ?assertException(throw, {'SecurityException', {"Function evaluation unauthorized", {imem_meta, schema, _, _}}},
                exec_fetch_sort(SKey, query3p4, 100, IsSec, Sql3p4)
            )
        ;
        false ->
            ?assertException(throw, {'SecurityException', {"Not logged in", _}},
                exec_fetch_sort(SKey, query3p1, 100, IsSec, Sql3p1)
            ),
            ok
    end,

    CsvFileName = <<"CsvTestFileName123abc.txt">>,
    file:write_file(CsvFileName, <<"Col1\tCol2\r\nA1\t1\r\nA2\t2\r\n">>),

    exec_fetch_sort_equal(SKey, query00, 100, IsSec, "
            select * from csv$." ++ ?DQFN(CsvFileName)
        ,
        [{CsvFileName, <<"0">>, <<"11">>, <<"Col1">>, <<"Col2">>}
            , {CsvFileName, <<"11">>, <<"6">>, <<"A1">>, <<"1">>}
            , {CsvFileName, <<"17">>, <<"6">>, <<"A2">>, <<"2">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query01, 100, IsSec, "
            select col1, col2 from csv$." ++ ?DQFN(CsvFileName)
        ,
        [{<<"Col1">>, <<"Col2">>}
            , {<<"A1">>, <<"1">>}
            , {<<"A2">>, <<"2">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query02, 100, IsSec, "
            select col1, col2 from csv$skip1." ++ ?DQFN(CsvFileName)
        ,
        [{<<"A1">>, <<"1">>}
            , {<<"A2">>, <<"2">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query03, 100, IsSec, "
            select col1, col2, col3 from csv$skip1$tab$3." ++ ?DQFN(CsvFileName)
        ,
        [{<<"A1">>, <<"1">>, <<>>}
            , {<<"A2">>, <<"2">>, <<>>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query04, 100, IsSec, "
            select * from csv$." ++ ?DQFN(CsvFileName) ++ " where col1 like 'A%'"
        ,
        [{CsvFileName, <<"11">>, <<"6">>, <<"A1">>, <<"1">>}
            , {CsvFileName, <<"17">>, <<"6">>, <<"A2">>, <<"2">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query05, 100, IsSec, "
            select * from csv$." ++ ?DQFN(CsvFileName) ++ " where bytes = 6"
        ,
        [{CsvFileName, <<"11">>, <<"6">>, <<"A1">>, <<"1">>}
            , {CsvFileName, <<"17">>, <<"6">>, <<"A2">>, <<"2">>}
        ]
    ),

    CsvFileNameLong = <<"CsvTestFileNameLong123abc.txt">>,
    BigField = binary:copy(<<"Test">>, 1500),
    file:write_file(CsvFileNameLong, <<"Col1\tCol2\r\nA1\t1\r\n", BigField/binary, "\t2\r\n">>),

    exec_fetch_sort_equal(SKey, query06, 100, IsSec, "
            select col1, col2, col3 from csv$skip1$tab$3." ++ ?DQFN(CsvFileNameLong)
        ,
        [{<<"A1">>, <<"1">>, <<>>}
            , {BigField, <<"2">>, <<>>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query0g, 100, IsSec, "
            select list(1,to_atom('a')) from dual"
        ,
        [{<<"[1,a]">>}]
    ),

    exec_fetch_sort_equal(SKey, query0h, 100, IsSec, "
            select tuple(1,to_boolean('true')) from dual"
        ,
        [{<<"{1,true}">>}]
    ),

    exec_fetch_sort_equal(SKey, query0i, 100, IsSec, "
            select list(1,dummy) from dual"
        ,
        [{<<"[1,\"X\"]">>}]
    ),

    exec_fetch_sort_equal(SKey, query0j, 100, IsSec, "
            select tuple(1,dummy) from dual"
        ,
        [{<<"{1,\"X\"}">>}]
    ),

    _QSTime = calendar:local_time(),

    R00 = exec_fetch_sort(SKey, query00, 100, IsSec, "
            select name, d.lastLoginTime
            from ddAccountDyn d, ddAccount a
            where d.lastLoginTime >= sysdate - 1.1574074074074073e-5
            and d.id = a.id"                    %% 1.0 * ?OneSecond
    ),
    _QETime = calendar:local_time(),
    case IsSec of
        false ->
            ok;
        % FIXME: Currently failing in Travis
        %?assertEqual(0, length(R00));
        true ->
            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Login time: ~p~n", [_LoginTime]),
            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Query start time: ~p~n", [_QSTime]),
            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Query end time: ~p~n", [_QETime]),
            _Accounts = imem_meta:read(ddAccount),
            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Accounts: ~p~n", [_Accounts]),
            ?assertEqual(1, length(R00))
    end,

    if
        IsSec -> ?assertEqual(<<"_test_admin_">>, imem_seco:account_name(SKey));
        true ->
            ?assertException(throw, {SeEx, {"Not logged in", none}}, imem_seco:account_name(SKey))
    end,

    %% test ddSysConf schema access

    {ok, Cwd} = file:get_cwd(),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Current directory:~n~p~n", [Cwd]),

    ?assertEqual(ok, imem_if_sys_conf:create_sys_conf("../../lib/imem/src/")),

    R9a = exec_fetch_sort(SKey, query9a, 100, IsSec, "
            select *
            from ddTable
            where element(1,qname) = to_atom('ddSysConf')"
    ),
    ?assert(length(R9a) >= 1),

    R9b = exec_fetch_sort(SKey, query9b, 100, IsSec, "
            select *
            from ddSysConf.\"imem.app.src\""
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Rows from ddSysConf.\"imem.app.src\":~n~p~n", [R9b]),
    ?assertEqual(5, length(R9b)),

    %% test table def

    ?assertMatch({ok, _}, imem_sql:exec(SKey,
        "create table def (
            col1 integer,
            col2 varchar2(2000),
            col3 date,
            col4 ipaddr,
            col5 tuple
        );", 0, [{schema, imem}], IsSec)),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Test json(3) :~n~p~n", [?TEST_JSON(3)]),
    ?assertEqual(ok, insert_json(SKey, 3, def, imem, IsSec)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Test table def :~n~p~n", [imem_meta:read(def)]),

    exec_fetch_sort_equal(SKey, query9a, 100, IsSec, "
            select col2
            from def
            "
        ,
        [{<<"{\"name\":\"John1\",\"age\":1,\"empty\":null}">>}
            , {<<"{\"name\":\"John2\",\"age\":2,\"empty\":null}">>}
            , {<<"{\"name\":\"John3\",\"age\":3,\"empty\":null}">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query9b, 100, IsSec, "
            select col2|#keys|
            from def
            "
        ,
        [{<<"[\"name\",\"age\",\"empty\"]">>}
            , {<<"[\"name\",\"age\",\"empty\"]">>}
            , {<<"[\"name\",\"age\",\"empty\"]">>}
        ]
        % [{<<"[<<\"name\">>,<<\"age\">>,<<\"empty\">>]">>}
        % ,{<<"[<<\"name\">>,<<\"age\">>,<<\"empty\">>]">>}
        % ,{<<"[<<\"name\">>,<<\"age\">>,<<\"empty\">>]">>}
        % ]
    ),

    exec_fetch_sort_equal(SKey, query9c, 100, IsSec, "
            select col2|#values|
            from def
            "
        ,
        [{<<"[\"John1\",1,null]">>}
            , {<<"[\"John2\",2,null]">>}
            , {<<"[\"John3\",3,null]">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query9d, 100, IsSec, "
            select col2|{}|
            from def
            "
        ,
        [{<<"{\"name\":\"John1\",\"age\":1,\"empty\":null}">>}
            , {<<"{\"name\":\"John2\",\"age\":2,\"empty\":null}">>}
            , {<<"{\"name\":\"John3\",\"age\":3,\"empty\":null}">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query9e, 100, IsSec, "
            select col2|{name,age}|
            from def
            "
        ,
        [{<<"{\"name\":\"John1\",\"age\":1}">>}
            , {<<"{\"name\":\"John2\",\"age\":2}">>}
            , {<<"{\"name\":\"John3\",\"age\":3}">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query9f, 100, IsSec, "
            select col2|{name,noattr}|
            from def
            "
        ,
        [{<<"{\"name\":\"John1\",\"noattr\":\"$not_a_value\"}">>}
            , {<<"{\"name\":\"John2\",\"noattr\":\"$not_a_value\"}">>}
            , {<<"{\"name\":\"John3\",\"noattr\":\"$not_a_value\"}">>}
        ]
    ),

    ?assertEqual(ok, imem_sql:exec(SKey, "truncate table def;", 0, [{schema, imem}], IsSec)),
    ?assertEqual(ok, insert_json_int_list(SKey, 5, def, imem, IsSec)),
    ?assertEqual(ok, insert_json_str_list(SKey, 2, def, imem, IsSec)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Test table def :~n~p~n", [imem_meta:read(def)]),

    exec_fetch_sort_equal(SKey, query10a, 100, IsSec, "
            select col2|[]|
            from def
            "
        ,
        [{<<"[\"a1\"]">>}
            , {<<"[\"a1\",\"a2\"]">>}
            , {<<"[1,2,3]">>}
            , {<<"[1,2,3,4]">>}
            , {<<"[1,2,3,4,5]">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query10b, 100, IsSec, "
            select is_list(col2|[]|)
            from def
            "
        ,
        [{<<"true">>}
            , {<<"true">>}
            , {<<"true">>}
            , {<<"true">>}
            , {<<"true">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query10c, 100, IsSec, "
            select col2|[1,3]|
            from def
            "
        ,
        [{<<"[\"a1\",\"$not_a_value\"]">>}
            , {<<"[\"a1\",\"$not_a_value\"]">>}
            , {<<"[1,3]">>}
            , {<<"[1,3]">>}
            , {<<"[1,3]">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query10d, 100, IsSec, "
            select col2|[0]|
            from def
            "
        ,
        [{<<>>}
            , {<<>>}
            , {<<>>}
            , {<<>>}
            , {<<>>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query10e, 100, IsSec, "
            select col1
            from def
            where col2|[1,3]| = to_list('[1,3]')
            "
        ,
        [{<<"3">>}
            , {<<"4">>}
            , {<<"5">>}
        ]
    ),

    R0a = exec_fetch_sort(SKey, query0a, 100, IsSec, "
            select *
            from ddTable
            where element(2,qname) = to_atom('def')"
    ),
    ?assertEqual(1, length(R0a)),

    exec_fetch_sort_equal(SKey, query0b, 100, IsSec, "
            select 1
            from ddTable
            where element(2,qname) = to_atom('def')"
        ,
        [{<<"1">>}]
    ),

    R1h = exec_fetch_sort(SKey, query1h, 100, IsSec, "
            select *
            from def
            where 1=1"
    ),
    ?assertEqual(5, length(R1h)),

    R1i = exec_fetch_sort(SKey, query1i, 100, IsSec, "
            select *
            from def
            where 1=0"
    ),
    ?assertEqual(0, length(R1i)),

    exec_fetch_sort_equal(SKey, query1j, 100, IsSec, "
            select col1
            from def
            where col1 between 3 and 5
            "
        ,
        [{<<"3">>}, {<<"4">>}, {<<"5">>}]
    ),

    R2 = exec_fetch_sort_equal(SKey, query2, 100, IsSec, "
            select col1, col2
            from def
            where col1>=3 and col1<=4"
        ,
        [{<<"3">>, <<"[1,2,3]">>}
            , {<<"4">>, <<"[1,2,3,4]">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query2a, 100, IsSec, "
            select col1, col2
            from def
            where col1 in (3,4)"
        ,
        R2
    ),

    exec_fetch_sort_equal(SKey, query2b, 100, IsSec, "
            select col1, col2
            from def
            where col2 in ('[1,2,3]','[1,2,3,4]')"
        ,
        R2
    ),

    ?assertEqual(ok, imem_sql:exec(SKey, "truncate table def;", 0, [{schema, imem}], IsSec)),
    ?assertEqual(ok, insert_range(SKey, 20, def, imem, IsSec)),

    {L0, true} = if_call_mfa(IsSec, select, [SKey, def, ?MatchAllRecords, 1000]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Test table def : ~p entries~n~p~n~p~n~p~n", [length(L0), hd(L0), '...', lists:last(L0)]),
    ?assertEqual(20, length(L0)),

    exec_fetch_sort_equal(SKey, query2c, 100, IsSec, "
            select col1, col2
            from def
            where col2 in (5,6)"
        ,
        []
    ),

    exec_fetch_sort_equal(SKey, query2d, 100, IsSec, "
            select col1, col2
            from def
            where col2 in ('5',col2) and col1 <= 10"
        ,
        [
            {<<"1">>, <<"1">>}, {<<"2">>, <<"2">>}, {<<"3">>, <<"3">>}, {<<"4">>, <<"4">>},
            {<<"5">>, <<"5">>}, {<<"6">>, <<"6">>}, {<<"7">>, <<"7">>}, {<<"8">>, <<"8">>},
            {<<"9">>, <<"9">>}, {<<"10">>, <<"10">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query2e, 100, IsSec, "
            select col4
            from def
            where col4 < '10.132.7.3'"
        ,
        [
            {<<"10.132.7.1">>}, {<<"10.132.7.2">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query2f, 100, IsSec, "
            select col2
            from def
            where col2 in (5,6)"
        ,
        []
    ),

    % exec_fetch_sort_equal(SKey, query2g, 100, IsSec,
    %     "select def from def where col1 = 2",
    %     [{<<"{def,2,<<\"2\">>,{{2014,3,16},{11,5,55}},{10,132,7,2},{'Atom2',2}}">>}]
    % ),
    if_call_mfa(IsSec, write, [SKey, def,
        {def, 100, <<"\"text_in_quotes\"">>, {{2001, 02, 03}, {4, 5, 6}}, {10, 132, 7, 92}, {'Atom100', 100}}
    ]),

    exec_fetch_sort_equal(SKey, query2h, 100, IsSec, "
            select col2
            from def
            where col1 = 100"
        ,
        [{<<"\"text_in_quotes\"">>}]
    ),

    exec_fetch_sort_equal(SKey, query2i, 100, IsSec, "
            select col1, col5
            from def
            where element(1,col5) = to_atom('Atom5')"
        ,
        [{<<"5">>, <<"{'Atom5',5}">>}]
    ),

    exec_fetch_sort_equal(SKey, query2l, 100, IsSec, "
            select col1, col5
            from def
            where element(2,col5) = 5"
        ,
        [{<<"5">>, <<"{'Atom5',5}">>}]
    ),

    exec_fetch_sort_equal(SKey, query2m, 100, IsSec, "
            select col1, col5
            from def
            where element(2,col5) = to_integer(4+1)"
        ,
        [{<<"5">>, <<"{'Atom5',5}">>}]
    ),

    exec_fetch_sort_equal(SKey, query2n, 100, IsSec, "
            select col1, col5
            from def
            where element(2,col5) = to_integer(5.0)"
        ,
        [{<<"5">>, <<"{'Atom5',5}">>}]
    ),

    exec_fetch_sort_equal(SKey, query2o, 100, IsSec, "
            select col1, col5
            from def
            where element(2,col5) = to_integer('5')"
        ,
        [{<<"5">>, <<"{'Atom5',5}">>}]
    ),

    exec_fetch_sort_equal(SKey, query2p, 100, IsSec, "
            select col1, col5
            from def where col5 = to_tuple('{''Atom5'', 5}')"
        ,
        [{<<"5">>, <<"{'Atom5',5}">>}]
    ),

    exec_fetch_sort_equal(SKey, query2q, 100, IsSec, "
            select col1, col5
            from def where col5 = '{''Atom100'',100}'"
        ,
        [{<<"100">>, <<"{'Atom100',100}">>}]
    ),

    % exec_fetch_equal(SKey, query3a, 100, IsSec,
    %     "select ip.item from def, integer ip where col1 = 1 and is_member(item,col4)",
    %     [{<<"10">>},{<<"132">>},{<<"7">>},{<<"1">>}]
    % ),

    % R3b = exec_fetch_sort(SKey, query3b, 100, IsSec,
    %     "select col3, item from def, integer where is_member(item,to_atom('$_')) and col1 <> 100"
    % ),
    % ?assertEqual(20, length(R3b)),

    exec_fetch_sort_equal(SKey, query3g, 100, IsSec, "
            select col1, col5
            from def, ddNode
            where element(2,col5) = name"
        ,
        []
    ),

    if_call_mfa(IsSec, write, [SKey, def,
        {def, 0, <<"0">>, calendar:local_time(), {10, 132, 7, 0}, {list_to_atom("Atom" ++ integer_to_list(0)), node()}}
    ]),

    exec_fetch_sort_equal(SKey, query3h, 100, IsSec, "
            select col1, col5
            from def, ddNode
            where element(2,col5) = name"
        ,
        [{<<"0">>, <<"{'Atom0',nonode@nohost}">>}]
    ),

    exec_fetch_sort_equal(SKey, query3i, 100, IsSec, "
            select col1, col5
            from def, ddNode
            where element(2,col5) = to_atom('nonode@nohost')"
        ,
        [{<<"0">>, <<"{'Atom0',nonode@nohost}">>}]
    ),

    exec_fetch_sort_equal(SKey, query3j, 100, IsSec, "
            select col1, col5
            from def, ddNode
            where element(2,col5) = to_atom('nonode@anotherhost')"
        ,
        []
    ),

    exec_fetch_sort_equal(SKey, query3k, 100, IsSec, "
            select item
            from integer
            where item >=1 and item <= 3"
        ,
        [{<<"1">>}, {<<"2">>}, {<<"3">>}]
    ),

    exec_fetch_sort_equal(SKey, query3l, 100, IsSec, "
            select first.item, second.item, first.item * second.item as product
            from integer first, integer second
            where first.item >=1 and first.item <= 3
            and second.item > 4 and second.item < 7"
        ,
        [{<<"1">>, <<"5">>, <<"5">>}
            , {<<"1">>, <<"6">>, <<"6">>}
            , {<<"2">>, <<"5">>, <<"10">>}
            , {<<"2">>, <<"6">>, <<"12">>}
            , {<<"3">>, <<"5">>, <<"15">>}
            , {<<"3">>, <<"6">>, <<"18">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query3m, 100, IsSec, "
            select first.item, second.item, first.item * second.item as product
            from integer first, integer second
            where first.item >=1 and first.item <= 3
            and second.item > first.item and second.item < 5"
        ,
        [{<<"1">>, <<"2">>, <<"2">>}
            , {<<"1">>, <<"3">>, <<"3">>}
            , {<<"1">>, <<"4">>, <<"4">>}
            , {<<"2">>, <<"3">>, <<"6">>}
            , {<<"2">>, <<"4">>, <<"8">>}
            , {<<"3">>, <<"4">>, <<"12">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query3n, 100, IsSec, "
            select item
            from dual,integer
            where is_member(item, to_list('[1,2,3]'))"
        ,
        [{<<"1">>}, {<<"2">>}, {<<"3">>}]
    ),

    exec_fetch_sort_equal(SKey, query3o, 100, IsSec, "
            select item
            from integer
            where is_member(item, to_list('[1,2,3]'))"
        ,
        [{<<"1">>}, {<<"2">>}, {<<"3">>}]
    ),


    exec_fetch_sort_equal(SKey, query3q, 100, IsSec, "
            select item from atom where is_member(item, list(to_atom('a')))"
        ,
        [
            {<<"a">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query3r, 100, IsSec, "
            select item from atom where is_member(item, list(to_atom('a'),to_atom('b')))"
        ,
        [
            {<<"a">>}
            , {<<"b">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query3s, 100, IsSec, "
            select item from term where is_member(item, list(1))"
        ,
        [
            {<<"1">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query3t, 100, IsSec, "
            select item from term where is_member(item, list(to_atom('a'),1,2.5))"
        ,
        [
            {<<"1">>}
            , {<<"2.5">>}
            , {<<"a">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query3u, 100, IsSec, "
            select item
            from integer
            where is_member(item, to_list('[1,2,3]'))"
        ,
        [{<<"1">>}, {<<"2">>}, {<<"3">>}]
    ),


    %% self joins

    exec_fetch_sort_equal(SKey, query4, 100, IsSec, "
            select t1.col1, t2.col1 j
            from def t1, def t2
            where t1.col1 in (5,6,7)
            and t2.col1 > t1.col1
            and t2.col1 > t1.col1
            and t2.col1 <> 9
            and t2.col1 <> 100
            and t2.col1 < 11"
        ,
        [
            {<<"5">>, <<"6">>}, {<<"5">>, <<"7">>}, {<<"5">>, <<"8">>}, {<<"5">>, <<"10">>},
            {<<"6">>, <<"7">>}, {<<"6">>, <<"8">>}, {<<"6">>, <<"10">>},
            {<<"7">>, <<"8">>}, {<<"7">>, <<"10">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query4a, 100, IsSec, "
            select t1.col1, t2.col1
            from def t1, def t2
            where t1.col1 in (5,6,7)
            and t2.col1 > t1.col1
            and t2.col1 <= t1.col1 + 2"
        ,
        [
            {<<"5">>, <<"6">>}, {<<"5">>, <<"7">>},
            {<<"6">>, <<"7">>}, {<<"6">>, <<"8">>},
            {<<"7">>, <<"8">>}, {<<"7">>, <<"9">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query4b, 100, IsSec, "
            select t1.col1, t2.col1
            from def t1, def t2
            where t1.col1 in (5,7)
            and abs(t2.col1-t1.col1) = 1"
        ,
        [
            {<<"5">>, <<"4">>}, {<<"5">>, <<"6">>},
            {<<"7">>, <<"6">>}, {<<"7">>, <<"8">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query4c, 100, IsSec, "
            select t1.col1, t2.col1
            from def t1, def t2
            where t1.col1=5
            and t2.col1 > t1.col1 / 2
            and t2.col1 <= t1.col1"
        ,
        [
            {<<"5">>, <<"3">>}, {<<"5">>, <<"4">>}, {<<"5">>, <<"5">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query4d, 100, IsSec, "
            select t1.col1, t2.col2
            from def t1, def t2
            where t1.col1 <> 5
            and t1.col1 <= 10
            and t1.col1 <> 0
            and not (t2.col2 = '7')
            and t2.col1 = t1.col1"
        ,
        [
            {<<"1">>, <<"1">>}, {<<"2">>, <<"2">>}, {<<"3">>, <<"3">>}, {<<"4">>, <<"4">>},
            {<<"6">>, <<"6">>}, {<<"8">>, <<"8">>}, {<<"9">>, <<"9">>}, {<<"10">>, <<"10">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query6a, 100, IsSec, "
            select col1, col2
            from def
            where col1 < 11
            and col1 <> 0
            order by col1 desc, col2"
        ,
        [
            {<<"10">>, <<"10">>}
            , {<<"9">>, <<"9">>}
            , {<<"8">>, <<"8">>}
            , {<<"7">>, <<"7">>}
            , {<<"6">>, <<"6">>}
            , {<<"5">>, <<"5">>}
            , {<<"4">>, <<"4">>}
            , {<<"3">>, <<"3">>}
            , {<<"2">>, <<"2">>}
            , {<<"1">>, <<"1">>}
        ]
    ),


    exec_fetch_sort_equal(SKey, query6b, 100, IsSec, "
            select 2*col1
            from def
            where col1 <= 5
            and col1 <> 0
            order by 1 desc, col2"
        ,
        [
            {<<"10">>}
            , {<<"8">>}
            , {<<"6">>}
            , {<<"4">>}
            , {<<"2">>}
        ]
    ),

    % Q6bExpected=
    % [{<<"1">>,<<"8.94736842105263160000e-01">>}
    % ,{<<"2">>,<<"1.57894736842105270000e+00">>}
    % ,{<<"3">>,<<"2.05263157894736860000e+00">>}
    % ,{<<"4">>,<<"2.31578947368421060000e+00">>}
    % ,{<<"5">>,<<"2.36842105263157880000e+00">>}
    % ,{<<"6">>,<<"2.21052631578947390000e+00">>}
    % ,{<<"7">>,<<"1.84210526315789470000e+00">>}
    % ,{<<"8">>,<<"1.26315789473684250000e+00">>}
    % ,{<<"9">>,<<"4.73684210526315040000e-01">>}
    % ],
    % exec_fetch_sort_equal(SKey, query6b, 100, IsSec, "
    %     select col1, col1 - col1*col1/9.5
    %     from def
    %     where col1 <= 9
    %     and col1 <> 0
    %     order by 1"
    %     ,
    %     Q6bExpected
    % ),

    % exec_fetch_sort_equal(SKey, query6c, 100, IsSec, "
    %     select col1, col1 - col1*col1/9.5
    %     from def
    %     where col1 <= 9
    %     and col1 <> 0
    %     order by 1 desc"
    %     ,
    %     lists:reverse(Q6bExpected)
    % ),

    % Q6dExpected=
    % [{<<"9">>,<<"4.73684210526315040000e-01">>}
    % ,{<<"1">>,<<"8.94736842105263160000e-01">>}
    % ,{<<"8">>,<<"1.26315789473684250000e+00">>}
    % ,{<<"2">>,<<"1.57894736842105270000e+00">>}
    % ,{<<"7">>,<<"1.84210526315789470000e+00">>}
    % ,{<<"3">>,<<"2.05263157894736860000e+00">>}
    % ,{<<"6">>,<<"2.21052631578947390000e+00">>}
    % ,{<<"4">>,<<"2.31578947368421060000e+00">>}
    % ,{<<"5">>,<<"2.36842105263157880000e+00">>}
    % ],
    % exec_fetch_sort_equal(SKey, query6d, 100, IsSec, "
    %     select col1, col1 - col1*col1/9.5
    %     from def
    %     where col1 <= 9
    %     and col1 <> 0
    %     order by 2"
    %     ,
    %     Q6dExpected
    % ),

    % exec_fetch_sort_equal(SKey, query6e, 100, IsSec, "
    %     select col1, col1 - col1*col1/9.5
    %     from def
    %     where col1 <= 9
    %     and col1 <> 0
    %     order by 2 desc"
    %     ,
    %     lists:reverse(Q6dExpected)
    % ),

    % exec_fetch_sort_equal(SKey, query6f, 100, IsSec, "
    %     select col1, col1 - col1*col1/9.5
    %     from def
    %     where col1 <= 9
    %     and col1 <> 0
    %     order by col1 - col1*col1/9.5 desc"
    %     ,
    %     lists:reverse(Q6dExpected)
    % ),

    % exec_fetch_sort_equal(SKey, query6g, 100, IsSec, "
    %     select col1, col1 - col1*col1/9.5
    %     from def
    %     where col1 <= 9
    %     and col1 <> 0
    %     order by '12' asc, col1 - col1*col1/9.5 desc"
    %     ,
    %     lists:reverse(Q6dExpected)
    % ),

    exec_fetch_sort_equal(SKey, query7a, 100, IsSec, "
            select col2
            from def
            where col2 like '1%'"
        ,
        [
            {<<"1">>}
            , {<<"10">>}
            , {<<"11">>}
            , {<<"12">>}
            , {<<"13">>}
            , {<<"14">>}
            , {<<"15">>}
            , {<<"16">>}
            , {<<"17">>}
            , {<<"18">>}
            , {<<"19">>}
        ]
    ),

    % exec_fetch_sort_equal(SKey, query7b, 100, IsSec, "
    %     select col1, col2 from def where col2 like '%_in_%'"
    %     ,
    %     [{<<"100">>, <<"\"text_in_quotes\"">>}]
    % ),

    exec_fetch_sort_equal(SKey, query7c, 100, IsSec, "
            select col1 from def where col2 like '%quotes\"'"
        ,
        [{<<"100">>}]
    ),

    exec_fetch_sort_equal(SKey, query7d, 100, IsSec, "
            select col1 from def where col2 like '_text_in%'"
        ,
        [{<<"100">>}]
    ),

    exec_fetch_sort_equal(SKey, query7e, 100, IsSec, "
            select col1 from def where col2 like 'text_in%'"
        ,
        []
    ),

    exec_fetch_sort_equal(SKey, query7f, 100, IsSec, "
            select col2
            from def
            where col2 like '%1' or col2 like '1%'"
        ,
        [
            {<<"1">>}
            , {<<"10">>}
            , {<<"11">>}
            , {<<"12">>}
            , {<<"13">>}
            , {<<"14">>}
            , {<<"15">>}
            , {<<"16">>}
            , {<<"17">>}
            , {<<"18">>}
            , {<<"19">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query7fa, 100, IsSec, "
            select col2
            from def
            where col2 like '%1' or col2 not like '1%'"
        ,
        [
            {<<"0">>}
            , {<<"1">>}
            , {<<"2">>}
            , {<<"3">>}
            , {<<"4">>}
            , {<<"5">>}
            , {<<"6">>}
            , {<<"7">>}
            , {<<"8">>}
            , {<<"9">>}
            , {<<"11">>}
            , {<<"20">>}
            , {<<"\"text_in_quotes\"">>}
        ]
    ),

    %% regexp_like()

    exec_fetch_sort_equal(SKey, query7g, 100, IsSec, "
            select col2 from def where regexp_like(col2,'0')"
        ,
        [{<<"0">>}, {<<"10">>}, {<<"20">>}]
    ),

    exec_fetch_sort_equal(SKey, query7h, 100, IsSec, "
            select col1 from def where regexp_like(col2,'^\"')"
        ,
        [{<<"100">>}]
    ),

    exec_fetch_sort_equal(SKey, query7i, 100, IsSec, "
            select col1 from def where regexp_like(col2,'s\"$')"
        ,
        [{<<"100">>}]
    ),

    exec_fetch_sort_equal(SKey, query7j, 100, IsSec, "
            select col1 from def where regexp_like(col2,'_.*_')"
        ,
        [{<<"100">>}]
    ),

    exec_fetch_sort_equal(SKey, query7k, 100, IsSec, "
            select col1 from def where regexp_like(col2,'^[^_]*_[^_]*$')"
        ,
        []
    ),

    %% like joins

    exec_fetch_sort_equal(SKey, query7l, 100, IsSec, "
            select d1.col1, d2.col1
            from def d1, def d2
            where d1.col1 > 10
            and d2.col1 like '%5%'
            and d2.col1 = d1.col1"
        ,
        [
            {<<"15">>, <<"15">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query7m, 100, IsSec, "
            select d1.col1, d2.col1
            from def d1, def d2
            where d1.col1 >= 5
            and d2.col1 like '%5%'
            and d2.col2 like '5%'
            and d2.col1 = d1.col1"
        ,
        [
            {<<"5">>, <<"5">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query7n, 100, IsSec, "
            select d1.col1, d2.col1
            from def d1, def d2
            where d1.col1 >= 5
            and d2.col1 like '%5%'
            and d2.col2 not like '1%'
            and d2.col1 = d1.col1"
        ,
        [
            {<<"5">>, <<"5">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8b, 100, IsSec, "
            select col2 || col2
            from def
            where col1 = 1 or col1=20"
        ,
        [
            {<<"11">>}, {<<"2020">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8c, 100, IsSec, "
            select col2 || to_binstr('XYZ')
            from def
            where col1 = 1 or col1=20"
        ,
        [
            {<<"1XYZ">>}, {<<"20XYZ">>}
        ]
    ),

% FIXME: Currently fails in Travis
%        exec_fetch_sort_equal(SKey, query8f, 100, IsSec, "
%            select col2 || to_string(sqrt(2.0))
%            from def
%            where col1 = 5"
%            ,
%            [
%                {<<"\"51.41421356237309510000e+00\"">>}
%            ]
%        ),
%
%        exec_fetch_sort_equal(SKey, query8g, 100, IsSec, "
%            select col2 || to_binstr(sqrt(2.0))
%            from def
%            where col1 = 5"
%            ,
%            [
%                {<<"51.41421356237309510000e+00">>}
%            ]
%        ),
%
%        exec_fetch_sort_equal(SKey, query8h, 100, IsSec, "
%            select col2
%            from def
%            where col2 || to_binstr(sqrt(2.0)) = to_binstr('51.41421356237309510000e+00')"
%            ,
%            [
%                {<<"5">>}
%            ]
%        ),

    exec_fetch_sort_equal(SKey, query8i, 100, IsSec, "
            select col2
            from def
            where byte_size(col2) > 1 and col1 < 11"
        ,
        [
            {<<"10">>}
        ]
    ),

    _ = {timeout, 5, fun() ->
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table member_test;", 0, [{schema, imem}], IsSec))
                     end},
    _ = {timeout, 5, fun() ->
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, [{schema, imem}], IsSec))
                     end},

    case IsSec of
        true -> ?imem_logout(SKey);
        false -> ok
    end,

    ok.

db1_with_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":db1_with_sec/1 - Start ===>~n", []),

    db1_with_or_without_sec(true).

db1_without_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":db1_without_sec/1 - Start ===>~n", []),

    db1_with_or_without_sec(false).

db2_with_or_without_sec(IsSec) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":db2_with_or_without_sec/1 - Start(~p) ===>~n", [IsSec]),

    ClEr = 'ClientError',
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":schema ~p~n", [imem_meta:schema()]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":data nodes ~p~n", [imem_meta:data_nodes()]),
    ?assertEqual(true, is_atom(imem_meta:schema())),
    ?assertEqual(true, lists:member({imem_meta:schema(), node()}, imem_meta:data_nodes())),

    ?assertEqual([], imem_statement:receive_raw()),

    ?assertEqual([imem], imem_datatype:field_value(tag, list, 0, 0, [], <<"[imem]">>)),

    SKey = case IsSec of
               true -> ?imem_test_admin_login();
               false -> none
           end,

    ?assertMatch({ok, _}, imem_sql:exec(SKey,
        "create table def (
            col1 integer,
            col2 varchar2(2000),
            col3 date,
            col4 ipaddr,
            col5 tuple
        );", 0, [{schema, imem}], IsSec)),

    ?assertEqual(ok, insert_range(SKey, 20, def, imem, IsSec)),

    %% test table member_test

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "
            create table member_test (
                  col1 integer
                , col2 list
                , col3 tuple
            );"
        , 0, [{schema, imem}], IsSec)),

    if_call_mfa(IsSec, write, [SKey, member_test,
        {member_test, 1, [a, b, c, [e]], undefined}
    ]),
    if_call_mfa(IsSec, write, [SKey, member_test,
        {member_test, 2, [1, 2, 3, {e}], 9}
    ]),
    if_call_mfa(IsSec, write, [SKey, member_test,
        {member_test, 3, [[e], 3, 4, 5], 1}
    ]),
    if_call_mfa(IsSec, write, [SKey, member_test,
        {member_test, 4, undefined, {a, d, e}}
    ]),
    if_call_mfa(IsSec, write, [SKey, member_test,
        {member_test, 5, [d, {e}, a], {a, d, e}}
    ]),

    {L1, true} = if_call_mfa(IsSec, select, [SKey, member_test, ?MatchAllRecords, 1000]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Test table member_test : ~p entries~n~p~n~p~n~p~n", [length(L1), hd(L1), '...', lists:last(L1)]),
    ?assertEqual(5, length(L1)),

    %% queries on meta table

    {L2, true} = if_call_mfa(IsSec, select, [SKey, ddTable, ?MatchAllRecords, 1000]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Table ddTable : ~p entries~n~p~n~p~n~p~n", [length(L2), hd(L2), '...', lists:last(L2)]),
    AllTableCount = length(L2),

    {L3, true} = if_call_mfa(IsSec, select, [SKey, dba_tables, ?MatchAllKeys]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Table dba_tables : ~p entries~n~p~n~p~n~p~n", [length(L3), hd(L3), '...', lists:last(L3)]),
    ?assertEqual(AllTableCount, length(L3)),

    {L4, true} = if_call_mfa(IsSec, select, [SKey, all_tables, ?MatchAllKeys]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Table all_tables : ~p entries~n~p~n~p~n~p~n", [length(L4), hd(L4), '...', lists:last(L4)]),
    ?assertEqual(AllTableCount, length(L4)),

    {L5, true} = if_call_mfa(IsSec, select, [SKey, user_tables, ?MatchAllKeys]),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Table user_tables : ~p entries~n~p~n~p~n~p~n", [length(L5), hd(L5), '...', lists:last(L5)]),
    case IsSec of
        false -> ?assertEqual(AllTableCount, length(L5));
        true -> ?assertEqual(2, length(L5))
    end,

    R0 = exec_fetch_sort(SKey, query0, AllTableCount + 1, IsSec, "
            select * from ddTable"
    ),
    ?assertEqual(AllTableCount, length(R0)),

    R0a = exec_fetch_sort(SKey, query0a, 100, IsSec, "
            select * from ddTable where rownum <= 10"
    ),
    ?assertEqual(10, length(R0a)),

    exec_fetch_sort_equal(SKey, query0c, 100, IsSec, "
            select 1 from dual"
        ,
        [{<<"1">>}]
    ),

    exec_fetch_sort_equal(SKey, query0d, 100, IsSec, "
            select list(1, to_atom('b'), 3.14, to_string('4')) from dual"
        ,
        [{<<"[1,b,3.14,\"4\"]">>}]
    ),

    exec_fetch_sort_equal(SKey, query0e, 100, IsSec, "
            select tuple(1, to_binstr('2'), 3, 4) from dual"
        ,
        [{<<"{1,<<\"2\">>,3,4}">>}]
    ),

    exec_fetch_sort_equal(SKey, query0f, 100, IsSec, "
            select list(col1,col3) from member_test
            where col1 = 5"
        ,
        [{<<"[5,{a,d,e}]">>}]
    ),
    exec_fetch_sort_equal(SKey, query0g, 100, IsSec, "
            select tuple(col1,col3) from member_test
            where col1 = 5"
        ,
        [{<<"{5,{a,d,e}}">>}]
    ),

    %% simple queries on meta fields

    exec_fetch_sort_equal(SKey, query1, 100, IsSec, "
            select dual.* from dual"
        ,
        [{<<"\"X\"">>, ?navio}]
    ),

    exec_fetch_sort_equal(SKey, query1a, 100, IsSec, "
            select dual.dummy from dual"
        ,
        [{<<"\"X\"">>}]
    ),

    R1b = exec_fetch_sort(SKey, query1b, 100, IsSec, "
            select sysdate from dual"
    ),
    ?assertEqual(19, size(element(1, hd(R1b)))),

    R1c = exec_fetch_sort(SKey, query1c, 100, IsSec, "
            select systimestamp from dual"
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":systimestamp ~p", [element(1, hd(R1c))]),
    ?assertEqual(26, size(element(1, hd(R1c)))),

    R1d = exec_fetch_sort(SKey, query1d, 100, IsSec, "
            select user from dual"
    ),
    case IsSec of
        false -> ?assertEqual([{<<"unknown">>}], R1d);
        true -> Acid = integer_to_binary(imem_seco:account_id(SKey)),
            ?assertEqual([{Acid}], R1d)
    end,

    R1e = exec_fetch_sort(SKey, query1e, AllTableCount + 1, IsSec, "
            select all_tables.*
            from all_tables
            where owner = 'system'"
    ),
    ?assert(length(R1e) =< AllTableCount),
    ?assert(length(R1e) >= 5),

    R1f = exec_fetch_sort(SKey, query1f, 100, IsSec, "
            select qname as qn
            from all_tables
            where owner=user"
    ),
    case IsSec of
        false -> ?assertEqual(0, length(R1f));
        true -> ?assertEqual(2, length(R1f))
    end,

    R1g = exec_fetch_sort(SKey, query1g, 100, IsSec, "
            select name, type
            from ddAccount
            where id=user
            and locked <> 'true'"
    ),
    case IsSec of
        false -> ?assertEqual(0, length(R1g));
        true -> ?assertEqual(1, length(R1g))
    end,

    exec_fetch_sort_equal(SKey, query1k, 100, IsSec, "
            select dummy
            from dual
            where rownum = 1"
        ,
        [{<<"\"X\"">>}]
    ),

    exec_fetch_sort_equal(SKey, query1l, 100, IsSec, "
            select dummy
            from dual
            where rownum <= 1"
        ,
        [{<<"\"X\"">>}]
    ),

    exec_fetch_sort_equal(SKey, query1m, 100, IsSec, "
            select dummy
            from dual
            where rownum = 2"
        ,
        []
    ),

    exec_fetch_sort_equal(SKey, query1n, 100, IsSec, "
            select dummy
            from dual
            where rownum = 0"
        ,
        []
    ),

    exec_fetch_sort_equal(SKey, query1o, 100, IsSec, "
            select dummy
            from dual
            where rownum <= -1"
        ,
        []
    ),

    %% simple queries on single table

    % R2g = exec_fetch(SKey, query2g, 100, IsSec,
    %     "select logTime, logLevel, module, function, fields, message
    %      from " ++ atom_to_list(?LOG_TABLE) ++ "
    %      where logTime > systimestamp - 1.1574074074074073e-5
    %      and rownum <= 100"   %% 1.0 * ?OneSecond
    % ),
    % ?assert(length(R2g) >= 1),
    % ?assert(length(R2g) =< 100),


    ?assertEqual(ok, imem_monitor:write_monitor()),

    % R2h = exec_fetch(SKey, query2h, 100, IsSec,
    %     "select time
    %      from " ++ atom_to_list(?MONITOR_TABLE) ++ "
    %      where time > systimestamp - 1.1574074074074073e-6
    %     "
    % ),
    % ?assert(length(R2h) >= 1),
    % ?assert(length(R2h) =< 6),

    % R2i = exec_fetch(SKey, query2i, 100, IsSec,
    %     "select time
    %      from " ++ atom_to_list(?MONITOR_TABLE) ++ "
    %      where time >  1 + systimestamp
    %     "
    % ),
    % ?assert(length(R2i) == 0),

    % R2j = exec_fetch(SKey, query2j, 100, IsSec,
    %     "select time
    %      from " ++ atom_to_list(?MONITOR_TABLE) ++ "
    %      where time >  -1.0/24.0  + systimestamp
    %     "
    % ),
    % ?assert(length(R2j) > 0),
    % ?assert(length(R2j) < 2000),

    %% joins with virtual (datatype) tables

    % ?assertException(throw,{ClEr,{"Virtual table can only be joined",<<"integer">>}},
    ?assertException(throw, {ClEr, {"Invalid virtual filter guard", true}},
        exec_fetch_sort(SKey, query3a1, 100, IsSec, "select item from integer")
    ),

    ?assertException(throw, {ClEr, {"Invalid virtual filter guard", true}},
        exec_fetch_sort(SKey, query3a2, 100, IsSec, "select name from ddSize where name like 'ddAcc%'")
    ),

    R3c = exec_fetch_sort(SKey, query3c, 100, IsSec, "
            select * from ddNode"
    ),
    ?assertEqual(1, length(R3c)),

    R3d = exec_fetch_sort(SKey, query3d, 100, IsSec, "
            select time, wall_clock
            from ddNode"
    ),
    ?assertEqual(1, length(R3d)),

    R3e = exec_fetch_sort(SKey, query3e, 100, IsSec, "
            select time, wall_clock
            from ddNode where name = '" ++ atom_to_list(node()) ++ "'"
    ),
    ?assertEqual(1, length(R3e)),

    % R3f = exec_fetch_sort(SKey, query3f, 100, IsSec, "
    %     select *
    %     from " ++ atom_to_list(?MONITOR_TABLE) ++ " m, ddNode n
    %     where rownum < 2 and m.node = n.name"
    % ),
    % ?assertEqual(1, length(R3f)),

    exec_fetch_sort_equal(SKey, query5h, 100, IsSec, "
            select d.col1, m.col1
            from def d, member_test m
            where is_member(d.col1,m.col2)"
        ,
        [
            {<<"1">>, <<"2">>},
            {<<"2">>, <<"2">>},
            {<<"3">>, <<"2">>}, {<<"3">>, <<"3">>},
            {<<"4">>, <<"3">>},
            {<<"5">>, <<"3">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query5i, 100, IsSec, "
            select d.col1, m.col1
            from def d, member_test m
            where d.col1 <> 0
            and is_member(d.col1+1,m.col2)"
        ,
        [
            {<<"1">>, <<"2">>},
            {<<"2">>, <<"2">>}, {<<"2">>, <<"3">>},
            {<<"3">>, <<"3">>},
            {<<"4">>, <<"3">>}
        ]
    ),  % ToDo: reversing the table names crashes the server, unsupported join filter at runtime

    exec_fetch_sort_equal(SKey, query5j, 100, IsSec, "
            select d.col1, m.col1
            from def d, member_test m
            where is_member(d.col1,m)"
        ,
        [
            {<<"1">>, <<"1">>}, {<<"1">>, <<"3">>},
            {<<"2">>, <<"2">>},
            {<<"3">>, <<"3">>},
            {<<"4">>, <<"4">>},
            {<<"5">>, <<"5">>},
            {<<"9">>, <<"2">>}
        ]
    ),

    %% is_xxx function conditions

    exec_fetch_sort_equal(SKey, query5, 100, IsSec, "
            select col1
            from member_test
            where is_list(col2)
            or is_tuple(col3)"
        ,
        [{<<"1">>}, {<<"2">>}, {<<"3">>}, {<<"4">>}, {<<"5">>}]
    ),

    exec_fetch_sort_equal(SKey, query5a, 100, IsSec, "
            select col1
            from member_test
            where is_member(3,col2)
            and col1 > 0"
        ,
        [{<<"2">>}, {<<"3">>}]
    ),

    exec_fetch_sort_equal(SKey, query5b, 100, IsSec, "
            select col1
            from member_test
            where is_member(to_atom('a'),col2)"
        ,
        [{<<"1">>}, {<<"5">>}]
    ),

    exec_fetch_sort_equal(SKey, query5c, 100, IsSec, "
            select col1
            from member_test
            where is_member(to_tuple('{e}'),col2)"
        ,
        [{<<"2">>}, {<<"5">>}]
    ),

    exec_fetch_sort_equal(SKey, query5d, 100, IsSec, "
            select col1
            from member_test
            where is_member(to_list('[e]'),col2)"
        ,
        [{<<"1">>}, {<<"3">>}]
    ),

    exec_fetch_sort_equal(SKey, query5e, 100, IsSec, "
            select col1
            from member_test
            where is_member(1,member_test)"
        ,
        [{<<"1">>}, {<<"3">>}]
    ),

    exec_fetch_sort_equal(SKey, query5f, 100, IsSec, "
            select col1
            from member_test
            where is_member(3,to_list('[1,2,3,4]'))"
        ,
        [{<<"1">>}, {<<"2">>}, {<<"3">>}, {<<"4">>}, {<<"5">>}]
    ),

    exec_fetch_sort_equal(SKey, query5g, 100, IsSec, "
            select col1
            from member_test a
            where is_member(to_atom('undefined'),a)"
        ,
        [{<<"1">>}, {<<"4">>}]
    ),

    R5k = exec_fetch_sort(SKey, query5k, AllTableCount + 1, IsSec, "
            select to_name(qname)
            from ddTable
            where is_member(to_tuple('{virtual, true}'), opts)"
    ),
    % ?assert(length(R5k) >= 18),
    ?assert(length(R5k) == 0),      % not used any more for DataTypes
    % ?assert(lists:member({"imem.atom"},R5k)),
    % ?assert(lists:member({"imem.userid"},R5k)),
    ?assertNot(lists:member({"imem.ddTable"}, R5k)),
    ?assertNot(lists:member({"imem.ddTable"}, R5k)),

    R5l = exec_fetch_sort(SKey, query5l, AllTableCount + 1, IsSec, "
            select to_name(qname)
            from ddTable
            where not is_member(to_tuple('{virtual, true}'), opts)"
    ),
    ?assert(length(R5l) >= 5),
    ?assertNot(lists:member({<<"imem.atom">>}, R5l)),
    ?assertNot(lists:member({<<"imem.userid">>}, R5l)),
    ?assert(lists:member({<<"imem.ddTable">>}, R5l)),
    ?assert(lists:member({<<"imem.ddAccount">>}, R5l)),

    R5m = exec_fetch_sort(SKey, query5m, AllTableCount + 1, IsSec, "
            select
                to_name(qname),
                item2(item) as field,
                item3(item) as \"type\",
                item4(item) as len,
                item5(item) as prec,
                item6(item) as def
            from ddTable, list
            where is_member(item, columns)"
    ),
    ?assert(length(R5m) >= 5),

    exec_fetch_sort_equal(SKey, query5n, 100, IsSec, "
            select col1
            from member_test
            where col3 = to_tuple('{a,d,e}')"
        ,
        [{<<"4">>}, {<<"5">>}]
    ),

    exec_fetch_sort_equal(SKey, query5o, 100, IsSec, "
            select col1
            from member_test
            where col3 = to_tuple('{x,d,e}')"
        ,
        []
    ),

    exec_fetch_sort_equal(SKey, query5p, 100, IsSec, "
            select col1
            from member_test
            where col3 = to_tuple('{''a'',d,e}')"
        ,
        [{<<"4">>}, {<<"5">>}]
    ),

    exec_fetch_sort_equal(SKey, query5q, 100, IsSec, "
            select col1
            from member_test
            where col3 = to_tuple('{a,{\"d\"},e}')"
        ,
        []
    ),

    R5r = exec_fetch_sort(SKey, query5r, AllTableCount + 1, IsSec, "
            select to_name(qname), size, memory
            from ddTable, ddSize
            where element(2,qname) = name "
    ),
    ?assert(length(R5r) > 0),

    R5s = exec_fetch_sort(SKey, query5s, AllTableCount + 1, IsSec, "
            select to_name(qname), nodef(tte)
            from ddTable, ddSize
            where name = element(2,qname)"
    ),
    ?assertEqual(length(R5s), length(R5r)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Full Result R5s: ~n~p~n", [R5s]),

    R5t = exec_fetch_sort(SKey, query5t, AllTableCount + 1, IsSec, "
            select to_name(qname), tte
            from ddTable, ddSize
            where element(2,qname) = name
            and tte <> to_atom('undefined')"
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result R5t DIFF: ~n~p~n", [R5s -- R5t]),
    ?assert(length(R5t) > 0),
    ?assert(length(R5t) < length(R5s)),

    R5u = exec_fetch_sort(SKey, query5u, AllTableCount + 1, IsSec, "
            select to_name(qname), tte
            from ddTable, ddSize
            where element(2,qname) = name
            and tte = to_atom('undefined')"
    ),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result R5u DIFF: ~n~p~n", [R5s -- R5u]),
    ?assert(length(R5u) > 0),
    ?assert(length(R5u) < length(R5s)),
    ?assert(length(R5t) + length(R5u) == length(R5s)),

    R5v = exec_fetch_sort(SKey, query5v, AllTableCount + 1, IsSec, "
            select to_name(qname), size, tte
            from ddTable, ddSize
            where element(2,qname) = name
            and tte <> to_atom('undefined') and tte > 0"
    ),
    ?assert(length(R5v) > 0),

    R5w = exec_fetch_sort(SKey, query5w, 100, IsSec, "
            select hkl
            from ddConfig
            where element ( 1 , hd ( hkl ) ) = to_atom('imem')"
    ),
    ?assert(length(R5w) > 0),

    if_call_mfa(IsSec, write, [SKey, member_test,
        {member_test, 6, [e, {f}, g], {imem_meta:schema(), node()}}
    ]),

    exec_fetch_sort_equal(SKey, query5x, 100, IsSec, "
            select col1
            from ddSchema, member_test
            where element ( 2 , col3 ) = element ( 2 , schemaNode )"
        ,
        [{<<"6">>}]
    ),

    exec_fetch_sort_equal(SKey, query5x0, 100, IsSec, "
            select col1
            from member_test
            where is_list(col2) and nth(1,col2) = 1"
        ,
        [{<<"2">>}]
    ),

    exec_fetch_sort_equal(SKey, query5x1, 100, IsSec, "
            select col1
            from member_test
            where safe_integer(nth(1,col2)) = 1"
        ,
        [{<<"2">>}]
    ),

    exec_fetch_sort_equal(SKey, query5x1, 100, IsSec, "
            select col1
            from member_test
            where safe_integer(nth(17,col2)) = 1"
        ,
        []
    ),

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "
            create table ddCmdTest (
                id integer,
                owner userid,
                opts term,
                \"roles\" list
            );", 0, [{schema, imem}], IsSec)),

    ?assertMatch({ok, _}, imem_sql:exec(SKey, "
            create table ddViewTest (
                id integer,
                owner userid,
                cmd integer
            );", 0, [{schema, imem}], IsSec)),

    if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 1, system, [a], []}]),
    if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 2, system, [a, b], [a, b]}]),
    if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 3, system, [a, b, c], []}]),
    if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 11, 111, [c], [c]}]),
    if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 12, 111, [b], [c]}]),
    if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 13, 111, [a], [c]}]),
    if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 22, 222, [a], [c]}]),
    if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 23, 222, [b], [c]}]),
    if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 24, 222, [c], [c]}]),

    if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1001, system, 1}]),
    if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1002, system, 2}]),
    if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1003, 111, 3}]),
    if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1004, 111, 11}]),
    if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1005, system, 13}]),
    if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1006, 222, 23}]),
    if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1007, system, 24}]),
    if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1008, 222, 12}]),
    if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1009, 222, 2}]),

    case IsSec of
        false -> ok;
        true -> MyAcid = imem_seco:account_id(SKey),
            if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 91, MyAcid, [c], []}]),
            if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 92, MyAcid, [b, c], []}]),
            if_call_mfa(IsSec, write, [SKey, ddCmdTest, {ddCmdTest, 93, MyAcid, [a, b, c], []}]),
            if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1010, MyAcid, 91}]),
            if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1011, MyAcid, 23}]),
            if_call_mfa(IsSec, write, [SKey, ddViewTest, {ddViewTest, 1013, MyAcid, 3}]),
            ok
    end,

    exec_fetch_sort_equal(SKey, query5x2, 100, IsSec, "
            select \"id\", \"roles\"
            from ddCmdTest
            where id <= 3"
        ,
        [{<<"1">>, <<"[]">>}
            , {<<"2">>, <<"[a,b]">>}
            , {<<"3">>, <<"[]">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query5y, 100, IsSec, "
            select v.id, c.id
            from ddViewTest v, ddCmdTest c
            where c.id = v.cmd
            and (c.owner = user or c.owner = to_atom('system'))
            and c.id in (1,2,3,91)
            order by v.id, c.id"
        ,
        case IsSec of
            false -> [{<<"1001">>, <<"1">>}
                , {<<"1002">>, <<"2">>}
                , {<<"1003">>, <<"3">>}
                , {<<"1009">>, <<"2">>}
            ];
            true -> [{<<"1001">>, <<"1">>}
                , {<<"1002">>, <<"2">>}
                , {<<"1003">>, <<"3">>}
                , {<<"1009">>, <<"2">>}
                , {<<"1010">>, <<"91">>}
                , {<<"1013">>, <<"3">>}
            ]
        end
    ),

    exec_fetch_sort_equal(SKey, query5z, 100, IsSec, "
            select v.id, c.id
            from ddCmdTest c, ddViewTest v
            where c.id = v.cmd
            and (c.owner = user or c.owner = to_atom('system'))
            and c.id in (1,2,3,91)
            and is_member(to_atom('b'),c.opts)
            order by v.id, c.id"
        ,
        case IsSec of
            false -> [{<<"1002">>, <<"2">>}
                , {<<"1003">>, <<"3">>}
                , {<<"1009">>, <<"2">>}
            ];
            true -> [{<<"1002">>, <<"2">>}
                , {<<"1003">>, <<"3">>}
                , {<<"1009">>, <<"2">>}
                , {<<"1013">>, <<"3">>}
            ]
        end
    ),

    exec_fetch_sort_equal(SKey, query5z1, 100, IsSec, "
            select v.id, c.id
            from ddViewTest v, ddCmdTest c
            where c.id = v.cmd
            and (c.owner = user or c.owner = to_atom('system'))
            and c.id in (1,2,3,91)
            and not is_member(to_atom('c'),c.opts)
            order by v.id, c.id"
        ,
        case IsSec of
            false -> [{<<"1001">>, <<"1">>}
                , {<<"1002">>, <<"2">>}
                , {<<"1009">>, <<"2">>}
            ];
            true -> [{<<"1001">>, <<"1">>}
                , {<<"1002">>, <<"2">>}
                , {<<"1009">>, <<"2">>}
            ]
        end
    ),

    _ = {timeout, 5, fun() ->
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table ddViewTest;", 0, [{schema, imem}], IsSec))
                     end},

    _ = {timeout, 5, fun() ->
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table ddCmdTest;", 0, [{schema, imem}], IsSec))
                     end},


    %% sorting


    exec_fetch_sort_equal(SKey, query6h, 100, IsSec, "
            select *
            from member_test
            order by is_tuple(col3), col1"
        ,
        [{<<"1">>, <<"[a,b,c,[e]]">>, <<"undefined">>}
            , {<<"2">>, <<"[1,2,3,{e}]">>, <<"9">>}
            , {<<"3">>, <<"[[e],3,4,5]">>, <<"1">>}
            , {<<"4">>, <<"undefined">>, <<"{a,d,e}">>}
            , {<<"5">>, <<"[d,{e},a]">>, <<"{a,d,e}">>}
            , {<<"6">>, <<"[e,{f},g]">>, <<"{imem,nonode@nohost}">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query6i, 100, IsSec, "
            select *
            from member_test
            order by is_tuple(col3), col1 desc"
        ,
        [{<<"3">>, <<"[[e],3,4,5]">>, <<"1">>}
            , {<<"2">>, <<"[1,2,3,{e}]">>, <<"9">>}
            , {<<"1">>, <<"[a,b,c,[e]]">>, <<"undefined">>}
            , {<<"6">>, <<"[e,{f},g]">>, <<"{imem,nonode@nohost}">>}
            , {<<"5">>, <<"[d,{e},a]">>, <<"{a,d,e}">>}
            , {<<"4">>, <<"undefined">>, <<"{a,d,e}">>}
        ]
    ),
    %% like

    %% expressions and concatenations

    exec_fetch_sort_equal(SKey, query8a, 100, IsSec, "
            select 'a' || 'b123'
            from dual"
        ,
        [
            {<<"ab123">>}
        ]
    ),


    exec_fetch_sort_equal(SKey, query8d, 100, IsSec, "
            select to_string('123') || to_string('XYZ')
            from member_test
            where col1 = 5"
        ,
        [
            {<<"\"123XYZ\"">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8e, 100, IsSec, "
            select col2 || to_string('XYZ')
            from member_test
            where col1 = 1"
        ,
        [
            {<<"[a,b,c,[e],88,89,90]">>}
        ]
    ),


    exec_fetch_sort_equal(SKey, query8j, 100, IsSec, "
            select reverse(col2), hd(col2), last(col2)
            from member_test
            where col1 = 1"
        ,
        [
            {<<"[[e],c,b,a]">>, <<"a">>, <<"[e]">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8k, 100, IsSec, "
            select is_list(col2), is_list(last(col2)), is_tuple(hd(col2))
            from member_test
            where col1 = 1"
        ,
        [
            {<<"true">>, <<"true">>, <<"false">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8l, 100, IsSec, "
            select col3
            from member_test
            where is_tuple(col3)"
        ,
        [
            {<<"{a,d,e}">>}, {<<"{a,d,e}">>}, {<<"{imem,nonode@nohost}">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8m, 100, IsSec, "
            select element(1,col3)
            from member_test
            where is_tuple(col3)"
        ,
        [
            {<<"a">>}, {<<"a">>}, {<<"imem">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8n, 100, IsSec, "
            select is_tuple(col3), element(1,col3)
            from member_test"
        ,
        [
            {<<"false">>, ?navio}
            , {<<"false">>, ?navio}
            , {<<"false">>, ?navio}
            , {<<"true">>, <<"a">>}
            , {<<"true">>, <<"a">>}
            , {<<"true">>, <<"imem">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8o, 100, IsSec, "
            select is_tuple(col3), element(1,col3)
            from member_test
            where is_nav(element(1,col3))"
        ,
        [
            {<<"false">>, ?navio}
            , {<<"false">>, ?navio}
            , {<<"false">>, ?navio}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8p, 100, IsSec, "
            select to_text(to_list('[1,64,3]') || '\"SomeText\"' || to_list('[7,64,9]'))
            from member_test
            where col1 = 1"
        ,
        [
            {<<".@.SomeText.@.">>}
        ]
    ),

    exec_fetch_sort_equal(SKey, query8q, 100, IsSec, "
            select to_text(to_list('[1,64,3]') || to_string(col1) || to_list('[7,64,9]'))
            from member_test
            where col1 = 1"
        ,
        [
            {<<".@.1.@.">>}
        ]
    ),

    _ = {timeout, 5, fun() ->
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table member_test;", 0, [{schema, imem}], IsSec))
                     end},

    case IsSec of
        true -> ?imem_logout(SKey);
        false -> ok
    end,

    ok.

db2_with_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":db2_with_sec/1 - Start ===>~n", []),

    db2_with_or_without_sec(true).

db2_without_sec(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":db2_without_sec/1 - Start ===>~n", []),

    db2_with_or_without_sec(false).

%%====================================================================
%% Helper functions.
%%====================================================================

%%exec_fetch(SKey, Id, BS, IsSec, Sql) ->
%%    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~n", []),
%%    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p : ~s~n", [Id, Sql]),
%%    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, [{schema, imem}], IsSec),
%%    ?assertEqual(ok, RetCode),
%%    #stmtResult{stmtRef = StmtRef, stmtCols = StmtCols, rowFun = RowFun} = StmtResult,
%%    List = imem_statement:fetch_recs(SKey, StmtRef, {self(), make_ref()}, 1000, IsSec),
%%    ?assertEqual([ok], imem_statement:close(SKey, StmtRef)),
%%    [?assert(is_binary(SC#stmtCol.alias)) || SC <- StmtCols],
%%    RT = imem_statement:result_tuples(List, RowFun),
%%    if
%%        length(RT) =< 10 ->
%%            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result:~n~p~n", [RT]);
%%        true ->
%%            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result: ~p items~n~p~n~p~n~p~n", [length(RT), hd(RT), '...', lists:last(RT)])
%%    end,
%%    RT.

%%exec_fetch_equal(SKey, _Id, BS, IsSec, Sql, Expected) ->
%%    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~n", []),
%%    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p : ~s~n", [_Id, Sql]),
%%    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, [{schema, imem}], IsSec),
%%    ?assertEqual(ok, RetCode),
%%    #stmtResult{stmtRef = StmtRef, stmtCols = StmtCols, rowFun = RowFun} = StmtResult,
%%    List = imem_statement:fetch_recs(SKey, StmtRef, {self(), make_ref()}, 1000, IsSec),
%%    ?assertEqual([ok], imem_statement:close(SKey, StmtRef)),
%%    [?assert(is_binary(SC#stmtCol.alias)) || SC <- StmtCols],
%%    RT = imem_statement:result_tuples(List, RowFun),
%%    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result:~n~p~n", [RT]),
%%    ?assertEqual(Expected, RT),
%%    RT.

exec_fetch_sort(SKey, _Id, BS, IsSec, Sql) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p : ~s~n", [_Id, Sql]),
    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, [{schema, imem}], IsSec),
    ?assertEqual(ok, RetCode),
    #stmtResults{stmtRefs=StmtRefs, stmtCols=StmtCols, rowFun=RowFun} = StmtResult,
    List = imem_statement:fetch_recs_sort(SKey, StmtResult, {self(), make_ref()}, 1000, IsSec),
    ?assertEqual([ok], lists:usort(imem_statement:close(SKey, StmtRefs))),
    [?assert(is_binary(SC#stmtCol.alias)) || SC <- StmtCols],
    RT = imem_statement:result_tuples(List, RowFun),
    if
        length(RT) =< 3 ->
            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result:~n~p~n", [RT]),
            ok;
        true ->
            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result: ~p items~n~p~n~p~n~p~n", [length(RT), hd(RT), '...', lists:last(RT)]),
            ok
    end,
    RT.

exec_fetch_sort_equal(SKey, _Id, BS, IsSec, Sql, Expected) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~n", []),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":~p : ~s~n", [_Id, Sql]),
    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, [{schema, imem}], IsSec),
    ?assertEqual(ok, RetCode),
    #stmtResults{stmtRefs=StmtRefs, stmtCols=StmtCols, rowFun=RowFun} = StmtResult,
    List = imem_statement:fetch_recs_sort(SKey, StmtResult, {self(), make_ref()}, 1000, IsSec),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":List:~n~p~n", [List]),
    ?assertEqual([ok], lists:usort(imem_statement:close(SKey, StmtRefs))),
    [?assert(is_binary(SC#stmtCol.alias)) || SC <- StmtCols],
    RT = imem_statement:result_tuples(List, RowFun),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result:~n~p~n", [RT]),
    ?assertEqual(Expected, RT),
    RT.

if_call_mfa(IsSec, Fun, Args) ->
    case IsSec of
        true -> apply(imem_sec, Fun, Args);
        _ -> apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

insert_json(_SKey, 0, _Table, _Schema, _IsSec) -> ok;
insert_json(SKey, N, Table, Schema, IsSec) when is_integer(N), N > 0 ->
    if_call_mfa(IsSec, write, [SKey, Table,
        {Table, N, ?TEST_JSON(N), calendar:local_time(), {10, 132, 7, N}, {list_to_atom("Atom" ++ integer_to_list(N)), N}}
    ]),
    insert_json(SKey, N - 1, Table, Schema, IsSec).

insert_json_int_list(_SKey, 0, _Table, _Schema, _IsSec) -> ok;
insert_json_int_list(SKey, N, Table, Schema, IsSec) when is_integer(N), N > 0 ->
    if_call_mfa(IsSec, write, [SKey, Table,
        {Table, N, ?TEST_JSON_LIST(N), calendar:local_time(), {10, 132, 7, N}, {list_to_atom("Atom" ++ integer_to_list(N)), N}}
    ]),
    insert_json_int_list(SKey, N - 1, Table, Schema, IsSec).

insert_json_str_list(_SKey, 0, _Table, _Schema, _IsSec) -> ok;
insert_json_str_list(SKey, N, Table, Schema, IsSec) when is_integer(N), N > 0 ->
    if_call_mfa(IsSec, write, [SKey, Table,
        {Table, N, ?TEST_JSON_STR_LIST(N), calendar:local_time(), {10, 132, 7, N}, {list_to_atom("Atom" ++ integer_to_list(N)), N}}
    ]),
    insert_json_str_list(SKey, N - 1, Table, Schema, IsSec).

insert_range(_SKey, 0, _Table, _Schema, _IsSec) -> ok;
insert_range(SKey, N, Table, Schema, IsSec) when is_integer(N), N > 0 ->
    if_call_mfa(IsSec, write, [SKey, Table,
        {Table, N, list_to_binary(integer_to_list(N)), calendar:local_time(), {10, 132, 7, N}, {list_to_atom("Atom" ++ integer_to_list(N)), N}}
    ]),
    insert_range(SKey, N - 1, Table, Schema, IsSec).
