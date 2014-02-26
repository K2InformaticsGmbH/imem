-module(imem_sql).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(GET_ROWNUM_LIMIT,?GET_IMEM_CONFIG(rownumDefaultLimit,[],10000)).
-define(MaxChar,16#FFFFFF).

-export([ exec/5
        , parse/1
        ]).

-export([ escape_sql/1
        , un_escape_sql/1
        ]).

parse(Sql) ->
    case sqlparse:parsetree(Sql) of
        {ok, {[{ParseTree,_}|_], _Tokens}}  ->  ParseTree;
        {lex_error, Error}              -> ?ClientError({"SQL lexer error", Error});
        {parse_error, Error}            -> ?ClientError({"SQL parser error", Error})
    end.

exec(SKey, Sql, BlockSize, Schema, IsSec) ->
    case sqlparse:parsetree(Sql) of
        {ok, {[{ParseTree,_}|_], _Tokens}} -> 
            exec(SKey, element(1,ParseTree), ParseTree, 
                #statement{stmtStr=Sql, stmtParse=ParseTree, blockSize=BlockSize}, 
                Schema, IsSec);
        {lex_error, Error}      -> ?ClientError({"SQL lexer error", Error});
        {parse_error, Error}    -> ?ClientError({"SQL parser error", Error})
    end.

exec(SKey, select, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, union, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, 'union all', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, minus, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, intersect, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Schema, IsSec);

exec(SKey, insert, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_insert:exec(SKey, ParseTree, Stmt, Schema, IsSec);

exec(SKey, 'create user', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, 'alter user', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, 'drop user', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, 'grant', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, 'revoke', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Schema, IsSec);
    
exec(SKey, 'create table', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, 'drop table', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, 'truncate table', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Schema, IsSec);

exec(SKey, Command, ParseTree, _Stmt, _Schema, _IsSec) ->
    ?UnimplementedException({"SQL command unimplemented", {SKey, Command, ParseTree}}).

escape_sql(Str) when is_list(Str) ->
    re:replace(Str, "(')", "''", [global, {return, list}]);
escape_sql(Bin) when is_binary(Bin) ->
    re:replace(Bin, "(')", "''", [global, {return, binary}]).

un_escape_sql(Str) when is_list(Str) ->
    re:replace(Str, "('')", "'", [global, {return, list}]);
un_escape_sql(Bin) when is_binary(Bin) ->
    re:replace(Bin, "('')", "'", [global, {return, binary}]).

%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) ->
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun test_without_sec/1
            % , fun test_with_sec/1
        ]}
    }.
    
test_without_sec(_) -> 
    test_with_or_without_sec(false).

% test_with_sec(_) ->
%     test_with_or_without_sec(true).

test_with_or_without_sec(IsSec) ->
    try
        % ClEr = 'ClientError',
        % SyEx = 'SystemException',    %% difficult to test
        % SeEx = 'SecurityException',
        ?Info("----------------------------------~n"),
        ?Info("TEST--- ~p ----Security ~p", [?MODULE, IsSec]),
        ?Info("----------------------------------~n"),

        ?Info("schema ~p~n", [imem_meta:schema()]),
        ?Info("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey = case IsSec of
            true -> ?imem_test_admin_login();
            _ ->    ok
        end,

        ?assertEqual("", escape_sql("")),
        ?assertEqual(<<"">>, escape_sql(<<"">>)),
        ?assertEqual("abc", escape_sql("abc")),
        ?assertEqual(<<"abc">>, escape_sql(<<"abc">>)),
        ?assertEqual("''abc", escape_sql("'abc")),
        ?assertEqual(<<"''abc">>, escape_sql(<<"'abc">>)),
        ?assertEqual("ab''c", escape_sql("ab'c")),
        ?assertEqual(<<"ab''c">>, escape_sql(<<"ab'c">>)),
        ?assertEqual(<<"''ab''''c''">>, escape_sql(<<"'ab''c'">>)),

        ?assertEqual("", un_escape_sql("")),
        ?assertEqual(<<"">>, un_escape_sql(<<"">>)),
        ?assertEqual("abc", un_escape_sql("abc")),
        ?assertEqual(<<"abc">>, un_escape_sql(<<"abc">>)),
        ?assertEqual("'abc", un_escape_sql("'abc")),
        ?assertEqual(<<"'abc">>, un_escape_sql(<<"'abc">>)),
        ?assertEqual("ab'c", un_escape_sql("ab''c")),
        ?assertEqual(<<"ab'c">>, un_escape_sql(<<"ab''c">>)),
        ?assertEqual(<<"'ab'c'">>, un_escape_sql(<<"'ab''c'">>)),

        ?Info("----TEST--~p:test_mnesia~n", [?MODULE]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?Info("success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        ?Info("success ~p~n", [data_nodes]),

        ?Info("----TEST--~p:test_database_operations~n", [?MODULE]),
        _Types1 =    [ #ddColumn{name=a, type=char, len=1}     %% key
                    , #ddColumn{name=b1, type=char, len=1}    %% value 1
                    , #ddColumn{name=c1, type=char, len=1}    %% value 2
                    ],
        _Types2 =    [ #ddColumn{name=a, type=integer, len=10}    %% key
                    , #ddColumn{name=b2, type=float, len=8, prec=3}   %% value
                    ],

        ?assertEqual(ok, exec(SKey, "create table meta_table_1 (a char, b1 char, c1 char);", 0, "imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, meta_table_1])),    

        ?assertEqual(ok, exec(SKey, "create table meta_table_2 (a integer, b2 float);", 0, "imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, meta_table_2])),    

        ?assertEqual(ok, exec(SKey, "create table meta_table_3 (a char, b3 integer, c1 char);", 0, "imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, meta_table_1])),    
        ?Info("success ~p~n", [create_tables]),

        ?assertEqual(ok, imem_meta:drop_table(meta_table_3)),
        ?assertEqual(ok, imem_meta:drop_table(meta_table_2)),
        ?assertEqual(ok, imem_meta:drop_table(meta_table_1)),
        ?Info("success ~p~n", [drop_tables]),

        case IsSec of
            true -> ?imem_logout(SKey);
            _ ->    ok
        end
    catch
        Class:Reason ->  ?Info("Exception~n~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

-endif.
