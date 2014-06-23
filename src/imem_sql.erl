-module(imem_sql).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(GET_ROWNUM_LIMIT,?GET_IMEM_CONFIG(rownumDefaultLimit,[],10000)).
-define(MaxChar,16#FFFFFF).

-export([ exec/5
        , parse/1
        , prune_fields/2
        ]).

-export([ escape_sql/1
        , un_escape_sql/1
        , meta_rec/5
        , params_from_opts/2
        ]).

parse(Sql) ->
    case sqlparse:parsetree(Sql) of
        {ok, {[{ParseTree,_}|_], _Tokens}}  ->  ParseTree;
        {lex_error, Error}              -> ?ClientError({"SQL lexer error", Error});
        {parse_error, Error}            -> ?ClientError({"SQL parser error", Error})
    end.

prune_fields(InFields, ParseTree) ->
    Pred = fun(P,{In,Out}) -> 
        case lists:member(P,In) of
            true -> {In,[P|Out]};       %% TODO: exclude alias names from match
            _ ->    {In,Out}
        end
    end,
    {InFields,OutFields} = sqlparse:foldtd(Pred,{InFields,[]},ParseTree),
    lists:usort(OutFields).

params_from_opts(Opts,ParseTree) when is_list(Opts) ->
    case lists:keyfind(params, 1, Opts) of
        false ->    
            [];
        {_, Params} ->
            SortedTriples = lists:sort(Params),
            Names = [element(1,T) || T <- SortedTriples],   
            case imem_sql:prune_fields(Names,ParseTree) of
                Names ->    SortedTriples;
                Less ->     ?ClientError({"Unused statement parameter(s)",{Names -- Less}})
            end
    end.

exec(SKey, Sql, BlockSize, Opts, IsSec) ->
    case sqlparse:parsetree(Sql) of
        {ok, {[{ParseTree,_}|_], _Tokens}} -> 
            exec(SKey, element(1,ParseTree), ParseTree, 
                #statement{stmtStr=Sql, stmtParse=ParseTree, blockSize=BlockSize}, 
                Opts, IsSec);
        {lex_error, Error}      -> ?ClientError({"SQL lexer error", Error});
        {parse_error, Error}    -> ?ClientError({"SQL parser error", Error})
    end.

exec(SKey, select, ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, union, ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, 'union all', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, minus, ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, intersect, ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_select:exec(SKey, ParseTree, Stmt, Opts, IsSec);

exec(SKey, insert, ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_insert:exec(SKey, ParseTree, Stmt, Opts, IsSec);

exec(SKey, 'create user', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, 'alter user', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, 'drop user', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, 'grant', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, 'revoke', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Opts, IsSec);
    
exec(SKey, 'create table', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, 'drop table', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, 'truncate table', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Opts, IsSec);

exec(SKey, Command, ParseTree, _Stmt, _Opts, _IsSec) ->
    ?UnimplementedException({"SQL command unimplemented", {SKey, Command, ParseTree}}).

escape_sql(Str) when is_list(Str) ->
    re:replace(Str, "(')", "''", [global, {return, list}]);
escape_sql(Bin) when is_binary(Bin) ->
    re:replace(Bin, "(')", "''", [global, {return, binary}]).

un_escape_sql(Str) when is_list(Str) ->
    re:replace(Str, "('')", "'", [global, {return, list}]);
un_escape_sql(Bin) when is_binary(Bin) ->
    re:replace(Bin, "('')", "'", [global, {return, binary}]).

meta_rec(_,_,[],[],_) -> ?EmptyMR;
% meta_rec(IsSec,SKey,MetaFields,Params,?EmptyMR) ->
%     meta_rec(IsSec,SKey,MetaFields,Params,undefined);
meta_rec(IsSec,SKey,MetaFields,[],undefined) ->
    list_to_tuple([if_call_mfa(IsSec, meta_field_value, [SKey, N]) || N <- MetaFields]);
meta_rec(_,_,[],Params,undefined) ->
    list_to_tuple([imem_datatype:io_to_db(N,undefined,T,0,P,undefined,false,Value) || {N,T,P,[Value|_]} <- Params]);
meta_rec(IsSec,SKey,MetaFields,Params,undefined) ->
    MetaRec = [if_call_mfa(IsSec, meta_field_value, [SKey, N]) || N <- MetaFields],
    ParamRec = [imem_datatype:io_to_db(N,undefined,T,0,P,undefined,false,Value) || {N,T,P,[Value|_]} <- Params],  
    list_to_tuple(MetaRec ++ ParamRec);
meta_rec(IsSec,SKey,MetaFields,[],_MR) ->
    list_to_tuple([if_call_mfa(IsSec, meta_field_value, [SKey, N]) || N <- MetaFields]);
meta_rec(_,_,[],_Params,MR) ->
    MR; 
meta_rec(IsSec,SKey,MetaFields,Params,MR) ->
    MetaRec = [if_call_mfa(IsSec, meta_field_value, [SKey, N]) || N <- MetaFields],
    list_to_tuple(MetaRec ++ lists:sublist(tuple_to_list(MR),length(MetaRec)+1,length(Params))).

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

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

        ?Info("---TEST---~p:test_mnesia~n", [?MODULE]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?Info("success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        ?Info("success ~p~n", [data_nodes]),

        ?Info("~p:test_database_operations~n", [?MODULE]),
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

-endif.
