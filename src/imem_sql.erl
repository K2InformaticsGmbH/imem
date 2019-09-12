-module(imem_sql).

-include("imem_sql.hrl").

-define(MaxChar,16#FFFFFF).

-export([ exec/5
        , parse/1
        , prune_fields/2
        ]).

-export([ escape_sql/1
        , un_escape_sql/1
        , meta_rec/5
        , params_from_opts/2
        , statement_class/1
        , parse_sql_name/1
        , physical_table_names/1
        , cluster_table_names/1
        ]).

physical_table_names({as, TableName, _Alias}) -> physical_table_names(TableName);
physical_table_names(TableName) -> imem_meta:physical_table_names(TableName).

cluster_table_names({as, TableName, _Alias}) -> cluster_table_names(TableName);
cluster_table_names(TableName) -> imem_meta:cluster_table_names(TableName).

statement_class({as, TableName, _Alias}) -> statement_class(TableName);
statement_class(TableAlias) ->
    case { imem_meta:is_time_partitioned_alias(TableAlias)
                 , imem_meta:is_node_sharded_alias(TableAlias)
                 , imem_meta:is_local_alias(TableAlias)
                 } of 
        {false,false,false} ->  "R";    % remote query of a simple table/partition on another node
        {false,false,true} ->   "L";    % local query (mapped to "" in GUI)
        {false,true, false} ->  "C";    % cluster sharded query, simple table/partition across all nodes
        {true ,false,false} ->  "RP";   % remote partitioned, time partitions on another node
        {true ,false,true} ->   "P";    % partitioned, time partitions on local node
        {true ,true ,false} ->  "CP"    % cluster partitioned, all partitions on all nodes
    end.

-spec parse_sql_name(ddString()) -> tuple().
parse_sql_name(SqlName) -> 
    case parse_sql_name(SqlName,0,[],[]) of
        [N] ->      {"",N};
        [S,N] ->    {S,N};
        Other ->    ?ClientError({"Bad SQL name", Other})
    end.

parse_sql_name([], _, Temp, Acc) ->
    lists:reverse([lists:reverse(Temp)|Acc]);
parse_sql_name([$.|Rest], L, Temp, Acc) when L rem 2 == 0 ->
    parse_sql_name(Rest, L, [], [lists:reverse(Temp)|Acc]);
parse_sql_name([$"|Rest], L, Temp, Acc) ->
    parse_sql_name(Rest, L+1, [$"|Temp], Acc);
parse_sql_name([Ch|Rest], L, Temp, Acc) ->
    parse_sql_name(Rest, L, [Ch|Temp], Acc).

parse(Sql) ->
    case sqlparse:parsetree(Sql) of
        {ok, [{ParseTree,_}|_]}  ->  ParseTree;
        {lex_error, Error}              -> ?ClientError({"SQL lexer error", Error});
        {parse_error, Error}            -> ?ClientError({"SQL parser error", Error})
    end.

prune_fields(InFields, ParseTree) ->
    imem_prune_fields:match(ParseTree, InFields).

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

% {RetCode, StmtResult} = imem_sql:exec(undefined, "SELECT * FROM ddAccount", 100, [], false).
% #stmtResults{stmtRefs=StmtRefs, rowCols=RowCols, rowFun=RowFun} = StmtResult.
% List = imem_statement:fetch_recs_sort(undefined, StmtResult, {self(), make_ref()}, 1000, false).
% imem_statement:close(SKey, StmtRefs).

% rr(imem_statement).
% CsvFileName = <<"CsvTestFileName123abc.txt">>.
% file:write_file(CsvFileName, <<"Col1\tCol2\r\nA1\t1\r\nA2\t2\r\n">>).
% {RetCode, StmtResult} = imem_sql:exec(undefined, "SELECT * FROM csv$.\"CsvTestFileName123abc.txt\"", 100, [], false).
% #stmtResults{stmtRefs=StmtRefs, rowCols=RowCols, rowFun=RowFun} = StmtResult.
% List = imem_statement:fetch_recs_sort(undefined, StmtResult, {self(), make_ref()}, 1000, false).
% imem_statement:close(SKey, StmtRefs).

% rr(imem_statement).
% Sql3p1 = "select item from dual,atom where is_member(item, mfa('imem_sql_funs','filter_funs','[]')) and item like 'list%'".
% {RetCode, StmtResult} = imem_sql:exec(undefined, Sql3p1, 100, [], false).
% #stmtResults{stmtRefs=StmtRefs, rowCols=RowCols, rowFun=RowFun} = StmtResult.
% List = imem_statement:fetch_recs_sort(undefined, StmtResult, {self(), make_ref()}, 1000, false).
% imem_statement:close(SKey, StmtRefs).


exec(SKey, Sql, BlockSize, Opts, IsSec) ->
    case sqlparse:parsetree(Sql) of
        {ok, [{ParseTree,_}|_]} ->
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
exec(SKey, 'create role', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, 'drop role', ParseTree, Stmt, Opts, IsSec) ->
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

exec(SKey, 'create index', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_index:exec(SKey, ParseTree, Stmt, Opts, IsSec);
exec(SKey, 'drop index', ParseTree, Stmt, Opts, IsSec) ->
    imem_sql_index:exec(SKey, ParseTree, Stmt, Opts, IsSec);

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


%% ----- TESTS ------------------------------------------------

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

statement_class_test_() ->
    [ {"R_1", ?_assertEqual("R", statement_class("ddTest@007"))}
    , {"R_2", ?_assertEqual("R", statement_class(<<"ddTest@007">>))} 
    , {"R_3", ?_assertEqual("R", statement_class(ddTest@007))} 
    , {"R_4", ?_assertEqual("R", statement_class({imem_meta:schema(),ddTest@007}))} 
    , {"R_5", ?_assertEqual("R", statement_class({as,ddTest@007,<<"TEST">>}))}

    , {"L_1", ?_assertEqual("L", statement_class("ddTest"))} 
    , {"L_2", ?_assertEqual("L", statement_class(<<"ddTest@local">>))} 
    , {"L_3", ?_assertEqual("L", statement_class(ddTest))}
    , {"L_4", ?_assertEqual("L", statement_class({imem_meta:schema(),ddTest@_}))} 
    , {"L_5", ?_assertEqual("L", statement_class({as,ddTest,<<"TEST">>}))}
    , {"L_6", ?_assertEqual("L", statement_class({as,ddTest_1234567890@local,<<"TEST">>}))}

    , {"P_1", ?_assertEqual("P", statement_class("ddTest_1234@local"))} 
    , {"P_2", ?_assertEqual("P", statement_class(<<"ddTest_1234@_">>))} 
    , {"P_3", ?_assertEqual("P", statement_class(ddTest_1234@local))}
    , {"P_4", ?_assertEqual("P", statement_class({imem_meta:schema(),ddTest_1234@_}))} 
    , {"P_5", ?_assertEqual("P", statement_class({as,ddTest_1234@local,<<"TEST">>}))}

    , {"C_1", ?_assertEqual("C", statement_class("ddTest@"))} 
    , {"C_2", ?_assertEqual("C", statement_class(<<"ddTest@">>))} 
    , {"C_3", ?_assertEqual("C", statement_class(ddTest@))}
    , {"C_4", ?_assertEqual("C", statement_class({imem_meta:schema(),ddTest@}))}
    , {"C_5", ?_assertEqual("C", statement_class({as,ddTest@,<<"TEST">>}))}
    , {"C_6", ?_assertEqual("C", statement_class(<<"ddTest_1234567890@">>))} 

    , {"PC_1", ?_assertEqual("PC",statement_class("ddTest_1234@"))}
    , {"PC_2", ?_assertEqual("PC",statement_class(<<"ddTest_1234@">>))}
    , {"PC_3", ?_assertEqual("PC",statement_class(ddTest_1234@))}
    , {"PC_4", ?_assertEqual("PC",statement_class({imem_meta:schema(),ddTest_1234@}))}
    , {"PC_5", ?_assertEqual("PC",statement_class({as,ddTest_1234@,<<"TEST">>}))}

    , {"PR_1", ?_assertEqual("PR", statement_class("ddTest_1234@007"))} 
    , {"PR_2", ?_assertEqual("PR", statement_class(<<"ddTest_1234@007">>))} 
    , {"PR_3", ?_assertEqual("PR", statement_class(ddTest_1234@007))}
    , {"PR_4", ?_assertEqual("PR", statement_class({imem_meta:schema(),ddTest_1234@007}))} 
    , {"PR_5", ?_assertEqual("PR", statement_class({as,ddTest_1234@007,<<"TEST">>}))}
    ].

parse_sql_name_test_() ->
    [ {"simple", ?_assertEqual({"","Simple_Name"}, parse_sql_name("Simple_Name"))}
    , {"schema", ?_assertEqual({"Schema","Name"}, parse_sql_name("Schema.Name"))} 
    , {"complex", ?_assertEqual({"","\"Complex.$@#Name\""}, parse_sql_name("\"Complex.$@#Name\""))} 
    , {"mixed", ?_assertEqual({"Schema","\"Complex.$@#Name\""}, parse_sql_name("Schema.\"Complex.$@#Name\""))}
    , {"double", ?_assertEqual({"\"Schema.1\"","\"Complex.$@#Name\""}, parse_sql_name("\"Schema.1\".\"Complex.$@#Name\""))}
    ].

-endif.