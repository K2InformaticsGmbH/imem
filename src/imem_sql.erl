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
        ]).

parse(Sql) ->
    case sqlparse:parsetree(Sql) of
        {ok, [{ParseTree,_}|_]}  ->  ParseTree;
        {lex_error, Error}              -> ?ClientError({"SQL lexer error", Error});
        {parse_error, Error}            -> ?ClientError({"SQL parser error", Error})
    end.

prune_fields(InFields, ParseTree) ->
    sqlparse_fold:top_down(imem_identifiers_match, ParseTree, InFields).

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
