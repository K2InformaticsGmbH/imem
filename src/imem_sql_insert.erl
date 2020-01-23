-module(imem_sql_insert).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-export([ exec/5
        ]).

% Functions applied with Common Test
-export([ if_call_mfa/3
        ]).


exec(SKey, {insert, TableName, {}, {}, _Returning}=ParseTree , Stmt, _Opts, IsSec) ->
    % ?LogDebug("insert default record into ~p~n", [TableName]),
    % ?LogDebug("parse tree~n~p~n", [ParseTree]),
    Params = Stmt#statement.stmtParams,
    % ?LogDebug("Params: ~p~n", [Params]),
    MetaFields = imem_sql:prune_fields(imem_meta:meta_field_list(),ParseTree),       
    FullMap0 = imem_sql_expr:column_map_tables([TableName],MetaFields,Params),
    % ?LogDebug("FullMap0:~n~p~n", [?FP(FullMap0,"23678")]),
    [Tbin] = [{TS,TN} || #bind{tind=Ti,cind=Ci,schema=TS,table=TN} <- FullMap0,Ti/=?MetaIdx,Ci==?FirstIdx],
    Table = imem_meta:qualified_table_name(Tbin),
    % ?LogDebug("Table: ~p~n", [Table]),
    DefRec = list_to_tuple([?nav|[?nav || #bind{tind=Ti} <- FullMap0,Ti==?MainIdx]]),
    [if_call_mfa(IsSec,insert,[SKey, Table, DefRec])];
exec(SKey, {insert, TableName, {_, Columns}, {_, Values}, _Returning}=ParseTree , Stmt, _Opts, IsSec) ->
    % ?LogDebug("insert ~p values ~p into ~p~n", [Columns, Values, TableName]),
    % ?LogDebug("parse tree~n~p~n", [ParseTree]),
    Params = Stmt#statement.stmtParams,
    % ?LogDebug("Params: ~p~n", [Params]),
    MetaFields = imem_sql:prune_fields(imem_meta:meta_field_list(),ParseTree),       
    FullMap0 = imem_sql_expr:column_map_tables([TableName],MetaFields,Params),
    % ?LogDebug("FullMap0:~n~p~n", [?FP(FullMap0,"23678")]),
    [Tbin] = [{TS,TN} || #bind{tind=Ti,cind=Ci,schema=TS,table=TN} <- FullMap0,Ti/=?MetaIdx,Ci==?FirstIdx],
    Table = imem_meta:qualified_table_name(Tbin),
    % ?LogDebug("Table: ~p~n", [Table]),
    DefRec = list_to_tuple([?nav|[?nav || #bind{tind=Ti} <- FullMap0,Ti==?MainIdx]]),
    ColMap0 = imem_sql_expr:column_map_columns(Columns, FullMap0),
    % ?LogDebug("ColMap0:~n~p~n", [?FP(ColMap0,"23678")]),
    CCount = length(ColMap0), 
    VCount = length(Values),
    if 
        VCount==CCount -> ok;
        VCount>CCount ->  ?ClientError({"Too many values", Values});
        VCount<CCount ->  ?ClientError({"Too few values", Values})
    end,
    ColBTrees0 = [{imem_sql_expr:expr(V, FullMap0, CMap), CMap} || {V,CMap} <- lists:zip(Values,ColMap0)],
    % ?LogDebug("ColBTrees0:~n~p~n", [ColBTrees0]),
    % TODO: Rest could be repeated for array bound Params (in transaction)
    ParamRec = Stmt#statement.stmtParamRec,
    % ?LogDebug("Params: ~p~n", [Params]),
    MR = imem_sql:meta_rec(IsSec,SKey,MetaFields,ParamRec),
    % ?LogDebug("Meta Rec: ~p~n", [MR]),
    ColBTrees1 = [{imem_sql_expr:bind_tree(T,{MR}),CMap} || {T,CMap} <- ColBTrees0],
    % ?LogDebug("ColBTrees1:~n~p~n", [ColBTrees1]),
    ColBTrees2 = [{ case imem_sql_funs:expr_fun(T) of
                        F when is_function(F) -> F({MR});
                        V -> V 
                    end, CMap} || {T,CMap} <- ColBTrees1],
    % ?LogDebug("ColBTrees2:~n~p~n", [ColBTrees2]),
    NewRec0 = merge_values(ColBTrees2, DefRec),
    [if_call_mfa(IsSec,insert,[SKey, Table, NewRec0])].

merge_values([], Rec) -> Rec;
merge_values([{V,#bind{cind=Ci,type=Type,len=Len,prec=Prec,default=Def}}|Values], Rec) ->
    case imem_datatype:type_check(V,Type,Len,Prec,Def) of
        ok ->       
            merge_values(Values, erlang:setelement(Ci, Rec, V));
        {error,Reason} ->    
            ?ClientError(Reason)
    end.

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.
