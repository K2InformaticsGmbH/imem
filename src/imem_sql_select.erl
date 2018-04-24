-module(imem_sql_select).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(DefaultRendering, str ).         %% gui (strings when necessary) | str (always strings) | raw (erlang terms) 

-define(GET_DATE_FORMAT(__IsSec),?GET_CONFIG(dateFormat,[__IsSec],eu,"Default date format (eu/us/iso/raw) to return in SQL queries.")).            %% eu | us | iso | raw
-define(GET_NUM_FORMAT(__IsSec),?GET_CONFIG(numberFormat,[__IsSec],{prec,2},"Default number formats and precision (not used yet).")).     %% not used yet
-define(GET_STR_FORMAT(__IsSec),?GET_CONFIG(stringFormat,[__IsSec],[],"Default string format to return in SQL queries (not used yet).")).           %% not used yet

-export([ exec/5
        ]).

exec(SKey, {select, SelectSections}=ParseTree, Stmt, Opts, IsSec) ->
    % ToDo: spawn imem_statement here and execute in its own security context (compile & run from same process)
    ?IMEM_SKEY_PUT(SKey), % store internal SKey in statement process, may be needed to authorize join functions
    % ?LogDebug("Putting SKey ~p to process dict of driver ~p",[SKey,self()]),
    {_, TableList0} = lists:keyfind(from, 1, SelectSections),
    % ?Info("TableList0: ~p~n", [TableList0]),
    Params = imem_sql:params_from_opts(Opts,ParseTree),
    % ?Info("Params: ~p~n", [Params]),
    TableList = bind_table_names(Params, TableList0),
    % ?Info("TableList: ~p~n", [TableList]),
    MetaFields = imem_sql:prune_fields(imem_meta:meta_field_list(),ParseTree),       
    FullMap = imem_sql_expr:column_map_tables(TableList,MetaFields,Params),
    % ?LogDebug("FullMap:~n~p~n", [?FP(FullMap,"23678")]),
    Tables = [imem_meta:qualified_table_name({TS,TN})|| #bind{tind=Ti,cind=Ci,schema=TS,table=TN} <- FullMap,Ti/=?MetaIdx,Ci==?FirstIdx],
    % ?LogDebug("Tables: (~p)~n~p~n", [length(Tables),Tables]),
    ColMap0 = case lists:keyfind(fields, 1, SelectSections) of
        false -> 
            imem_sql_expr:column_map_columns([],FullMap);
        {_, ParsedFieldList} -> 
            imem_sql_expr:column_map_columns(ParsedFieldList, FullMap)
    end,
    % ?Info("ColMap0: (~p)~n~p~n", [length(ColMap0),?FP(ColMap0,"23678(15)")]),
    % ?LogDebug("ColMap0: (~p)~n~p~n", [length(ColMap0),ColMap0]),
    StmtCols = [#stmtCol{tag=Tag,alias=A,type=T,len=L,prec=P,readonly=R} || #bind{tag=Tag,alias=A,type=T,len=L,prec=P,readonly=R} <- ColMap0],
    % ?LogDebug("Statement columns: ~n~p~n", [StmtCols]),
    {_, WPTree} = lists:keyfind(where, 1, SelectSections),
    % ?LogDebug("WhereParseTree~n~p", [WPTree]),
    WBTree0 = case WPTree of
        ?EmptyWhere ->  
            true;
        _ ->            
            #bind{btree=WBT} = imem_sql_expr:expr(WPTree, FullMap, #bind{type=boolean,default=true}),
            WBT
    end,
    % ?LogDebug("WhereBindTree0~n~p~n", [WBTree0]),
    MainSpec = imem_sql_expr:main_spec(WBTree0,FullMap),
    % ?LogDebug("MainSpec:~n~p", [MainSpec]),
    JoinSpecs = imem_sql_expr:join_specs(?TableIdx(length(Tables)), WBTree0, FullMap), %% start with last join table, proceed to first 
    % ?Info("JoinSpecs:~n~p~n", [JoinSpecs]),
    ColMap1 = [ if (Ti==0) and (Ci==0) -> CMap#bind{func=imem_sql_funs:expr_fun(BTree)}; true -> CMap end 
                || #bind{tind=Ti,cind=Ci,btree=BTree}=CMap <- ColMap0],
    % ?Info("ColMap1:~n~p", [ColMap1]),
    RowFun = case ?DefaultRendering of
        raw ->  imem_datatype:select_rowfun_raw(ColMap1);
        str ->  imem_datatype:select_rowfun_str(ColMap1, ?GET_DATE_FORMAT(IsSec), ?GET_NUM_FORMAT(IsSec), ?GET_STR_FORMAT(IsSec))
    end,
    % ?Info("RowFun:~n~p~n", [RowFun]),
    SortFun = imem_sql_expr:sort_fun(SelectSections, FullMap, ColMap1),
    SortSpec = imem_sql_expr:sort_spec(SelectSections, FullMap, ColMap1),
    % ?LogDebug("SortSpec:~p~n", [SortSpec]),
    Statement = Stmt#statement{
                    stmtParse = {select, SelectSections},
                    stmtParams = Params,
                    metaFields=MetaFields, tables=Tables,
                    colMap=ColMap1, fullMap=FullMap,
                    rowFun=RowFun, sortFun=SortFun, sortSpec=SortSpec,
                    mainSpec=MainSpec, joinSpecs=JoinSpecs
                },
    {ok, StmtRef} = imem_statement:create_stmt(Statement, SKey, IsSec),
    {ok, #stmtResult{stmtRef=StmtRef,stmtCols=StmtCols,rowFun=RowFun,sortFun=SortFun,sortSpec=SortSpec}}.


bind_table_names([], TableList) -> TableList;
bind_table_names(Params, TableList) -> bind_table_names(Params, TableList, []).

bind_table_names(_Params, [], Acc) -> lists:reverse(Acc);
bind_table_names(Params, [{param,ParamKey}|Rest], Acc) ->
    case lists:keyfind(ParamKey, 1, Params) of
        {_,binstr,_,[T]} -> bind_table_names(Params, Rest, [T|Acc]);
        {_,atom,_,[A]} ->   bind_table_names(Params, Rest, [atom_to_binary(A,utf8)|Acc]);
        _ ->                ?ClientError({"Cannot resolve table name parameter",ParamKey})
    end;
bind_table_names(Params, [Table|Rest], Acc) -> bind_table_names(Params, Rest, [Table|Acc]). 
