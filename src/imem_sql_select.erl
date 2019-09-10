-module(imem_sql_select).

-include("imem_meta.hrl").
-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(DefaultRendering, str ).         %% gui (strings when necessary) | str (always strings) | raw (erlang terms) 

-define(GET_DATE_FORMAT(__IsSec),?GET_CONFIG(dateFormat,[__IsSec],eu,"Default date format (eu/us/iso/raw) to return in SQL queries.")).            %% eu | us | iso | raw
-define(GET_NUM_FORMAT(__IsSec),?GET_CONFIG(numberFormat,[__IsSec],{prec,2},"Default number formats and precision (not used yet).")).     %% not used yet
-define(GET_STR_FORMAT(__IsSec),?GET_CONFIG(stringFormat,[__IsSec],[],"Default string format to return in SQL queries (not used yet).")).           %% not used yet

-export([ exec/5
        , flatten_tables/1
        ]).

exec(SKey, {select, SelectSections}=ParseTree, Stmt, Opts, IsSec) ->
    % ToDo: spawn imem_statement here and execute in its own security context (compile & run from same process)
    ?IMEM_SKEY_PUT(SKey), % store internal SKey in statement process, may be needed to authorize join functions
    %Info("Putting SKey ~p to process dict of driver ~p",[SKey,self()]),
    {_, TableList0} = lists:keyfind(from, 1, SelectSections),
    %?Info("TableList0: ~p~n", [TableList0]),
    Params = imem_sql:params_from_opts(Opts,ParseTree),
    %?Info("Params: ~p~n", [Params]),
    TableList = bind_table_names(Params, TableList0),
    %?Info("TableList: ~p~n", [TableList]),
    Class = imem_sql:statement_class(hd(TableList)),
    %?Info("Statement Class: ~p", [Class]),
    ClusterTableNames = lists:sort(imem_sql:cluster_table_names(hd(TableList))),
    %?Info("ClusterTableNames: ~p", [ClusterTableNames]),
    LT = fun({N,_S,_T}) -> (N==node()) end,
    LocalClusterTableNames = lists:filter(LT,ClusterTableNames),
    %?Info("LocalClusterTableNames: ~p", [LocalClusterTableNames]),
    RT = fun({N,_S,_T}) -> (N=/=node()) end,
    RemoteClusterTableNames = lists:filter(RT,ClusterTableNames),
    %?Info("RemoteClusterTableNames: ~p", [RemoteClusterTableNames]),
    MetaFields = lists:usort(imem_sql:prune_fields(imem_meta:meta_field_list(),ParseTree))--[?META_ROWNUM,?META_NODE],
    AllMetaFields = [?META_ROWNUM,?META_NODE|MetaFields],  
    ?Info("AllMetaFields ~p",[AllMetaFields]), 
    FullMap = imem_sql_expr:column_map_tables(TableList,AllMetaFields,Params),
    %?Info("FullMap:~n~p~n", [?FP(FullMap,"23678")]),
    %?Info("FullMap:~n~p", [FullMap]),
    Tables = [imem_meta:qualified_table_name({TS,TN})|| #bind{tind=Ti,cind=Ci,schema=TS,table=TN} <- FullMap,Ti/=?MetaIdx,Ci==?FirstIdx],
    %?Info("Tables: (~p)~n~p", [length(Tables),Tables]),
    ClusterTables = span_tables(Tables, LocalClusterTableNames ++ RemoteClusterTableNames),
    %?Info("ClusterTables: (~p)~n~p", [length(ClusterTables), ClusterTables]),
    {_, ParsedFields} = lists:keyfind(fields, 1, SelectSections),
    ColMap0 = imem_sql_expr:column_map_columns(ParsedFields, FullMap),
    %?Info("ColMap0: (~p)~n~p~n", [length(ColMap0),?FP(ColMap0,"23678(15)")]),
    %?Info("ColMap0: (~p)~n~p~n", [length(ColMap0),ColMap0]),
    RowCols = [#rowCol{tag=Tag,alias=A,type=T,len=L,prec=P,readonly=R} || #bind{tag=Tag,alias=A,type=T,len=L,prec=P,readonly=R} <- ColMap0],
    %?Info("Row columns: ~n~p~n", [RowCols]),
    {_, WPTree} = lists:keyfind(where, 1, SelectSections),
    %?Info("WhereParseTree~n~p", [WPTree]),
    WBTree0 = case WPTree of
        ?EmptyWhere ->  
            true;
        _ ->            
            #bind{btree=WBT} = imem_sql_expr:expr(WPTree, FullMap, #bind{type=boolean,default=true}),
            WBT
    end,
    %?Info("WhereBindTree0~n~p~n", [WBTree0]),
    MainSpec = imem_sql_expr:main_spec(WBTree0,FullMap),
    %?Info("MainSpec:~n~p", [MainSpec]),
    JoinSpecs = imem_sql_expr:join_specs(?TableIdx(length(Tables)), WBTree0, FullMap), %% start with last join table, proceed to first 
    %?Info("JoinSpecs:~n~p~n", [JoinSpecs]),
    ColMap1 = [ if (Ti==0) and (Ci==0) -> CMap#bind{func=imem_sql_funs:expr_fun(BTree)}; true -> CMap end 
                || #bind{tind=Ti,cind=Ci,btree=BTree}=CMap <- ColMap0],
    %?Info("ColMap1: (~p)~n~p~n", [length(ColMap1),ColMap1]),
    RowFun = case ?DefaultRendering of
        raw ->  imem_datatype:select_rowfun_raw(ColMap1);
        str ->  imem_datatype:select_rowfun_str(ColMap1, ?GET_DATE_FORMAT(IsSec), ?GET_NUM_FORMAT(IsSec), ?GET_STR_FORMAT(IsSec))
    end,
    %?Info("RowFun:~n~p~n", [RowFun]),
    SortFun = imem_sql_expr:sort_fun(SelectSections, FullMap, ColMap1),
    SortSpec = imem_sql_expr:sort_spec(SelectSections, FullMap, ColMap1),
    %Info("SortSpec:~p~n", [SortSpec]),
    Statements = [Stmt#statement{
                    stmtParse = {select, SelectSections},
                    stmtParams = Params,
                    stmtClass = Class,
                    metaFields=AllMetaFields, 
                    tables=CTabs,
                    colMap=ColMap1, fullMap=FullMap,
                    rowFun=RowFun, sortFun=SortFun, sortSpec=SortSpec,
                    mainSpec=MainSpec, joinSpecs=JoinSpecs
                    }
                  || CTabs <- ClusterTables 
                 ],
    CreateResult = [imem_statement:create_stmt(S, SKey, IsSec) || S <- Statements],
    case lists:usort([element(1,R1) || R1 <- CreateResult]) of 
        [ok] -> 
            StmtRefs = [element(2,R2) || R2 <- CreateResult],
            {ok, #stmtResults{stmtRefs=StmtRefs,stmtClass=Class,rowCols=RowCols,rowFun=RowFun,sortFun=SortFun,sortSpec=SortSpec}};
        [Error|_] ->
            Pred = fun(Res) -> (element(1,Res) == ok) end,
            RollbackRefs = [element(2,R3) || R3 <- lists:filter(Pred, CreateResult)],
            imem_statement:close(SKey, RollbackRefs),
            ?ClientError({"Exec error",Error})
    end.

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

span_tables(FullTableNames, ClusterTableNames) ->
    span_tables(FullTableNames, ClusterTableNames, []).

span_tables(_, [], Acc) -> lists:reverse(Acc);
span_tables([TN|JoinTableNames], [CT|ClusterTableNames], Acc) ->
    span_tables([TN|JoinTableNames], ClusterTableNames, [[CT|JoinTableNames]|Acc]).

flatten_tables([]) -> [];
flatten_tables([E|_] = L) when is_list(E) ->
    D = lists:max([length(T) || T <- L]),
    flatten_tables(L,D,0,[]).

flatten_tables(_,D,D,Acc) -> lists:reverse(Acc);
flatten_tables(L,D,N,Acc) ->
    flatten_tables(L,D,N+1,[flatten_one(L,D,N+1)|Acc]).

flatten_one(L,D,N) ->
    [flatten_pick(L,D,N,I)|| I <- lists:seq(1,length(L))].

flatten_pick(L,D,N,I) ->
    case lists:nth(I,L) of 
        [T] -> T;
        Tabs ->  
            case length(Tabs) of
                D ->    lists:nth(N,Tabs);
                _ ->    ?ClientError({"Missing Partition(s)",Tabs})
            end
    end.

%% ----- TESTS ------------------------------------------------

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

flatten_tables_test_() ->
    [ {"F_1", ?_assertEqual([[a1,b1,c1]], flatten_tables([[a1], [b1], [c1]]))}
    , {"F_2", ?_assertEqual([[a1,b1,c1], [a1,b1,c2]], flatten_tables([[a1], [b1], [c1,c2]]))}
    , {"F_3", ?_assertEqual([[a1],[a2],[a3]], flatten_tables([[a1,a2,a3]]))}
    , {"F_4", ?_assertEqual([[a1,b1,c1], [a2,b1,c2]], flatten_tables([[a1,a2], [b1], [c1,c2]]))}
    , {"F_5", ?_assertEqual([[a1,b1,c1], [a2,b2,c1]], flatten_tables([[a1,a2], [b1,b2], [c1]]))}
    , {"F_6", ?_assertEqual([[a1,b1,c1], [a2,b2,c2]], flatten_tables([[a1,a2], [b1,b2], [c1,c2]]))}
    ].

-endif.
