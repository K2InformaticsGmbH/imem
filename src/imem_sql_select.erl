-module(imem_sql_select).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(DefaultRendering, str ).         %% gui (strings when necessary) | str (always strings) | raw (erlang terms) 
-define(DefaultDateFormat, eu ).         %% eu | us | iso | raw
-define(DefaultStrFormat, []).           %% escaping not implemented
-define(DefaultNumFormat, [{prec,2}]).   %% prec, no 

-export([ exec/5
        ]).

exec(SKey, {select, SelectSections}, Stmt, _Schema, IsSec) ->
    Tables = case lists:keyfind(from, 1, SelectSections) of
        {_, TNames} ->  Tabs = [imem_sql:table_qname(T) || T <- TNames],
                        [{_,MainTab,_}|_] = Tabs,
                        case lists:member(MainTab,?DataTypes) of
                            true ->     ?ClientError({"Virtual table can only be joined", MainTab});
                            false ->    Tabs
                        end;
        TError ->       ?ClientError({"Invalid from in select structure", TError})
    end,
    % ?Log("Tables: ~p~n", [Tables]),
    ColMaps0 = case lists:keyfind(fields, 1, SelectSections) of
        false -> 
            imem_sql:column_map(Tables,[]);
        {_, FieldList} -> 
            imem_sql:column_map(Tables, FieldList);
        CError ->        
            ?ClientError({"Invalid select structure", CError})
    end,
    ColMaps1 = [Item#ddColMap{tag=list_to_atom([$$|integer_to_list(I)])} || {I,Item} <- lists:zip(lists:seq(1,length(ColMaps0)), ColMaps0)],
    % ?Log("Column map: ~p~n", [ColMaps1]),
    StmtCols = [#stmtCol{tag=Tag,alias=A,type=T,len=L,prec=P,readonly=R} || #ddColMap{tag=Tag,alias=A,type=T,len=L,prec=P,readonly=R} <- ColMaps1],
    % ?Log("Statement rows: ~p~n", [StmtCols]),
    RowFun = case ?DefaultRendering of
        raw ->  imem_datatype:select_rowfun_raw(ColMaps1);
        str ->  imem_datatype:select_rowfun_str(ColMaps1, ?DefaultDateFormat, ?DefaultNumFormat, ?DefaultStrFormat)
    end,
    WhereTree = case lists:keyfind(where, 1, SelectSections) of
        {_, WT} ->  WT;
        WError ->   ?ClientError({"Invalid where structure", WError})
    end,
    % ?Log("WhereTree ~p~n", [WhereTree]),
    MetaTabIdx = length(Tables) + 1,
    MetaFields0 = [ N || {_,N} <- lists:usort([{C#ddColMap.cind, C#ddColMap.name} || C <- ColMaps1, C#ddColMap.tind==MetaTabIdx])],
    MetaFields1= add_where_clause_meta_fields(MetaFields0, WhereTree, if_call_mfa(IsSec,meta_field_list,[SKey])),
    % ?Log("MetaFields: ~p~n", [MetaFields]),
    RawMap = case MetaFields1 of
        [] ->   
            imem_sql:column_map(Tables,[]);
        MF ->   
            % ?Log("MetaFields (~p)~n~p~n", [length(MF),MF]),
            MetaMap0 = [{#ddColMap{name=N,tind=MetaTabIdx,cind=Ci},if_call_mfa(IsSec,meta_field_info,[SKey,N])} || {Ci,N} <- lists:zip(lists:seq(1,length(MF)),MF)],
            MetaMap1 = [CM#ddColMap{type=T,len=L,prec=P} || {CM,#ddColumn{type=T, len=L, prec=P}} <- MetaMap0],
            imem_sql:column_map(Tables,[]) ++ MetaMap1
    end,
    FullMap = [Item#ddColMap{tag=list_to_atom([$$|integer_to_list(T)])} || {T,Item} <- lists:zip(lists:seq(1,length(RawMap)), RawMap)],
    % ?Log("FullMap (~p)~n~p~n", [length(FullMap),FullMap]),
    MainSpec = build_main_spec(SKey,length(Tables),1,WhereTree,FullMap),
    % ?Log("MainSpec  : ~p~n", [MainSpec]),
    JoinSpecs = build_join_specs(SKey,length(Tables),length(Tables), WhereTree, FullMap, []),
    % ?Log("JoinSpecs: ~p~n", [JoinSpecs]),
    SortFun = imem_sql:build_sort_fun(SelectSections,FullMap),
    SortSpec = imem_sql:build_sort_spec(SelectSections,FullMap,ColMaps1),
    Statement = Stmt#statement{
                    stmtParse = {select, SelectSections},
                    tables=Tables, fullMaps=FullMap,
                    colMaps=ColMaps1, metaFields=MetaFields1, 
                    rowFun=RowFun, sortFun=SortFun, sortSpec=SortSpec,
                    mainSpec=MainSpec, joinSpecs=JoinSpecs
                },
    {ok, StmtRef} = imem_statement:create_stmt(Statement, SKey, IsSec),
    {ok, #stmtResult{stmtRef=StmtRef,stmtCols=StmtCols,rowFun=RowFun,sortFun=SortFun,sortSpec=SortSpec}}.

build_main_spec(SKey,Tmax,Ti,WhereTree,FullMap) when (Ti==1) ->
    SGuards= query_guards(SKey,Tmax,Ti,WhereTree,FullMap),
    imem_sql:create_scan_spec(Tmax,Ti,FullMap,SGuards).

build_join_specs(_SKey, _Tmax, 1, _WhereTree, _FullMap, Acc)-> Acc;
build_join_specs(SKey, Tmax, Ti, WhereTree, FullMap, Acc)->
    SGuards = query_guards(SKey,Tmax,Ti,WhereTree,FullMap),
    JoinSpec = imem_sql:create_scan_spec(Tmax,Ti,FullMap,SGuards),
    build_join_specs(SKey,Tmax, Ti-1, WhereTree, FullMap, [JoinSpec|Acc]).

add_where_clause_meta_fields(MetaFields, _WhereTree, []) -> 
    MetaFields;
add_where_clause_meta_fields(MetaFields, WhereTree, [F|FieldList]) ->
    case lists:member(F,MetaFields) of
        true ->         
            add_where_clause_meta_fields(MetaFields, WhereTree, FieldList);
        false ->
            case imem_sql:operand_member(list_to_binary(atom_to_list(F)),WhereTree) of
                false ->
                    add_where_clause_meta_fields(MetaFields, WhereTree, FieldList);
                true ->
                    add_where_clause_meta_fields(MetaFields++[F], WhereTree, FieldList)
            end
    end.

query_guards(_SKey,_Tmax,_Ti,[],_FullMap) -> [];
query_guards(SKey,Tmax,Ti,WhereTree,FullMap) ->
    % ?Log("WhereTree  : ~p~n", [WhereTree]),
    Walked = tree_walk(SKey,Tmax,Ti,WhereTree,FullMap),
    % ?Log("Walked     : ~p~n", [Walked]),
    Simplified = imem_sql:simplify_guard(Walked), 
    % ?Log("Simplified : ~p~n", [Simplified]),
    [Simplified].

tree_walk(_SKey,_,_,<<"true">>,_FullMap) -> true;
tree_walk(_SKey,_,_,<<"false">>,_FullMap) -> false;
tree_walk(SKey,Tmax,Ti,{'not',WC},FullMap) ->
    {'not', tree_walk(SKey,Tmax,Ti,WC,FullMap)};
% tree_walk(_SKey,_Tmax,_Ti,{Op,_WC},_FullMap) -> 
%     ?UnimplementedException({"Operator not supported in where clause",Op});
tree_walk(_SKey,_Tmax,_Ti,{'=',A,A},_FullMap) -> true;
tree_walk(SKey,Tmax,Ti,{'=',A,B},FullMap) ->
    condition(SKey,Tmax,Ti,'==',A,B,FullMap);
tree_walk(SKey,Tmax,Ti,{'<>',A,B},FullMap) ->
    condition(SKey,Tmax,Ti,'/=',A,B,FullMap);
tree_walk(SKey,Tmax,Ti,{'<',A,B},FullMap) ->
    condition(SKey,Tmax,Ti,'<',A,B,FullMap);
tree_walk(SKey,Tmax,Ti,{'<=',A,B},FullMap) ->
    condition(SKey,Tmax,Ti,'=<',A,B,FullMap);
tree_walk(SKey,Tmax,Ti,{'>',A,B},FullMap) ->
    condition(SKey,Tmax,Ti,'>',A,B,FullMap);
tree_walk(SKey,Tmax,Ti,{'>=',A,B},FullMap) ->
    condition(SKey,Tmax,Ti,'>=',A,B,FullMap);
tree_walk(SKey,Tmax,Ti,{'in',A,{list,InList}},FullMap) when is_binary(A), is_list(InList) ->
    in_condition(SKey,Tmax,Ti,A,InList,FullMap);
tree_walk(SKey,Tmax,Ti,{'fun',F,[P1]},FullMap) ->
    % ?Log("Function Arg: ~p~n", [P1]),
    Arg = tree_walk(SKey,Tmax,Ti,P1,FullMap),
    % ?Log("Unary function Arg: ~p~n", [Arg]),
    {F,Arg};                 %% F = unary function like abs | is_list | to_atom
tree_walk(SKey,Tmax,Ti,{'fun',is_member,[P1,P2]},FullMap) ->
    condition(SKey,Tmax,Ti,is_member,P1,P2,FullMap); 
tree_walk(SKey,Tmax,Ti,{'fun',F,[P1,P2]},FullMap) -> 
    {F,tree_walk(SKey,Tmax,Ti,P1,FullMap),tree_walk(SKey,Tmax,Ti,P2,FullMap)};    %% F = binary function like element(E,Tuple) | is_member | is_element
tree_walk(SKey,Tmax,Ti,{Op,WC1,WC2},FullMap) ->
    {Op, tree_walk(SKey,Tmax,Ti,WC1,FullMap), tree_walk(SKey,Tmax,Ti,WC2,FullMap)};
tree_walk(SKey,Tmax,Ti,Expr,FullMap) ->
    case expr_lookup(SKey,Tmax,Ti,Expr,FullMap) of
        {0,V1,integer,_,_,_,_} ->   field_value(0,integer,0,0,?nav,V1);   
        {0,V2,float,_,_,_,_} ->     field_value(0,float,0,0,?nav,V2);   
        {0,V3,string,_,_,_,_} ->    field_value(0,term,0,0,?nav,V3);   
        {_,Tag,_,_,_,_,_} ->        Tag
    end.

condition(SKey,Tmax,Ti,OP,A,B,FullMap) ->
    try 
        ExA = expr_lookup(SKey,Tmax,Ti,A,FullMap),
        ExB = expr_lookup(SKey,Tmax,Ti,B,FullMap),
        compguard(Tmax,Ti,OP,ExA,ExB)
    catch
        throw:{'JoinEvent','join_condition'} -> true;
        _:Reason ->
            ?Log("Failing condition eval Tmax/Ti/OP: ~p ~p ~p~n", [Tmax,Ti,OP]),
            ?Log("Failing condition eval A: ~p~n", [A]),
            ?Log("Failing condition eval B: ~p~n", [B]),
            throw(Reason)
    end.

compguard(Tm,1, _ , {A,_,_,_,_,_,_},   {B,_,_,_,_,_,_}) when A>1,A=<Tm; B>1,B=<Tm -> join;   %% join condition
compguard(_ ,1, is_member, {0,A,string,_,_,_,_},{0,B,_,_,_,_,_}) ->     {is_member,field_value(B,term,0,0,?nav,A),field_value(A,term,0,0,?nav,B)};           
compguard(_ ,1, is_member, {0,A,string,_,_,_,_},{_,B,_,_,_,_,_}) ->     {is_member,field_value(B,term,0,0,?nav,A),B};           
compguard(_ ,1, is_member, {_,A,_,_,_,_,_},     {0,B,_,_,_,_,_}) ->     {is_member,A,field_value(A,term,0,0,?nav,B)};           
compguard(_ ,1, is_member, {_,A,_,_,_,_,_},     {_,B,_,_,_,_,_}) ->     {is_member,A,B};           
compguard(_ ,1, OP, {0,A,string,_,_,_,_},   {0,B,string,_,_,_,_}) ->    {OP,field_value(B,string,0,0,?nav,A),field_value(A,string,0,0,?nav,B)};           
compguard(_ ,1, OP, {0,A,string,_,_,_,_},   {_,B,string,_,_,_,_}) ->    {OP,field_value(B,string,0,0,?nav,A),B};           
compguard(_ ,1, OP, {_,A,string,_,_,_,_},   {0,B,string,_,_,_,_}) ->    {OP,A,field_value(A,string,0,0,?nav,B)};           
compguard(_ ,1, OP, {_,A,T,_,_,_,_},        {_,B,T,_,_,_,_}) ->         {OP,A,B};           
compguard(_ ,1, OP, {1,A,timestamp,_,_,_,_}, {0,B,string,_,_,_,_}) ->   {OP,A,field_value(A,float,0,0,?nav,B)};
compguard(_ ,1, OP, {1,A,datetime,_,_,_,_}, {0,B,string,_,_,_,_}) ->    {OP,A,field_value(A,float,0,0,?nav,B)};
compguard(_ ,1, OP, {0,A,string,_,_,_,_}, {1,B,timestamp,_,_,_,_}) ->   {OP,field_value(B,float,0,0,?nav,A),B};
compguard(_ ,1, OP, {0,A,string,_,_,_,_}, {1,B,datetime,_,_,_,_}) ->    {OP,field_value(B,float,0,0,?nav,A),B};
compguard(_ ,1, OP, {1,A,T,L,P,D,_},      {0,B,string,_,_,_,_}) ->      {OP,A,field_value(A,T,L,P,D,B)};
compguard(_ ,1, OP, {1,A,term,_,_,_,_},     {0,B,_,_,_,_,_}) ->         {OP,A,B};
compguard(_ ,1, OP, {0,A,string,_,_,_,_},   {1,B,T,L,P,D,_}) ->         {OP,field_value(B,T,L,P,D,A),B};
compguard(_ ,1, OP, {0,A,_,_,_,_,_},        {1,B,term,_,_,_,_}) ->      {OP,A,B};
compguard(_ ,1, _,  {_,_,AT,_,_,_,AN}, {_,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types for comparison in where clause", {{AN,AT},{BN,BT}}});
compguard(_ ,1, OP, A, B) ->                                   ?SystemException({"Unexpected guard pattern", {1,OP,A,B}});

compguard(Tm,J, _,  {N,A,_,_,_,_,_},   {J,B,_,_,_,_,_}) when N>J, N=<Tm -> ?UnimplementedException({"Unsupported join order",{A,B}});
compguard(Tm,J, _,  {J,A,_,_,_,_,_},   {N,B,_,_,_,_,_}) when N>J, N=<Tm -> ?UnimplementedException({"Unsupported join order",{A,B}});
compguard(_ ,_, is_member, {0,A,string,_,_,_,_},{0,B,_,_,_,_,_}) ->     {is_member,field_value(B,term,0,0,?nav,A),field_value(A,term,0,0,?nav,B)};           
compguard(_ ,_, is_member, {0,A,string,_,_,_,_},{_,B,_,_,_,_,_}) ->     {is_member,field_value(B,term,0,0,?nav,A),B};           
compguard(_ ,_, is_member, {_,A,_,_,_,_,_},     {0,B,_,_,_,_,_}) ->     {is_member,A,field_value(A,term,0,0,?nav,B)};           
compguard(_ ,_, is_member, {_,A,_,_,_,_,_},     {_,B,_,_,_,_,_}) ->     {is_member,A,B};           
compguard(_ ,_, OP, {0,A,string,_,_,_,_},   {0,B,string,_,_,_,_}) ->    {OP,field_value(B,string,0,0,?nav,A),field_value(A,string,0,0,?nav,B)};           
compguard(_ ,_, OP, {0,A,string,_,_,_,_},   {_,B,string,_,_,_,_}) ->    {OP,field_value(B,string,0,0,?nav,A),B};           
compguard(_ ,_, OP, {_,A,string,_,_,_,_},   {0,B,string,_,_,_,_}) ->    {OP,A,field_value(A,string,0,0,?nav,B)};           
compguard(_ ,_, OP, {_,A,T,_,_,_,_},        {_,B,T,_,_,_,_}) ->         {OP,A,B};           
compguard(_ ,J, OP, {J,A,T,L,P,D,_},        {0,B,string,_,_,_,_})->     {OP,A,field_value(A,T,L,P,D,B)};
compguard(_ ,J, OP, {0,A,string,_,_,_,_},   {J,B,T,L,P,D,_}) ->         {OP,field_value(B,T,L,P,D,A),B};
compguard(_ ,J, _,  {J,_,AT,_,_,_,AN}, {J,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types in where clause", {{AN,AT},{BN,BT}}});
compguard(_ ,J, _,  {J,_,AT,_,_,_,AN}, {_,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types in where clause", {{AN,AT},{BN,BT}}});
compguard(_ ,J, _,  {_,_,AT,_,_,_,AN}, {J,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types in where clause", {{AN,AT},{BN,BT}}});
compguard(_ ,_, _,  {_,_,_,_,_,_,_},   {_,_,_,_,_,_,_}) ->              join.

in_condition(SKey,Tmax,Ti,A,InList,FullMap) ->
    in_condition_loop(SKey,Tmax,Ti,expr_lookup(SKey,Tmax,Ti,A,FullMap),InList,FullMap).

in_condition_loop(_SKey,_Tmax,_Ti,_ALookup,[],_FullMap) -> false;    
in_condition_loop(SKey,Tmax,Ti,ALookup,[B],FullMap) ->
    compguard(Tmax,Ti, '==', ALookup, expr_lookup(SKey,Tmax,Ti,B,FullMap));
in_condition_loop(SKey,Tmax,Ti,ALookup,[B|Rest],FullMap) ->
    {'or',
        compguard(Tmax,Ti, '==', ALookup, expr_lookup(SKey,Tmax,Ti,B,FullMap)),
            in_condition_loop(SKey,Tmax,Ti,ALookup,Rest,FullMap)}.

field_value(Tag,Type,Len,Prec,Def,Val) when is_binary(Val);is_list(Val) ->
    case imem_datatype:io_to_db(Tag,?nav,Type,Len,Prec,Def,false,Val) of
        T when is_tuple(T) ->   {const,T};
        V ->                    V
    end;
field_value(_,_,_,_,_,Val) when is_tuple(Val) -> {const,Val};
field_value(_,_,_,_,_,Val) -> Val.

value_lookup(Val) when is_binary(Val) ->
    Str = imem_datatype:io_to_string(Val),  %% binary_to_list(Val),
    Int = (catch list_to_integer(Str)),
    Float = (catch list_to_float(Str)),
    if 
        is_integer(Int) ->  {Int,integer};
        is_float(Float) ->  {Float,float};
        true ->             {Str,string}     %% promote as strings, convert to atoms/dates/lists/tuples when type is known
    end.

field_lookup(<<"rownum">>,_FullMap) -> {0,rownum,integer,0,0,1,<<"rownum">>};
field_lookup(Name,FullMap) ->
    U = undefined,
    ML = case imem_sql:field_qname(Name) of
        {U,U,N} ->  [C || #ddColMap{name=Nam}=C <- FullMap, Nam==N];
        {U,T1,N} -> [C || #ddColMap{name=Nam,table=Tab}=C <- FullMap, (Nam==N), (Tab==T1)];
        {S,T2,N} -> [C || #ddColMap{name=Nam,table=Tab,schema=Sch}=C <- FullMap, (Nam==N), ((Tab==T2) or (Tab==U)), ((Sch==S) or (Sch==U))];
        {} ->       []
    end,
    case length(ML) of
        0 ->    {Value,Type} = value_lookup(Name),
                {0,Value,Type,U,U,U,Name};
        1 ->    #ddColMap{tag=Tag,type=T,tind=Ti,len=L,prec=P,default=D} = hd(ML),
                {Ti,Tag,T,L,P,D,Name};
        _ ->    ?ClientError({"Ambiguous column name in where clause", Name})
    end.

expr_lookup(_SKey,_Tmax,_Ti,A,FullMap) when is_binary(A)->
    field_lookup(A,FullMap);
expr_lookup(SKey,Tmax,Ti,{'fun',F,[Param]},FullMap) ->  %% F = unary value function like 'abs' 
    % ?Log("expr_lookup {'fun',F,[Param]}: ~p ~p ~p ~p~n", [Tmax,Ti,F,Param]),
    {Ta,A,T,L,P,D,AN} = expr_lookup(SKey,Tmax,Ti,Param,FullMap),
    case {Ta,F,T} of
        {0,to_integer,integer} ->   {Ta,A,integer,L,P,D,AN};
        {0,to_string,integer} ->    {Ta,integer_to_list(A),string,L,P,D,AN};
        {0,to_float,integer} ->     {Ta,float(A),float,L,P,D,AN};
        {0,to_float,float} ->       {Ta,A,float,L,P,D,AN};
        {0,to_integer,float} ->     {Ta,round(A),integer,L,P,D,AN};
        {0,to_string,float} ->      {Ta,float_to_list(A),string,L,P,D,AN};
        {0,to_atom,string} ->       {Ta,field_value(to_atom,atom,0,0,?nav,A),atom,0,0,?nav,AN};
        {0,to_binary,string} ->     {Ta,field_value(to_binary,binary,0,0,?nav,imem_datatype:strip_quotes(A)),binary,0,0,?nav,AN};
        {0,to_binstr,string} ->     {Ta,field_value(to_binstr,binstr,0,0,?nav,A),binstr,0,0,?nav,AN};
        {0,to_boolean,string} ->    {Ta,field_value(to_boolean,boolean,0,0,?nav,imem_datatype:strip_quotes(A)),boolean,0,0,?nav,AN};
        {0,to_string,string} ->     {Ta,A,string,0,0,?nav,AN};
        {0,to_datetime,string} ->   {Ta,field_value(to_datetime,datetime,0,0,?nav,imem_datatype:strip_quotes(A)),datetime,0,0,?nav,AN};
        {0,to_decimal,integer} ->   {Ta,field_value(to_decimal,decimal,0,0,?nav,integer_to_list(A)),decimal,0,0,?nav,AN};
        {0,to_decimal,float} ->     {Ta,field_value(to_decimal,decimal,0,0,?nav,float_to_list(A)),decimal,0,0,?nav,AN};
        {0,to_integer,string} ->    {Ta,field_value(to_integer,integer,0,0,?nav,imem_datatype:strip_quotes(A)),integer,0,0,?nav,AN};
        {0,to_list,string} ->       {Ta,field_value(to_list,list,0,0,?nav,imem_datatype:strip_quotes(A)),list,0,0,?nav,AN};
        {0,to_fun,string} ->        {Ta,field_value(to_fun,'fun',0,0,?nav,A),'fun',0,0,?nav,AN};
        {0,to_ipaddr,string} ->     {Ta,field_value(to_ipaddr,ipaddr,0,0,?nav,imem_datatype:strip_quotes(A)),ipaddr,0,0,?nav,AN};
        {0,to_pid,string} ->        {Ta,field_value(to_pid,pid,0,0,?nav,imem_datatype:strip_quotes(A)),pid,0,0,?nav,AN};
        {0,to_term,string} ->       {Ta,field_value(to_term,term,0,0,?nav,A),term,0,0,?nav,AN};
        {0,to_timestamp,string} ->  {Ta,field_value(to_timestamp,timestamp,0,0,?nav,imem_datatype:strip_quotes(A)),timestamp,0,0,?nav,AN};
        {0,to_tuple,string} ->      {Ta,field_value(to_tuple,tuple,0,0,?nav,imem_datatype:strip_quotes(A)),tuple,0,0,?nav,AN};
        {0,to_userid,string} ->     {Ta,field_value(to_userid,userid,0,0,?nav,A),userid,0,0,?nav,AN};
        {0,to_userid,integer} ->    {Ta,A,userid,0,0,?nav,AN};
        _ ->                        {Ta,{F,A},T,L,P,D,AN}
    end;          
expr_lookup(SKey,Tmax,Ti,{'fun','element'=F,[P1,P2]},FullMap) ->  %% F = binary value function like 'element' 
    {0,A,integer,_,_,_,_} = expr_lookup(SKey,Tmax,Ti,P1,FullMap),
    {Tb,B,_,_,_,_,BN} = expr_lookup(SKey,Tmax,Ti,P2,FullMap),
    {Tb,{F,A,B},term,0,0,0,BN};          
expr_lookup(SKey,Tmax,Ti,{OP,A,B},FullMap) ->
    % ?Log("expr_lookup {OP,A,B}: ~p ~p ~p ~p ~p~n", [Tmax,Ti,OP,A,B]),
    exprguard(Tmax,Ti,OP,expr_lookup(SKey,Tmax,Ti,A,FullMap), expr_lookup(SKey,Tmax,Ti,B,FullMap)).

exprguard(Tm,1, _ , {A,_,_,_,_,_,_},   {B,_,_,_,_,_,_}) when A>1, A=<Tm; B>1,B=<Tm -> throw({'JoinEvent','join_condition'});
exprguard(_ ,1, OP, {X,A,T,L,P,D,AN},  {Y,B,T,_,_,_,_}) when X >= Y ->      {X,{OP,A,B},T,L,P,D,AN};           
exprguard(_ ,1, OP, {_,A,T,_,_,_,_},   {Y,B,T,L,P,D,BN}) ->                 {Y,{OP,A,B},T,L,P,D,BN};           
exprguard(_ ,1, OP, {X,A,timestamp,L,P,D,AN}, {0,B,integer,_,_,_,_}) ->     {X,{OP,A,field_value(A,float,0,0,0.0,B)},timestamp,L,P,D,AN};
exprguard(_ ,1, OP, {X,A,timestamp,L,P,D,AN}, {0,B,float,_,_,_,_}) ->       {X,{OP,A,field_value(A,float,0,0,0.0,B)},timestamp,L,P,D,AN};
exprguard(_ ,1, OP, {X,A,datetime,L,P,D,AN}, {0,B,integer,_,_,_,_}) ->      {X,{OP,A,field_value(A,float,0,0,0.0,B)},datetime,L,P,D,AN};
exprguard(_ ,1, OP, {X,A,datetime,L,P,D,AN}, {0,B,float,_,_,_,_}) ->        {X,{OP,A,field_value(A,float,0,0,0.0,B)},datetime,L,P,D,AN};
exprguard(_ ,1, OP, {1,A,T,L,P,D,AN},  {0,B,string,_,_,_,_}) ->             {1,{OP,A,field_value(A,T,L,P,D,B)},T,L,P,D,AN};
exprguard(_ ,1, OP, {0,A,integer,_,_,_,_}, {1,B,timestamp,L,P,D,BN}) ->     {1,{OP,field_value(B,float,0,0,0.0,A),B},timestamp,L,P,D,BN};
exprguard(_ ,1, OP, {0,A,float,_,_,_,_}, {1,B,timestamp,L,P,D,BN}) ->       {1,{OP,field_value(B,float,0,0,0.0,A),B},timestamp,L,P,D,BN};
exprguard(_ ,1, OP, {0,A,integer,_,_,_,_}, {1,B,datetime,L,P,D,BN}) ->      {1,{OP,field_value(B,float,0,0,0.0,A),B},datetime,L,P,D,BN};
exprguard(_ ,1, OP, {0,A,float,_,_,_,_}, {1,B,datetime,L,P,D,BN}) ->        {1,{OP,field_value(B,float,0,0,0.0,A),B},datetime,L,P,D,BN};
exprguard(_ ,1, OP, {0,A,string,_,_,_,_},   {1,B,T,L,P,D,BN}) ->            {1,{OP,field_value(B,T,L,P,D,A),B},T,L,P,D,BN};
exprguard(_ ,1, _,  {_,_,AT,_,_,_,AN}, {_,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types in where clause", {{AN,AT},{BN,BT}}});
exprguard(_ ,1, OP, A, B) ->                                   ?SystemException({"Unexpected guard pattern", {1,OP,A,B}});
exprguard(Tm,J, _,  {N,A,_,_,_,_,_},   {J,B,_,_,_,_,_}) when N>J,N=<Tm -> ?UnimplementedException({"Unsupported join order",{A,B}});
exprguard(Tm,J, _,  {J,A,_,_,_,_,_},   {N,B,_,_,_,_,_}) when N>J,N=<Tm -> ?UnimplementedException({"Unsupported join order",{A,B}});
exprguard(_ ,_, OP, {X,A,T,L,P,D,AN},  {Y,B,T,_,_,_,_}) when X >= Y ->      {X,{OP,A,B},T,L,P,D,AN};           
exprguard(_ ,_, OP, {_,A,T,_,_,_,_},   {Y,B,T,L,P,D,BN}) ->                 {Y,{OP,A,B},T,L,P,D,BN};           
exprguard(_ ,_, OP, {N,A,T,L,P,D,AN},  {0,B,string,_,_,_,_}) when N > 0 ->  {N,{OP,A,field_value(A,T,L,P,D,B)},T,L,P,D,AN};
exprguard(_ ,_, OP, {0,A,string,_,_,_,_},   {N,B,T,L,P,D,BN}) when N > 0 -> {N,{OP,field_value(B,T,L,P,D,A),B},T,L,P,D,BN};
exprguard(_ ,_, _,  {_,_,AT,_,_,_,AN}, {_,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types in where clause", {{AN,AT},{BN,BT}}});
exprguard(_ ,J, OP, A, B) ->                                   ?SystemException({"Unexpected guard pattern", {J,OP,A,B}}).

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.


%% TESTS ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_SKey) -> 
    catch imem_meta:drop_table(member_test),
    catch imem_meta:drop_table(def),
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun test_without_sec/1
            , fun test_with_sec/1
        ]}
    }.
    
test_without_sec(_) -> 
    test_with_or_without_sec(false).

test_with_sec(_) ->
    test_with_or_without_sec(true).

test_with_or_without_sec(IsSec) ->
    try
        ClEr = 'ClientError',
        SeEx = 'SecurityException',

        ?Log("~n----TEST--- ~p ----Security ~p~n", [?MODULE, IsSec]),

        ?Log("schema ~p~n", [imem_meta:schema()]),
        ?Log("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        ?assertEqual([],imem_statement:receive_raw()),

        ?assertEqual([imem], field_value(tag,list,0,0,[],<<"[imem]">>)),

        % {TMega,TSec,TMicro} = erlang:now(),

        SKey=case IsSec of
            true ->     ?imem_test_admin_login();
            false ->    none
        end,

        if
            IsSec ->    ?assertEqual(<<"admin">>, imem_seco:account_name(SKey));
            true ->     ?assertException(throw,{SeEx,{"Not logged in",none}}, imem_seco:account_name(SKey))
        end,

        case {IsSec,catch(imem_meta:check_table(ddView))} of
            {false,_} ->    ok;
            {true,ok} ->     
                R_a = exec_fetch_sort(SKey, query_a, 100, IsSec, 
                    "select v.name 
                     from ddView as v, ddCmd as c 
                     where c.id = v.cmd 
                     and c.adapters = \"[imem]\" 
                     and (c.owner = user or c.owner = system)"
                ),
                ?assert(length(R_a) > 0);

            {true,_} ->     ok                
        end,
        

    %% test table def

        ?assertEqual(ok, imem_sql:exec(SKey,
            "create table def (
                col1 integer, 
                col2 char(20), 
                col3 date,
                col4 ipaddr,
                col5 tuple
            );", 0, 'Imem', IsSec)),

        ?assertEqual(ok, insert_range(SKey, 10, def, 'Imem', IsSec)),

        {L0, true} = if_call_mfa(IsSec,select,[SKey, def, ?MatchAllRecords, 1000]),
        ?Log("Test table def : ~p entries~n~p~n~p~n~p~n", [length(L0),hd(L0), '...', lists:last(L0)]),
        ?assertEqual(10, length(L0)),

    %% test table member_test

        ?assertEqual(ok, imem_sql:exec(SKey, 
            "create table member_test (col1 integer, col2 list, col3 tuple);"
            , 0, 'Imem', IsSec)),

        if_call_mfa(IsSec, write,[SKey,member_test,
            {member_test,1, [a,b,c,[e]] ,   undefined}
        ]),
        if_call_mfa(IsSec, write,[SKey,member_test,
            {member_test,2, [1,2,3,{e}] ,           9}
        ]),
        if_call_mfa(IsSec, write,[SKey,member_test,
            {member_test,3, [[e],3,4,5] ,           1}
        ]),
        if_call_mfa(IsSec, write,[SKey,member_test,
            {member_test,4, undefined   ,     {a,d,e}}
        ]),
        if_call_mfa(IsSec, write,[SKey,member_test,
            {member_test,5, [d,{e},a]   ,     {a,d,e}}
        ]),

        {L1, true} = if_call_mfa(IsSec,select,[SKey, member_test, ?MatchAllRecords, 1000]),
        ?Log("Test table member_test : ~p entries~n~p~n~p~n~p~n", [length(L1),hd(L1), '...', lists:last(L1)]),
        ?assertEqual(5, length(L1)),

    %% queries on meta table

        {L2, true} =  if_call_mfa(IsSec,select,[SKey, ddTable, ?MatchAllRecords, 1000]),
        % ?Log("Table ddTable : ~p entries~n~p~n~p~n~p~n", [length(L2),hd(L2), '...', lists:last(L2)]),
        AllTableCount = length(L2),

        {L3, true} = if_call_mfa(IsSec,select,[SKey, dba_tables, ?MatchAllKeys]),
        % ?Log("Table dba_tables : ~p entries~n~p~n~p~n~p~n", [length(L3),hd(L3), '...', lists:last(L3)]),
        ?assertEqual(AllTableCount, length(L3)),

        {L4, true} = if_call_mfa(IsSec,select,[SKey, all_tables, ?MatchAllKeys]),
        % ?Log("Table all_tables : ~p entries~n~p~n~p~n~p~n", [length(L4),hd(L4), '...', lists:last(L4)]),
        ?assertEqual(AllTableCount, length(L4)),

        {L5, true} = if_call_mfa(IsSec,select,[SKey, user_tables, ?MatchAllKeys]),
        % ?Log("Table user_tables : ~p entries~n~p~n~p~n~p~n", [length(L5),hd(L5), '...', lists:last(L5)]),   
        case IsSec of
            false ->    ?assertEqual(AllTableCount, length(L5));
            true ->     ?assertEqual(2, length(L5))
        end,

        R0 = exec_fetch_sort(SKey, query0, 100, IsSec, 
            "select * from ddTable"
        ),
        ?assertEqual(AllTableCount, length(R0)),

    %% simple queries on meta fields

        exec_fetch_sort_equal(SKey, query1, 100, IsSec, 
            "select dual.* from dual", 
            [{<<"X">>,<<"'$not_a_value'">>}]
        ),

        exec_fetch_sort_equal(SKey, query1a, 100, IsSec, 
            "select dual.dummy from dual",
            [{<<"X">>}]
        ),

        R1b = exec_fetch_sort(SKey, query1b, 100, IsSec, 
            "select sysdate from dual"
        ),
        ?assertEqual(19, size(element(1,hd(R1b)))),

        R1c = exec_fetch_sort(SKey, query1c, 100, IsSec, 
            "select systimestamp from dual"
        ),
        ?assertEqual(26, size(element(1,hd(R1c)))),

        R1d = exec_fetch_sort(SKey, query1d, 100, IsSec, 
            "select user from dual"
        ),
        case IsSec of
            false ->    ?assertEqual([{<<"unknown">>}], R1d);
            true ->     Acid = imem_datatype:integer_to_io(imem_seco:account_id(SKey)),
                        ?assertEqual([{Acid}], R1d)
        end,

        R1e = exec_fetch_sort(SKey, query1e, 100, IsSec, 
            "select all_tables.* from all_tables where owner = system"
        ),
        ?assert(length(R1e) =< AllTableCount),
        ?assert(length(R1e) >= 5),
        % case IsSec of
        %     false -> ?assertEqual(AllTableCount, length(R1e));
        %     true ->  ?assertEqual(AllTableCount-2, length(R1e))
        % end,

        R1f = exec_fetch_sort(SKey, query1f, 100, IsSec, 
            "select qname as qn from all_tables where owner=user"
        ),
        case IsSec of
            false -> ?assertEqual(0, length(R1f));
            true ->  ?assertEqual(2, length(R1f))
        end,

        R1g = exec_fetch_sort(SKey, query1g, 100, IsSec, 
            "select name, type from ddAccount where id=user and locked <> true"
        ),
        case IsSec of
            false -> ?assertEqual(0, length(R1g));
            true ->  ?assertEqual(1, length(R1g))
        end,

    %% simple queries on single table

        R2 = exec_fetch_sort_equal(SKey, query2, 100, IsSec, 
            "select col1, col2 from def where col1>=5 and col1<=6", 
            [{<<"5">>,<<"5">>},{<<"6">>,<<"6">>}]
        ),

        exec_fetch_sort_equal(SKey, query2a, 100, IsSec, 
            "select col1, col2 from def where col1 in (5,6)", 
            R2
        ),

        exec_fetch_sort_equal(SKey, query2b, 100, IsSec, 
            "select col1, col2 from def where col2 in ('5','6')", 
            R2
        ),

        ?assertException(throw,{ClEr,{"Inconsistent field types for comparison in where clause",{{<<"col2">>,string},{<<"5">>,integer}}}}, 
            exec_fetch_sort(SKey, query2c, 100, IsSec, "select col1, col2 from def where col2 in (5,6)")
        ), 

        exec_fetch_sort_equal(SKey, query2d, 100, IsSec, 
            "select col1, col2 from def where col2 in ('5',col2)", 
            [
                {<<"1">>,<<"1">>},{<<"2">>,<<"2">>},{<<"3">>,<<"3">>},{<<"4">>,<<"4">>},
                {<<"5">>,<<"5">>},{<<"6">>,<<"6">>},{<<"7">>,<<"7">>},{<<"8">>,<<"8">>},
                {<<"9">>,<<"9">>},{<<"10">>,<<"10">>}
            ]
        ),

        R2e = exec_fetch_sort(SKey, query2e, 100, IsSec, 
            "select * from def where col4 < \"10.132.7.3\""
        ),
        ?assertEqual(2, length(R2e)),

        % TestTime = erlang:now(),

        R2f = exec_fetch_sort(SKey, query2f, 100, IsSec, 
            "select name, lastLoginTime 
             from ddAccount 
             where lastLoginTime > sysdate - 1.1574074074074073e-4"   %% 10.0 * ?OneSecond
        ),
        case IsSec of
            false -> ?assertEqual(0, length(R2f));
            true ->  ?assertEqual(1, length(R2f))
        end,

        R2g = exec_fetch(SKey, query2g, 100, IsSec, 
            "select logTime, logLevel, module, function, fields, message 
             from " ++ atom_to_list(?LOG_TABLE) ++ "  
             where logTime > systimestamp - 1.1574074074074073e-5 
             and rownum <= 100"   %% 1.0 * ?OneSecond
        ),
        ?assert(length(R2g) >= 1),
        ?assert(length(R2g) =< 100),

        if_call_mfa(IsSec, write,[SKey,def,
            {def,100,"\"text_in_quotes\"",{{2001,02,03},{4,5,6}},{10,132,7,92},{'Atom100',100}}
        ]),

        exec_fetch_sort_equal(SKey, query2h, 100, IsSec, 
            "select col2 from def where col1 = 100",
            [{<<"\"text_in_quotes\"">>}]
        ),

        exec_fetch_sort_equal(SKey, query2i, 100, IsSec, 
            "select col1, col5 from def where element(1,col5) = to_atom(Atom5)",
            [{<<"5">>,<<"{'Atom5',5}">>}]
        ),

        exec_fetch_sort_equal(SKey, query2j, 100, IsSec, 
            "select col1, col5 from def where element(1,col5) = to_atom('Atom5')",
            [{<<"5">>,<<"{'Atom5',5}">>}]
        ),

        exec_fetch_sort_equal(SKey, query2k, 100, IsSec, 
            "select col1, col5 from def where element(1,col5) = to_atom(\"Atom5\")",
            [{<<"5">>,<<"{'Atom5',5}">>}]
        ),

        exec_fetch_sort_equal(SKey, query2l, 100, IsSec, 
            "select col1, col5 from def where element(2,col5) = 5",
            [{<<"5">>,<<"{'Atom5',5}">>}]
        ),

        exec_fetch_sort_equal(SKey, query2m, 100, IsSec, 
            "select col1, col5 from def where element(2,col5) = to_integer(4+1)",
            [{<<"5">>,<<"{'Atom5',5}">>}]
        ),

        exec_fetch_sort_equal(SKey, query2n, 100, IsSec, 
            "select col1, col5 from def where element(2,col5) = to_integer(5.0)",
            [{<<"5">>,<<"{'Atom5',5}">>}]
        ),

        exec_fetch_sort_equal(SKey, query2o, 100, IsSec, 
            "select col1, col5 from def where element(2,col5) = to_integer(\"5\")",
            [{<<"5">>,<<"{'Atom5',5}">>}]
        ),

        exec_fetch_sort_equal(SKey, query2p, 100, IsSec, 
            "select col1, col5 from def where col5 = to_tuple(\"{'Atom5', 5}\")",
            [{<<"5">>,<<"{'Atom5',5}">>}]
        ),

        exec_fetch_sort_equal(SKey, query2q, 100, IsSec, 
            "select col1, col5 from def where col5 = \"{'Atom100',100}\"",
            [{<<"100">>,<<"{'Atom100',100}">>}]
        ),

    %% joins with virtual (datatype) tables

        ?assertException(throw,{ClEr,{"Virtual table can only be joined",integer}}, 
            exec_fetch_sort(SKey, query3, 100, IsSec, "select item from integer")
        ),

        exec_fetch_equal(SKey, query3a, 100, IsSec, 
            "select ip.item from def, integer as ip where col1 = 1 and is_member(item,col4)", 
            [{<<"10">>},{<<"132">>},{<<"7">>},{<<"1">>}]
        ),

        R3b = exec_fetch_sort(SKey, query3b, 100, IsSec, 
            "select col3, item from def, integer where is_member(item,\"'$_'\") and col1 <> 100"
        ),
        ?assertEqual(10, length(R3b)),

        R3c = exec_fetch_sort(SKey, query3c, 100, IsSec, 
            "select * from ddNode"
        ),
        ?assertEqual(1, length(R3c)),

        R3d = exec_fetch_sort(SKey, query3d, 100, IsSec, 
            "select time, wall_clock from ddNode"
        ),
        ?assertEqual(1, length(R3d)),

        R3e = exec_fetch_sort(SKey, query3e, 100, IsSec, 
            "select time, wall_clock from ddNode where name = '" ++ atom_to_list(node()) ++ "'"
        ),
        ?assertEqual(1, length(R3e)),

        ?assertEqual(ok , imem_meta:monitor()),

        R3f = exec_fetch_sort(SKey, query3f, 100, IsSec, 
            "select * from " ++ atom_to_list(?MONITOR_TABLE) ++ " m, ddNode n where rownum < 2 and m.node = n.name"
        ),
        ?assertEqual(1, length(R3f)),

        % exec_fetch_sort_equal(SKey, query3g, 100, IsSec, 
        %     "select col1, col5 from def, ddNode where element(2,col5) = node",
        %     []
        % ),

        if_call_mfa(IsSec, write,[SKey,def,
            {def,0,integer_to_list(0),calendar:local_time(),{10,132,7,0},{list_to_atom("Atom" ++ integer_to_list(0)),node()}}
        ]),

        % exec_fetch_sort_equal(SKey, query3h, 100, IsSec, 
        %     "select col1, col5 from def, ddNode where element(2,col5) = node",
        %     [{<<"0">>,<<"{'Atom0',nonode@nohost}">>}]
        % ),

        %% self joins 

        exec_fetch_sort_equal(SKey, query4, 100, IsSec, 
            "select t1.col1, t2.col1 
             from def t1, def t2 
             where t1.col1 in (5,6,7)
             and t2.col1 > t1.col1 
             and t2.col1 > t1.col1 
             and t2.col1 <> 9
             and t2.col1 <> 100",
            [
                {<<"5">>,<<"6">>},{<<"5">>,<<"7">>},{<<"5">>,<<"8">>},{<<"5">>,<<"10">>},
                {<<"6">>,<<"7">>},{<<"6">>,<<"8">>},{<<"6">>,<<"10">>},
                {<<"7">>,<<"8">>},{<<"7">>,<<"10">>}
            ]
        ),

        exec_fetch_sort_equal(SKey, query4a, 100, IsSec, 
            "select t1.col1, t2.col1
             from def t1, def t2 
             where t1.col1 in (5,6,7) 
             and t2.col1 > t1.col1 
             and t2.col1 <= t1.col1 + 2",
            [
                {<<"5">>,<<"6">>},{<<"5">>,<<"7">>},
                {<<"6">>,<<"7">>},{<<"6">>,<<"8">>},
                {<<"7">>,<<"8">>},{<<"7">>,<<"9">>}
            ]
        ),

        exec_fetch_sort_equal(SKey, query4b, 100, IsSec, 
            "select t1.col1, t2.col1 
             from def t1, def t2 
             where t1.col1 in (5,7) 
             and abs(t2.col1-t1.col1) = 1", 
            [
                {<<"5">>,<<"4">>},{<<"5">>,<<"6">>},
                {<<"7">>,<<"6">>},{<<"7">>,<<"8">>}
            ]
        ),

        exec_fetch_sort_equal(SKey, query4c, 100, IsSec, 
            "select t1.col1, t2.col1 
             from def t1, def t2 
             where t1.col1=5 
             and t2.col1 > t1.col1 / 2 
             and t2.col1 <= t1.col1", 
            [
                {<<"5">>,<<"3">>},{<<"5">>,<<"4">>},{<<"5">>,<<"5">>}
            ]
        ),

        exec_fetch_sort_equal(SKey, query4d, 100, IsSec, 
            "select t1.col1, t2.col2 
             from def t1, def t2 
             where t1.col1 <> 5 
             and t1.col1 <= 10
             and t1.col1 <> 0
             and not (t2.col2 = '7') 
             and t2.col1 = t1.col1", 
            [
                {<<"1">>,<<"1">>},{<<"2">>,<<"2">>},{<<"3">>,<<"3">>},{<<"4">>,<<"4">>},
                {<<"6">>,<<"6">>},{<<"8">>,<<"8">>},{<<"9">>,<<"9">>},{<<"10">>,<<"10">>}
            ]
        ),

    %% is_xxx function conditions

        exec_fetch_sort_equal(SKey, query5, 100, IsSec, 
            "select col1 from member_test 
             where is_list(col2) 
             or is_tuple(col3)",  
            [{<<"1">>},{<<"2">>},{<<"3">>},{<<"4">>},{<<"5">>}]
        ),

        exec_fetch_sort_equal(SKey, query5a, 100, IsSec, 
            "select col1 
             from member_test 
             where is_member(3,col2) 
             and col1 > 0",
            [{<<"2">>},{<<"3">>}]
        ),

        exec_fetch_sort_equal(SKey, query5b, 100, IsSec, 
            "select col1 from member_test where is_member(a,col2)",
            [{<<"1">>},{<<"5">>}]
        ),

        exec_fetch_sort_equal(SKey, query5c, 100, IsSec, 
            "select col1 from member_test where is_member(\"{e}\",col2)",
            [{<<"2">>},{<<"5">>}]
        ),

        exec_fetch_sort_equal(SKey, query5d, 100, IsSec, 
            "select col1 from member_test where is_member(\"[e]\",col2)",
            [{<<"1">>},{<<"3">>}]
        ),

        exec_fetch_sort_equal(SKey, query5e, 100, IsSec, 
            "select col1 from member_test where is_member(1,\"'$_'\")",
            [{<<"1">>},{<<"3">>}]
        ),

        exec_fetch_sort_equal(SKey, query5f, 100, IsSec, 
            "select col1 from member_test where is_member(3,\"[1,2,3,4]\")",
            [{<<"1">>},{<<"2">>},{<<"3">>},{<<"4">>},{<<"5">>}]
        ),


        exec_fetch_sort_equal(SKey, query5g, 100, IsSec, 
            "select col1 from member_test where is_member(undefined,\"'$_'\")",
            [{<<"1">>},{<<"4">>}]
        ),

        exec_fetch_sort_equal(SKey, query5h, 100, IsSec, 
            "select d.col1, m.col1 
             from def as d, member_test as m 
             where is_member(d.col1,m.col2)",
            [
                {<<"1">>,<<"2">>},
                {<<"2">>,<<"2">>},
                {<<"3">>,<<"2">>},{<<"3">>,<<"3">>},
                {<<"4">>,<<"3">>},
                {<<"5">>,<<"3">>}
            ]
        ),

        exec_fetch_sort_equal(SKey, query5i, 100, IsSec, 
            "select d.col1, m.col1 from def as d, member_test as m where d.col1 <> 0 and is_member(d.col1+1,m.col2)",
            [
                {<<"1">>,<<"2">>},
                {<<"2">>,<<"2">>},{<<"2">>,<<"3">>},
                {<<"3">>,<<"3">>},
                {<<"4">>,<<"3">>}
            ]
        ),

        % exec_fetch_sort_equal(SKey, query5j, 100, IsSec, 
        %     "select d.col1, m.col1 from def d, member_test m where is_member(d.col1,m.\"'$_'\")",
        %     [
        %         {<<"1">>,<<"1">>},{<<"1">>,<<"3">>},
        %         {<<"2">>,<<"2">>},
        %         {<<"3">>,<<"3">>},
        %         {<<"4">>,<<"4">>},
        %         {<<"5">>,<<"5">>},
        %         {<<"9">>,<<"2">>}
        %     ]
        % ),

        R5k = exec_fetch_sort(SKey, query5k, 100, IsSec, 
            "select name(qname) 
             from ddTable
             where is_member(\"{virtual,true}\",opts)"
        ),
        % ?assert(length(R5k) >= 18),
        ?assert(length(R5k) == 0),      % not used any more for DataTypes
        % ?assert(lists:member({"Imem.atom"},R5k)),
        % ?assert(lists:member({"Imem.userid"},R5k)),
        ?assertNot(lists:member({"Imem.ddTable"},R5k)),
        ?assertNot(lists:member({"Imem.ddTable"},R5k)),

        R5l = exec_fetch_sort(SKey, query5l, 100, IsSec, 
            "select name(qname) 
             from ddTable
             where not is_member(\"{virtual,true}\",opts)"
        ),
        ?assert(length(R5l) >= 5),
        ?assertNot(lists:member({<<"Imem.atom">>},R5l)),
        ?assertNot(lists:member({<<"Imem.userid">>},R5l)),
        ?assert(lists:member({<<"Imem.ddTable">>},R5l)),
        ?assert(lists:member({<<"Imem.ddAccount">>},R5l)),

        R5m = exec_fetch_sort(SKey, query5m, 100, IsSec, 
            "select 
                name(qname),  
                item2(item) as field,  
                item3(item) as type,   
                item4(item) as len,   
                item5(item) as prec,   
                item6(item) as def
             from ddTable, list
             where is_member(item,columns)   
             "
        ),
        ?assert(length(R5m) >= 5),

    %% sorting

        exec_fetch_sort_equal(SKey, query6a, 100, IsSec, 
            "select col1, col2 
             from def
             where col1 <> 100 
             and col1 <> 0 
             order by col1 desc, col2"
            , 
            [
                {<<"10">>,<<"10">>}
                ,{<<"9">>,<<"9">>}
                ,{<<"8">>,<<"8">>}
                ,{<<"7">>,<<"7">>}
                ,{<<"6">>,<<"6">>}
                ,{<<"5">>,<<"5">>}
                ,{<<"4">>,<<"4">>}
                ,{<<"3">>,<<"3">>}
                ,{<<"2">>,<<"2">>}
                ,{<<"1">>,<<"1">>}
            ]
        ),

        ?assertEqual(ok, imem_sql:exec(SKey, "drop table member_test;", 0, 'Imem', IsSec)),

        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, 'Imem', IsSec)),

        case IsSec of
            true ->     ?imem_logout(SKey);
            false ->    ok
        end

    catch
        Class:Reason ->  ?Log("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

insert_range(_SKey, 0, _Table, _Schema, _IsSec) -> ok;
insert_range(SKey, N, Table, Schema, IsSec) when is_integer(N), N > 0 ->
    if_call_mfa(IsSec, write,[SKey,Table,
        {Table,N,integer_to_list(N),calendar:local_time(),{10,132,7,N},{list_to_atom("Atom" ++ integer_to_list(N)),N}}
    ]),
    insert_range(SKey, N-1, Table, Schema, IsSec).

exec_fetch_equal(SKey,Id, BS, IsSec, Sql, Expected) ->
    ?Log("~n", []),
    ?Log("~p : ~s~n", [Id,Sql]),
    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, 'Imem', IsSec),
    ?assertEqual(ok, RetCode),
    #stmtResult{stmtRef=StmtRef,stmtCols=StmtCols,rowFun=RowFun} = StmtResult,
    List = imem_statement:fetch_recs(SKey, StmtRef, {self(), make_ref()}, 1000, IsSec),
    ?assertEqual(ok, imem_statement:close(SKey, StmtRef)),
    [?assert(is_binary(SC#stmtCol.alias)) || SC <- StmtCols],
    RT = imem_statement:result_tuples(List,RowFun),
    ?Log("Result: ~p~n", [RT]),
    ?assertEqual(Expected, RT),
    RT.

exec_fetch_sort_equal(SKey,Id, BS, IsSec, Sql, Expected) ->
    ?Log("~n", []),
    ?Log("~p : ~s~n", [Id,Sql]),
    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, 'Imem', IsSec),
    ?assertEqual(ok, RetCode),
    #stmtResult{stmtRef=StmtRef,stmtCols=StmtCols,rowFun=RowFun} = StmtResult,
    List = imem_statement:fetch_recs_sort(SKey, StmtResult, {self(), make_ref()}, 1000, IsSec),
    ?assertEqual(ok, imem_statement:close(SKey, StmtRef)),
    [?assert(is_binary(SC#stmtCol.alias)) || SC <- StmtCols],
    RT = imem_statement:result_tuples(List,RowFun),
    ?Log("Result  : ~p~n", [RT]),
    ?assertEqual(Expected, RT),
    RT.

exec_fetch_sort(SKey,Id, BS, IsSec, Sql) ->
    ?Log("~n", []),
    ?Log("~p : ~s~n", [Id,Sql]),
    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, 'Imem', IsSec),
    ?assertEqual(ok, RetCode),
    #stmtResult{stmtRef=StmtRef,stmtCols=StmtCols,rowFun=RowFun} = StmtResult,
    List = imem_statement:fetch_recs_sort(SKey, StmtResult, {self(), make_ref()}, 1000, IsSec),
    ?assertEqual(ok, imem_statement:close(SKey, StmtRef)),
    [?assert(is_binary(SC#stmtCol.alias)) || SC <- StmtCols],
    RT = imem_statement:result_tuples(List,RowFun),
    if 
        length(RT) =< 3 ->
            ?Log("Result  : ~p~n", [RT]);
        true ->
            ?Log("Result  :  ~p items~n~p~n~p~n~p~n", [length(RT),hd(RT), '...', lists:last(RT)])
    end,            
    RT.

exec_fetch(SKey,Id, BS, IsSec, Sql) ->
    ?Log("~n", []),
    ?Log("~p : ~s~n", [Id,Sql]),
    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, 'Imem', IsSec),
    ?assertEqual(ok, RetCode),
    #stmtResult{stmtRef=StmtRef,stmtCols=StmtCols,rowFun=RowFun} = StmtResult,
    List = imem_statement:fetch_recs(SKey, StmtRef, {self(), make_ref()}, 1000, IsSec),
    ?assertEqual(ok, imem_statement:close(SKey, StmtRef)),
    [?assert(is_binary(SC#stmtCol.alias)) || SC <- StmtCols],
    RT = imem_statement:result_tuples(List,RowFun),
    if 
        length(RT) =< 10 ->
            ?Log("Result  : ~p~n", [RT]);
        true ->
            ?Log("Result  : ~p items~n~p~n~p~n~p~n", [length(RT),hd(RT), '...', lists:last(RT)])
    end,            
    RT.
