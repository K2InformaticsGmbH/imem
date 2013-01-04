-module(imem_sql_select).

-include("imem_seco.hrl").

-define(DefaultRendering, gui ).         %% gui (strings when necessary) | str (strings) | raw (erlang terms)
-define(DefaultDateFormat, eu ).         %% eu | us | iso | raw
-define(DefaultStrFormat, []).           %% escaping not implemented
-define(DefaultNumFormat, [{prec,2}]).   %% precision, no 

-export([ exec/5
        ]).

exec(SKey, {select, SelectSections}, Stmt, _Schema, IsSec) ->
    Tables = case lists:keyfind(from, 1, SelectSections) of
        {_, TNames} ->  [imem_sql:table_qname(T) || T <- TNames];
        TError ->       ?ClientError({"Invalid select structure", TError})
    end,
    ColMap = case lists:keyfind(fields, 1, SelectSections) of
        false -> 
            imem_sql:column_map(Tables,[]);
        {_, FieldList} -> 
            imem_sql:column_map(Tables, FieldList);
        CError ->        
            ?ClientError({"Invalid select structure", CError})
    end,
    % io:format(user, "ColMap (~p)~n~p~n", [length(ColMap),ColMap]),
    RowFun = case ?DefaultRendering of
        raw ->  imem_datatype:select_rowfun_raw(ColMap);
        str ->  imem_datatype:select_rowfun_str(ColMap, ?DefaultDateFormat, ?DefaultNumFormat, ?DefaultStrFormat);
        gui ->  imem_datatype:select_rowfun_gui(ColMap, ?DefaultDateFormat, ?DefaultNumFormat, ?DefaultStrFormat)
    end,
    WhereTree = case lists:keyfind(where, 1, SelectSections) of
        {_, WT} ->  % io:format(user, "WhereTree ~p~n", [WT]),
                    WT;
        WError ->   ?ClientError({"Invalid where structure", WError})
    end,
    MetaTabIdx = length(Tables) + 1,
    MetaFields0 = [ N || {_,N} <- lists:usort([{C#ddColMap.cind, C#ddColMap.name} || C <- ColMap, C#ddColMap.tind==MetaTabIdx])],
    MetaFields1= add_where_clause_meta_fields(MetaFields0, WhereTree, if_call_mfa(IsSec,meta_field_list,[SKey])),
    RawMap = case MetaFields1 of
        [] ->   
            imem_sql:column_map(Tables,[]);
        MF ->   
            % io:format(user, "MetaFields (~p)~n~p~n", [length(MF),MF]),
            MetaMap0 = [{#ddColMap{name=N,tind=MetaTabIdx,cind=Ci},if_call_mfa(IsSec,meta_field_info,[SKey,N])} || {Ci,N} <- lists:zip(lists:seq(1,length(MF)),MF)],
            MetaMap1 = [CM#ddColMap{type=T,length=L,precision=P} || {CM,#ddColumn{type=T, length=L, precision=P}} <- MetaMap0],
            imem_sql:column_map(Tables,[]) ++ MetaMap1
    end,
    FullMap = [Item#ddColMap{tag=list_to_atom([$$|integer_to_list(T)])} || {T,Item} <- lists:zip(lists:seq(1,length(RawMap)), RawMap)],
    % io:format(user, "FullMap (~p)~n~p~n", [length(FullMap),FullMap]),
    MatchHead = list_to_tuple(['_'|[Tag || #ddColMap{tag=Tag, tind=Ti} <- FullMap, Ti==1]]),
    % io:format(user, "MatchHead (~p) ~p~n", [1,MatchHead]),
    Guards = master_query_guards(SKey,length(Tables),WhereTree,FullMap),
    % io:format(user, "Guards (~p) ~p~n", [1,Guards]),
    Result = '$_',
    MatchSpec = [{MatchHead, Guards, [Result]}],
    Binds = binds([{Tag,Ti,Ci} || #ddColMap{tag=Tag, tind=Ti, cind=Ci} <- FullMap, (Ti==MetaTabIdx)], Guards,[]),
    JoinSpec = build_join_spec(SKey,length(Tables),length(Tables), WhereTree, FullMap, []),
    % io:format(user, "Join Spec ~p~n", [JoinSpec]),
    Statement = Stmt#statement{
                    tables=Tables, cols=ColMap, meta=MetaFields1, rowfun=RowFun,
                    matchspec={MatchSpec,Binds}, joinspec=JoinSpec
                },
    {ok, StmtRef} = imem_statement:create_stmt(Statement, SKey, IsSec),
    % io:format(user,"Statement : ~p~n", [Stmt]),
    % io:format(user,"Tables: ~p~n", [Tables]),
    % io:format(user,"Column map: ~p~n", [ColMap]),
    % io:format(user,"Meta map: ~p~n", [MetaFields]),
    % io:format(user,"MatchSpec: ~p~n", [MatchSpec]),
    % io:format(user,"JoinSpecs: ~p~n", [JoinSpec]),
    {ok, ColMap, RowFun, StmtRef}.

build_join_spec(_SKey, _Tmax, 1, _WhereTree, _FullMap, Acc)-> Acc;
build_join_spec(SKey, Tmax, Tind, WhereTree, FullMap, Acc)->
    MatchHead = list_to_tuple(['_'|[Tag || #ddColMap{tag=Tag, tind=Ti} <- FullMap, Ti==Tind]]),
    % io:format(user, "Join MatchHead (~p) ~p~n", [Tind,MatchHead]),
    Guards = join_query_guards(SKey,Tmax,Tind,WhereTree,FullMap),
    % io:format(user, "Join Guards (~p) ~p~n", [Tind,Guards]),
    Result = '$_',
    MatchSpec = [{MatchHead, Guards, [Result]}],
    Binds = binds([{Tag,Ti,Ci} || #ddColMap{tag=Tag, tind=Ti, cind=Ci} <- FullMap, (Ti<Tind) or (Ti==Tmax+1)], Guards,[]),
    build_join_spec(SKey,Tmax, Tind-1, WhereTree, FullMap, [{MatchSpec,Binds}|Acc]).

join_query_guards(SKey,Tmax,Tind,WhereTree,FullMap) ->
    [imem_sql:simplify_matchspec(tree_walk(SKey,Tmax,Tind,WhereTree,FullMap))].

binds(_, [], []) -> [];
binds(_, [true], []) -> [];
binds([], _Guards, Acc) -> Acc;
binds([{Tx,Ti,Ci}|Rest], [Guard], Acc) ->
    case tree_member(Tx,Guard) of
        true ->     binds(Rest,[Guard],[{Tx,Ti,Ci}|Acc]);
        false ->    binds(Rest,[Guard],Acc)
    end.

tree_member(Tx,{_,R}) -> tree_member(Tx,R);
tree_member(Tx,{_,Tx,_}) -> true;
tree_member(Tx,{_,_,Tx}) -> true;
tree_member(Tx,{_,L,R}) -> tree_member(Tx,L) orelse tree_member(Tx,R);
tree_member(Tx,Tx) -> true;
tree_member(_,_) -> false.

add_where_clause_meta_fields(MetaFields, _WhereTree, []) -> MetaFields;
add_where_clause_meta_fields(MetaFields, WhereTree, [F|FieldList]) ->
    case lists:member(F,MetaFields) of
        true ->         
            add_where_clause_meta_fields(MetaFields, WhereTree, FieldList);
        false ->
            case tree_member(list_to_binary(atom_to_list(F)),WhereTree) of
                false ->
                    add_where_clause_meta_fields(MetaFields, WhereTree, FieldList);
                true ->
                    add_where_clause_meta_fields(MetaFields++[F], WhereTree, FieldList)
            end
    end.

master_query_guards(_SKey,_Tmax,[],_FullMap) -> [];
master_query_guards(SKey,Tmax,WhereTree,FullMap) ->
    [imem_sql:simplify_matchspec(tree_walk(SKey,Tmax,1,WhereTree,FullMap))].

tree_walk(_SKey,_,_,<<"true">>,_FullMap) -> true;
tree_walk(_SKey,_,_,<<"false">>,_FullMap) -> false;
tree_walk(SKey,Tmax,Ti,{'not',WC},FullMap) ->
    {'not', tree_walk(SKey,Tmax,Ti,WC,FullMap)};
tree_walk(_SKey,_Tmax,_Ti,{Op,_WC},_FullMap) -> 
    ?UnimplementedException({"Operator not supported in where clause",Op});
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
tree_walk(SKey,Tmax,Ti,{'fun',F,[Param]},FullMap) -> 
    {F,tree_walk(SKey,Tmax,Ti,Param,FullMap)};    %% F = unary boolean function like 'is_list' 
tree_walk(SKey,Tmax,Ti,{Op,WC1,WC2},FullMap) ->
    {Op, tree_walk(SKey,Tmax,Ti,WC1,FullMap), tree_walk(SKey,Tmax,Ti,WC2,FullMap)}.

% condition(SKey,Tmax,1,OP,A,B,FullMap) -> 
%     compguard(Tmax,1,OP,expr_lookup(SKey,Tmax,1,A,FullMap),expr_lookup(SKey,Tmax,1,B,FullMap));
condition(SKey,Tmax,Ti,OP,A,B,FullMap) ->
    try 
        ExA = expr_lookup(SKey,Tmax,Ti,A,FullMap),
        ExB = expr_lookup(SKey,Tmax,Ti,B,FullMap),
        compguard(Tmax,Ti,OP,ExA,ExB)
    catch
        throw:{'JoinEvent','join_condition'} -> true;
        _:Reason ->
            io:format(user, "Failing condition eval Tmax/Ti/OP: ~p ~p ~p~n", [Tmax,Ti,OP]),
            io:format(user, "Failing condition eval A: ~p~n", [A]),
            io:format(user, "Failing condition eval B: ~p~n", [B]),
            throw(Reason)
    end.

compguard(Tm,1, _ , {A,_,_,_,_,_,_},   {B,_,_,_,_,_,_}) when A>1,A=<Tm; B>1,B=<Tm -> true;   %% join condition
compguard(_ ,1, OP, {_,A,T,_,_,_,_},   {_,B,T,_,_,_,_}) ->     {OP,A,B};           
compguard(_ ,1, OP, {1,A,datetime,_,_,_,_}, {0,B,string,_,_,_,_}) -> {OP,A,field_value(A,float,0,0,0.0,B)};
compguard(_ ,1, OP, {0,A,string,_,_,_,_}, {1,B,datetime,_,_,_,_}) -> {OP,field_value(B,float,0,0,0.0,A),B};
compguard(_ ,1, OP, {1,A,T,L,P,D,_},   {0,B,string,_,_,_,_}) -> {OP,A,field_value(A,T,L,P,D,B)};
compguard(_ ,1, OP, {0,A,string,_,_,_,_},   {1,B,T,L,P,D,_}) -> {OP,field_value(B,T,L,P,D,A),B};
compguard(_ ,1, _,  {_,_,AT,_,_,_,AN}, {_,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types for comparison in where clause", {{AN,AT},{BN,BT}}});
compguard(_ ,1, OP, A, B) ->                                   ?SystemException({"Unexpected guard pattern", {1,OP,A,B}});

compguard(Tm,J, _,  {N,A,_,_,_,_,_},   {J,B,_,_,_,_,_}) when N>J, N=<Tm -> ?UnimplementedException({"Unsupported join order",{A,B}});
compguard(Tm,J, _,  {J,A,_,_,_,_,_},   {N,B,_,_,_,_,_}) when N>J, N=<Tm -> ?UnimplementedException({"Unsupported join order",{A,B}});
compguard(_ ,_, OP, {0,A,T,_,_,_,_},   {0,B,T,_,_,_,_}) ->     {OP,A,B};           
compguard(_ ,J, OP, {J,A,T,_,_,_,_},   {J,B,T,_,_,_,_}) ->     {OP,A,B};
compguard(_ ,J, OP, {J,A,T,_,_,_,_},   {_,B,T,_,_,_,_}) ->     {OP,A,B};
compguard(_ ,J, OP, {_,A,T,_,_,_,_},   {J,B,T,_,_,_,_}) ->     {OP,A,B};
compguard(_ ,J, OP, {J,A,T,_,_,_,_},   {0,B,T,_,_,_,_}) ->     {OP,A,B};
compguard(_ ,J, OP, {J,A,T,L,P,D,_},   {0,B,string,_,_,_,_})-> {OP,A,field_value(A,T,L,P,D,B)};
compguard(_ ,J, OP, {0,A,T,_,_,_,_},   {J,B,T,_,_,_,_}) ->     {OP,A,B};
compguard(_ ,J, OP, {0,A,string,_,_,_,_}, {J,B,T,L,P,D,_}) ->  {OP,field_value(B,T,L,P,D,A),B};
compguard(_ ,J, _,  {J,_,AT,_,_,_,AN}, {J,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types in where clause", {{AN,AT},{BN,BT}}});
compguard(_ ,J, _,  {J,_,AT,_,_,_,AN}, {_,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types in where clause", {{AN,AT},{BN,BT}}});
compguard(_ ,J, _,  {_,_,AT,_,_,_,AN}, {J,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types in where clause", {{AN,AT},{BN,BT}}});
compguard(_ ,_, _,  {_,_,_,_,_,_,_},   {_,_,_,_,_,_,_}) ->     true.

in_condition(SKey,Tmax,Ti,A,InList,FullMap) ->
    in_condition_loop(SKey,Tmax,Ti,expr_lookup(SKey,Tmax,Ti,A,FullMap),InList,FullMap).

in_condition_loop(_SKey,_Tmax,_Ti,_ALookup,[],_FullMap) -> false;    
in_condition_loop(SKey,Tmax,Ti,ALookup,[B],FullMap) ->
    compguard(Tmax,Ti, '==', ALookup, expr_lookup(SKey,Tmax,Ti,B,FullMap));
in_condition_loop(SKey,Tmax,Ti,ALookup,[B|Rest],FullMap) ->
    {'or',
        compguard(Tmax,Ti, '==', ALookup, expr_lookup(SKey,Tmax,Ti,B,FullMap)),
            in_condition_loop(SKey,Tmax,Ti,ALookup,Rest,FullMap)}.

field_value(Tag,Type,Len,Prec,Def,Val) ->
    imem_datatype:value_to_db(Tag,?nav,Type,Len,Prec,Def,false,Val).

value_lookup(Val) when is_binary(Val) ->
    Str = binary_to_list(Val),
    Int = (catch list_to_integer(Str)),
    Float = (catch list_to_float(Str)),
    if 
        is_integer(Int) ->
            {Int,integer};
        is_float(Float) ->
            {Float,float};
        true ->
            Unquoted = imem_datatype:strip_quotes(Str),
            {Unquoted,string}  %% assume strings, convert to atoms/dates/lists/tuples when type is known
    end.

field_lookup(Name,FullMap) ->
    U = undefined,
    ML = case imem_sql:field_qname(Name) of
        {U,U,N} ->  [C || #ddColMap{name=Nam}=C <- FullMap, Nam==N];
        {U,T1,N} -> [C || #ddColMap{name=Nam,table=Tab}=C <- FullMap, (Nam==N), (Tab==T1)];
        {S,T2,N} -> [C || #ddColMap{name=Nam,table=Tab,schema=Sch}=C <- FullMap, (Nam==N), ((Tab==T2) or (Tab==U)), ((Sch==S) or (Sch==U))]
    end,
    case length(ML) of
        0 ->    {Value,Type} = value_lookup(Name),
                {0,Value,Type,U,U,U,Name};
        1 ->    #ddColMap{tag=Tag,type=T,tind=Ti,length=L,precision=P,default=D} = hd(ML),
                {Ti,Tag,T,L,P,D,Name};
        _ ->    ?ClientError({"Ambiguous column name in where clause", Name})
    end.

expr_lookup(_SKey,_Tmax,_Ti,A,FullMap) when is_binary(A)->
    field_lookup(A,FullMap);
expr_lookup(SKey,Tmax,Ti,{'fun',F,[Param]},FullMap) ->  %% F = unary value function like 'abs' 
    % io:format(user, "Fun condition eval Tmax/Ti/Fun: ~p ~p ~p~n", [Tmax,Ti,F]),
    {Ti,A,T,L,P,D,AN} = expr_lookup(SKey,Tmax,Ti,Param,FullMap),
    % io:format(user, "Fun parameter: ~p~n", [{Ti,A,T,L,P,D,AN}]),    
    {Ti,{F,A},T,L,P,D,AN};          
expr_lookup(SKey,Tmax,Ti,{OP,A,B},FullMap) ->
    exprguard(Tmax,Ti,OP,expr_lookup(SKey,Tmax,Ti,A,FullMap), expr_lookup(SKey,Tmax,Ti,B,FullMap)).

exprguard(Tm,1, _ , {A,_,_,_,_,_,_},   {B,_,_,_,_,_,_}) when A>1, A=<Tm; B>1,B=<Tm -> throw({'JoinEvent','join_condition'});
exprguard(_ ,1, OP, {X,A,T,L,P,D,AN},  {Y,B,T,_,_,_,_}) when X >= Y -> {X,{OP,A,B},T,L,P,D,AN};           
exprguard(_ ,1, OP, {_,A,T,_,_,_,_},   {Y,B,T,L,P,D,BN}) ->            {Y,{OP,A,B},T,L,P,D,BN};           
exprguard(_ ,1, OP, {X,A,datetime,L,P,D,AN}, {0,B,integer,_,_,_,_}) -> {X,{OP,A,field_value(A,float,0,0,0.0,B)},datetime,L,P,D,AN};
exprguard(_ ,1, OP, {X,A,datetime,L,P,D,AN}, {0,B,float,_,_,_,_}) ->   {X,{OP,A,field_value(A,float,0,0,0.0,B)},datetime,L,P,D,AN};
exprguard(_ ,1, OP, {1,A,T,L,P,D,AN},  {0,B,string,_,_,_,_}) ->        {1,{OP,A,field_value(A,T,L,P,D,B)},T,L,P,D,AN};
exprguard(_ ,1, OP, {0,A,integer,_,_,_,_}, {1,B,datetime,L,P,D,BN}) -> {1,{OP,field_value(B,float,0,0,0.0,A),B},datetime,L,P,D,BN};
exprguard(_ ,1, OP, {0,A,float,_,_,_,_}, {1,B,datetime,L,P,D,BN}) ->   {1,{OP,field_value(B,float,0,0,0.0,A),B},datetime,L,P,D,BN};
exprguard(_ ,1, OP, {0,A,string,_,_,_,_},   {1,B,T,L,P,D,BN}) ->       {1,{OP,field_value(B,T,L,P,D,A),B},T,L,P,D,BN};
exprguard(_ ,1, _,  {_,_,AT,_,_,_,AN}, {_,_,BT,_,_,_,BN}) ->   ?ClientError({"Inconsistent field types in where clause", {{AN,AT},{BN,BT}}});
exprguard(_ ,1, OP, A, B) ->                                   ?SystemException({"Unexpected guard pattern", {1,OP,A,B}});
exprguard(Tm,J, _,  {N,A,_,_,_,_,_},   {J,B,_,_,_,_,_}) when N>J,N=<Tm -> ?UnimplementedException({"Unsupported join order",{A,B}});
exprguard(Tm,J, _,  {J,A,_,_,_,_,_},   {N,B,_,_,_,_,_}) when N>J,N=<Tm -> ?UnimplementedException({"Unsupported join order",{A,B}});
exprguard(_ ,_, OP, {X,A,T,L,P,D,AN},  {Y,B,T,_,_,_,_}) when X >= Y -> {X,{OP,A,B},T,L,P,D,AN};           
exprguard(_ ,_, OP, {_,A,T,_,_,_,_},   {Y,B,T,L,P,D,BN}) ->            {Y,{OP,A,B},T,L,P,D,BN};           
exprguard(_ ,_, OP, {N,A,T,L,P,D,AN},  {0,B,string,_,_,_,_}) when N > 0 -> {N,{OP,A,field_value(A,T,L,P,D,B)},T,L,P,D,AN};
exprguard(_ ,_, OP, {0,A,string,_,_,_,_},   {N,B,T,L,P,D,BN}) when N > 0 ->{N,{OP,field_value(B,T,L,P,D,A),B},T,L,P,D,BN};
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
        % SeEx = 'SecurityException',
        Timeout = 2000,

        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        io:format(user, "schema ~p~n", [imem_meta:schema()]),
        io:format(user, "data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=case IsSec of
            true ->     ?imem_test_admin_login();
            false ->    none
        end,

        ?assertEqual(ok, imem_sql:exec(SKey, "
                create table def (
                    col1 integer, 
                    col2 char(2), 
                    col3 date default fun() -> calendar:local_time() end.
                );", 0, 'Imem', IsSec)),

        ?assertEqual(ok, insert_range(SKey, 10, "def", 'Imem', IsSec)),

        Result0 = if_call_mfa(IsSec,select,[SKey, ddTable, ?MatchAllRecords, 1000]),
        {List0, true} = Result0,
        % io:format(user, "ddTable MatchAllRecords (~p)~n~p~n...~n~p~n", [length(List0),hd(List0),lists:last(List0)]),
        AllTableCount = length(List0),

        Result1 = if_call_mfa(IsSec,select,[SKey, all_tables, ?MatchAllKeys]),
        {List1, true} = Result1,
        % io:format(user, "all_tables MatchAllKeys (~p)~n~p~n", [length(List1),List1]),
        ?assertEqual(AllTableCount, length(List1)),

        Result2 = if_call_mfa(IsSec,select,[SKey, def, ?MatchAllRecords, 1000]),
        {_List2, true} = Result2,
        % io:format(user, "def MatchAllRecords (~p)~n~p~n...~n~p~n", [length(_List2),hd(List2),lists:last(_List2)]),

        ?assertEqual([imem], field_value(tag,list,0,0,[],"[imem]")),

        Sql6 = "select col1, col2 from def where col1>=5 and col1<=6",
        io:format(user, "Query: ~p~n", [Sql6]),
        {ok, _Clm6, RowFun6, StmtRef6} = imem_sql:exec(SKey, Sql6, 100, 'Imem', IsSec),
        List6 = imem_statement:fetch_recs_sort(SKey, StmtRef6, self(), Timeout, IsSec),
        io:format(user, "Result: (~p)~n~p~n", [length(List6),lists:map(RowFun6,List6)]),
        ?assertEqual(2, length(List6)),

        Sql7 = "select col1, col2 from def where col1 in (5,6)",
        io:format(user, "Query: ~p~n", [Sql7]),
        {ok, _Clm7, _RowFun7, StmtRef7} = imem_sql:exec(SKey, Sql7, 100, 'Imem', IsSec),
        List7 = if_call_mfa(IsSec,fetch_recs_sort,[SKey, StmtRef7, self(), Timeout]),
        % io:format(user, "Result: (~p)~n~p~n", [length(List7),lists:map(_RowFun7,List7)]),
        ?assertEqual(List6, List7),

        Sql8 = "select col1, col2 from def where col2 in (5,6)",
        io:format(user, "Query: ~p~n", [Sql8]),
        ?assertException(throw,{ClEr,{"Inconsistent field types for comparison in where clause",{{<<"col2">>,string},{<<"5">>,integer}}}}, imem_sql:exec(SKey, Sql8, 100, 'Imem', IsSec)),
 
        Sql9 = "select col1, col2 from def where col2 in (\"5\",\"6\")",
        io:format(user, "Query: ~p~n", [Sql9]),
        {ok, _Clm9, _RowFun9, StmtRef9} = imem_sql:exec(SKey, Sql9, 100, 'Imem', IsSec),
        List9 = imem_statement:fetch_recs_sort(SKey, StmtRef9, self(), Timeout, IsSec),
        % io:format(user, "Result: (~p)~n~p~n", [length(List9),lists:map(_RowFun9,List9)]),
        ?assertEqual(List6, List9),

        List9a = imem_statement:fetch_recs_sort(SKey, StmtRef9, self(), Timeout, IsSec),
        % io:format(user, "Result: (~p)~n~p~n", [length(List9),lists:map(RowFun8,List9)]),
        ?assertEqual(List6, List9a),

        Sql10 = "select col1, col2 from def where col2 in ('5',col2)",
        io:format(user, "Query: ~p~n", [Sql10]),
        {ok, _Clm10, _RowFun10, StmtRef10} = imem_sql:exec(SKey, Sql10, 100, 'Imem', IsSec),
        List10 = imem_statement:fetch_recs_sort(SKey, StmtRef10, self(), Timeout, IsSec),
        % io:format(user, "Result: (~p)~n~p~n", [length(List10),lists:map(_RowFun10,List10)]),
        ?assertEqual(10, length(List10)),

        Sql3 = "select name(qname) from Imem.ddTable",
        io:format(user, "Query: ~p~n", [Sql3]),
        {ok, _Clm3, _RowFun3, StmtRef3} = imem_sql:exec(SKey, Sql3, 100, 'Imem', IsSec),  %% all_tables
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef3, self(), IsSec)),
        Result3 = receive 
            R3 ->    R3
        end,
        {StmtRef3, {List3, true}} = Result3,
        % io:format(user, "Result: (~p)~n~p~n", [length(List3),[tl(R)|| R <- lists:map(_RowFun3,List3)]]),
        ?assertEqual(AllTableCount, length(List3)),

        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef3, self(), IsSec)),
        Result3a = receive 
            R3a ->    R3a
        end,
        {StmtRef3, {List3a, true}} = Result3a,
        % io:format(user, "Result: (~p) reread~n~p~n", [length(List3a),lists:map(_RowFun3,List3a)]),
        ?assertEqual(AllTableCount, length(List3a)),

        List3b = imem_statement:fetch_recs_sort(SKey, StmtRef3, self(), Timeout, IsSec),
        % io:format(user, "Result: (~p)~n~p~n", [length(List9),lists:map(RowFun8,List9)]),
        ?assertEqual(AllTableCount, length(List3b)),

%        Sql4 = "select all_tables.* from all_tables where qname = erl(\"{'Imem',ddRole}")",
        Sql4 = "select all_tables.* from all_tables where owner = undefined",
        io:format(user, "Query: ~p~n", [Sql4]),
        {ok, _Clm4, _RowFun4, StmtRef4} = imem_sql:exec(SKey, Sql4, 100, 'Imem', IsSec),  %% all_tables
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef4, self(), IsSec)),
        Result4 = receive 
            R4 ->    R4
        end,
        {StmtRef4, {List4, true}} = Result4,
        % io:format(user, "Result: (~p)~n~p~n", [length(List4),lists:map(_RowFun4,List4)]),
        case IsSec of
            false -> ?assertEqual(1, length(List4));
            true ->  ?assertEqual(0, length(List4))
        end,

        Sql5 = "select col1, col2, col3, user from def where 1=1 and col2 = \"7\"",
        io:format(user, "Query: ~p~n", [Sql5]),
        {ok, _Clm5, _RowFun5, StmtRef5} = imem_sql:exec(SKey, Sql5, 100, 'Imem', IsSec),
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef5, self(), IsSec)),
        Result5 = receive 
            R5 ->    R5
        end,
        {StmtRef5, {List5, true}} = Result5,
        % io:format(user, "Result: (~p)~n~p~n", [length(List5),lists:map(_RowFun5,List5)]),
        ?assertEqual(1, length(List5)),            

        ?assertEqual(ok, imem_statement:close(SKey, StmtRef3)),
        ?assertEqual(ok, imem_statement:close(SKey, StmtRef4)),
        ?assertEqual(ok, imem_statement:close(SKey, StmtRef5)),

        Sql11 = "select t1.col1, t2.col1 from def t1, def t2 where t1.col1 in (5,6,7) and t2.col1 > t1.col1 and t2.col1 > t1.col1 and t2.col1 <> 9 ", %% and t2.col1 <= t1.col1 + 2 
        io:format(user, "Query: ~p~n", [Sql11]),
        {ok, _Clm11, _RowFun11, StmtRef11} = imem_sql:exec(SKey, Sql11, 100, 'Imem', IsSec),
        List11 = imem_statement:fetch_recs_sort(SKey, StmtRef11, self(), Timeout, IsSec),
        io:format(user, "Result: (~p)~n~p~n", [length(List11),lists:map(_RowFun11,List11)]),
        ?assertEqual(9, length(List11)),
        % 5,6
        % 5,7
        % 6,7
        % 6,8
        % 7,8

        case IsSec of
            false ->    ok;
            true ->     
                Sql12 = "select * from all_tables where owner=user",
                io:format(user, "Query: ~p~n", [Sql12]),
                {ok, _Clm12, _RowFun12, StmtRef12} = imem_sql:exec(SKey, Sql12, 100, 'Imem', IsSec),
                List12 = imem_statement:fetch_recs_sort(SKey, StmtRef12, self(), Timeout, IsSec),
                io:format(user, "Result: (~p)~n~p~n", [length(List12),lists:map(_RowFun12,List12)]),
                ?assertEqual(1, length(List12)),                

                Sql17 = "select name, type from ddAccount where id=user and locked <> true", 
                io:format(user, "Query: ~p~n", [Sql17]),
                {ok, _Clm17, _RowFun17, StmtRef17} = imem_sql:exec(SKey, Sql17, 100, 'Imem', IsSec),
                List17 = imem_statement:fetch_recs_sort(SKey, StmtRef17, self(), Timeout, IsSec),
                io:format(user, "Result: (~p)~n~p~n", [length(List17),lists:map(_RowFun17,List17)]),
                ?assertEqual(1, length(List17)),
                % "admin", user

                Sql18 = "select name, lastLoginTime from ddAccount where id=user and lastLoginTime > sysdate - (1/2 + 1/24)",    
                io:format(user, "Query: ~p~n", [Sql18]),
                {ok, _Clm18, _RowFun18, StmtRef18} = imem_sql:exec(SKey, Sql18, 100, 'Imem', IsSec),
                List18 = imem_statement:fetch_recs_sort(SKey, StmtRef18, self(), Timeout, IsSec),
                io:format(user, "Result: (~p)~n~p~n", [length(List18),lists:map(_RowFun18,List18)]),
                ?assertEqual(1, length(List18))
                
        end,

        Sql13 = "select t1.col1, t2.col1 from def t1, def t2 where t1.col1 in (5,6,7) and t2.col1 > t1.col1 and t2.col1 <= t1.col1 + 2 ",  
        io:format(user, "Query: ~p~n", [Sql13]),
        {ok, _Clm13, _RowFun13, StmtRef13} = imem_sql:exec(SKey, Sql13, 100, 'Imem', IsSec),
        List13 = imem_statement:fetch_recs_sort(SKey, StmtRef13, self(), Timeout, IsSec),
        io:format(user, "Result: (~p)~n~p~n", [length(List13),lists:map(_RowFun13,List13)]),
        ?assertEqual(6, length(List13)),
        % 5,6
        % 5,7
        % 6,7
        % 6,8
        % 7,8
        % 7,9

        Sql14 = "select t1.col1, t2.col1 from def t1, def t2 where t1.col1 in (5,7) and abs(t2.col1-t1.col1) = 1", 
        io:format(user, "Query: ~p~n", [Sql14]),
        {ok, _Clm14, _RowFun14, StmtRef14} = imem_sql:exec(SKey, Sql14, 100, 'Imem', IsSec),
        List14 = imem_statement:fetch_recs_sort(SKey, StmtRef14, self(), Timeout, IsSec),
        io:format(user, "Result: (~p)~n~p~n", [length(List14),lists:map(_RowFun14,List14)]),
        ?assertEqual(4, length(List14)),
        % 5,4
        % 5,6
        % 7,6
        % 7,8 

        Sql16 = "select t1.col1, t2.col1 from def t1, def t2 where t1.col1=5 and t2.col1 > t1.col1 / 2 and t2.col1 <= t1.col1", 
        io:format(user, "Query: ~p~n", [Sql16]),
        {ok, _Clm16, _RowFun16, StmtRef16} = imem_sql:exec(SKey, Sql16, 100, 'Imem', IsSec),
        List16 = imem_statement:fetch_recs_sort(SKey, StmtRef16, self(), Timeout, IsSec),
        io:format(user, "Result: (~p)~n~p~n", [length(List16),lists:map(_RowFun16,List16)]),
        ?assertEqual(3, length(List16)),
        % 5,3
        % 5,4
        % 5,5

        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, 'Imem', IsSec)),

        case IsSec of
            true ->     ?imem_logout(SKey);
            false ->    ok
        end

    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

insert_range(_SKey, 0, _TableName, _Schema, _IsSec) -> ok;
insert_range(SKey, N, TableName, Schema, IsSec) when is_integer(N), N > 0 ->
    imem_sql:exec(SKey, "insert into " ++ TableName ++ " (col1, col2) values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');", 0, Schema, IsSec),
    insert_range(SKey, N-1, TableName, Schema, IsSec).
