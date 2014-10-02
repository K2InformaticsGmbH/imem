-module(imem_sql_expr).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(MaxChar,16#FFFFFF).
-define(Star,<<"*">>).
-define(Join,'$join$').
-define(GET_ROWNUM_LIMIT,?GET_IMEM_CONFIG(rownumDefaultLimit,[],10000)).
-define(MetaTab,<<"_meta_">>).
-define(ParamTab,<<"_param_">>).
-define(ParamNameIdx,1).
-define(ParamTypeIdx,2).
-define(ParamPrecisionIdx,3).

-export([ column_map_tables/3
        , column_map_columns/2
        , column_map_items/2
        , expr/3
        , bind_scan/3
        , bind_virtual/3
        , bind_tree/2
        ]).

-export([ main_spec/2
        , join_specs/3
        , sort_fun/3
        , sort_spec/3
        , is_readonly/1
        , filter_spec_where/3
        , sort_spec_order/3
        , sort_spec_fun/3
        ]).

-export([ binstr_to_qname3/1
        , binstr_to_qname2/1
        , binstr_to_qname/1
        , uses_operator/2
        , uses_operand/2
        ]).

%% @doc Reforms the main scan specification for the select statement 
%% by binding now known values for tables with index smaller (scan) or equal (filter) to Ti. 
%% Ti:      Table index (?MainIdx=2,JoinTables=3,4..)
%% X:       Tuple structure known so far e.g. {{MetaRec},{MainRec}} for main table scan (Ti=2)
%% ScanSpec:Scan specification record to be reworked and updated
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec bind_scan(integer(),tuple(), #scanSpec{}) -> {#scanSpec{},any(),any()}.
bind_scan(Ti,X,ScanSpec0) ->
    #scanSpec{sspec=SSpec0,stree=STree0,ftree=FTree0,tailSpec=TailSpec0,filterFun=FilterFun0} = ScanSpec0,
    % ?LogDebug("STree before scan (~p) bind :~n~p~n", [Ti,to_guard(STree0)]),
    % ?LogDebug("FTree before scan (~p) bind :~n~p~n", [Ti,to_guard(FTree0)]),
    case {STree0,FTree0} of
        {true,true} ->
            {SSpec0,TailSpec0,FilterFun0};          %% use pre-calculated SSpec0
        {_,true} ->                                 %% no filter fun (pre-calculated to true)
            [{SHead, [undefined], [Result]}] = SSpec0,
            STree1 = bind_table(Ti, STree0, X),
            % ?LogDebug("STree after scan (~p) bind :~n~p~n", [Ti,to_guard(STree1)]),
            SSpec1 = [{SHead, [to_guard(STree1)], [Result]}],
            case Ti of
                ?MainIdx -> {SSpec1,ets:match_spec_compile(SSpec1),FilterFun0};
                _ ->        {SSpec1,TailSpec0,FilterFun0}
            end;
        {_,_} ->                     %% both filter funs needs to be evaluated
            [{SHead, [undefined], [Result]}] = SSpec0,
            STree1 = bind_table(Ti, STree0, X),
            % ?LogDebug("STree after scan (~p) bind :~n~p~n", [Ti,to_guard(STree1)]),
            {STree2,FTree} = split_filter_from_guard(STree1),
            % ?LogDebug("STree after split (~p) :~n~p~n", [Ti,to_guard(STree2)]),
            % ?LogDebug("FTree after split (~p) :~n~p~n", [Ti,to_guard(FTree)]),
            SSpec1 = [{SHead, [to_guard(STree2)], [Result]}],
            FilterFun1 = imem_sql_funs:filter_fun(FTree),
            case Ti of
                ?MainIdx -> {SSpec1,ets:match_spec_compile(SSpec1),FilterFun1};
                _ ->        {SSpec1,TailSpec0,FilterFun1}
            end
    end.

%% @doc Reforms the main scan specification for a select statement on a virtual table
%% by binding now known values for tables with index smaller (scan) or equal (filter) to Ti. 
%% Ti:      Table index (JoinTables=3,4..)
%% X:       Tuple structure known so far e.g. {{MetaRec},{MainRec},{MainTab}} for first join table
%% ScanSpec:Scan specification record to be reworked and updated
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec bind_virtual(integer(),tuple(), #scanSpec{}) -> {#scanSpec{},any(),any()}.
bind_virtual(Ti,X,ScanSpec0) ->
    #scanSpec{sspec=SSpec0,stree=STree0,ftree=FTree0,tailSpec=TailSpec0,filterFun=FilterFun0} = ScanSpec0,
    % ?LogDebug("STree before scan (~p) bind :~n~p~n", [Ti,to_guard(STree0)]),
    % ?LogDebug("FTree before scan (~p) bind :~n~p~n", [Ti,to_guard(FTree0)]),
    case {STree0,FTree0} of
        {true,true} ->
            {SSpec0,TailSpec0,FilterFun0};          %% use pre-calculated SSpec0
        {_,true} ->                                 %% no filter fun (pre-calculated to true)
            [{SHead, [undefined], [Result]}] = SSpec0,
            STree1 = bind_table(Ti, STree0, X),
            % ?LogDebug("STree after scan (~p) bind :~n~p~n", [Ti,to_guard(STree1)]),
            SSpec1 = [{SHead, [to_guard(STree1)], [Result]}],
            {SSpec1,TailSpec0,FilterFun0};
        {_,_} ->                                    %% filter fun needs to be evaluated
            [{SHead, [undefined], [Result]}] = SSpec0,
            STree1 = bind_table(Ti, STree0, X),
            % ?LogDebug("STree after scan (~p) bind :~n~p~n", [Ti,to_guard(STree1)]),
            %% TODO: splitting into generator conditions and filter conditions
            %% For now, we assume that we only have generator conditions which define
            %% the raw virtual rows (e.g. is_member() or item >=1 and item <=10) 
            SSpec1 = [{SHead, [to_guard(STree1)], [Result]}],
            FilterFun1 = imem_sql_funs:filter_fun(STree1),
            {SSpec1,TailSpec0,FilterFun1}
    end.

%% Does expression tree use a bind with Ti ?
uses_bind(_,{const,_}) ->              false;
uses_bind(Ti,#bind{tind=Ti}) -> true;
uses_bind(Ti,#bind{tind=0,cind=0,btree=BTree}) -> uses_bind(Ti,BTree);
uses_bind(Ti,{_,A}) -> uses_bind(Ti,A);
uses_bind(Ti,{_,A,B}) -> uses_bind(Ti,A) orelse uses_bind(Ti,B);
uses_bind(Ti,{_,A,B,C}) -> uses_bind(Ti,A) orelse uses_bind(Ti,B) orelse uses_bind(Ti,C);
uses_bind(Ti,{_,A,B,C,D}) -> uses_bind(Ti,A) orelse uses_bind(Ti,B) orelse uses_bind(Ti,C)  orelse uses_bind(Ti,D);
uses_bind(_,_) -> false.

%% Does expression tree use a bind with Ti/Ci ?
% uses_bind(_, _ ,{const,_}) ->              false;
% uses_bind(Ti,Ci,#bind{tind=Ti,cind=Ci}) -> true;
% uses_bind(Ti,Ci,#bind{tind=0,cind=0,btree=BTree}) -> uses_bind(Ti,Ci,BTree);
% uses_bind(Ti,Ci,{_,A}) -> uses_bind(Ti,Ci,A);
% uses_bind(Ti,Ci,{_,A,B}) -> uses_bind(Ti,Ci,A) orelse uses_bind(Ti,Ci,B);
% uses_bind(Ti,Ci,{_,A,B,C}) -> uses_bind(Ti,Ci,A) orelse uses_bind(Ti,Ci,B) orelse uses_bind(Ti,Ci,C);
% uses_bind(Ti,Ci,{_,A,B,C,D}) -> uses_bind(Ti,Ci,A) orelse uses_bind(Ti,Ci,B) orelse uses_bind(Ti,Ci,C)  orelse uses_bind(Ti,Ci,D);
% uses_bind(_,_,_) -> false.

%% Does this guard use the rownum meta field? If yes, return the comparison expression.   
rownum_match({_,R}) ->                  rownum_match(R);
rownum_match({_,?RownumBind,_}=C1) ->   C1;
rownum_match({_,_,?RownumBind}=C2) ->   C2;
rownum_match({_,L,R}) ->                case rownum_match(L) of
                                            false ->    rownum_match(R);
                                            Else ->     Else
                                        end;    
rownum_match(_) ->                      false.

%% Does expression tree contain given operator Op?
uses_operator(_, {const,_}) ->              false;
uses_operator(Op,#bind{tind=0,cind=0,btree=BTree}) ->   uses_operator(Op,BTree);
uses_operator(Op,{Op}) ->           true;
uses_operator(Op,{Op,_}) ->         true;
uses_operator(Op,{Op,_,_}) ->       true;
uses_operator(Op,{Op,_,_,_}) ->     true;
uses_operator(Op,{Op,_,_,_,_}) ->   true;
uses_operator(Op,{_,A}) ->          uses_operator(Op,A);
uses_operator(Op,{_,A,B}) ->        uses_operator(Op,A) orelse uses_operator(Op,B);
uses_operator(Op,{_,A,B,C}) ->      uses_operator(Op,A) orelse uses_operator(Op,B) orelse uses_operator(Op,C);
uses_operator(Op,{_,A,B,C,D}) ->    uses_operator(Op,A) orelse uses_operator(Op,B) orelse uses_operator(Op,C) orelse uses_operator(Op,D);
uses_operator(_,_) ->               false.

%% Does guard contain given operand V ?
uses_operand(V,V) ->                true;
uses_operand(_,{const,_}) ->        false;
uses_operand(V,#bind{tind=0,cind=0,btree=BTree}) -> uses_operand(V,BTree);
uses_operand(V,{_,A}) ->            uses_operand(V,A);
uses_operand(V,{_,A,B}) ->          uses_operand(V,A) orelse uses_operand(V,B);
uses_operand(V,{_,A,B,C}) ->        uses_operand(V,A) orelse uses_operand(V,B) orelse uses_operand(V,C);
uses_operand(V,{_,A,B,C,D}) ->      uses_operand(V,A) orelse uses_operand(V,B) orelse uses_operand(V,C) orelse uses_operand(V,D);
uses_operand(_,_) ->                false.


%% Does guard contain any of the filter operators?
%% ToDo: bad tuple tolerance for element/2 (add element to function category?)
%% ToDo: bad number tolerance for numeric expressions and functions (add numeric operators to function category?)
uses_filter(true) ->      false;
uses_filter(false) ->     false;
uses_filter(BTree) ->
    uses_filter(BTree,imem_sql_funs:filter_funs()).

uses_filter(_,[]) ->  false;
uses_filter(BTree,[Op|Ops]) ->
    case uses_operator(Op,BTree) of
        true ->             true;
        false ->            uses_filter(BTree,Ops)
    end.

%% pass value for bind variable, tuples T are returned as {const,T}
bind_value({const,Tup}) when is_tuple(Tup) -> {const,Tup};    %% ToDo: Is this neccessary?
bind_value(Tup) when is_tuple(Tup) ->         {const,Tup};
bind_value(Other) ->                          Other.   

%% Is this expression tree completely bound?
bind_done({_,A}) -> bind_done(A);
bind_done(#bind{tind=0,cind=0,btree=BTree}) -> bind_done(BTree);
bind_done(#bind{}) -> false;
bind_done({_,A,B}) -> bind_done(A) andalso bind_done(B);
bind_done({_,A,B,C}) -> bind_done(A) andalso bind_done(B) andalso bind_done(C);
bind_done({_,A,B,C,D}) -> bind_done(A) andalso bind_done(B) andalso bind_done(C) andalso bind_done(D);
bind_done(_) -> true.


% Unary eval rules
bind_eval({_, ?nav}) ->             ?nav;
% Binary eval rules
bind_eval({'or', true, _}) ->       true; 
bind_eval({'or', _, true}) ->       true; 
bind_eval({'or', false, false}) ->  false; 
bind_eval({'or', Left, false}) ->   Left;           % bind_eval(Left); 
bind_eval({'or', false, Right}) ->  Right;          % bind_eval(Right); 
bind_eval({'and', false, _}) ->     false; 
bind_eval({'and', _, false}) ->     false; 
bind_eval({'and', true, true}) ->   true; 
bind_eval({'and', Left, true}) ->   Left;           % bind_eval(Left); 
bind_eval({'and', true, Right}) ->  Right;          % bind_eval(Right); 
bind_eval({'not', true}) ->         false; 
bind_eval({'not', false}) ->        true;
bind_eval({'not', {'/=', Left, Right}}) -> {'==', Left, Right};
bind_eval({'not', {'==', Left, Right}}) -> {'/=', Left, Right};
bind_eval({'not', {'=<', Left, Right}}) -> {'>',  Left, Right};
bind_eval({'not', {'<', Left, Right}}) ->  {'>=', Left, Right};
bind_eval({'not', {'>=', Left, Right}}) -> {'<',  Left, Right};
bind_eval({'not', {'>', Left, Right}}) ->  {'=<', Left, Right};
bind_eval({_, A, B}) when A==?nav;B==?nav -> ?nav; 
bind_eval({'or', Same, Same}) ->    Same;           % bind_eval(Same); 
bind_eval({'and', Same, Same}) ->    Same;          % bind_eval(Same); 
bind_eval({'or', {'and', C, B}, A}) ->  
    case {uses_filter(C),uses_filter(B),uses_filter(A)} of
        {true,false,false} ->       {'and', {'or', C, A}, {'or', A, B}};
        {false,true,false} ->       {'and', {'or', B, A}, {'or', C, A}};
        _ ->                        {'or', {'and', C, B}, A}
    end;
bind_eval({'and', {'and', C, B}, A}) ->  
    case {uses_filter(C),uses_filter(B),uses_filter(A)} of
        {true,false,false} ->       {'and', C, {'and', A, B}};
        {false,true,false} ->       {'and', B, {'and', C, A}};
        _ ->                        {'and', {'and', C, B}, A}
    end;
bind_eval({'or', B, A} = G) ->  
    case {uses_filter(B),uses_filter(A)} of
        {false,true} ->             {'or', A, B};
        _ ->                        G
    end;
bind_eval({'and', B, A} = G) ->  
    case {uses_filter(B),uses_filter(A)} of
        {false,true} ->             {'and', A, B};
        _ ->                        G
    end;
% Operators and functions with 3 parameters
bind_eval({_, A, B, C}) when A==?nav;B==?nav;C==?nav -> ?nav; 
% Functions with 4 parameters
bind_eval({_, A, B, C, D}) when A==?nav;B==?nav;C==?nav;D==?nav -> ?nav; 
bind_eval(BTree) ->
    case bind_done(BTree) of
        false ->    %% cannot simplify BTree here
            BTree;  
        true ->     %% BTree evaluates to a value
            BTF = imem_sql_funs:expr_fun(BTree),
            case is_function(BTF) of
                true ->     bind_value(BTF(anything));
                false ->    bind_value(BTF)
            end
    end.

%% @doc Binds unbound variables for Table Ti in a condition tree, means that all variables  
%% for tables with index smaller than Ti must be bound to values.
%% Ti:      Table index (?MainIdx=2,JoinTables=3,4..)
%% BTree:   Bind tree expression to be simplified by binding values to unbound variables.
%% X:       Tuple structure known so far e.g. {{MetaRec}} for main table scan (Ti=2)
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec bind_table(integer(), tuple(), tuple()) -> tuple().
bind_table(Ti, BTree, X) ->
    case bind_tab(Ti, BTree, X) of
        ?nav ->     false;
        B ->        B
    end.

bind_tab(_, {const,T}, _) when is_tuple(T) -> {const,T};
bind_tab(Ti, #bind{tind=0,cind=0,btree=BT}, X) -> bind_eval(bind_tab(Ti, BT, X));
bind_tab(Ti, #bind{tind=Tind}=Bind, X) when Tind<Ti -> bind_value(?BoundVal(Bind,X));
bind_tab(_ , #bind{}=Bind, _) -> Bind;
bind_tab(Ti, {Op,A}, X) ->       bind_eval({Op,bind_tab(Ti,A,X)}); %% unary functions and operators
bind_tab(Ti, {Op,A,B}, X) ->     bind_eval({Op,bind_tab(Ti,A,X),bind_tab(Ti,B,X)}); %% binary functions/op.
bind_tab(Ti, {Op,A,B,C}, X) ->   bind_eval({Op,bind_tab(Ti,A,X),bind_tab(Ti,B,X),bind_tab(Ti,C,X)});
bind_tab(Ti, {Op,A,B,C,D}, X) -> bind_eval({Op,bind_tab(Ti,A,X),bind_tab(Ti,B,X),bind_tab(Ti,C,X),bind_tab(Ti,D,X)});
bind_tab(_ , A, _) ->            bind_value(A).


%% @doc Transforms an expression tree into a matchspec guard by replacing bind records with their tag value.  
%% BTree:   Bind tree expression to be simplified by binding values to unbound variables.
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec to_guard(tuple()) -> tuple().
to_guard({const,T}) when is_tuple(T) -> {const,T};
to_guard(#bind{tag=Tag}) ->     Tag;
to_guard({Op,A}) ->             {Op,to_guard(A)}; %% unary functions and operators
to_guard({Op,A,B}) ->           {Op,to_guard(A),to_guard(B)}; %% binary functions/op.
to_guard({Op,A,B,C}) ->         {Op,to_guard(A),to_guard(B),to_guard(C)};
to_guard({Op,A,B,C,D}) ->       {Op,to_guard(A),to_guard(B),to_guard(C),to_guard(D)};
to_guard(A) ->                  A.

%% @doc Binds all unbound variables in an expression tree in one pass.
%% BTree:   Bind tree expression to be simplified by binding values to unbound variables.
%% X:       Tuple structure known so far e.g. {{MetaRec},{MainRec}} for main table scan (Ti=2)
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec bind_tree(tuple(), tuple()) -> tuple().
bind_tree(BTree, X) ->  bind_t(BTree, X).

bind_t({const,T}, _) when is_tuple(T) -> {const,T};
bind_t(#bind{tind=0,cind=0,btree=BT}, X) ->    bind_eval(bind_t(BT,X));
bind_t(#bind{}=Bind, X) ->       bind_value(?BoundVal(Bind,X));
bind_t({Op}, _) ->               bind_eval({Op});
bind_t({Op,A}, X) ->             bind_eval({Op,bind_t(A,X)});
bind_t({Op,A,B}, X) ->           bind_eval({Op,bind_t(A,X),bind_t(B,X)});
bind_t({Op,A,B,C}, X) ->         bind_eval({Op,bind_t(A,X),bind_t(B,X),bind_t(C,X)});
bind_t({Op,A,B,C,D}, X) ->       bind_eval({Op,bind_t(A,X),bind_t(B,X),bind_t(C,X),bind_t(D,X)});
bind_t(A, _) ->                  bind_value(A).


%% @doc Reforms the select field expression tree by evaluating
%% constant terms in subtree (leaving header bind in place). 
%% BTree:   Expression bind tree, to be simplified and transformed
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec bind_subtree_const(binary()|tuple()) -> list().
bind_subtree_const(#bind{tind=0,cind=0,btree=BT}=BTree) ->
    % ?LogDebug("Bind subtree constants~n~p~n",[BTree]),
    case bind_table(1,BT,unknown) of
        ?nav ->     ?ClientError({"Cannot bind subtree constants", BT});
        Tree ->     BTree#bind{btree=Tree}
    end;
bind_subtree_const(BTree) ->
    BTree.

%% @doc Reforms the where clause boolean expression tree by pruning off
%% terms which can only be known in the (next) join operation. 
%% Ti:      Table Index
%% WBTree:  where clause bind tree, to be simplified and transformed
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec prune_tree(integer(), binary()|tuple()) -> list().
prune_tree(Ti, WBTree) ->
    case prune_eval(prune_walk(Ti, WBTree)) of
        ?nav ->     ?ClientError({"Cannot evaluate pruned where clause", {Ti,WBTree}});
        ?Join ->    true;
        Tree ->     Tree
    end.

prune_walk(_ , {const,T}) when is_tuple(T) -> {const,T};
prune_walk(Ti, #bind{tind=T}) when T>Ti -> ?Join;
prune_walk(Ti, #bind{tind=0,cind=0,btree=BTree}) -> prune_eval(prune_walk(Ti, BTree));
prune_walk(_ , #bind{}=Bind) -> Bind;
prune_walk(_ , {Op}) -> prune_eval({Op});
prune_walk(Ti, {Op,A}) -> prune_eval({Op,prune_walk(Ti,A)});
prune_walk(Ti, {Op,A,B}) -> prune_eval({Op,prune_walk(Ti,A),prune_walk(Ti,B)});
prune_walk(Ti, {Op,A,B,C}) -> prune_eval({Op,prune_walk(Ti,A),prune_walk(Ti,B),prune_walk(Ti,C)});
prune_walk(Ti, {Op,A,B,C,D}) -> prune_eval({Op,prune_walk(Ti,A),prune_walk(Ti,B),prune_walk(Ti,C),prune_walk(Ti,D)});
prune_walk(_ , BTree) -> BTree.

prune_eval({_,?Join}) -> ?Join;
prune_eval({'and',?Join,?Join}) -> ?Join;
prune_eval({'and',A,?Join}) -> A;
prune_eval({'and',?Join,B}) -> B;
prune_eval({'and',Same,Same}) -> Same;
prune_eval({Op,_,?Join}) when Op/='and' -> ?Join;
prune_eval({Op,?Join,_}) when Op/='and' -> ?Join;
prune_eval(BTree) -> bind_eval(BTree).

%% @doc Reforms the where clause bind tree for the whole select into
%% a database access description. DB access is done in a mnesia range query
%% (described by a mnesia matchspec) and an optional filter function to be
%% applied on the intermediate mnesia result. 
%% WBTree:  Where clause bind tree, to be simplified and transformed
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec main_spec(#bind{}, list(#bind{})) -> #scanSpec{}.
main_spec(?EmptyWhere, FullMap) ->
    scan_spec(?MainIdx, true, FullMap);
main_spec(WBTree, FullMap) ->
    PrunedTree = prune_tree(?MainIdx, WBTree),
    % ?LogDebug("Pruned where tree for main scan~n~p~n",[to_guard(PrunedTree)]),
    scan_spec(?MainIdx, PrunedTree, FullMap).

%% @doc Reforms the where clause bind tree for the whole select into
%% a database access description for all necessary join steps. 
%% Ti:      Table Index for the table to be joined
%% WBTree:  Where clause bind tree, to be simplified and transformed
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec join_specs(integer(), #bind{}, list(#bind{})) -> list(#scanSpec{}).
join_specs(Ti, WBTree, FullMap) -> 
    join_specs(Ti, WBTree, FullMap, []).

join_specs(?MainIdx, _, _, Acc)-> Acc;  %% done when looking at main table
join_specs(Ti, WBTree, FullMap, Acc)->
    PrunedTree = prune_tree(Ti, WBTree),
    % ?LogDebug("Pruned where tree for join ~p~n~p~n",[Ti,to_guard(PrunedTree)]),
    JoinSpec = scan_spec(Ti, PrunedTree, FullMap),
    % ?LogDebug("Join spec ~p pushed~n~p~n", [Ti,JoinSpec]),
    join_specs(Ti-1, WBTree, FullMap, [JoinSpec|Acc]).

%% @doc Creates a scan specification for a MNESIA select and associated filter
%% prescriptions which cannot be cast into ETS matchspecs. Pre-evaluates these
%% values if no bindings to parent tables exist. In this case, the bind step
%% will be skipped in the fetch and the prescriptions must pre-exist.
%% Removes any optional rownum SQL condition by pretending that rownum = 1 (first row).
%% The guard simplification will simplify the resulting condition into a true/false for
%% the whole fetch. The limit given in the SQL is parsed out and passed to the scan spec
%% where it will be used to clip the result rows. 
%% Guards:  Where clause bind tree, wrapped into a list, to be transformed to a scan spec
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec scan_spec(integer(), list(), list(#bind{})) -> #scanSpec{}.
scan_spec(Ti,Logical,FullMap) when Logical==true;Logical==false ->
    MatchHead = list_to_tuple(['_'|[Tag || #bind{tag=Tag, tind=Tind} <- FullMap, Tind==Ti]]),
    #scanSpec{sspec=[{MatchHead, [Logical], ['$_']}], limit=?GET_ROWNUM_LIMIT};
scan_spec(Ti,STree0,FullMap) ->
    % ?LogDebug("STree0 (~p)~n~p~n", [Ti,STree0]),
    MatchHead = list_to_tuple(['_'|[Tag || #bind{tag=Tag, tind=Tind} <- FullMap, Tind==Ti]]),
    % ?LogDebug("MatchHead (~p)~n~p~n", [Ti,MatchHead]),
    Limit = case rownum_match(STree0) of
        false ->                                    ?GET_ROWNUM_LIMIT;
        {'<',?RownumBind,L} when is_integer(L) ->   L-1;
        {'=<',?RownumBind,L} when is_integer(L) ->  L;
        {'>',L,?RownumBind} when is_integer(L) ->   L-1;
        {'>=',L,?RownumBind} when is_integer(L) ->  L;
        {'==',L,?RownumBind} when is_integer(L) ->  L;
        {'==',?RownumBind,L} when is_integer(L) ->  L;
        Else ->
            ?UnimplementedException({"Unsupported use of rownum",{Else}})
    end,
    % ?LogDebug("STree0 (~p)~n~p~n", [Ti,to_guard(STree0)]),
    case {uses_bind(Ti-1,STree0),uses_filter(STree0)} of
        {false,true} ->     
            %% we can do the split upfront here and pre-calculate SSpec, TailSpec and FilterFun
            {STree1,FTree} = split_filter_from_guard(STree0),
            % ?LogDebug("STree1 after split (~p)~n~p~n", [Ti,to_guard(STree1)]),
            % ?LogDebug("FTree after split (~p)~n~p~n", [Ti,to_guard(FTree)]),
            SSpec = [{MatchHead, [to_guard(STree1)], ['$_']}],
            TailSpec = if Ti==?MainIdx -> ets:match_spec_compile(SSpec); true -> true end,
            FilterFun = imem_sql_funs:filter_fun(FTree),  %% TODO: Use bind tree and implicit binding
            #scanSpec{sspec=SSpec,stree=true,tailSpec=TailSpec,ftree=true,filterFun=FilterFun,limit=Limit}; 
        {true,true} ->     
            %% we may  need a filter function, depending on meta binds at fetch time
            SSpec = [{MatchHead, [undefined], ['$_']}],       %% will be split and reworked at fetch time
            #scanSpec{sspec=SSpec,stree=STree0,tailSpec=undefined,ftree=undefined,filterFun=undefined,limit=Limit}; 
        {false,false} ->
            %% we don't need filters and pre-calculate SSpec, TailSpec and FilterFun
            SSpec = [{MatchHead, [to_guard(STree0)], ['$_']}],
            TailSpec = if Ti==?MainIdx -> ets:match_spec_compile(SSpec); true -> true end,
            #scanSpec{sspec=SSpec,stree=true,tailSpec=TailSpec,ftree=true,filterFun=true,limit=Limit};
        {true,false} ->
            %% we cannot bind upfront but we know to get away without filters after bind
            SSpec = [{MatchHead, [undefined], ['$_']}],
            #scanSpec{sspec=SSpec,stree=STree0,tailSpec=undefined,ftree=true,filterFun=true,limit=Limit}
    end.

%% @doc Decomposes a binary or string, assuming SQL dot notation
%% into a "field qualified name" of 2 levels.
%% <<"Schema.Table...">> -> [<<"Schema">>,<<"Table">>,...]
%% throws   ?ClientError
-spec binstr_to_qname(binary()) -> [].
binstr_to_qname(Bin) when is_binary(Bin) ->
    case re:run(Bin, "^(?<PART>((\"[^\"]*\")|([^\".]*)))[.]*(?<REST>.*)$"
                , [{capture, ['PART','REST'], binary}]) of
        {match, [<<>>,<<>>]} -> [];
        {match, [Part,<<>>]} -> [Part];
        {match, [Part,Rest]} -> lists:flatten([Part, binstr_to_qname(Rest)]);
        nomatch -> [Bin]
    end.

%% @doc Decomposes a binary or string, assuming SQL dot notation
%% into a "field qualified name" of 2 levels.
%% <<"Schema.Table">> -> {<<"Schema">>,<<"Table">>}
%% throws   ?ClientError
-spec binstr_to_qname2(binary()) -> {undefined|binary(),binary()}.
binstr_to_qname2(Bin) when is_binary(Bin) ->    
    case binstr_to_qname(Bin) of
        [T] ->      {undefined, T};
        [S,T] ->    {S, T};
        Parts ->    ?ClientError({"Invalid qualified name", {Bin, Parts}})
    end.

%% @doc Decomposes a binary or string, assuming SQL dot notation
%% into a "field qualified name" of 3 levels.
%% <<"Schema.Table.Field">> -> {'Schema','Table','Field'}
%% throws   ?ClientError
-spec binstr_to_qname3(binary()) -> {undefined|binary(),undefined|binary(),undefined|binary()}.
binstr_to_qname3(Bin) when is_binary(Bin) ->
    case binstr_to_qname(Bin) of
        [N] ->      {undefined, undefined, N};          %% may need a left shift for a table name
        [T,N] ->    {undefined, T, N};  %% may need a left shift for a table name
        [S,T,N] ->  {S, T, N};
        Parts ->    ?ClientError({"Invalid qualified name", {Bin, Parts}})
    end.

%% @doc Convert a "field qualified name" of 2 levels into a binary string.
%% <<"Table.Field">> -> {'Table','Field'}
%% throws   ?ClientError
-spec qname2_to_binstr({undefined|binary(),binary()}) -> binary().
qname2_to_binstr({undefined,N}) when is_binary(N) -> N;
qname2_to_binstr({T,N}) when is_binary(T),is_binary(N) -> list_to_binary([T, ".", N]). 

%% @doc Convert a "field qualified name" of 3 levels into a binary string.
%% <<"Schema.Table.Field">> -> {'Schema','Table','Field'}
%% throws   ?ClientError
-spec qname3_to_binstr({undefined|binary(),undefined|binary(),undefined|binary()}) -> binary().
qname3_to_binstr({undefined,T,undefined}) when is_binary(T) -> T; 
qname3_to_binstr({S,T,undefined}) when is_binary(S),is_binary(T) -> list_to_binary([S,".",T]); 
qname3_to_binstr({undefined,undefined,N}) when is_binary(N) -> N;
qname3_to_binstr({undefined,T,N}) when is_binary(T),is_binary(N) -> list_to_binary([T, ".", N]); 
qname3_to_binstr({S,T,N}) when is_binary(S),is_binary(T),is_binary(N) -> list_to_binary([S,".",T,".",N]). 


%% @doc Projects by name one record field out of a list of column maps.
%% Map:     list of bind items
%% Field:   atomic name in the record or constructed convenience field qname 
-spec column_map_items(list(#bind{}),atom()) -> list().
column_map_items(Map, tag) ->
    [C#bind.tag || C <- Map];
column_map_items(Map, schema) ->
    [C#bind.schema || C <- Map];
column_map_items(Map, table) ->
    [C#bind.table || C <- Map];
column_map_items(Map, alias) ->
    [C#bind.alias || C <- Map];
column_map_items(Map, name) ->
    [C#bind.name || C <- Map];
column_map_items(Map, qname) ->
    [qname3_to_binstr({C#bind.schema,C#bind.table,C#bind.name}) || C <- Map];
column_map_items(Map, tind) ->
    [C#bind.tind || C <- Map];
column_map_items(Map, cind) ->
    [C#bind.cind || C <- Map];
column_map_items(Map, type) ->
    [C#bind.type || C <- Map];
column_map_items(Map, len) ->
    [C#bind.len || C <- Map];
column_map_items(Map, prec) ->
    [C#bind.prec || C <- Map];
column_map_items(Map, ptree) ->
    [C#bind.ptree || C <- Map];
column_map_items(_Map, Item) ->
    ?ClientError({"Invalid item",Item}).

-spec is_readonly(#bind{}) -> boolean().
is_readonly(#bind{tind=Ti}) when Ti > ?MainIdx -> true;
is_readonly(#bind{tind=?MainIdx,cind=Ci}) when Ci>0 -> false;   
is_readonly(#bind{tind=?MainIdx,cind=0}) -> true;               %% Vector field cannot be edited
is_readonly(#bind{tind=0,cind=0,btree={_,#bind{tind=?MainIdx,cind=0}}}) -> true; %% Vector field cannot be edited
is_readonly(#bind{tind=0,cind=0,btree={_,_,#bind{tind=?MainIdx,cind=0}}}) -> true; %% Vector field cannot be edited
is_readonly(#bind{tind=0,cind=0,btree={Op,#bind{tind=?MainIdx}}}) when Op=='hd';Op=='last' -> false;        %% editable projection
is_readonly(#bind{tind=0,cind=0,btree={Op,_,#bind{tind=?MainIdx}}}) when Op=='element';Op=='nth' -> false;  %% editable projection
is_readonly(_BTree) -> 
    ?LogDebug("Positive readonly test for ~n~p~n",[_BTree]),
    true.

%% @doc Creates full map (all fields of all tables) of bind information to which column
%% names can be assigned in column_map_columns. A virtual table binding for metadata is prepended.
%% Unnecessary meta fields will be purged later and remaining meta field bind positions 
%% are corrected (not yet implemented).
%% Tables:  given as list of parse tree 'from' descriptions. Table names are converted to physical table names.
%% throws   ?ClientError
-spec column_map_tables(list(binary()|{as,_,_}), list(binary()), list(tuple())) -> list(#bind{}).
column_map_tables(Tables, MetaFields, Params) ->
    MetaBinds = column_map_meta_fields(MetaFields,?MetaIdx,[]),
    ParamBinds = column_map_param_fields(Params,?MetaIdx,lists:reverse(MetaBinds)),
    TableBinds = column_map_table_fields(Tables, ?MainIdx, []),
    ParamBinds ++ TableBinds.

-spec column_map_meta_fields(list(atom()), integer(), list(#bind{})) -> list(#bind{}).
column_map_meta_fields([], _Ti, Acc) -> lists:reverse(Acc);
column_map_meta_fields([Name|Names], Ti, Acc) ->
    Cindex = length(Acc) + 1,    %% Ci of next meta field (starts with 1, not 2)
    #ddColumn{type=Type,len=Len,prec=P,default=D} = imem_meta:meta_field_info(Name),
    S = ?atom_to_binary(imem_meta:schema()),
    Tag = list_to_atom(lists:flatten([$$,integer_to_list(?MetaIdx),integer_to_list(Cindex)])),
    Bind=#bind{schema=S,table=?MetaTab,alias=?MetaTab,name=Name,tind=Ti,cind=Cindex,type=Type,len=Len,prec=P,default=D,tag=Tag}, 
    column_map_meta_fields(Names, Ti, [Bind|Acc]).

-spec column_map_param_fields(list(atom()), integer(), list(#bind{})) -> list(#bind{}).
column_map_param_fields([], _Ti, Acc) -> lists:reverse(Acc);
column_map_param_fields([Param|Params], Ti, Acc) ->
    case imem_datatype:is_datatype(element(?ParamTypeIdx,Param)) of
        true -> 
            N = element(?ParamNameIdx,Param),           %% Parameter name as binary in first element of triple
            Type = ?binary_to_atom(element(?ParamTypeIdx,Param)),   %% Parameter type (imem datatype) as second element
            Prec = list_to_integer(binary_to_list(element(?ParamPrecisionIdx,Param))),  %% Parameter precision (for decimals)
            Cindex = length(Acc) + 1,       %% Ci of next param field, appended to meta fields
            Tag = list_to_atom(lists:flatten([$$,integer_to_list(?MetaIdx),integer_to_list(Cindex)])),
            Bind=#bind{table=?ParamTab,alias=?ParamTab,name=N,tind=Ti,cind=Cindex,type=Type,prec=Prec,tag=Tag}, 
            column_map_param_fields(Params, Ti, [Bind|Acc]);
        false ->
            ?ClientError({"Invalid data type for parameter",{element(1,Param),element(2,Param)}})
    end.

-spec column_map_table_fields(list(),integer(),list(#bind{})) -> list(#bind{}).
column_map_table_fields([], _Ti, Acc) -> Acc;
column_map_table_fields([{as,Table,Alias}|Tables], Ti, Acc) when is_binary(Table),is_binary(Alias) ->
    {S,T} = binstr_to_qname2(Table),
    column_map_table_fields([{S,T,Alias}|Tables], Ti, Acc);
column_map_table_fields([Table|Tables], Ti, Acc) when is_binary(Table) ->
    {S,T} = binstr_to_qname2(Table),
    column_map_table_fields([{S,T,T}|Tables], Ti, Acc);
column_map_table_fields([{undefined,T,A}|Tables], Ti, Acc) ->
    S = ?atom_to_binary(imem_meta:schema()),
    column_map_table_fields([{S,T,A}|Tables], Ti, Acc);
column_map_table_fields([{S,T,A}|Tables], Ti, Acc) ->
    Cols = imem_meta:column_infos({?binary_to_atom(S),?binary_to_atom(T)}),
    case Ti of
        ?MainIdx ->      
            case imem_meta:is_virtual_table(?binary_to_atom(T)) of
                true ->     ?ClientError({"Virtual table can only be joined", T});
                false ->    ok
            end;
        _ -> ok
    end,
    Binds = [ #bind{schema=S,table=T,alias=A,tind=Ti,cind=Ci
                   ,type=Type,len=Len,prec=P,name=?atom_to_binary(N)
                   ,default=D,tag=list_to_atom(lists:flatten([$$,integer_to_list(Ti),integer_to_list(Ci)]))
                   } 
          || {Ci, #ddColumn{name=N,type=Type,len=Len,prec=P,default=D}} <- 
          lists:zip(lists:seq(?FirstIdx,length(Cols)+1), Cols)
        ],
    column_map_table_fields(Tables, Ti+1, Acc ++ Binds).

%% @doc Generates list of column information (bind records) for a select list.
%% Bind records will be tagged with integers corresponding to the position in the select list (1..n).
%% Bind records for metadata values will have tind=?MetaIdx and cind>0
%% Expressions or functions will have tind=0 and cind=0 and are stored in btree as values or fun()
%% Names pointing to table records have binds with tind>0 and cind=0. 
%% Constant tuple values are wrapped with {const,Tup}   
%% Columns: list of field names or sql expression tuples (extended by erlang expression types)
%% FullMap: list of #bind{}, one per declared field for involved tables
%% Acc:     list of bind records
-spec column_map_columns(list(),list(#bind{})) -> list(#bind{}).
%% throws ?ClientError, ?UnimplementedException
column_map_columns(Columns, FullMap) ->
    ColMap = column_map_columns(Columns, FullMap, []),
    [bind_subtree_const(Item#bind{tag=I}) || {I,Item} <- lists:zip(lists:seq(1,length(ColMap)), ColMap)].

-spec column_map_columns(list(),list(tuple()),list(#bind{})) -> list(#bind{}).
column_map_columns([#bind{schema=undefined,table=undefined,name=?Star}|Columns], FullMap, Acc) ->
    % Handle * column
    Cmaps = [ case  length([N || #bind{name=N} <- FullMap,N==Name]) of
                1 -> Bind#bind{table=A,alias=Name,ptree=Name};
                _ -> Bind#bind{table=A,alias=qname3_to_binstr({undefined,A,Name}),ptree=qname3_to_binstr({undefined,A,Name})}
              end
              || #bind{tind=Ti,name=Name,alias=A}=Bind <- FullMap,Ti/=?MetaIdx
            ],
    % ?LogDebug("column_map *~n~p~n", [Cmaps]),
    column_map_columns(Cmaps ++ Columns, FullMap, Acc);
column_map_columns([#bind{schema=undefined,name=?Star}=Cmap0|Columns], FullMap, Acc) ->
    % Handle table.* column
    % ?LogDebug("column_map 2 ~p~n", [Cmap0]),
    S = ?atom_to_binary(imem_meta:schema()),
    column_map_columns([Cmap0#bind{schema=S}|Columns], FullMap, Acc);
column_map_columns([#bind{schema=Schema,table=Table,name=?Star}=_Cmap0|Columns], FullMap, Acc) ->
    % Handle schema.table.* column
    % ?LogDebug("column_map 3 ~p~n", [_Cmap0]),
    Prefix = case ?atom_to_binary(imem_meta:schema()) of
        Schema ->   undefined;
        _ ->        Schema
    end,
    Cmaps = [ case  length([N || #bind{name=N} <- FullMap,N==Name]) of
                1 -> Bind#bind{table=A,alias=Name,ptree=Name};
                _ -> Bind#bind{table=A,alias=qname3_to_binstr({Prefix,A,Name}),ptree=qname3_to_binstr({Prefix,A,Name})}
              end
              || #bind{tind=Ti,schema=S,name=Name,alias=A}=Bind <- FullMap,Ti/=?MetaIdx,S==Schema,A==Table
            ],
    column_map_columns(Cmaps ++ Columns, FullMap, Acc);
column_map_columns([#bind{schema=Schema,table=Table,name=Name,alias=Alias,ptree=PTree}|Columns], FullMap, Acc) ->
    % Handle expanded * columns of all 3 types
    % ?Debug("column_map 4 ~p ~p ~p ~n", [Schema,Table,Name]),
    % ?Debug("column_map 4 FullMap~n~p~n", [FullMap0]),
    Bind = column_map_lookup({Schema,Table,Name},FullMap),
    column_map_columns(Columns, FullMap, [Bind#bind{alias=Alias,ptree=PTree}|Acc]);
column_map_columns([{as, Expr, Alias}=PTree|Columns], FullMap, Acc) ->
    % ?LogDebug("column_map 7 ~p~n", [{as, Expr, Alias}]),
    Bind = expr(Expr,FullMap,#bind{}),
    R = is_readonly(Bind),
    column_map_columns(Columns, FullMap, [Bind#bind{alias=Alias,readonly=R,ptree=PTree}|Acc]);
column_map_columns([PTree|Columns], FullMap, Acc) ->
    % ?LogDebug("column_map 9 ~p ~p ~p~n", [PTree, is_binary(PTree),is_integer(PTree)]),
    case expr(PTree,FullMap,#bind{}) of 
        #bind{name=?Star} = CMap ->
            %% one * column retured for expansion
            column_map_columns([CMap|Columns], FullMap, Acc);
        #bind{} = CMap ->
            %% one select column returned
            Alias = sqlparse:pt_to_string({fields,[PTree]}),
            R = is_readonly(CMap),
            column_map_columns(Columns, FullMap, [CMap#bind{alias=Alias,readonly=R,ptree=PTree}|Acc])
    end;
column_map_columns([], _FullMap, Acc) -> lists:reverse(Acc);
column_map_columns(Columns, FullMap, Acc) ->
    ?Warn("column_map_columns error Columns ~p~n", [Columns]),
    ?Warn("column_map_columns error FullMap ~p~n", [FullMap]),
    ?Warn("column_map_columns error Acc ~p~n", [Acc]),
    ?ClientError({"Column map invalid columns",Columns}).

column_map_lookup(QN3,FullMap) ->
    case field_map_lookup(QN3,FullMap) of
        #bind{type=term,btree={from_binterm,Bind}} ->   Bind;   %% represent as binterm to the rowfuns
        Other ->                                        Other   
    end.                                

field_map_lookup({Schema,Table,Name}=QN3,FullMap) ->
    % ?LogDebug("column_map lookup ~p ~p ~p~n", [Schema,Table,Name]),
    Pred = fun(__FM) ->
        ((Name == undefined) orelse (Name == __FM#bind.name)) 
        andalso ((Table == undefined) orelse (Table == __FM#bind.alias)) 
        andalso ((Schema == undefined) orelse (Schema == __FM#bind.schema))
    end,
    Bmatch = lists:filter(Pred, FullMap),
    % ?LogDebug("column_map matching tables ~p~n", [Bmatch]),
    Tcount = length(lists:usort([{B#bind.schema, B#bind.alias} || B <- Bmatch])),
    % ?Debug("column_map matching table count ~p~n", [Tcount]),
    if 
        (Tcount==0) andalso (Schema == undefined) andalso (Name /= undefined) ->
            %% Maybe we got a table name {undefined,Schema,Table}  
            field_map_lookup({Table,Name,undefined},FullMap);
        (Tcount==0) ->  
            ?ClientError({"Unknown field or table name", qname3_to_binstr(QN3)});
        (Tcount > 1) ->
            ?ClientError({"Ambiguous field or table name", qname3_to_binstr(QN3)});
        (Name == undefined) ->         
            Bind = hd(Bmatch),
            Bind#bind{type=tuple,cind=0};       %% bind to whole table record
        true ->    
            Bind = hd(Bmatch),     
            case Bind of
                #bind{type=binterm} ->  
                    %% db field encoded as binary, must decode to term in where tree
                    %% this conversion is removed for simple column expressions in column_map_lookup
                    #bind{type=term,btree={from_binterm,Bind}};
                _ ->    
                    %% db field is not stored in encoded form, no transformation needed
                    Bind
            end
    end.

%% 
%% @doc Convert a parse tree item (hierarchical tree of binstr names, atom operators and erlang values)
%% to an expression tree with embedded bind structures. Similar to ETS matchspec guards but using #bind{}
%% instead of simple atomic v#bind{tind=0,cind=0,type=Type,default=D,len=L,prec=Prec,readonly=true,btree=ValWrap}ariable names like '$1'or '$123'. Constant tuple values are wrapped with {const,Tup}   
%% PTree:   ParseTree, binary text or tuple correcponding to a field name, a constant field value or an expression which can
%%          depend on other constants or field variables      
%% FullMap: List of #bind{}, one per declared field for involved tables
%% BindTemplate:    Bind record signalling the expected datatype properties of the expression to be evaluated.
%% 
-spec expr(list(),list(#bind{}),#bind{}) -> list(#bind{}).
%% throws ?ClientError, ?UnimplementedException
expr(PTree, FullMap, BindTemplate) when is_binary(PTree) -> 
    case {imem_datatype:strip_squotes(PTree),BindTemplate} of
        {PTree,_} ->
            %% This is not a string, must be a name or a number
            case (catch imem_datatype:io_to_term(PTree)) of
                I when is_integer(I) ->  
                    #bind{tind=0,cind=0,type=integer,readonly=true,btree=I};
                V when is_float(V) -> 
                    #bind{tind=0,cind=0,type=float,readonly=true,btree=V};
                _ ->
                    {S,T,N} = binstr_to_qname3(PTree),
                    case N of
                        ?Star ->    #bind{schema=S,table=T,name=?Star};
                        _ ->        field_map_lookup({S,T,N},FullMap)  %% N could be a table name here
                    end
            end;
        {B,Tbind} when Tbind==#bind{} ->    %% assume binstr, use to_<datatype>() to override
            #bind{tind=0,cind=0,type=binstr,default= <<>>,readonly=true,btree=imem_sql:un_escape_sql(B)};
        {B,#bind{type=binstr}} ->           %% just take the literal value from SQL text
            #bind{tind=0,cind=0,type=binstr,default= <<>>,readonly=true,btree=imem_sql:un_escape_sql(B)};
        {B,#bind{type=T,len=L,prec=P,default=D,tag=Tag}} ->     %% best effort conversion to proposed type
            {_,ValWrap,Type,Prec} = imem_datatype:field_value_type(Tag,T,L,P,D,imem_sql:un_escape_sql(B)),
            #bind{tind=0,cind=0,type=Type,default=D,len=L,prec=Prec,readonly=true,btree=ValWrap}
    end;
expr({param,Name}, FullMap, _) when is_binary(Name) -> 
    field_map_lookup({undefined,?ParamTab,Name},FullMap);
expr({'fun',Fname,[A]}=PTree, FullMap, _) -> 
    case imem_datatype:is_rowfun_extension(Fname,1) of
        true ->
            {S,T,N} = binstr_to_qname3(A),
            CMapA = field_map_lookup({S,T,N},FullMap),
            CMapA#bind{func=binary_to_existing_atom(Fname,utf8), ptree=PTree};
        false ->
            case imem_sql_funs:unary_fun_bind_type(Fname) of
                undefined ->    
                    ?UnimplementedException({"Unsupported unary sql function", Fname});
                BT ->
                    try            
                        Func = binary_to_existing_atom(Fname,utf8),
                        CMapA = expr(A,FullMap,BT),
                        #bind{type=Type} = imem_sql_funs:unary_fun_result_type(Fname),
                        #bind{type=Type,btree={Func,CMapA}}
                    catch
                        _:{'ClientError',Reason} -> 
                            ?ClientError(Reason);
                        _:_ -> 
                            ?UnimplementedException({"Bad parameter for unary sql function", Fname})
                    end
            end
    end;        
expr({'fun',<<"regexp_like">>,[A,B]}, FullMap, BT) -> 
    expr({'fun',<<"is_regexp_like">>,[A,B]}, FullMap, BT); 
expr({'||',A,B}, FullMap, _) -> 
    CMapA = expr(A,FullMap,#bind{type=binstr}),
    CMapB = expr(B,FullMap,#bind{type=binstr}),
    % ?LogDebug("Concatenation CMapA~n~p~n", [CMapA]),
    % ?LogDebug("Concatenation CMapB~n~p~n", [CMapB]),
    expr_concat(CMapA, CMapB);
expr({'fun',Fname,[A,B]}, FullMap, _) -> 
    CMapA = case imem_sql_funs:binary_fun_bind_type1(Fname) of
        undefined ->    ?UnimplementedException({"Unsupported binary sql function", Fname});
        BA ->           expr(A,FullMap,BA)
    end,
    CMapB = case imem_sql_funs:binary_fun_bind_type2(Fname) of
        undefined ->    ?UnimplementedException({"Unsupported binary sql function", Fname});
        BB ->           expr(B,FullMap,BB)
    end,
    try 
        Func = binary_to_existing_atom(Fname,utf8),
        #bind{type=Type} = imem_sql_funs:binary_fun_result_type(Fname),
        #bind{type=Type,btree={Func,CMapA,CMapB}}
    catch
        _:_ -> ?UnimplementedException({"Unsupported binary sql function", Fname})
    end;
expr({Op,A}, FullMap, _) when Op=='+';Op=='-' ->
    CMapA = expr(A,FullMap,#bind{type=number,default=?nav}),
    #bind{type=number,btree={Op,CMapA}};
expr({Op,A,B}, FullMap, BT) when Op=='+';Op=='-';Op=='*';Op=='/';Op=='div';Op=='rem' -> 
    CMapA = expr(A, FullMap, default_to_number(BT)),     
    CMapB = expr(B, FullMap, default_to_number(BT)),
    % ?LogDebug("CMapA ~p~n",[CMapA]),
    % ?LogDebug("CMapB ~p~n",[CMapB]),    
    case {CMapA#bind.tind, CMapB#bind.tind} of
        {0,0} -> 
            expr_math(Op, CMapA, CMapB, BT);
        {0,_} when CMapB#bind.type==datetime;CMapB#bind.type==timestamp ->
            case CMapA#bind.type of
                integer ->  expr_time(Op, CMapA, CMapB, BT);
                float ->    expr_time(Op, CMapA, CMapB, BT);
                number ->   expr_time(Op, CMapA, CMapB, BT);
                _ ->        CMapA1 = expr(A,FullMap,#bind{type=number,default=?nav}),
                            expr_time(Op, CMapA1, CMapB, BT)
            end;
        {0,_} ->
            case CMapA#bind.type of
                integer ->  expr_math(Op, CMapA, CMapB, BT);
                float ->    expr_math(Op, CMapA, CMapB, BT);
                number ->   expr_math(Op, CMapA, CMapB, BT);
                _ ->        CMapA1 = expr(A,FullMap,#bind{type=number,default=?nav}),
                            expr_math(Op, CMapA1, CMapB, BT)
            end;
        {_,0} when CMapA#bind.type==datetime;CMapA#bind.type==timestamp ->
            case CMapB#bind.type of
                integer ->  expr_time(Op, CMapA, CMapB, BT);
                float ->    expr_time(Op, CMapA, CMapB, BT);
                number ->   expr_time(Op, CMapA, CMapB, BT);
                _ ->        CMapB1 = expr(B,FullMap,#bind{type=number,default=?nav}),
                            expr_time(Op, CMapA, CMapB1, BT)
            end;
        {_,0} ->
            case CMapB#bind.type of
                integer ->  expr_math(Op, CMapA, CMapB, BT);
                float ->    expr_math(Op, CMapA, CMapB, BT);
                number ->   expr_math(Op, CMapA, CMapB, BT);
                _ ->        CMapB1 = expr(B,FullMap,#bind{type=number,default=?nav}),
                            expr_math(Op, CMapA, CMapB1, BT)
            end;
        {_,_} when CMapA#bind.type/=datetime,CMapA#bind.type/=timestamp ->
            expr_math(Op, CMapA, CMapB, BT);
        {_,_} ->
            expr_time(Op, CMapA, CMapB, BT)
    end;
expr({Op, A}, FullMap, _) when Op=='not' ->
    CMapA = expr(A,FullMap,#bind{type=boolean,default=?nav}),
    #bind{type=boolean,btree={Op,CMapA}};
expr({Op, A, B}, FullMap, _) when Op=='and';Op=='or' ->
    CMapA = expr(A,FullMap,#bind{type=boolean,default= ?nav}),
    CMapB = expr(B,FullMap,#bind{type=boolean,default= ?nav}),
    #bind{type=boolean,btree={Op, CMapA, CMapB}};
expr({'between', A, Low, High}, FullMap, BT) ->
    expr({'and', {'>=',A,Low}, {'<=',A,High}}, FullMap, BT);
expr({Op, A, B}, FullMap, _) when Op=='=';Op=='>';Op=='>=';Op=='<';Op=='<=';Op=='<>' ->
    CMapA = expr(A,FullMap,#bind{type=binstr}),
    CMapB = expr(B,FullMap,#bind{type=binstr}),
    % ?LogDebug("Comparison ~p CMapA~n~p~n", [Op,CMapA]),
    % ?LogDebug("Comparison ~p CMapB~n~p~n", [Op,CMapB]),
    BTree = case {CMapA#bind.tind, CMapB#bind.tind} of
        {0,0} -> 
            case CMapA#bind.type > CMapB#bind.type of    
                true->      expr_comp(reverse(Op), CMapB, CMapA);
                false ->    expr_comp(Op, CMapA, CMapB)
            end;
        {0,_} ->
            CMapA1 = expr(A,FullMap,CMapB),
            case CMapA1#bind.type > CMapB#bind.type of
                true ->     expr_comp(reverse(Op), CMapB, CMapA1);
                false ->    expr_comp(Op, CMapA1, CMapB)
            end;
        {_,0} ->
            CMapB1 = expr(B,FullMap,CMapA),
            case CMapA#bind.type > CMapB1#bind.type of
                true ->     expr_comp(reverse(Op), CMapB1, CMapA);
                false ->    expr_comp(Op, CMapA, CMapB1)
            end;
        {_,_} ->
            case CMapA#bind.type > CMapB#bind.type of    
                true->      expr_comp(reverse(Op), CMapB, CMapA);
                false ->    expr_comp(Op, CMapA, CMapB)
            end
    end,
    #bind{type=boolean,btree=BTree};
expr({'in', ?nav, {list,_}}, _FullMap, _) -> ?nav;
expr({'in', _, {list,[]}}, _FullMap, _) -> false;
expr({'in', A, {list,[B|Rest]}}, FullMap, _) ->
    CMapA = expr({'=', A, B}, FullMap, #bind{}),
    CMapR = expr({'in', A, {list,Rest}}, FullMap, #bind{}),
    #bind{type=boolean,btree={'or',CMapA,CMapR}};
expr({'like',Str,Pat,<<>>}, FullMap, _) ->
    CMapA = expr(Str,FullMap,#bind{type=binstr,default=?nav}),
    CMapB = expr(Pat,FullMap,#bind{type=binstr,default=?nav}),
    #bind{type=boolean,btree={'is_like', CMapA, CMapB}};
expr({'regexp_like',Str,Pat,<<>>}, FullMap, _) ->
    CMapA = expr(Str,FullMap,#bind{type=binstr,default=?nav}),
    CMapB = expr(Pat,FullMap,#bind{type=binstr,default=?nav}),
    #bind{type=boolean,btree={'is_regexp_like', CMapA, CMapB}};
expr(RawExpr, _FullMap0, _Type) ->
    ?UnimplementedException({"Unsupported sql expression", RawExpr}).

default_to_number(#bind{type=datetime}=BT) -> BT;
default_to_number(#bind{type=timestamp}=BT) -> BT;
default_to_number(_) -> #bind{type=number,default=?nav}.

expr_concat(#bind{type=T}=CMapA, #bind{type=T}=CMapB) ->
    #bind{type=T,btree={'concat',CMapA,CMapB}};
expr_concat(#bind{type=list}=CMapA, #bind{type=string}=CMapB) ->
    #bind{type=list,btree={'concat',CMapA,CMapB}};
expr_concat(#bind{type=string}=CMapA, #bind{type=list}=CMapB) ->
    #bind{type=list,btree={'concat',CMapA,CMapB}};
expr_concat(CMapA, #bind{type=TB}=CMapB) when TB==list;TB==string ->
    #bind{type=TB,btree={'concat',{'to_string',CMapA},CMapB}};
expr_concat(#bind{type=TA}=CMapA, CMapB) when TA==list;TA==string ->
    #bind{type=TA,btree={'concat',CMapA,{'to_string',CMapB}}}.

expr_math(Op, CMapA, CMapB, BT) ->
    case C={CMapA#bind.type,CMapB#bind.type,Op,BT#bind.type} of
        {decimal,_,_,_} ->
            ?UnimplementedException({"Unsupported number conversion", C});
        {_,decimal,_,_} -> 
            ?UnimplementedException({"Unsupported number conversion", C});
        {_,_,_,decimal} -> 
            ?UnimplementedException({"Unsupported number conversion", C});
        {_,_,'/',_} ->
            #bind{type=float,btree={Op,CMapA,CMapB}};
        {integer,integer,_,_} ->
            #bind{type=integer,btree={Op,CMapA,CMapB}};
        {float,_,_,_} ->
            #bind{type=float,btree={Op,CMapA,CMapB}};
        {_,float,_,_} ->
            #bind{type=float,btree={Op,CMapA,CMapB}};
        {_,_,_,_} ->
            #bind{type=number,btree={Op,CMapA,CMapB}}
    end.

expr_time(Op, CMapA, CMapB, BT) ->
    case C={CMapA#bind.type,CMapB#bind.type,Op,BT#bind.type} of
        {datetime,datetime,'-', T} when T==integer;T==float;T==number;T==undefined->
            #bind{type=float,btree={'diff_dt',CMapA,CMapB}};
        {timestamp,timestamp,'-', T} when T==integer;T==float;T==number;T==undefined->
            #bind{type=float,btree={'diff_ts',CMapA,CMapB}};
        {_,_,_,RT} when RT/=timestamp, RT/=datetime, RT/=binstr -> 
            ?ClientError({"Invalid time arithmetic", C});
        {datetime,T,'+',_} when T==integer;T==float;T==number ->
            #bind{type=datetime,btree={'add_dt',CMapA,CMapB}};
        {datetime,T,'-',_} when T==integer;T==float;T==number ->
            #bind{type=datetime,btree={'add_dt',CMapA,{'-',CMapB}}};
        {T,datetime,'+',_} when T==integer;T==float;T==number ->
            #bind{type=datetime,btree={'add_dt',CMapB,CMapA}};
        {T,datetime,'-',_} when T==integer;T==float;T==number ->
            #bind{type=datetime,btree={'add_dt',CMapB,{'-',CMapA}}};
        {timestamp,T,'+',_} when T==integer;T==float;T==number ->
            #bind{type=timestamp,btree={'add_ts',CMapA,CMapB}};
        {timestamp,T,'-',_} when T==integer;T==float;T==number ->
            #bind{type=timestamp,btree={'add_ts',CMapA,{'-',CMapB}}};
        {T,timestamp,'+',_} when T==integer;T==float;T==number ->
            #bind{type=timestamp,btree={'add_ts',CMapB,CMapA}};
        {T,timestamp,'-',_} when T==integer;T==float;T==number ->
            #bind{type=timestamp,btree={'add_ts',CMapB,{'-',CMapA}}};
        {_,_,_,_} ->
            ?UnimplementedException({"Unsupported time arithmetic", C})
    end.

expr_comp('=', A, B) -> expr_comp('==', A, B);
expr_comp('<>', A, B) -> expr_comp('/=', A, B);
expr_comp('<=', A, B) -> expr_comp('=<', A, B);
expr_comp(Op, #bind{type=T}=CMapA, #bind{type=T}=CMapB) ->
    {Op, CMapA, CMapB};                           %% equal types, direct comparison
expr_comp(Op, #bind{type=binstr,btree=BTA}, #bind{type=string}=CMapB) ->
    {Op, binstr_to_string(BTA), CMapB};                           
expr_comp(Op, #bind{type=decimal,prec=0}=CMapA, #bind{type=integer}=CMapB) ->
    {Op, CMapA, CMapB}; 
expr_comp(Op, #bind{type=decimal}=CMapA, #bind{type=integer}=CMapB) ->
    {Op, CMapA, integer_to_decimal(CMapB,CMapA#bind.prec)};  %% convert integer to decimal before comparing   
expr_comp(Op, #bind{type=decimal}=CMapA, #bind{type=T}=CMapB) when T==float;T==integer;T==number ->
    {Op, decimal_to_float(CMapA,CMapA#bind.prec), CMapB};    %% convert decimal to float before comparing
expr_comp(Op, #bind{type=T}=CMapA, #bind{type=number}=CMapB) when T==float;T==integer ->
    {Op, CMapA, CMapB};                           %% compatible types, direct comparison 
expr_comp(Op, CMapA, CMapB) ->
    {Op, CMapA, CMapB}.                           %% erlang can compare anything
    %% ?ClientError({"Incompatible types for comparison",{Op, n_or_t(CMapA), n_or_t(CMapB)}}).

% n_or_t(#bind{name=undefined,type=Type}) -> Type;
% n_or_t(#bind{name=Name}) -> Name.

binstr_to_string(B) when is_binary(B) -> binary_to_list(B);
binstr_to_string(#bind{btree=BTree}=B) -> B#bind{type=string,btree={'to_string',BTree}}.

% string_to_binstr(S) when is_list(S) -> list_to_binary(S);
% string_to_binstr(#bind{btree=BTree}=S) -> S#bind{type=binstr,btree={'to_binstr',BTree}}.

integer_to_decimal(I , 0) when is_integer(I)-> 
    I;
integer_to_decimal(I , Prec) when is_integer(I), is_integer(Prec), Prec>=0 -> 
    erlang:round(math:pow(10, Prec)) * I;
integer_to_decimal(#bind{btree=BTree}=I,0) -> 
    I#bind{type=integer,btree=BTree};
integer_to_decimal(#bind{btree=BTree}=I,Prec) when is_integer(Prec), Prec>=0 -> 
    M = erlang:round(math:pow(10, Prec)),
    I#bind{type=decimal,prec=Prec,btree={'*',M,BTree}};
integer_to_decimal(_ , _) -> ?nav.

decimal_to_float(D , 0) when is_integer(D) -> 
    D;
decimal_to_float(D , Prec) when is_integer(D), is_integer(Prec), Prec>=0 -> 
    math:pow(10, -Prec) * D;
decimal_to_float(#bind{btree=BTree}=D, 0)  -> 
    D#bind{type=float,btree=BTree};
decimal_to_float(#bind{btree=BTree}=D,Prec) when is_integer(Prec), Prec>=0 -> 
    F = math:pow(10, -Prec),
    D#bind{type=float,btree={'*',F,BTree}};
decimal_to_float(_ , _) -> ?nav.

reverse('=') -> '=';
reverse('<>') -> '<>';
reverse('>=') -> '<=';
reverse('<=') -> '>=';
reverse('<') -> '>';
reverse('>') -> '<';
reverse(OP) -> ?UnimplementedException({"Cannot reverse sql operator",OP}).

%% Split guard into two pieces:
%% -  a scan guard for mnesia
%% -  a filter guard to be applied to the scan result set
split_filter_from_guard(true) -> {true,true};
split_filter_from_guard(false) -> {false,false};
split_filter_from_guard({'and',L, R}) ->
    case {uses_filter(L),uses_filter(R)} of
        {true,true} ->      {true, {'and',L, R}};
        {true,false} ->     {R, L};
        {false,true} ->     {L, R};
        {false,false} ->    {{'and',L, R},true}
    end;
split_filter_from_guard(Guard) ->
    case uses_filter(Guard) of
        true ->             {true, Guard};
        false ->            {Guard,true}
    end.

sort_fun(SelectSections,FullMap,ColMap) ->
    case lists:keyfind('order by', 1, SelectSections) of
        {_, []} ->      fun(_X) -> {} end;
        {_, Sorts} ->   ?Debug("Sorts: ~p~n", [Sorts]),
                        SortFuns = [sort_fun_item(Name,Direction,FullMap,ColMap) || {Name,Direction} <- Sorts],
                        fun(X) -> list_to_tuple([F(X)|| F <- SortFuns]) end;
        SError ->       ?ClientError({"Invalid order by in select structure", SError})
    end.

sort_spec(SelectSections,FullMap,ColMap) ->
    case lists:keyfind('order by', 1, SelectSections) of
        {_, []} ->      [];
        {_, Sorts} ->   ?Debug("Sorts: ~p~n", [Sorts]),
                        [sort_spec_item(Name,Direction,FullMap,ColMap) || {Name,Direction} <- Sorts];
        SError ->       ?ClientError({"Invalid order by in select structure", SError})
    end.

sort_spec_item(Expr,<<>>,FullMap,ColMap) ->
    sort_spec_item(Expr,<<"asc">>,FullMap,ColMap);
sort_spec_item(Expr,Direction,_FullMap,ColMap) ->
    % ?LogDebug("Sort Expression ~p~n",[Expr]),
    % ?LogDebug("Sort ColMap~n~p~n",[ColMap]),
    % ?LogDebug("Sort FullMap~n~p~n",[FullMap]),
    IDs = case (catch list_to_integer(binary_to_list(Expr))) of
        CP when is_integer(CP) ->
            [ Tag || #bind{tag=Tag} <- ColMap, Tag==CP];     %% Index to select column given
        _ ->
            case [ Tag || #bind{tag=Tag,alias=A} <- ColMap, A==Expr] of
                [] ->   
                    case [ Tag || #bind{tag=Tag,ptree=PTree} <- ColMap, PTree==Expr] of
                        [] ->   [sqlparse:pt_to_string({fields,[Expr]})]; 
                        TT ->   TT      %% parse tree found (identical to select expression)
                    end;
                TA ->
                    TA  %% select column alias given
            end  
    end,
    case IDs of
        [] ->   ?UnimplementedException({"Unknown sort field name", Expr});
        [ID] -> {ID,Direction};
        _ ->    ?ClientError({"Ambiguous column name in sort spec", Expr})
    end.

sort_fun_item(Expr,<<>>,FullMap,ColMap) ->
    sort_fun_item(Expr,<<"asc">>,FullMap,ColMap);
sort_fun_item(Expr,Direction,FullMap,ColMap) ->
    ML = case (catch list_to_integer(binary_to_list(Expr))) of
        CP when is_integer(CP) ->
            [ B || #bind{tag=Tag}=B <- ColMap, Tag==CP];     %% Index to select column given
        _ ->
            case [ B || #bind{alias=A}=B <- ColMap, A==Expr] of
                [] ->   
                    case [ B || #bind{alias=PTree}=B <- ColMap, PTree==Expr] of
                        [] ->
                            case bind_subtree_const(expr(Expr,FullMap,#bind{})) of
                                #bind{tind=0,cind=0,btree=BT}=B0 -> 
                                    [B0#bind{func=imem_sql_funs:expr_fun(BT)}];
                                B1 ->
                                    [B1]
                            end; 
                        BT ->
                            BT  %% parse tree found (identical to select expression)
                    end;
                BA ->
                    BA  %% select column alias given
            end  
    end,
    case ML of
        [] ->   
            ?UnimplementedException({"Unsupported sort expression or unknown sort field name", Expr});
        [#bind{tind=0,cind=0,type=Type,func=Func}] when is_function(Func) ->    
            sort_fun_impl(Type,Func,Direction);
        [#bind{tind=0,cind=0,type=Type,func=Func}] ->  %% TODO: constant, could be ignored in sort
            F = fun(_) -> Func end, 
            sort_fun_impl(Type,F,Direction);
        [#bind{type=Type}=Bind] ->    
            Func = fun(X) -> ?BoundVal(Bind,X) end, 
            sort_fun_impl(Type,Func,Direction);
        _ ->    
            ?ClientError({"Ambiguous column name in order by clause", Expr})
    end.

filter_spec_where(?NoMoreFilter, _, WhereTree) -> 
    WhereTree;
filter_spec_where({FType,[ColF|ColFs]}, ColMap, WhereTree) ->
    FCond = filter_condition(ColF, ColMap),
    filter_spec_where({FType,ColFs}, ColMap, WhereTree, FCond). 

filter_spec_where(?NoMoreFilter, _, ?EmptyWhere, LeftTree) ->
    LeftTree;
filter_spec_where(?NoMoreFilter, _, WhereTree, LeftTree) ->
    {'and', LeftTree, WhereTree};
filter_spec_where({FType,[ColF|ColFs]}, ColMap, WhereTree, LeftTree) ->
    FCond = filter_condition(ColF, ColMap),
    filter_spec_where({FType,ColFs}, ColMap, WhereTree, {FType,LeftTree,FCond}).    

filter_condition({Idx,[Val]}, ColMap) ->
    #bind{schema=S,table=T,name=N,type=Type,len=L,prec=P,default=D} = lists:nth(Idx,ColMap),
    Tag = "Col" ++ integer_to_list(Idx),
    Value = filter_field_value(Tag,Type,L,P,D,Val),     % list_to_binary(
    {'=',qname3_to_binstr({S,T,N}),Value};
filter_condition({Idx,Vals}, ColMap) ->
    #bind{schema=S,table=T,name=N,type=Type,len=L,prec=P,default=D} = lists:nth(Idx,ColMap),
    Tag = "Col" ++ integer_to_list(Idx),
    Values = [filter_field_value(Tag,Type,L,P,D,Val) || Val <- Vals],       % list_to_binary(
    {'in',qname3_to_binstr({S,T,N}),{'list',Values}}.

filter_field_value(_Tag,integer,_Len,_Prec,_Def,Val) -> Val;
filter_field_value(_Tag,float,_Len,_Prec,_Def,Val) -> Val;
filter_field_value(_Tag,decimal,_Len,_Prec,_Def,Val) -> Val;
filter_field_value(_Tag,number,_Len,_Prec,_Def,Val) -> Val;
filter_field_value(_Tag,_Type,_Len,_Prec,_Def,Val) -> imem_datatype:add_squotes(imem_sql:escape_sql(Val)).    

sort_spec_order([],_,_) -> [];
sort_spec_order(SortSpec,FullMap,ColMap) ->
    sort_spec_order(SortSpec,FullMap,ColMap,[]).

sort_spec_order([],_,_,Acc) -> 
    lists:reverse(Acc);        
sort_spec_order([SS|SortSpecs],FullMap,ColMap, Acc) ->
    sort_spec_order(SortSpecs,FullMap,ColMap,[sort_order(SS,FullMap,ColMap)|Acc]).

sort_order({Ti,Ci,Direction},FullMap,_ColMap) ->
    %% SortSpec given referencing FullMap Ti,Ci    
    case [{S,T,A,N} || #bind{tind=Tind,cind=Cind,schema=S,table=T,alias=A,name=N} <- FullMap, Tind==Ti, Cind==Ci] of
        [{_,Tab,Tab,Name}] ->  
            {Name,Direction};
        [{_,_,Alias,Name}] ->  
            {qname2_to_binstr({Alias,Name}),Direction};
        _ ->       
            ?ClientError({"Bad sort field reference", {Ti,Ci}})
    end;
sort_order({Cp,Direction},_FullMap,_ColMap) when is_integer(Cp) ->
    %% SortSpec given referencing ColMap position    
    %% #bind{alias=A} = lists:nth(Cp,ColMap),
    %% {A,Direction};
    {list_to_binary(integer_to_list(Cp)),Direction};
sort_order({CName,Direction},_,_) ->
    {CName,Direction}.

sort_spec_fun([],_,_) -> 
    fun(_X) -> {} end;
sort_spec_fun(SortSpec,FullMap,ColMap) ->
    SortFuns = sort_spec_fun(SortSpec,FullMap,ColMap,[]),
    fun(X) -> list_to_tuple([F(X)|| F <- SortFuns]) end.

sort_spec_fun([],_,_,Acc) -> lists:reverse(Acc);
sort_spec_fun([SS|SortSpecs],FullMap,ColMap,Acc) ->
    sort_spec_fun(SortSpecs,FullMap,ColMap,[sort_fun_any(SS,FullMap,ColMap)|Acc]).

sort_fun_any({Ti,Ci,Direction},FullMap,_) ->
    %% SortSpec given referencing FullMap Ti,Ci    
    case [B || #bind{tind=Tind,cind=Cind}=B <- FullMap, Tind==Ti, Cind==Ci] of
        [Bind] ->
            Func = fun(X) -> ?BoundVal(Bind,X) end, 
            sort_fun_impl(Bind#bind.type,Func,Direction);
        Else ->     
            ?ClientError({"Bad sort field binding", Else})
    end;
sort_fun_any({Cp,Direction},_,ColMap) when is_integer(Cp) ->
    %% SortSpec given referencing ColMap position
    case lists:nth(Cp,ColMap) of
        #bind{tind=0,cind=0,type=T,func=Func} when is_function(Func) -> 
            sort_fun_impl(T,Func,Direction);
        #bind{tind=0,cind=0,type=T,func=Func} ->
            F = fun(_) -> Func end, 
            sort_fun_impl(T,F,Direction);
        Bind -> 
            Func = fun(X) -> ?BoundVal(Bind,X) end, 
            sort_fun_impl(Bind#bind.type,Func,Direction)
    end.

sort_fun_impl(atom,F,<<"desc">>) -> 
    fun(X) -> 
        case F(X) of 
            A when is_atom(A) ->
                [ -Item || Item <- atom_to_list(A)] ++ [?MaxChar];
            V -> V
        end
    end;
sort_fun_impl(binstr,F,<<"desc">>) -> 
    fun(X) -> 
        case F(X) of 
            B when is_binary(B) ->
                [ -Item || Item <- binary_to_list(B)] ++ [?MaxChar];
            V -> V
        end
    end;
sort_fun_impl(binterm,F,<<"desc">>) -> 
    fun(X) -> 
        case F(X) of 
            B when is_binary(B) ->
                [ -Item || Item <- binary_to_list(B)] ++ [?MaxChar];
            V -> V
        end
    end;
sort_fun_impl(boolean,F,<<"desc">>) ->
    fun(X) -> 
        V = F(X),
        case V of
            true ->         false;
            false ->        true;
            _ ->            V
        end 
    end;
sort_fun_impl(datetime,F,<<"desc">>) -> 
    fun(X) -> 
        case F(X) of 
            {{Y,M,D},{Hh,Mm,Ss}} when is_integer(Y), is_integer(M), is_integer(D), is_integer(Hh), is_integer(Mm), is_integer(Ss) -> 
                {{-Y,-M,-D},{-Hh,-Mm,-Ss}};
            V -> V
        end 
    end;
sort_fun_impl(decimal,F,<<"desc">>) -> sort_fun_impl(number,F,<<"desc">>);
sort_fun_impl(float,F,<<"desc">>) ->   sort_fun_impl(number,F,<<"desc">>);
sort_fun_impl(integer,F,<<"desc">>) -> sort_fun_impl(number,F,<<"desc">>);
sort_fun_impl(ipadr,F,<<"desc">>) -> 
    fun(X) -> 
        case F(X) of 
            {A,B,C,D} when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
                {-A,-B,-C,-D};
            {A,B,C,D,E,F,G,H} when is_integer(A), is_integer(B), is_integer(C), is_integer(D), is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
                {-A,-B,-C,-D,-E,-F,-G,-H};
            V -> V
        end
    end;
sort_fun_impl(number,F,<<"desc">>) ->
    fun(X) -> 
        V = F(X),
        case is_number(V) of
            true ->         (-V);
            false ->        V
        end 
    end;
sort_fun_impl(string,F,<<"desc">>) -> 
    fun(X) -> 
        case F(X) of 
            [H|T] when is_integer(H) ->
                [ -Item || Item <- [H|T]] ++ [?MaxChar];
            V -> V
        end
    end;
sort_fun_impl(timestamp,F,<<"desc">>) -> 
    fun(X) -> 
        case F(X) of 
            {Meg,Sec,Micro} when is_integer(Meg), is_integer(Sec), is_integer(Micro)->
                {-Meg,-Sec,-Micro};
            V -> V
        end    
    end;
sort_fun_impl({atom,atom},F,<<"desc">>) ->
    fun(X) -> 
        case F(X) of 
            {T,A} when is_atom(T), is_atom(A) ->
                {[ -ItemT || ItemT <- atom_to_list(T)] ++ [?MaxChar]
                ,[ -ItemA || ItemA <- atom_to_list(A)] ++ [?MaxChar]
                };
            V -> V
        end    
    end;
sort_fun_impl({atom,integer},F,<<"desc">>) ->    sort_fun_impl({atom,number},F,<<"desc">>);
sort_fun_impl({atom,decimal},F,<<"desc">>) ->    sort_fun_impl({atom,number},F,<<"desc">>);
sort_fun_impl({atom,float},F,<<"desc">>) ->      sort_fun_impl({atom,number},F,<<"desc">>);
sort_fun_impl({atom,userid},F,<<"desc">>) ->     sort_fun_impl({atom,number},F,<<"desc">>);
sort_fun_impl({atom,number},F,<<"desc">>) ->
    fun(X) -> 
        case F(X) of 
            {T,N} when is_atom(T), is_number(N) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-N};
            V -> V
        end    
    end;
sort_fun_impl({atom,ipaddr},F,<<"desc">>) ->
    fun(X) -> 
        case F(X) of 
            {T,{A,B,C,D}} when is_atom(T), is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-A,-B,-C,-D};
            {T,{A,B,C,D,E,F,G,H}} when is_atom(T),is_integer(A), is_integer(B), is_integer(C), is_integer(D), is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-A,-B,-C,-D,-E,-F,-G,-H};
            V -> V   
        end    
    end;
sort_fun_impl(tuple,F,<<"desc">>) -> 
    fun(X) -> 
        case F(X) of 
            {T,A} when is_atom(T), is_atom(A) ->
                {[ -ItemT || ItemT <- atom_to_list(T)] ++ [?MaxChar]
                ,[ -ItemA || ItemA <- atom_to_list(A)] ++ [?MaxChar]
                };
            {T,N} when is_atom(T), is_number(N) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-N};
            {T,{A,B,C,D}} when is_atom(T), is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-A,-B,-C,-D};
            {T,{A,B,C,D,E,F,G,H}} when is_atom(T),is_integer(A), is_integer(B), is_integer(C), is_integer(D), is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-A,-B,-C,-D,-E,-F,-G,-H};
            {T,R} when is_atom(T) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar], R};
            V -> V
        end    
    end;
sort_fun_impl(userid,F,<<"desc">>) ->    sort_fun_impl(number,F,<<"desc">>);
sort_fun_impl(Type,_F,<<"desc">>) ->     ?UnimplementedException({"Unsupported datatype for sort desc", Type});
sort_fun_impl(_,F,_) ->                  F.

% pick(Ci,Ti,X) -> pick(Ci,element(Ti,X)).

% pick(_,undefined) -> ?nav;
% pick(Ci,Tuple) -> element(Ci,Tuple).


%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) ->
    catch imem_meta:drop_table(meta_table_3), 
    catch imem_meta:drop_table(meta_table_2), 
    catch imem_meta:drop_table(meta_table_1), 
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun test_without_sec/1
        ]}
    }.
    
test_without_sec(_) -> 
    test_with_or_without_sec(false).

test_with_or_without_sec(IsSec) ->
    try
        ClEr = 'ClientError',
        ?LogDebug("----------------------------------~n"),
        ?LogDebug("---TEST--- ~p ----Security ~p", [?MODULE, IsSec]),
        ?LogDebug("----------------------------------~n"),

        ?LogDebug("schema ~p~n", [imem_meta:schema()]),
        ?LogDebug("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        % field names
        ?assertEqual({undefined,undefined,<<"field">>}, binstr_to_qname3(<<"field">>)),
        ?assertEqual({undefined,<<"table">>,<<"field">>}, binstr_to_qname3(<<"table.field">>)),
        ?assertEqual({<<"schema">>,<<"table">>,<<"field">>}, binstr_to_qname3(<<"schema.table.field">>)),

        ?assertEqual(<<"field">>, qname3_to_binstr(binstr_to_qname3(<<"field">>))),
        ?assertEqual(<<"table.field">>, qname3_to_binstr(binstr_to_qname3(<<"table.field">>))),
        ?assertEqual(<<"schema.table.field">>, qname3_to_binstr(binstr_to_qname3(<<"schema.table.field">>))),

        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?LogDebug("success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        ?LogDebug("success ~p~n", [data_nodes]),

    %% uses_filter
        ?assertEqual(true, uses_filter({'is_member', {'+','$2',1}, '$3'})),
        ?assertEqual(false, uses_filter({'==', {'+','$2',1}, '$3'})),
        ?assertEqual(true, uses_filter({'==', {'safe',{'+','$2',1}}, '$3'})),
        ?assertEqual(false, uses_filter({'or', {'==','$2',1}, {'==','$3',1}})),
        ?assertEqual(true, uses_filter({'and', {'==','$2',1}, {'is_member',1,'$3'}})),

        BTreeSample = 
            {'>',{ bind,2,7,<<"imem">>,<<"ddAccount">>,<<"ddAccount">>,<<"lastLoginTime">>,
                   datetime,undefined,undefined,undefined,false,undefined,undefined,undefined,'$27'}
                ,{ bind,0,0,undefined,undefined,undefined,undefined,datetime,0,0,undefined,false,undefined,undefined
                    , {add_dt, {bind,1,4,<<"imem">>,<<"meta">>,<<"meta">>,<<"sysdate">>,
                                datetime,20,0,undefined,true,undefined,undefined,undefined,'$14'}
                             , {'-', {bind,0,0,undefined,undefined,undefined,undefined,
                                      float,0,0,undefined,true,undefined,undefined,1.1574074074074073e-5,[]}
                               }
                      }
                    ,[]
                }
            },
        % ?assertEqual(true, uses_bind(2,7,BTreeSample)),
        % ?assertEqual(false, uses_bind(2,6,BTreeSample)),
        % ?assertEqual(true, uses_bind(1,4,BTreeSample)),
        % ?assertEqual(true, uses_bind(0,0,BTreeSample)),

        ColMapSample =  { bind,0,0,undefined,undefined,<<"'a' || 'b123'">>,undefined,binstr,0,0,undefined,false,undefined
                        , {'||',<<"'a'">>,<<"'b123'">>}
                        , {concat,{bind,0,0,undefined,undefined,undefined,undefined,binstr,0,0,<<>>,true,undefined,undefined,<<"a">>,[]}
                                 ,{bind,0,0,undefined,undefined,undefined,undefined,binstr,0,0,<<>>,true,undefined,undefined,<<"b123">>,[]}
                          }
                        , 1
                        },
        ColMapExpected= { bind,0,0,undefined,undefined,<<"'a' || 'b123'">>,undefined,binstr,0,0,undefined,false,undefined
                        , {'||',<<"'a'">>,<<"'b123'">>}
                        , <<"ab123">>
                        , 1
                        },
        ?assertEqual(ColMapExpected, bind_subtree_const(ColMapSample)),

        ?LogDebug("~p:test_database_operations~n", [?MODULE]),
        _Types1 =    [ #ddColumn{name=a, type=char, len=1}     %% key
                    , #ddColumn{name=b1, type=char, len=1}    %% value 1
                    , #ddColumn{name=c1, type=char, len=1}    %% value 2
                    ],
        _Types2 =    [ #ddColumn{name=a, type=integer, len=10}    %% key
                    , #ddColumn{name=b2, type=float, len=8, prec=3}   %% value
                    ],

        ?assertEqual(ok, imem_sql:exec(anySKey, "create table meta_table_1 (a char, b1 char, c1 char);", 0, "imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [anySKey, meta_table_1])),    

        ?assertEqual(ok, imem_sql:exec(anySKey, "create table meta_table_2 (a integer, b2 float);", 0, "imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [anySKey, meta_table_2])),    

        ?assertEqual(ok, imem_sql:exec(anySKey, "create table meta_table_3 (a char, b3 integer, c1 char);", 0, "imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [anySKey, meta_table_1])),    
        ?LogDebug("success ~p~n", [create_tables]),

        Table1 =    <<"imem.meta_table_1">>,
        Table2 =    <<"meta_table_2">>,
        Table3 =    <<"meta_table_3">>,
        TableX =    {as, <<"meta_table_x">>, <<"meta_table_1">>},

        Alias1 =    {as, <<"meta_table_1">>, <<"alias1">>},
        Alias2 =    {as, <<"imem.meta_table_1">>, <<"alias2">>},

        ?assertException(throw, {ClEr, {"Table does not exist", {imem, meta_table_x}}}, column_map_tables([Table1,TableX,Table3],[],[])),
        ?LogDebug("success ~p~n", [table_no_exists]),

        FullMap0 =  column_map_tables([],imem_meta:meta_field_list(),[]),
        ?LogDebug("FullMap0~n~p~n", [FullMap0]),
        MetaFieldCount = length(imem_meta:meta_field_list()),
        ?assertEqual(MetaFieldCount, length(FullMap0)),

        FullMap1 = column_map_tables([Table1],imem_meta:meta_field_list(),[]),
        ?assertEqual(MetaFieldCount+3, length(FullMap1)),
        ?LogDebug("success ~p~n", [full_map_1]),

        FullMap13 = column_map_tables([Table1,Table3],imem_meta:meta_field_list(),[]),
        ?assertEqual(MetaFieldCount+6, length(FullMap13)),
        ?LogDebug("success ~p~n", [full_map_13]),

        FullMap123 = column_map_tables([Table1,Table2,Table3],imem_meta:meta_field_list(),[]),
        ?assertEqual(MetaFieldCount+8, length(FullMap123)),
        ?LogDebug("success ~p~n", [full_map_123]),

        AliasMap1 = column_map_tables([Alias1],imem_meta:meta_field_list(),[]),
        ?LogDebug("AliasMap1~n~p~n", [AliasMap1]),
        ?assertEqual(MetaFieldCount+3, length(AliasMap1)),
        ?LogDebug("success ~p~n", [alias_map_1]),

        AliasMap123 = column_map_tables([Alias1,Alias2,Table3],imem_meta:meta_field_list(),[]),    
        %% select from 
        %%            meta_table_1 as alias1        (a char, b1 char    , c1 char)
        %%          , imem.meta_table1 as alias2    (a char, b1 char    , c1 char)
        %%          , meta_table_3                  (a char, b3 integer , c1 char)
        ?LogDebug("AliasMap123~n~p~n", [AliasMap123]),
        ?assertEqual(MetaFieldCount+9, length(AliasMap123)),
        ?LogDebug("success ~p~n", [alias_map_123]),

        % ColsE1=     [ #bind{tag="A1", schema= <<"imem">>, table= <<"meta_table_1">>, name= <<"a">>}
        %             , #bind{tag="A2", name= <<"x">>}
        %             , #bind{tag="A3", name= <<"c1">>}
        %             ],
        ColsE1=     [ <<"imem.meta_table_1.a">>
                    , <<"x">>
                    , <<"c1">>
                    ],

        ?assertException(throw, {ClEr,{"Unknown field or table name", <<"x">>}}, column_map_columns(ColsE1,FullMap1)),
        ?LogDebug("success ~p~n", [unknown_column_name_1]),

        % ColsE2=     [ #bind{tag="A1", schema= <<"imem">>, table= <<"meta_table_1">>, name= <<"a">>}
        %             , #bind{tag="A2", table= <<"meta_table_x">>, name= <<"b1">>}
        %             , #bind{tag="A3", name= <<"c1">>}
        %             ],
        ColsE2=     [ <<"imem.meta_table_1.a">>
                    , <<"meta_table_x.b1">>
                    , <<"c1">>
                    ],

        ?assertException(throw, {ClEr,{"Unknown field or table name", <<"meta_table_x.b1">>}}, column_map_columns(ColsE2,FullMap1)),
        ?LogDebug("success ~p~n", [unknown_column_name_2]),

        % ColsF =     [ {as, <<"imem.meta_table_1.a">>, <<"a">>}
        %             , {as, <<"meta_table_1.b1">>, <<"b1">>}
        %             , {as, <<"c1">>, <<"c1">>}
        %             ],

        ColsA =     [ {as, <<"imem.meta_table_1.a">>, <<"a">>}
                    , {as, <<"meta_table_1.b1">>, <<"b1">>}
                    , {as, <<"c1">>, <<"c1">>}
                    ],

        ?assertException(throw, {ClEr,{"Ambiguous field or table name", <<"a">>}}, column_map_columns([<<"a">>],FullMap13)),
        ?LogDebug("success ~p~n", [columns_ambiguous_a]),

        ?assertException(throw, {ClEr,{"Ambiguous field or table name", <<"c1">>}}, column_map_columns(ColsA,FullMap13)),
        ?LogDebug("success ~p~n", [columns_ambiguous_c1]),

        ?assertEqual(3, length(column_map_columns(ColsA,FullMap1))),
        ?LogDebug("success ~p~n", [columns_A]),

        ?assertEqual(6, length(column_map_columns([<<"*">>],FullMap13))),
        ?LogDebug("success ~p~n", [columns_13_join]),

        Cmap3 = column_map_columns([<<"*">>], FullMap123),
        % ?LogDebug("ColMap3 ~p~n", [Cmap3]),        
        ?assertEqual(8, length(Cmap3)),
        ?assertEqual(lists:sort(Cmap3), Cmap3),
        ?LogDebug("success ~p~n", [columns_123_join]),


        ?LogDebug("AliasMap1~n~p~n", [AliasMap1]),

        Abind1 = column_map_columns([<<"*">>],AliasMap1),
        ?LogDebug("AliasBind1~n~p~n", [Abind1]),        

        Abind2 = column_map_columns([<<"alias1.*">>],AliasMap1),
        ?LogDebug("AliasBind2~n~p~n", [Abind2]),        
        ?assertEqual(Abind1, Abind2),

        Abind3 = column_map_columns([<<"imem.alias1.*">>],AliasMap1),
        ?LogDebug("AliasBind3~n~p~n", [Abind3]),        
        ?assertEqual(Abind1, Abind3),

        ?assertEqual(3, length(Abind1)),
        ?LogDebug("success ~p~n", [alias_1]),

        ?assertEqual(9, length(column_map_columns([<<"*">>],AliasMap123))),
        ?LogDebug("success ~p~n", [alias_113_join]),

        ?assertEqual(3, length(column_map_columns([<<"meta_table_3.*">>],AliasMap123))),
        ?LogDebug("success ~p~n", [columns_113_star1]),

        ?assertEqual(4, length(column_map_columns([<<"alias1.*">>,<<"meta_table_3.a">>],AliasMap123))),
        ?LogDebug("success ~p~n", [columns_alias_1]),

        ?assertEqual(2, length(column_map_columns([<<"alias1.a">>,<<"alias2.a">>],AliasMap123))),
        ?LogDebug("success ~p~n", [columns_alias_2]),

        ?assertEqual(2, length(column_map_columns([<<"alias1.a">>,<<"sysdate">>],AliasMap1))),
        ?LogDebug("success ~p~n", [sysdate]),

        ?assertException(throw, {ClEr,{"Unknown field or table name",  <<"any.sysdate">>}}, column_map_columns([<<"alias1.a">>,<<"any.sysdate">>],AliasMap1)),
        ?LogDebug("success ~p~n", [sysdate_reject]),

        ColsFS =    [ #bind{tag="A", tind=1, cind=1, schema= <<"imem">>, table= <<"meta_table_1">>, name= <<"a">>, type=integer, alias= <<"a">>}
                    , #bind{tag="B", tind=1, cind=2, table= <<"meta_table_1">>, name= <<"b1">>, type=string, alias= <<"b1">>}
                    , #bind{tag="C", tind=1, cind=3, name= <<"c1">>, type=ipaddr, alias= <<"c1">>}
                    ],

        ?assertEqual([], filter_spec_where(?NoFilter, ColsFS, [])),
        ?assertEqual({wt}, filter_spec_where(?NoFilter, ColsFS, {wt})),
        FA1 = {1,[<<"111">>]},
        CA1 = {'=',<<"imem.meta_table_1.a">>,<<"111">>},
        ?assertEqual({'and',CA1,{wt}}, filter_spec_where({'or',[FA1]}, ColsFS, {wt})),
        FB2 = {2,[<<"222">>]},
        CB2 = {'=',<<"meta_table_1.b1">>,<<"'222'">>},
        ?assertEqual({'and',{'and',CA1,CB2},{wt}}, filter_spec_where({'and',[FA1,FB2]}, ColsFS, {wt})),
        FC3 = {3,[<<"3.1.2.3">>,<<"3.3.2.1">>]},
        CC3 = {'in',<<"c1">>,{'list',[<<"'3.1.2.3'">>,<<"'3.3.2.1'">>]}},
        ?assertEqual({'and',{'or',{'or',CA1,CB2},CC3},{wt}}, filter_spec_where({'or',[FA1,FB2,FC3]}, ColsFS, {wt})),
        ?assertEqual({'and',{'and',{'and',CA1,CB2},CC3},{wt}}, filter_spec_where({'and',[FA1,FB2,FC3]}, ColsFS, {wt})),

        FB2a = {2,[<<"22'2">>]},
        CB2a = {'=',<<"meta_table_1.b1">>,<<"'22''2'">>},
        ?assertEqual({'and',{'and',CA1,CB2a},{wt}}, filter_spec_where({'and',[FA1,FB2a]}, ColsFS, {wt})),

        ?LogDebug("success ~p~n", [filter_spec_where]),

        ?assertEqual([], sort_spec_order([], ColsFS, ColsFS)),
        SA = {1,1,<<"desc">>},
        OA = {<<"a.a">>,<<"desc">>}, %% bad test setup FullMap alias
        ?assertEqual([OA], sort_spec_order([SA], ColsFS, ColsFS)),
        SB = {1,2,<<"asc">>},
        OB = {<<"b1.b1">>,<<"asc">>}, %% bad test setup FullMap alias
        ?assertEqual([OB], sort_spec_order([SB], ColsFS, ColsFS)),
        SC = {1,3,<<"desc">>},
        OC = {<<"c1.c1">>,<<"desc">>}, %% bad test setup FullMap alias
        ?assertEqual([OC], sort_spec_order([SC], ColsFS, ColsFS)),
        ?assertEqual([OC,OA], sort_spec_order([SC,SA], ColsFS, ColsFS)),
        ?assertEqual([OB,OC,OA], sort_spec_order([SB,SC,SA], ColsFS, ColsFS)),

        ?assertEqual([OC], sort_spec_order([OC], ColsFS, ColsFS)),
        ?assertEqual([OC,OA], sort_spec_order([OC,SA], ColsFS, ColsFS)),
        ?assertEqual([OC,OA], sort_spec_order([SC,OA], ColsFS, ColsFS)),
        ?assertEqual([OB,OC,OA], sort_spec_order([OB,OC,OA], ColsFS, ColsFS)),

        ?LogDebug("success ~p~n", [sort_spec_order]),


        ?assertEqual(ok, imem_meta:drop_table(meta_table_3)),
        ?assertEqual(ok, imem_meta:drop_table(meta_table_2)),
        ?assertEqual(ok, imem_meta:drop_table(meta_table_1)),
        ?LogDebug("success ~p~n", [drop_tables]),

        case IsSec of
            true -> ?imem_logout(anySKey);
            _ ->    ok
        end
    catch
        Class:Reason ->  ?LogDebug("Exception~n~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

-endif.
