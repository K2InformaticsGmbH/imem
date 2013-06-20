-module(imem_sql).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-export([ exec/5
        , parse/1
        ]).

-define(MaxChar,16#FFFFFF).

-export([ field_qname/1
        , field_name/1
        , field_name/3
        , table_qname/1
        , table_qname/2
        , column_map_items/2
        , column_map/2
        , simplify_guard/1
        , create_scan_spec/4
        , operand_member/2
        , un_escape_sql/1
        , build_sort_fun/2
        , build_sort_spec/3
        , filter_spec_where/3
        , sort_spec_order/3
        , sort_spec_fun/3
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


field_qname(B) when is_binary(B) ->
    field_qname(binary_to_list(B));
field_qname(Str) when is_list(Str) ->
    case string:tokens(Str, ".") of
        [N] ->      {undefined, undefined, list_to_atom(N)};
        [T,N] ->    {undefined, list_to_atom(T), list_to_atom(N)};
        [S,T,N] ->  {list_to_atom(S), list_to_atom(T), list_to_atom(N)};
        _ ->        {}
        % _ ->        ?ClientError({"Invalid field name", Str})
    end;
field_qname(S) ->
    ?ClientError({"Invalid field name", S}).

field_name({S,T,N}) -> field_name(S,T,N).

field_name(undefined,undefined,N) -> atom_to_list(N); 
field_name(undefined,T,N) -> atom_to_list(T) ++ "." ++ atom_to_list(N); 
field_name(S,T,N) -> atom_to_list(S) ++ "." ++ atom_to_list(T) ++ "." ++ atom_to_list(N). 


table_qname(B) when is_binary(B) ->
    table_qname(binary_to_list(B));
table_qname(T) when is_atom(T) ->
    {imem_meta:schema(), T, T};
table_qname(Str) when is_list(Str) ->
    case string:tokens(Str, ".") of
        [T] ->      {imem_meta:schema(), list_to_atom(T), list_to_atom(T)};
        [S,T] ->    {list_to_atom(S), list_to_atom(T), list_to_atom(T)};
        _ ->        ?ClientError({"Invalid table name", Str})
    end;
table_qname({as,Table,Alias}) ->
    table_qname(Table, Alias);
table_qname({Table, Alias}) ->
    table_qname(Table, Alias);
table_qname(N) -> 
    ?ClientError({"Invalid table name", N}).

table_qname(Table, Alias) when is_binary(Alias) ->    
    table_qname(Table, binary_to_list(Alias));
table_qname(Table, Alias) when is_list(Alias) ->    
    table_qname(Table, list_to_atom(Alias));
table_qname(T, A) when is_atom(T), is_atom(A) ->
    {imem_meta:schema(), T, A};
table_qname({S, T}, A) when is_atom(S), is_atom(T), is_atom(A) ->
    {S, T, A};
table_qname(B , A) when is_binary(B), is_atom(A) ->
    table_qname(binary_to_list(B), A);
table_qname(Str, A) when is_list(Str), is_atom(A) ->
    case string:tokens(Str, ".") of
        [T] ->      {imem_meta:schema(), list_to_atom(T), A};
        [S,T] ->    {list_to_atom(S), list_to_atom(T), A};
        _ ->        ?ClientError({"Invalid table name", Str})
    end;
table_qname(S, _A) ->
    ?ClientError({"Invalid table name", S}).

column_map_items(Map, tag) ->
    [C#ddColMap.tag || C <- Map];
column_map_items(Map, schema) ->
    [C#ddColMap.schema || C <- Map];
column_map_items(Map, table) ->
    [C#ddColMap.table || C <- Map];
column_map_items(Map, name) ->
    [C#ddColMap.name || C <- Map];
column_map_items(Map, qname) ->
    [lists:flatten(io_lib:format("~p.~p.~p", [C#ddColMap.schema,C#ddColMap.table,C#ddColMap.name])) || C <- Map];
column_map_items(Map, qtype) ->
    [lists:flatten(io_lib:format("~p(~p,~p)", [C#ddColMap.type,C#ddColMap.len,C#ddColMap.prec])) || C <- Map];
column_map_items(Map, tind) ->
    [C#ddColMap.tind || C <- Map];
column_map_items(Map, cind) ->
    [C#ddColMap.cind || C <- Map];
column_map_items(Map, type) ->
    [C#ddColMap.type || C <- Map];
column_map_items(Map, len) ->
    [C#ddColMap.len || C <- Map];
column_map_items(Map, prec) ->
    [C#ddColMap.prec || C <- Map];
column_map_items(Map, ptree) ->
    [C#ddColMap.ptree || C <- Map];
column_map_items(_Map, Item) ->
    ?ClientError({"Invalid item",Item}).

column_map([], Columns) ->
    ?ClientError({"Empty table list",Columns});
column_map(Tables, []) ->
    column_map(Tables, [#ddColMap{name='*'}]);    
column_map(Tables, Columns) ->
    column_map([{S,imem_meta:physical_table_name(T),A}||{S,T,A} <- Tables], Columns, 1, [], [], []).

column_map([{undefined,Table,Alias}|Tables], Columns, Tindex, Lookup, Meta, Acc) ->
    column_map([{imem_meta:schema(),Table,Alias}|Tables], Columns, Tindex, Lookup, Meta, Acc);
column_map([{Schema,Table,Alias}|Tables], Columns, Tindex, Lookup, Meta, Acc) ->
    Cols = imem_meta:column_infos({Schema,Table}),
    L = [{Tindex, Cindex, Schema, Alias, Cinfo#ddColumn.name, Cinfo} || {Cindex, Cinfo} <- lists:zip(lists:seq(2,length(Cols)+1), Cols)],
    % ?Log("column_map lookup ~p~n", [Lookup++L]),
    % ?Log("column_map columns ~p~n", [Columns]),
    column_map(Tables, Columns, Tindex+1, Lookup++L, Meta, Acc);

column_map([], [#ddColMap{schema=undefined, table=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Log("column_map 1 ~p~n", [Cmap0]),
    Cmaps = [Cmap0#ddColMap{
                schema=S, table=T, tind=Ti, cind=Ci, type=Type, len=Len, prec=P, name=N, 
                alias=?atom_to_binary(N), ptree=?atom_to_binary(N)
            } ||  {Ti, Ci, S, T, N, #ddColumn{type=Type, len=Len, prec=P}} <- Lookup],
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Meta, Acc);
column_map([], [#ddColMap{schema=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    ?Log("column_map 2 ~p~n", [Cmap0]),
    column_map([], [Cmap0#ddColMap{schema=imem_meta:schema()}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name='*'}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Log("column_map 3 ~p~n", [Cmap0]),
    Prefix = case imem_meta:schema() of
        Schema ->   atom_to_list(Table);
        _ ->        atom_to_list(Schema) ++ "." ++ atom_to_list(Table)
    end,
    Cmaps = [Cmap0#ddColMap{
                schema=S, table=T, tind=Ti, cind=Ci, type=Type, len=Len, prec=P, name=N, 
                alias=list_to_binary([Prefix, ".", atom_to_list(N)]),
                ptree=list_to_binary([Prefix, ".", atom_to_list(N)])
            } || {Ti, Ci, S, T, N, #ddColumn{type=Type, len=Len, prec=P}} <- Lookup, S==Schema, T==Table],
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Meta, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name=Name}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Log("column_map 4 ~p~n", [Cmap0]),
    Pred = fun(L) ->
        (Name == element(5, L)) andalso
        ((Table == undefined) or (Table == element(4, L))) andalso
        ((Schema == undefined) or (Schema == element(3, L)))
    end,
    Lmatch = lists:filter(Pred, Lookup),
    %% ?Log("column_map matching tables ~p~n", [Lmatch]),
    Tcount = length(lists:usort([{element(3, X), element(4, X)} || X <- Lmatch])),
    %% ?Log("column_map matching table count ~p~n", [Tcount]),
    MetaField = imem_meta:meta_field(Name),
    if 
        (Tcount==0) andalso (Schema==undefined) andalso (Table==undefined) andalso MetaField ->
            {Cindex, Meta1} = case index_of(Name, Meta) of
                false ->    {length(Meta)+1, Meta ++ [Name]};
                Index ->    {Index, Meta}
            end, 
            #ddColumn{type=Type, len=Len, prec=P} = imem_meta:meta_field_info(Name),
            Cmap1 = Cmap0#ddColMap{tind=Tindex, cind=Cindex, type=Type, len=Len, prec=P},
            column_map([], Columns, Tindex, Lookup, Meta1, [Cmap1|Acc]);                          
        (Tcount==0) andalso (Schema==undefined) andalso (Table==undefined)->  
            ?ClientError({"Unknown column name", Name});
        (Tcount==0) andalso (Schema==undefined)->  
            ?ClientError({"Unknown column name", {Table, Name}});
        (Tcount==0) ->  
            ?ClientError({"Unknown column name", {Schema, Table, Name}});
        (Tcount > 1) andalso (Schema==undefined) andalso (Table==undefined)-> 
            ?ClientError({"Ambiguous column name", Name});
        (Tcount > 1) andalso (Schema==undefined) -> 
            ?ClientError({"Ambiguous column name", {Table, Name}});
        (Tcount > 1) -> 
            ?ClientError({"Ambiguous column name", {Schema, Table, Name}});
        true ->         
            {Ti, Ci, S, T, Name, #ddColumn{type=Type, len=Len, prec=P, default=D}} = hd(Lmatch),
            R = (Ti > 1),   %% Only first table is editable, key not editable -> (Ci < 3),
            Cmap1 = Cmap0#ddColMap{schema=S, table=T, tind=Ti, cind=Ci, type=Type, len=Len, prec=P, default=D, readonly=R},
            column_map([], Columns, Tindex, Lookup, Meta, [Cmap1|Acc])
    end;
column_map([], [{'fun',Fname,[Name]}=PTree|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Log("column_map 5 ~p~n", [PTree]),
    {S,T,N} = field_qname(Name),
    Alias = list_to_binary(atom_to_list(Fname) ++ "(" ++ binary_to_list(Name) ++ ")"),
    column_map([], [#ddColMap{schema=S, table=T, name=N, alias=Alias, func=Fname, ptree=PTree}|Columns], Tindex, Lookup, Meta, Acc);    
column_map([], [{as, {'fun',Fname,[Name]}, Alias}=PTree|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Log("column_map 6 ~p~n", [PTree]),
    {S,T,N} = field_qname(Name),
    column_map([], [#ddColMap{schema=S, table=T, name=N, func=Fname, alias=Alias, ptree=PTree}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [{as, Name, Alias}=PTree|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Log("column_map 7 ~p~n", [PTree]),
    {S,T,N} = field_qname(Name),
    column_map([], [#ddColMap{schema=S, table=T, name=N, alias=Alias, ptree=PTree}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [Name|Columns], Tindex, Lookup, Meta, Acc) when is_binary(Name)->
    % ?Log("column_map 8 ~p~n", [Name]),
    {S,T,N} = field_qname(Name),
    column_map([], [#ddColMap{schema=S, table=T, name=N, alias=Name, ptree=Name}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [Expression|_], _Tindex, _Lookup, _Meta, _Acc)->
    % ?Log("column_map 9 ~p~n", [Expression]),
    ?UnimplementedException({"Expressions not supported", Expression});
column_map([], [], _Tindex, _Lookup, _Meta, Acc) ->
    lists:reverse(Acc);
column_map(Tables, Columns, Tmax, Lookup, Meta, Acc) ->
    ?Log("column_map error Tables ~p~n", [Tables]),
    ?Log("column_map error Columns ~p~n", [Columns]),
    ?Log("column_map error Tmax ~p~n", [Tmax]),
    ?Log("column_map error Lookup ~p~n", [Lookup]),
    ?Log("column_map error Meta ~p~n", [Meta]),
    ?Log("column_map error Acc ~p~n", [Acc]),
    ?ClientError({"Column map invalid parameter",{Tables,Columns}}).


index_of(Item, List) -> index_of(Item, List, 1).

index_of(_, [], _)  -> false;
index_of(Item, [Item|_], Index) -> Index;
index_of(Item, [_|Tl], Index) -> index_of(Item, Tl, Index+1).

simplify_guard(Term) ->
    case  simplify_once(Term) of
        join ->                             true;
        {'and',join,R1} ->                  simplify_guard(R1);
        {'and',{'and',join,R2}} ->          simplify_guard(R2);
        {'and',{'and',{'and',join,R3}}} ->  simplify_guard(R3);
        Term ->                             Term;
        T ->                                simplify_guard(T)
    end.

simplify_once({'or', true, _}) ->       true; 
simplify_once({'or', _, true}) ->       true; 
simplify_once({'or', false, false}) ->  false; 
simplify_once({'or', join, join}) ->    join; 
simplify_once({'or', join, Right}) ->   ?UnimplementedException({"Join cannot be factored out",{Right}}); 
simplify_once({'or', Left, join}) ->    ?UnimplementedException({"Join cannot be factored out",{Left}}); 
simplify_once({'or', Left, false}) ->   simplify_once(Left); 
simplify_once({'or', false, Right}) ->  simplify_once(Right); 
simplify_once({'or', Same, Same}) ->    simplify_once(Same); 
simplify_once({'and', false, _}) ->     false; 
simplify_once({'and', _, false}) ->     false; 
simplify_once({'and', true, true}) ->   true; 
simplify_once({'and', Left, true}) ->   simplify_once(Left); 
simplify_once({'and', true, Right}) ->  simplify_once(Right); 
simplify_once({'and', Left, join}) ->   {'and', join, simplify_once(Left)}; 
simplify_once({'and', join, Right}) ->  {'and', join, simplify_once(Right)}; 
simplify_once({'and', Same, Same}) ->   simplify_once(Same); 
simplify_once({'+', Left, Right}) when  is_number(Left), is_number(Right) -> Left + Right;
simplify_once({'-', Left, Right}) when  is_number(Left), is_number(Right) -> Left - Right;
simplify_once({'*', Left, Right}) when  is_number(Left), is_number(Right) -> Left * Right;
simplify_once({'/', Left, Right}) when  is_number(Left), is_number(Right) -> Left / Right;
simplify_once({'div', Left, Right}) when is_number(Left), is_number(Right) -> Left div Right;
simplify_once({'>', Left, Right}) when  is_number(Left), is_number(Right) -> (Left > Right);
simplify_once({'>=', Left, Right}) when is_number(Left), is_number(Right) -> (Left >= Right);
simplify_once({'<', Left, Right}) when  is_number(Left), is_number(Right) -> (Left < Right);
simplify_once({'=<', Left, Right}) when is_number(Left), is_number(Right) -> (Left =< Right);
simplify_once({'==', Left, Right}) when is_number(Left), is_number(Right) -> (Left == Right);
simplify_once({'/=', Left, Right}) when is_number(Left), is_number(Right) -> (Left /= Right);
simplify_once({ _Op, _, join}) ->     join;
simplify_once({ _Op, join, _Right}) ->    join;
simplify_once({ Op, Left, Right}) ->    {Op, simplify_once(Left), simplify_once(Right)};
simplify_once({'not', join}) ->         join; 
simplify_once({'not', true}) ->         false; 
simplify_once({'not', false}) ->        true; 
simplify_once({'not', {'is_member', Left, Right}}) -> {'nis_member', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'nis_member', Left, Right}}) -> {'is_member', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'/=', Left, Right}}) -> {'==', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'==', Left, Right}}) -> {'/=', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'=<', Left, Right}}) -> {'>',  simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'<', Left, Right}}) ->  {'>=', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'>=', Left, Right}}) -> {'<',  simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'>', Left, Right}}) ->  {'=<', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', Result}) ->       {'not', simplify_once(Result)};
simplify_once({ _Op, join}) ->           join;
simplify_once({ Op, Result}) ->         {Op, Result};
simplify_once(Result) ->                Result.


create_scan_spec(_Tmax,Ti,FullMap,[]) ->
    MatchHead = list_to_tuple(['_'|[Tag || #ddColMap{tag=Tag, tind=Tind} <- FullMap, Tind==Ti]]),
    #scanSpec{sspec=[{MatchHead, [], ['$_']}]};
create_scan_spec(_Tmax,Ti,FullMap,[SGuard0]) ->
    % ?Log("SGuard0 ~p~n", [SGuard0]),
    Limit = case operand_match(rownum,SGuard0) of
        false ->  #scanSpec{}#scanSpec.limit;
        {'<',rownum,L} when is_integer(L) ->    L-1;
        {'=<',rownum,L} when is_integer(L) ->   L;
        {'>',L,rownum} when is_integer(L) ->    L-1;
        {'>=',L,rownum} when is_integer(L) ->   L;
        Else ->
            ?UnimplementedException({"Unsupported use of rownum",{Else}})
    end,
    MatchHead = list_to_tuple(['_'|[Tag || #ddColMap{tag=Tag, tind=Tind} <- FullMap, Tind==Ti]]),
    % ?Log("MatchHead (~p) ~p~n", [Ti,MatchHead]),
    SGuard1 = simplify_guard(replace_rownum(SGuard0)),
    % ?Log("SGuard1 ~p~n", [SGuard1]),
    {FGuard,SGuard2} = case operator_match('is_member',SGuard1) of
        false ->    case operator_match('nis_member',SGuard1) of
                        false ->    {true,SGuard1};   %% no filtering needed
                        NF ->       {NF,simplify_guard(replace_nis_member(SGuard1))}
                    end;
        F ->        {F,simplify_guard(replace_is_member(SGuard1))}
    end,
    SSpec = [{MatchHead, [SGuard2], ['$_']}],
    % ?Log("FGuard ~p~n", [FGuard]),
    SBinds = binds([{Tag,Tind,Ci} || #ddColMap{tag=Tag, tind=Tind, cind=Ci} <- FullMap, (Tind/=Ti)], [SGuard2],[]),
    % ?Log("SBinds ~p~n", [SBinds]),
    MBinds = binds([{Tag,Tind,Ci} || #ddColMap{tag=Tag, tind=Tind, cind=Ci} <- FullMap, (Tind/=Ti)], [FGuard],[]),
    % ?Log("MBinds ~p~n", [MBinds]),
    FBinds = binds([{Tag,Tind,Ci} || #ddColMap{tag=Tag, tind=Tind, cind=Ci} <- FullMap, (Tind==Ti)], [FGuard],[]),
    % ?Log("FBinds ~p~n", [FBinds]),
    #scanSpec{sspec=SSpec,sbinds=SBinds,fguard=FGuard,mbinds=MBinds,fbinds=FBinds,limit=Limit}.

operand_member(Tx,{_,R}) -> operand_member(Tx,R);
operand_member(Tx,{_,Tx,_}) -> true;
operand_member(Tx,{_,_,Tx}) -> true;
operand_member(Tx,{_,L,R}) -> operand_member(Tx,L) orelse operand_member(Tx,R);
operand_member(Tx,Tx) -> true;
operand_member(_,_) -> false.

binds(_, [], []) -> [];
binds(_, [true], []) -> [];
binds([], _Guards, Acc) -> Acc;
binds([{Tx,Ti,Ci}|Rest], [Guard], Acc) ->
    case operand_member(Tx,Guard) of
        true ->     binds(Rest,[Guard],[{Tx,Ti,Ci}|Acc]);
        false ->    binds(Rest,[Guard],Acc)
    end.

operand_match(Tx,{_,Tx}=C0) ->      C0;
operand_match(Tx,{_,R}) ->          operand_match(Tx,R);
operand_match(Tx,{_,Tx,_}=C1) ->    C1;
operand_match(Tx,{_,_,Tx}=C2) ->    C2;
operand_match(Tx,{_,L,R}) ->        
    case operand_match(Tx,L) of
        false ->    operand_match(Tx,R);
        Else ->     Else
    end;    
operand_match(Tx,Tx) ->             Tx;
operand_match(_,_) ->               false.

operator_match(Tx,{Tx,_}=C0) ->     C0;
operator_match(Tx,{_,R}) ->         operator_match(Tx,R);
operator_match(Tx,{Tx,_,_}=C1) ->   C1;
operator_match(Tx,{_,L,R}) ->       
    case operator_match(Tx,L) of
        false ->    operator_match(Tx,R);
        Else ->     Else
    end;
operator_match(_,_) ->              false.

replace_rownum({Op,rownum,Right}) -> {Op,1,Right};
replace_rownum({Op,Left,rownum}) ->  {Op,Left,1};
replace_rownum({Op,Left,Right})->    {Op,replace_rownum(Left),replace_rownum(Right)};
replace_rownum({Op,Result}) ->       {Op,replace_rownum(Result)};
replace_rownum(Result) ->            Result.

replace_is_member({is_member,_Left,_Right})->   true;
replace_is_member({Op,Left,Right})->            {Op,replace_is_member(Left),replace_is_member(Right)};
replace_is_member({Op,Result}) ->               {Op,replace_is_member(Result)};
replace_is_member(Result) ->                    Result.

replace_nis_member({nis_member,_Left,_Right})-> true;
replace_nis_member({Op,Left,Right})->           {Op,replace_nis_member(Left),replace_nis_member(Right)};
replace_nis_member({Op,Result}) ->              {Op,replace_nis_member(Result)};
replace_nis_member(Result) ->                   Result.

un_escape_sql(Str) when is_list(Str) ->
    re:replace(Str, "('')", "'", [global, {return, list}]).


build_sort_fun(SelectSections,FullMap) ->
    case lists:keyfind('order by', 1, SelectSections) of
        {_, []} ->      fun(_X) -> {} end;
        {_, Sorts} ->   % ?Log("Sorts  : ~p~n", [Sorts]),
                        SortFuns = [sort_fun_item(Name,Direction,FullMap) || {Name,Direction} <- Sorts],
                        fun(X) -> list_to_tuple([F(X)|| F <- SortFuns]) end;
        SError ->       ?ClientError({"Invalid order by in select structure", SError})
    end.

build_sort_spec(SelectSections,FullMaps,ColMaps) ->
    case lists:keyfind('order by', 1, SelectSections) of
        {_, []} ->      [];
        {_, Sorts} ->   % ?Log("Sorts  : ~p~n", [Sorts]),
                        [sort_spec_item(Name,Direction,FullMaps,ColMaps) || {Name,Direction} <- Sorts];
        SError ->       ?ClientError({"Invalid order by in select structure", SError})
    end.

sort_spec_item(Name,<<>>,FullMaps,ColMaps) ->
    sort_spec_item(Name,<<"asc">>,FullMaps,ColMaps);
sort_spec_item(Name,Direction,FullMaps,ColMaps) ->
    U = undefined,
    ML = case imem_sql:field_qname(Name) of
        {U,U,N} ->  [C || #ddColMap{name=Nam}=C <- FullMaps, Nam==N];
        {U,T1,N} -> [C || #ddColMap{name=Nam,table=Tab}=C <- FullMaps, (Nam==N), (Tab==T1)];
        {S,T2,N} -> [C || #ddColMap{name=Nam,table=Tab,schema=Sch}=C <- FullMaps, (Nam==N), ((Tab==T2) or (Tab==U)), ((Sch==S) or (Sch==U))];
        {} ->       []
    end,
    case length(ML) of
        0 ->    ?ClientError({"Bad sort expression", Name});
        1 ->    #ddColMap{tind=Ti,cind=Ci,alias=A} = hd(ML),
                case [Cp || {Cp,#ddColMap{tind=Tind,cind=Cind}} <- lists:zip(lists:seq(1,length(ColMaps)),ColMaps), Tind==Ti, Cind==Ci] of
                    [CP|_] ->   {CP,Direction};
                     _ ->       {A,Direction}
                end;
        _ ->    ?ClientError({"Ambiguous column name in where clause", Name})
    end.

sort_fun_item(Name,<<>>,FullMap) ->
    sort_fun_item(Name,<<"asc">>,FullMap);
sort_fun_item(Name,Direction,FullMap) ->
    U = undefined,
    ML = case imem_sql:field_qname(Name) of
        {U,U,N} ->  [C || #ddColMap{name=Nam}=C <- FullMap, Nam==N];
        {U,T1,N} -> [C || #ddColMap{name=Nam,table=Tab}=C <- FullMap, (Nam==N), (Tab==T1)];
        {S,T2,N} -> [C || #ddColMap{name=Nam,table=Tab,schema=Sch}=C <- FullMap, (Nam==N), ((Tab==T2) or (Tab==U)), ((Sch==S) or (Sch==U))];
        {} ->       []
    end,
    case length(ML) of
        0 ->    ?ClientError({"Bad sort expression", Name});
        1 ->    #ddColMap{type=Type, tind=Ti, cind=Ci} = hd(ML),
                sort_fun(Type,Ti,Ci,Direction);
        _ ->    ?ClientError({"Ambiguous column name in where clause", Name})
    end.

filter_spec_where(?NoMoreFilter, _, WhereTree) -> 
    WhereTree;
filter_spec_where({FType,[ColF|ColFs]}, ColMaps, WhereTree) ->
    FCond = filter_condition(ColF, ColMaps),
    filter_spec_where({FType,ColFs}, ColMaps, WhereTree, FCond). 

filter_spec_where(?NoMoreFilter, _, [], LeftTree) ->
    LeftTree;
filter_spec_where(?NoMoreFilter, _, WhereTree, LeftTree) ->
    {'and', LeftTree, WhereTree};
filter_spec_where({FType,[ColF|ColFs]}, ColMaps, WhereTree, LeftTree) ->
    FCond = filter_condition(ColF, ColMaps),
    filter_spec_where({FType,ColFs}, ColMaps, WhereTree, {FType,LeftTree,FCond}).    

filter_condition({Idx,[Val]}, ColMaps) ->
    #ddColMap{schema=S,table=T,name=N,type=Type,len=L,prec=P,default=D} = lists:nth(Idx,ColMaps),
    Tag = "Col" ++ integer_to_list(Idx),
    Value = filter_field_value(Tag,Type,L,P,D,Val),     % list_to_binary(
    {'=',list_to_binary(field_name(S,T,N)),Value};
filter_condition({Idx,Vals}, ColMaps) ->
    #ddColMap{schema=S,table=T,name=N,type=Type,len=L,prec=P,default=D} = lists:nth(Idx,ColMaps),
    Tag = "Col" ++ integer_to_list(Idx),
    Values = [filter_field_value(Tag,Type,L,P,D,Val) || Val <- Vals],       % list_to_binary(
    {'in',list_to_binary(field_name(S,T,N)),{'list',Values}}.

filter_field_value(_Tag,integer,_Len,_Prec,_Def,Val) -> Val;
filter_field_value(_Tag,float,_Len,_Prec,_Def,Val) -> Val;
filter_field_value(_Tag,_Type,_Len,_Prec,_Def,Val) -> imem_datatype:add_squotes(Val).    

sort_spec_order([],_,_) -> [];
sort_spec_order(SortSpec,FullMaps,ColMaps) ->
    sort_spec_order(SortSpec,FullMaps,ColMaps,[]).

sort_spec_order([],_,_,Acc) -> 
    lists:reverse(Acc);        
sort_spec_order([SS|SortSpecs],FullMaps,ColMaps, Acc) ->
    sort_spec_order(SortSpecs,FullMaps,ColMaps,[sort_order(SS,FullMaps,ColMaps)|Acc]).

sort_order({Ti,Ci,Direction},FullMaps,_) ->
    %% SortSpec given referencing FullMap Ti,Ci    
    case [{S,T,N} || #ddColMap{tind=Tind,cind=Cind,schema=S,table=T,name=N} <- FullMaps, Tind==Ti, Cind==Ci] of
        [QN] -> {list_to_binary(field_name(QN)),Direction};
        _ ->    ?ClientError({"Bad sort field reference", {Ti,Ci}})
    end;
sort_order({Cp,Direction},_,ColMaps) when is_integer(Cp) ->
    %% SortSpec given referencing ColMap position    
    #ddColMap{schema=S,table=T,name=N} = lists:nth(Cp,ColMaps),
    {list_to_binary(field_name({S,T,N})),Direction};
sort_order({CName,Direction},FullMaps,_) ->
    %% SortSpec given referencing FullMap alias    
    case lists:keysearch(CName, #ddColMap.alias, FullMaps) of
        #ddColMap{schema=S,table=T,name=N} ->
            {list_to_binary(field_name({S,T,N})),Direction};
        _ -> 
            ?ClientError({"Bad sort field name", CName})
    end.

sort_spec_fun([],_,_) -> 
    fun(_X) -> {} end;
sort_spec_fun(SortSpec,FullMaps,ColMaps) ->
    SortFuns = sort_spec_fun(SortSpec,FullMaps,ColMaps,[]),
    fun(X) -> list_to_tuple([F(X)|| F <- SortFuns]) end.

sort_spec_fun([],_,_,Acc) -> lists:reverse(Acc);
sort_spec_fun([SS|SortSpecs],FullMaps,ColMaps,Acc) ->
    sort_spec_fun(SortSpecs,FullMaps,ColMaps,[sort_fun_any(SS,FullMaps,ColMaps)|Acc]).

sort_fun_any({Ti,Ci,Direction},FullMaps,_) ->
    %% SortSpec given referencing FullMap Ti,Ci    
    case [Type || #ddColMap{tind=Tind,cind=Cind,type=Type} <- FullMaps, Tind==Ti, Cind==Ci] of
        [Typ] ->    sort_fun(Typ,Ti,Ci,Direction);
        Else ->     ?ClientError({"Bad sort field type", Else})
    end;
sort_fun_any({Cp,Direction},_,ColMaps) when is_integer(Cp) ->
    %% SortSpec given referencing ColMap position
    #ddColMap{tind=Ti,cind=Ci,type=Type} = lists:nth(Cp,ColMaps),
    % ?Log("sort on col position ~p Ti=~p Ci=~p ~p~n",[Cp,Ti,Ci,lists:nth(Cp,ColMaps)]),    
    sort_fun(Type,Ti,Ci,Direction);
sort_fun_any({CName,Direction},FullMaps,_) ->
    %% SortSpec given referencing FullMap alias    
    case lists:keysearch(CName, #ddColMap.alias, FullMaps) of
        #ddColMap{tind=Ti,cind=Ci,type=Type} ->
            % ?Log("sort on col name  ~p Ti=~p Ci=~p ~p~n",[CName,Ti,Ci,Type]),    
            sort_fun(Type,Ti,Ci,Direction);
        Else ->     
            ?ClientError({"Bad sort field", Else})
    end.

sort_fun(atom,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
            A when is_atom(A) ->
                [ -Item || Item <- atom_to_list(A)] ++ [?MaxChar];
            _ ->
                element(Ci,element(Ti,X))
        end
    end;
sort_fun(binstr,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
            B when is_binary(B) ->
                [ -Item || Item <- binary_to_list(B)] ++ [?MaxChar];
            _ ->
                element(Ci,element(Ti,X))
        end
    end;
sort_fun(boolean,Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        V = element(Ci,element(Ti,X)),
        case V of
            true ->         false;
            false ->        true;
            _ ->            element(Ci,element(Ti,X))
        end 
    end;
sort_fun(datetime,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
            {{Y,M,D},{Hh,Mm,Ss}} when is_integer(Y), is_integer(M), is_integer(D), is_integer(Hh), is_integer(Mm), is_integer(Ss) -> 
                {{-Y,-M,-D},{-Hh,-Mm,-Ss}};
            _ ->
                element(Ci,element(Ti,X))
        end 
    end;
sort_fun(decimal,Ti,Ci,<<"desc">>) -> sort_fun(number,Ti,Ci,<<"desc">>);
sort_fun(float,Ti,Ci,<<"desc">>) ->   sort_fun(number,Ti,Ci,<<"desc">>);
sort_fun(integer,Ti,Ci,<<"desc">>) -> sort_fun(number,Ti,Ci,<<"desc">>);
sort_fun(ipadr,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
            {A,B,C,D} when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
                {-A,-B,-C,-D};
            {A,B,C,D,E,F,G,H} when is_integer(A), is_integer(B), is_integer(C), is_integer(D), is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
                {-A,-B,-C,-D,-E,-F,-G,-H};
            _ ->
                element(Ci,element(Ti,X))
        end
    end;
sort_fun(number,Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        V = element(Ci,element(Ti,X)),
        case is_number(V) of
            true ->         (-V);
            false ->        element(Ci,element(Ti,X))
        end 
    end;
sort_fun(string,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
            [H|T] when is_integer(H) ->
                [ -Item || Item <- [H|T]] ++ [?MaxChar];
            _ ->
                element(Ci,element(Ti,X))
        end
    end;
sort_fun(timestamp,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
            {Meg,Sec,Micro} when is_integer(Meg), is_integer(Sec), is_integer(Micro)->
                {-Meg,-Sec,-Micro};
            _ ->
                element(Ci,element(Ti,X))
        end    
    end;
sort_fun({atom,atom},Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
            {T,A} when is_atom(T), is_atom(A) ->
                {[ -ItemT || ItemT <- atom_to_list(T)] ++ [?MaxChar]
                ,[ -ItemA || ItemA <- atom_to_list(A)] ++ [?MaxChar]
                };
            _ ->
                element(Ci,element(Ti,X))
        end    
    end;
sort_fun({atom,integer},Ti,Ci,<<"desc">>) -> sort_fun({atom,number},Ti,Ci,<<"desc">>);
sort_fun({atom,decimal},Ti,Ci,<<"desc">>) -> sort_fun({atom,number},Ti,Ci,<<"desc">>);
sort_fun({atom,float},Ti,Ci,<<"desc">>) -> sort_fun({atom,number},Ti,Ci,<<"desc">>);
sort_fun({atom,userid},Ti,Ci,<<"desc">>) -> sort_fun({atom,number},Ti,Ci,<<"desc">>);
sort_fun({atom,number},Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
            {T,N} when is_atom(T), is_number(N) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-N};
            _ ->
                element(Ci,element(Ti,X))
        end    
    end;
sort_fun({atom,ipaddr},Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
            {T,{A,B,C,D}} when is_atom(T), is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-A,-B,-C,-D};
            {T,{A,B,C,D,E,F,G,H}} when is_atom(T),is_integer(A), is_integer(B), is_integer(C), is_integer(D), is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-A,-B,-C,-D,-E,-F,-G,-H};
            _ ->
                element(Ci,element(Ti,X))
        end    
    end;
sort_fun(tuple,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case element(Ci,element(Ti,X)) of 
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
            _ ->
                element(Ci,element(Ti,X))
        end    
    end;
sort_fun(userid,Ti,Ci,<<"desc">>) ->   sort_fun(number,Ti,Ci,<<"desc">>);
sort_fun(Type,_Ti,_Ci,<<"desc">>) ->
    ?SystemException({"Unsupported datatype for sort desc", Type});
sort_fun(_Type,Ti,Ci,_) -> 
    % ?Log("Sort ~p  : ~p ~p~n", [_Type, Ti,Ci]), 
    fun(X) -> element(Ci,element(Ti,X)) end.



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
        %% SyEx = 'SystemException',    %% difficult to test
        % SeEx = 'SecurityException',
        ?Log("----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        ?Log("schema ~p~n", [imem_meta:schema()]),
        ?Log("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey = case IsSec of
            true -> ?imem_test_admin_login();
            _ ->    ok
        end,
        % field names
        ?assertEqual({undefined,undefined,field}, field_qname(<<"field">>)),
        ?assertEqual({undefined,table,field}, field_qname(<<"table.field">>)),
        ?assertEqual({schema,table,field}, field_qname(<<"schema.table.field">>)),

        ?assertEqual("field", field_name(field_qname(<<"field">>))),
        ?assertEqual("table.field", field_name(field_qname(<<"table.field">>))),
        ?assertEqual("schema.table.field", field_name(field_qname(<<"schema.table.field">>))),

        % table names without alias
        ?assertEqual({'Imem',table,table}, table_qname(<<"table">>)),
        ?assertEqual({schema,table,table}, table_qname(<<"schema.table">>)),
        ?assertEqual({schema,table,alias}, table_qname(<<"schema.table">>, <<"alias">>)),
        ?assertEqual({schema,table,alias}, table_qname(<<"schema.table">>, "alias")),

        % table names with alias
        ?assertEqual({'Imem',table,alias}, table_qname({<<"table">>,"alias"})),
        ?assertEqual({schema,table,alias}, table_qname({<<"schema.table">>, "alias"})),
        ?assertEqual({'Imem',table,alias}, table_qname({<<"table">>,<<"alias">>})),
        ?assertEqual({schema,table,alias}, table_qname({<<"schema.table">>, <<"alias">>})),

        ?Log("----TEST--~p:test_mnesia~n", [?MODULE]),

        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?Log("success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        ?Log("success ~p~n", [data_nodes]),

        ?Log("----TEST--~p:test_database_operations~n", [?MODULE]),
        _Types1 =    [ #ddColumn{name=a, type=char, len=1}     %% key
                    , #ddColumn{name=b1, type=char, len=1}    %% value 1
                    , #ddColumn{name=c1, type=char, len=1}    %% value 2
                    ],
        _Types2 =    [ #ddColumn{name=a, type=integer, len=10}    %% key
                    , #ddColumn{name=b2, type=float, len=8, prec=3}   %% value
                    ],

        ?assertEqual(ok, exec(SKey, "create table meta_table_1 (a char, b1 char, c1 char);", 0, "Imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, meta_table_1])),    

        ?assertEqual(ok, exec(SKey, "create table meta_table_2 (a integer, b2 float);", 0, "Imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, meta_table_2])),    

        ?assertEqual(ok, exec(SKey, "create table meta_table_3 (a char, b3 integer, c1 char);", 0, "Imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, meta_table_1])),    
        ?Log("success ~p~n", [create_tables]),

        Table1 =    {'Imem', meta_table_1, meta_table_1},
        Table2 =    {undefined, meta_table_2, meta_table_2},
        Table3 =    {undefined, meta_table_3, meta_table_3},
        TableX =    {undefined,meta_table_x, meta_table_1},

        Alias1 =    {undefined, meta_table_1, alias1},
        Alias2 =    {'Imem', meta_table_1, alias2},

        ?assertException(throw, {ClEr, {"Table does not exist", {'Imem', meta_table_x}}}, column_map([TableX], [])),
        ?Log("success ~p~n", [table_no_exists]),

        ColsE1=     [ #ddColMap{tag="A1", schema='Imem', table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", name=x}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Unknown column name", x}}, column_map([Table1], ColsE1)),
        ?Log("success ~p~n", [unknown_column_name_1]),

        ColsE2=     [ #ddColMap{tag="A1", schema='Imem', table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", table=meta_table_x, name=b1}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Unknown column name", {meta_table_x,b1}}}, column_map([Table1, Table2], ColsE2)),
        ?Log("success ~p~n", [unknown_column_name_2]),

        ?assertMatch([_,_,_], column_map([Table1], [])),
        ?Log("success ~p~n", [empty_select_columns]),


        ColsF =     [ #ddColMap{tag="A", tind=1, cind=1, schema='Imem', table=meta_table_1, name=a, type=integer}
                    , #ddColMap{tag="B", tind=1, cind=2, table=meta_table_1, name=b1, type=string}
                    , #ddColMap{tag="C", tind=1, cind=3, name=c1, type=ipaddr}
                    ],

        ?assertEqual([], filter_spec_where(?NoFilter, ColsF, [])),
        ?assertEqual({wt}, filter_spec_where(?NoFilter, ColsF, {wt})),
        FA1 = {1,[<<"111">>]},
        CA1 = {'=',<<"Imem.meta_table_1.a">>,<<"111">>},
        ?assertEqual({'and',CA1,{wt}}, filter_spec_where({'or',[FA1]}, ColsF, {wt})),
        FB2 = {2,[<<"222">>]},
        CB2 = {'=',<<"meta_table_1.b1">>,<<"'222'">>},
        ?assertEqual({'and',{'and',CA1,CB2},{wt}}, filter_spec_where({'and',[FA1,FB2]}, ColsF, {wt})),
        FC3 = {3,[<<"3.1.2.3">>,<<"3.3.2.1">>]},
        CC3 = {'in',<<"c1">>,{'list',[<<"'3.1.2.3'">>,<<"'3.3.2.1'">>]}},
        ?assertEqual({'and',{'or',{'or',CA1,CB2},CC3},{wt}}, filter_spec_where({'or',[FA1,FB2,FC3]}, ColsF, {wt})),
        ?assertEqual({'and',{'and',{'and',CA1,CB2},CC3},{wt}}, filter_spec_where({'and',[FA1,FB2,FC3]}, ColsF, {wt})),
        ?Log("success ~p~n", [filter_spec_where]),

        ?assertEqual([], sort_spec_order([], ColsF, ColsF)),
        SA = {1,1,<<"desc">>},
        OA = {<<"Imem.meta_table_1.a">>,<<"desc">>},
        ?assertEqual([OA], sort_spec_order([SA], ColsF, ColsF)),
        SB = {1,2,<<"asc">>},
        OB = {<<"meta_table_1.b1">>,<<"asc">>},
        ?assertEqual([OB], sort_spec_order([SB], ColsF, ColsF)),
        SC = {1,3,<<"desc">>},
        OC = {<<"c1">>,<<"desc">>},
        ?assertEqual([OC], sort_spec_order([SC], ColsF, ColsF)),
        ?assertEqual([OC,OA], sort_spec_order([SC,SA], ColsF, ColsF)),
        ?assertEqual([OB,OC,OA], sort_spec_order([SB,SC,SA], ColsF, ColsF)),
        ?Log("success ~p~n", [sort_spec_order]),

        ColsA =     [ #ddColMap{tag="A1", schema='Imem', table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", table=meta_table_1, name=b1}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Empty table list", _}}, column_map([], ColsA)),
        ?Log("success ~p~n", [empty_table_list]),

        ?assertException(throw, {ClEr,{"Ambiguous column name", a}}, column_map([Table1, Table3], [#ddColMap{name=a}])),
        ?Log("success ~p~n", [columns_ambiguous_a]),

        ?assertException(throw, {ClEr,{"Ambiguous column name", c1}}, column_map([Table1, Table3], ColsA)),
        ?Log("success ~p~n", [columns_ambiguous_c1]),

        ?assertMatch([_,_,_], column_map([Table1], ColsA)),
        ?Log("success ~p~n", [columns_A]),

        ?assertMatch([_,_,_,_,_,_], column_map([Table1, Table3], [#ddColMap{name='*'}])),
        ?Log("success ~p~n", [columns_13_join]),

        Cmap3 = column_map([Table1, Table2, Table3], [#ddColMap{name='*'}]),
        % ?Log("ColMap3 ~p~n", [Cmap3]),        
        ?assertMatch([_,_,_,_,_,_,_,_], Cmap3),
        ?assertEqual(lists:sort(Cmap3), Cmap3),
        ?Log("success ~p~n", [columns_123_join]),

        ?assertMatch([_,_,_,_,_,_,_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{name='*'}])),
        ?Log("success ~p~n", [alias_113_join]),

        ?assertMatch([_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=meta_table_3, name='*'}])),
        ?Log("success ~p~n", [columns_113_star1]),

        ?assertMatch([_,_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=alias1, name='*'},#ddColMap{table=meta_table_3, name='a'}])),
        ?Log("success ~p~n", [columns_alias_1]),

        ?assertMatch([_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=alias1, name=a},#ddColMap{table=alias2, name='a'}])),
        ?Log("success ~p~n", [columns_alias_2]),

        ?assertMatch([_,_], column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{name=sysdate}])),
        ?Log("success ~p~n", [sysdate]),

        ?assertException(throw, {ClEr,{"Unknown column name", {any,sysdate}}}, column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{table=any, name=sysdate}])),
        ?Log("success ~p~n", [sysdate_reject]),

        ?assertEqual(["'Imem'.alias1.a","undefined.undefined.user"], column_map_items(column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{name=user}]),qname)),
        ?Log("success ~p~n", [user]),

        ?assertEqual(ok, imem_meta:drop_table(meta_table_3)),
        ?assertEqual(ok, imem_meta:drop_table(meta_table_2)),
        ?assertEqual(ok, imem_meta:drop_table(meta_table_1)),
        ?Log("success ~p~n", [drop_tables]),

        case IsSec of
            true -> ?imem_logout(SKey);
            _ ->    ok
        end
    catch
        Class:Reason ->  ?Log("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
