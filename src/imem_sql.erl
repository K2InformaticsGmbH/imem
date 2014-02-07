-module(imem_sql).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-export([ exec/5
        , parse/1
        ]).

-define(GET_ROWNUM_LIMIT,?GET_IMEM_CONFIG(rownumDefaultLimit,[],10000)).

-define(MaxChar,16#FFFFFF).

-define(FilterFuns, ['is_member', 'is_like', 'is_regexp_like', 'safe']).

-export([ field_qname/1
        , field_name/1
        , field_name/3
        , table_qname/1
        , table_qname/2
        , column_map_items/2
        , column_map/2
        , simplify_guard/1
        , guard_bind/3
        , create_scan_spec/3
        , operand_member/2
        , operand_match/2
        , escape_sql/1
        , un_escape_sql/1
        , build_sort_fun/2
        , build_sort_spec/3
        , filter_spec_where/3
        , sort_spec_order/3
        , sort_spec_fun/3
        ]).

-export([ re_compile/1
        , re_match/2
        , like_compile/1
        , like_compile/2
        , make_expr_fun/3
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
    column_map([{S,imem_meta:physical_table_name(T),A}||{S,T,A} <- Tables], Columns, ?MainIdx, [], [], []).    %% First table has index 2

column_map([{undefined,Table,Alias}|Tables], Columns, Tindex, Lookup, Meta, Acc) ->
    column_map([{imem_meta:schema(),Table,Alias}|Tables], Columns, Tindex, Lookup, Meta, Acc);
column_map([{Schema,Table,Alias}|Tables], Columns, Tindex, Lookup, Meta, Acc) ->
    Cols = imem_meta:column_infos({Schema,Table}),
    L = [{Tindex, Cindex, Schema, Alias, Cinfo#ddColumn.name, Cinfo} || {Cindex, Cinfo} <- lists:zip(lists:seq(2,length(Cols)+1), Cols)],
    % ?Debug("column_map lookup ~p~n", [Lookup++L]),
    % ?Debug("column_map columns ~p~n", [Columns]),
    column_map(Tables, Columns, Tindex+1, Lookup++L, Meta, Acc);

column_map([], [#ddColMap{schema=undefined, table=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Debug("column_map 1 ~p~n", [Cmap0]),
    NameList = [N ||  {_, _, _, _, N, _} <- Lookup],
    NameDups = length(Lookup) - length(lists:usort(NameList)),
    Cmaps = case NameDups of
        0 -> [Cmap0#ddColMap{
                schema=S, table=T, tind=Ti, cind=Ci, type=Type, len=Len, prec=P, name=N
                , alias=?atom_to_binary(N)
                , ptree=?atom_to_binary(N)
              } ||  {Ti, Ci, S, T, N, #ddColumn{type=Type, len=Len, prec=P}} <- Lookup
             ];
        _ -> [Cmap0#ddColMap{
                schema=S, table=T, tind=Ti, cind=Ci, type=Type, len=Len, prec=P, name=N
                , alias=list_to_binary([atom_to_list(T), ".", atom_to_list(N)])
                , ptree=list_to_binary([atom_to_list(T), ".", atom_to_list(N)])
              } ||  {Ti, Ci, S, T, N, #ddColumn{type=Type, len=Len, prec=P}} <- Lookup
             ]
    end,
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Meta, Acc);
column_map([], [#ddColMap{schema=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Debug("column_map 2 ~p~n", [Cmap0]),
    column_map([], [Cmap0#ddColMap{schema=imem_meta:schema()}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name='*'}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Debug("column_map 3 ~p~n", [Cmap0]),
    Prefix = case imem_meta:schema() of
        Schema ->   atom_to_list(Table);
        _ ->        atom_to_list(Schema) ++ "." ++ atom_to_list(Table)
    end,
    Cmaps = [Cmap0#ddColMap{
                schema=S, table=T, tind=Ti, cind=Ci, type=Type, len=Len, prec=P, name=N
                , alias=list_to_binary([Prefix, ".", atom_to_list(N)])
                , ptree=list_to_binary([Prefix, ".", atom_to_list(N)])
             } || {Ti, Ci, S, T, N, #ddColumn{type=Type, len=Len, prec=P}} <- Lookup, S==Schema, T==Table
            ],
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Meta, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name=Name}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Debug("column_map 4 ~p~n", [Cmap0]),
    Pred = fun(L) ->
        (Name == element(5, L)) andalso
        ((Table == undefined) or (Table == element(4, L))) andalso
        ((Schema == undefined) or (Schema == element(3, L)))
    end,
    Lmatch = lists:filter(Pred, Lookup),
    % ?Debug("column_map matching tables ~p~n", [Lmatch]),
    Tcount = length(lists:usort([{element(3, X), element(4, X)} || X <- Lmatch])),
    % ?Debug("column_map matching table count ~p~n", [Tcount]),
    MetaField = imem_meta:meta_field(Name),
    if 
        (Tcount==0) andalso (Schema==undefined) andalso (Table==undefined) andalso MetaField ->
            {Cindex, Meta1} = case index_of(Name, Meta) of
                false ->    {length(Meta)+1, Meta ++ [Name]};
                Index ->    {Index, Meta}
            end, 
            #ddColumn{type=Type, len=Len, prec=P} = imem_meta:meta_field_info(Name),
            Cmap1 = Cmap0#ddColMap{tind=?MetaIdx, cind=Cindex, type=Type, len=Len, prec=P},
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
            R = (Ti /= ?MainIdx),   %% Only main table is editable
            Cmap1 = Cmap0#ddColMap{schema=S, table=T, tind=Ti, cind=Ci, type=Type, len=Len, prec=P, default=D, readonly=R},
            column_map([], Columns, Tindex, Lookup, Meta, [Cmap1|Acc])
    end;
column_map([], [{'fun',Fname,[Name]}=PTree|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Debug("column_map 5 ~p~n", [PTree]),
    case imem_datatype:is_rowfun_extension(Fname,1) of
        true ->
            {S,T,N} = field_qname(Name),
            Alias = list_to_binary([Fname, "(", Name, ")"]),
            column_map([], [#ddColMap{schema=S, table=T, name=N, alias=Alias, func=binary_to_atom(Fname,utf8), ptree=PTree}|Columns], Tindex, Lookup, Meta, Acc);
        false ->
            ?UnimplementedException({"Unimplemented row function",{Fname,1}})
    end;        
column_map([], [{as, {'fun',Fname,[Name]}, Alias}=PTree|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Debug("column_map 6 ~p~n", [PTree]),
    case imem_datatype:is_rowfun_extension(Fname,1) of
        true ->
            {S,T,N} = field_qname(Name),
            column_map([], [#ddColMap{schema=S, table=T, name=N, func=binary_to_atom(Fname,utf8), alias=Alias, ptree=PTree}|Columns], Tindex, Lookup, Meta, Acc);
        false ->
            ?UnimplementedException({"Unimplemented row function",{Fname,1}})
    end;                    
column_map([], [{as, Name, Alias}=PTree|Columns], Tindex, Lookup, Meta, Acc) ->
    % ?Debug("column_map 7 ~p~n", [PTree]),
    {S,T,N} = field_qname(Name),
    column_map([], [#ddColMap{schema=S, table=T, name=N, alias=Alias, ptree=PTree}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [Name|Columns], Tindex, Lookup, Meta, Acc) when is_binary(Name)->
    % ?Debug("column_map 8 ~p~n", [Name]),
    {S,T,N} = field_qname(Name),
    column_map([], [#ddColMap{schema=S, table=T, name=N, alias=Name, ptree=Name}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [Expression|_], _Tindex, _Lookup, _Meta, _Acc)->
    % ?Debug("column_map 9 ~p~n", [Expression]),
    ?UnimplementedException({"Expressions not supported", Expression});
column_map([], [], _Tindex, _Lookup, _Meta, Acc) ->
    lists:reverse(Acc);
column_map(Tables, Columns, Tmax, Lookup, Meta, Acc) ->
    ?Warn("column_map error Tables ~p~n", [Tables]),
    ?Warn("column_map error Columns ~p~n", [Columns]),
    ?Warn("column_map error Tmax ~p~n", [Tmax]),
    ?Warn("column_map error Lookup ~p~n", [Lookup]),
    ?Warn("column_map error Meta ~p~n", [Meta]),
    ?Warn("column_map error Acc ~p~n", [Acc]),
    ?ClientError({"Column map invalid parameter",{Tables,Columns}}).


index_of(Item, List) -> index_of(Item, List, 1).

index_of(_, [], _)  -> false;
index_of(Item, [Item|_], Index) -> Index;
index_of(Item, [_|Tl], Index) -> index_of(Item, Tl, Index+1).

guard_bind_value({const,Tup}) when is_tuple(Tup) ->   {const,Tup};    %% ToDo: Is this necesary?
guard_bind_value(Tup) when is_tuple(Tup) ->           {const,Tup};
guard_bind_value(Other) ->                            Other.

guard_bind(_Rec, Guard, []) -> Guard;
guard_bind(Rec, Guard0, [B|Binds]) ->
    % ?Debug("bind rec: ~p guard: ~p bind: ~p~n", [Rec, Guard0, B]),
    Guard1 = imem_sql:simplify_guard(guard_bind_one(Rec, Guard0, B)),
    % ?Debug("bind result: ~p~n", [Guard1]),
    guard_bind(Rec, Guard1, Binds).

guard_bind_one(_Rec,{const,T}, _) when is_tuple(T) -> {const,T};
guard_bind_one(Rec, {Op,Tag}, {Tag,Ti,Ci}) ->    {Op,guard_bind_value(element(Ci,element(Ti,Rec)))};
guard_bind_one(Rec, {Op,A}, {Tag,Ti,Ci}) ->      {Op,guard_bind_one(Rec,A,{Tag,Ti,Ci})};
guard_bind_one(Rec, {Op,Tag,B}, {Tag,Ti,Ci}) -> 
    % ?Debug("guard_bind_one A rec: ~p guard: ~p bind: ~p~n", [Rec, {Op,Tag,B}, {Tag,Ti,Ci}]),
    case element(Ci,element(Ti,Rec)) of
        {{_,_,_},{_,_,_}} = DT ->
            guard_bind_value(imem_datatype:offset_datetime(Op,DT,B));
        {Mega,Sec,Micro} when is_integer(Mega), is_integer(Sec), is_integer(Micro) ->
            guard_bind_value(imem_datatype:offset_timestamp(Op,{Mega,Sec,Micro},B));
        Other ->
            % ?Debug("guard_bind_one A result: ~p~n", [{Op,guard_bind_value(Other),B}]),
            {Op,guard_bind_value(Other),B}
    end;
guard_bind_one(Rec, {Op,A,Tag}, {Tag,Ti,Ci}) ->  
    % ?Debug("guard_bind_one B rec: ~p guard: ~p bind: ~p~n", [Rec, {Op,A,Tag}, {Tag,Ti,Ci}]),
    case element(Ci,element(Ti,Rec)) of
        {{_,_,_},{_,_,_}} = DT ->
            guard_bind_value(imem_datatype:offset_datetime(Op,DT,A));
        {Mega,Sec,Micro} when is_integer(Mega), is_integer(Sec), is_integer(Micro) ->
            guard_bind_value(imem_datatype:offset_timestamp(Op,{Mega,Sec,Micro},A));
        Other ->
            % ?Debug("guard_bind_one B result: ~p~n", [{Op,A,guard_bind_value(Other)}]),
            {Op,A,guard_bind_value(Other)}
    end;
guard_bind_one(Rec, {Op,A,B}, {Tag,Ti,Ci}) ->
    {Op,guard_bind_one(Rec,A,{Tag,Ti,Ci}),guard_bind_one(Rec,B,{Tag,Ti,Ci})};
guard_bind_one(_, A, _) ->
    guard_bind_value(A).

simplify_guard(Term) ->
    case  simplify_once(Term) of
        join ->                             true;
        {'and',join,R1} ->                  simplify_guard(R1);
%        {'and',R2,join} ->                  simplify_guard(R2);
        Term ->                             Term;
        T ->                                simplify_guard(T)
    end.

%% warning: guard may contain unbound variables '$x' which must not be treated as atom values
simplify_once({'or', true, _}) ->       true; 
simplify_once({'or', _, true}) ->       true; 
simplify_once({'or', false, false}) ->  false; 
simplify_once({'or', _, join}) ->       true;
simplify_once({'or', join, _}) ->       true;
simplify_once({'or', Left, false}) ->   simplify_once(Left); 
simplify_once({'or', false, Right}) ->  simplify_once(Right); 
simplify_once({'or', Same, Same}) ->    simplify_once(Same); 
simplify_once({'and', false, _}) ->     false; 
simplify_once({'and', _, false}) ->     false; 
simplify_once({'and', true, true}) ->   true; 
simplify_once({'and', Left, true}) ->   simplify_once(Left); 
simplify_once({'and', true, Right}) ->  simplify_once(Right); 
simplify_once({'and', join, join}) ->   true; 
simplify_once({'and', Left, join}) ->   simplify_once(Left); 
simplify_once({'and', join, Right}) ->  simplify_once(Right); 
simplify_once({'and', Same, Same}) ->   simplify_once(Same); 
simplify_once({'+', Left, Right}) when  is_number(Left), is_number(Right) -> (Left + Right);
simplify_once({'-', Left, Right}) when  is_number(Left), is_number(Right) -> (Left - Right);
simplify_once({'*', Left, Right}) when  is_number(Left), is_number(Right) -> (Left * Right);
simplify_once({'/', Left, Right}) when  is_number(Left), is_number(Right) -> (Left / Right);
simplify_once({'div', Left, Right}) when is_number(Left), is_number(Right) -> (Left div Right);
simplify_once({'rem', Left, Right}) when is_number(Left), is_number(Right) -> (Left rem Right);
simplify_once({'>', Left, Right}) when  is_number(Left), is_number(Right) -> (Left > Right);
simplify_once({'>=', Left, Right}) when is_number(Left), is_number(Right) -> (Left >= Right);
simplify_once({'<', Left, Right}) when  is_number(Left), is_number(Right) -> (Left < Right);
simplify_once({'=<', Left, Right}) when is_number(Left), is_number(Right) -> (Left =< Right);
simplify_once({'==', Left, Right}) when is_number(Left), is_number(Right) -> (Left == Right);
simplify_once({'/=', Left, Right}) when is_number(Left), is_number(Right) -> (Left /= Right);
simplify_once({'element', N, {const,Tup}}) when is_integer(N),is_tuple(Tup) ->          element(N,Tup);
simplify_once({'element', _, Val}) when is_integer(Val);is_binary(Val);is_list(Val) ->  throw(no_match);
simplify_once({'size', {const,Tup}}) when is_tuple(Tup) ->                              size(Tup);
simplify_once({'hd', List}) when is_list(List) ->                                       hd(List);
simplify_once({'hd', Val}) when is_integer(Val);is_binary(Val) ->                       throw(no_match);
simplify_once({'tl', List}) when is_list(List) ->                                       tl(List);
simplify_once({'length', List}) when is_list(List) ->                                   length(List);
simplify_once({'abs', N}) when is_number(N) ->                                          abs(N);
simplify_once({'round', N}) when is_number(N) ->                                        round(N);
simplify_once({'trunc', N}) when is_number(N) ->                                        trunc(N);
simplify_once({'not', join}) ->         join; 
simplify_once({'not', true}) ->         false; 
simplify_once({'not', false}) ->        true; 
simplify_once({'not', {'/=', Left, Right}}) -> {'==', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'==', Left, Right}}) -> {'/=', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'=<', Left, Right}}) -> {'>',  simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'<', Left, Right}}) ->  {'>=', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'>=', Left, Right}}) -> {'<',  simplify_once(Left), simplify_once(Right)};
simplify_once({'not', {'>', Left, Right}}) ->  {'=<', simplify_once(Left), simplify_once(Right)};
simplify_once({'not', Result}) ->       {'not', simplify_once(Result)};
simplify_once({'or', {'and', C, B}, A}) ->  
    case {filter_member(C),filter_member(B),filter_member(A)} of
        {true,false,false} ->       {'and', {'or', C, A}, {'or', A, B}};
        {false,true,false} ->       {'and', {'or', B, A}, {'or', C, A}};
        _ ->                        {'or', simplify_once({'and', C, B}), simplify_once(A)}
    end;
simplify_once({'and', {'and', C, B}, A}) ->  
    case {filter_member(C),filter_member(B),filter_member(A)} of
        {true,false,false} ->       {'and', C, {'and', A, B}};
        {false,true,false} ->       {'and', B, {'and', C, A}};
        _ ->                        {'and', simplify_once({'and', C, B}), simplify_once(A)}
    end;
simplify_once({'or', B, A} = G) ->  
    case {filter_member(B),filter_member(A)} of
        {false,true} ->             {'or', A, B};
        _ ->                        G
    end;
simplify_once({ Op, Left, Right}) ->    {Op, simplify_once(Left), simplify_once(Right)};
simplify_once(Result) ->                Result.


create_scan_spec(Ti,FullMap,[]) ->
    MatchHead = list_to_tuple(['_'|[Tag || #ddColMap{tag=Tag, tind=Tind} <- FullMap, Tind==Ti]]),
    #scanSpec{sspec=[{MatchHead, [], ['$_']}], limit=?GET_ROWNUM_LIMIT};
create_scan_spec(Ti,FullMap,[SGuard0]) ->
    % ?Debug("SGuard0 ~p~n", [SGuard0]),
    Limit = case operand_match(rownum,SGuard0) of
        false ->  ?GET_ROWNUM_LIMIT;            % #scanSpec{}#scanSpec.limit;
        {'<',rownum,L} when is_integer(L) ->    L-1;
        {'=<',rownum,L} when is_integer(L) ->   L;
        {'>',L,rownum} when is_integer(L) ->    L-1;
        {'>=',L,rownum} when is_integer(L) ->   L;
        {'==',L,rownum} when is_integer(L) ->   L;
        {'==',rownum,L} when is_integer(L) ->   L;
        Else ->
            ?UnimplementedException({"Unsupported use of rownum",{Else}})
    end,
    MatchHead = list_to_tuple(['_'|[Tag || #ddColMap{tag=Tag, tind=Tind} <- FullMap, Tind==Ti]]),
    % ?Debug("MatchHead (~p) ~p~n", [Ti,MatchHead]),
    SGuard1 = simplify_guard(replace_rownum(SGuard0)),
    % ?Debug("SGuard1 ~p~n", [SGuard1]),
    {SGuard2,FGuard} = split_filter_from_guard(SGuard1), 
    SSpec = [{MatchHead, [SGuard2], ['$_']}],
    % ?Debug("SGuard2 ~p~n", [SGuard2]),
    % ?Debug("FGuard ~p~n", [FGuard]),
    SBinds = binds([{Tag,Tind,Ci} || #ddColMap{tag=Tag, tind=Tind, cind=Ci} <- FullMap, (Tind/=Ti)], [SGuard2],[]),
    % ?Debug("SBinds ~p~n", [SBinds]),
    MBinds = binds([{Tag,Tind,Ci} || #ddColMap{tag=Tag, tind=Tind, cind=Ci} <- FullMap, (Tind/=Ti)], [FGuard],[]),
    % ?Debug("MBinds ~p~n", [MBinds]),
    FBinds = binds([{Tag,Tind,Ci} || #ddColMap{tag=Tag, tind=Tind, cind=Ci} <- FullMap, (Tind==Ti)], [FGuard],[]),
    % ?Debug("FBinds ~p~n", [FBinds]),
    #scanSpec{sspec=SSpec,sbinds=SBinds,fguard=FGuard,mbinds=MBinds,fbinds=FBinds,limit=Limit}.


%% Split guard into two pieces:
%% -  a scan guard for mnesia
%% -  a filter guard to be applied to the scan result set
split_filter_from_guard(true) -> {true,true};
split_filter_from_guard(false) -> {false,false};
split_filter_from_guard({'and',L, R}) ->
    case {filter_member(L),filter_member(R)} of
        {true,true} ->      {true, {'and',L, R}};
        {true,false} ->     {R, L};
        {false,true} ->     {L, R};
        {false,false} ->    {{'and',L, R},true}
    end;
split_filter_from_guard(Guard) ->
    case filter_member(Guard) of
        true ->             {true, Guard};
        false ->            {Guard,true}
    end.

%% Does guard contain any of the filter operators?
%% ToDo: bad tuple tolerance for element/2 (add element to function category?)
%% ToDo: bad number tolerance for numeric expressions and functions (add numeric operators to function category?)
filter_member(true) ->      false;
filter_member(false) ->     false;
filter_member(Guard) ->
    filter_member(Guard,?FilterFuns).

filter_member(_,[]) ->  false;
filter_member(Guard,[Op|Ops]) ->
    case operator_member(Op,Guard) of
        true ->             true;
        false ->            filter_member(Guard,Ops)
    end.

%% Does guard contain given operator Tx ?
operator_member(Tx,{Tx,_}) ->        true;
operator_member(Tx,{_,R}) ->         operator_member(Tx,R);
operator_member(Tx,{Tx,_,_}) ->      true;
operator_member(Tx,{_,L,R}) ->       
    case operator_member(Tx,L) of
        true ->     true;
        false ->    operator_member(Tx,R)
    end;
operator_member(_,_) ->              false.

%% First guard tuple for given operator or false if not found
% operator_match(Tx,{Tx,_}=C0) ->     C0;
% operator_match(Tx,{_,R}) ->         operator_match(Tx,R);
% operator_match(Tx,{Tx,_,_}=C1) ->   C1;
% operator_match(Tx,{_,L,R}) ->       
%     case operator_match(Tx,L) of
%         false ->    operator_match(Tx,R);
%         Else ->     Else
%     end;
% operator_match(_,_) ->              false.

% replace_match(Match,Match) ->               true;
% replace_match({Op,Match,Right},Match) ->    {Op,true,Right};
% replace_match({Op,Left,Match},Match) ->     {Op,Left,true};
% replace_match({Op,Left,Right},Match) when is_tuple(Left) ->     
%     NewLeft = replace_match(Left,Match),
%     case NewLeft of
%         Left when is_tuple(Right) ->    {Op,Left,replace_match(Right,Match)};
%         _ ->                            {Op,NewLeft,Right}
%     end;
% replace_match({Op,Left,Right},Match) when is_tuple(Right) ->
%     {Op,Left,replace_match(Right,Match)};
% replace_match({Op,Left,Right}, _) -> {Op,Left,Right}.

%% Does guard contain given operand Tx ?
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

replace_rownum({Op,rownum,Right}) -> {Op,1,Right};
replace_rownum({Op,Left,rownum}) ->  {Op,Left,1};
replace_rownum({Op,Left,Right})->    {Op,replace_rownum(Left),replace_rownum(Right)};
replace_rownum({Op,Result}) ->       {Op,replace_rownum(Result)};
replace_rownum(Result) ->            Result.

escape_sql(Str) when is_list(Str) ->
    re:replace(Str, "(')", "''", [global, {return, list}]);
escape_sql(Bin) when is_binary(Bin) ->
    re:replace(Bin, "(')", "''", [global, {return, binary}]).

un_escape_sql(Str) when is_list(Str) ->
    re:replace(Str, "('')", "'", [global, {return, list}]);
un_escape_sql(Bin) when is_binary(Bin) ->
    re:replace(Bin, "('')", "'", [global, {return, binary}]).

build_sort_fun(SelectSections,FullMap) ->
    case lists:keyfind('order by', 1, SelectSections) of
        {_, []} ->      fun(_X) -> {} end;
        {_, Sorts} ->   ?Debug("Sorts: ~p~n", [Sorts]),
                        SortFuns = [sort_fun_item(Name,Direction,FullMap) || {Name,Direction} <- Sorts],
                        fun(X) -> list_to_tuple([F(X)|| F <- SortFuns]) end;
        SError ->       ?ClientError({"Invalid order by in select structure", SError})
    end.

build_sort_spec(SelectSections,FullMaps,ColMaps) ->
    case lists:keyfind('order by', 1, SelectSections) of
        {_, []} ->      [];
        {_, Sorts} ->   ?Debug("Sorts: ~p~n", [Sorts]),
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

filter_spec_where(?NoMoreFilter, _, ?EmptyWhere, LeftTree) ->
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
filter_field_value(_Tag,_Type,_Len,_Prec,_Def,Val) -> imem_datatype:add_squotes(imem_sql:escape_sql(Val)).    

sort_spec_order([],_,_) -> [];
sort_spec_order(SortSpec,FullMaps,ColMaps) ->
    sort_spec_order(SortSpec,FullMaps,ColMaps,[]).

sort_spec_order([],_,_,Acc) -> 
    lists:reverse(Acc);        
sort_spec_order([SS|SortSpecs],FullMaps,ColMaps, Acc) ->
    sort_spec_order(SortSpecs,FullMaps,ColMaps,[sort_order(SS,FullMaps,ColMaps)|Acc]).

sort_order({Ti,Ci,Direction},FullMaps,ColMaps) ->
    %% SortSpec given referencing FullMap Ti,Ci    
    case [{S,T,N} || #ddColMap{tind=Tind,cind=Cind,schema=S,table=T,name=N} <- FullMaps, Tind==Ti, Cind==Ci] of
        [QN] ->    {sort_name_short(QN,FullMaps,ColMaps),Direction};
        _ ->       ?ClientError({"Bad sort field reference", {Ti,Ci}})
    end;
sort_order({Cp,Direction},FullMaps,ColMaps) when is_integer(Cp) ->
    %% SortSpec given referencing ColMap position    
    #ddColMap{schema=S,table=T,name=N} = lists:nth(Cp,ColMaps),
    {sort_name_short({S,T,N},FullMaps,ColMaps),Direction};
sort_order({CName,Direction},_,_) ->
    {CName,Direction}.

sort_name_short({_S,T,N},FullMaps,_ColMaps) ->
    case length(lists:usort([{Su,Tu,Nu} || #ddColMap{schema=Su,table=Tu,name=Nu} <- FullMaps, Nu==N])) of
        1 ->    list_to_binary(field_name({undefined,undefined,N}));
        _ ->    list_to_binary(field_name({undefined,T,N}))
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
    % ?Debug("sort on col position ~p Ti=~p Ci=~p ~p~n",[Cp,Ti,Ci,lists:nth(Cp,ColMaps)]),    
    sort_fun(Type,Ti,Ci,Direction);
sort_fun_any({CName,Direction},FullMaps,_) ->
    %% SortSpec given referencing FullMap alias    
    case lists:keysearch(CName, #ddColMap.alias, FullMaps) of
        {value,#ddColMap{tind=Ti,cind=Ci,type=Type}} ->
            % ?Debug("sort on col name  ~p Ti=~p Ci=~p ~p~n",[CName,Ti,Ci,Type]),    
            sort_fun(Type,Ti,Ci,Direction);
        Else ->     
            ?ClientError({"Bad sort field", Else})
    end.

sort_fun(atom,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case pick(Ci,Ti,X) of 
            A when is_atom(A) ->
                [ -Item || Item <- atom_to_list(A)] ++ [?MaxChar];
            V -> V
        end
    end;
sort_fun(binstr,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case pick(Ci,Ti,X) of 
            B when is_binary(B) ->
                [ -Item || Item <- binary_to_list(B)] ++ [?MaxChar];
            V -> V
        end
    end;
sort_fun(boolean,Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        V = pick(Ci,Ti,X),
        case V of
            true ->         false;
            false ->        true;
            _ ->            V
        end 
    end;
sort_fun(datetime,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case pick(Ci,Ti,X) of 
            {{Y,M,D},{Hh,Mm,Ss}} when is_integer(Y), is_integer(M), is_integer(D), is_integer(Hh), is_integer(Mm), is_integer(Ss) -> 
                {{-Y,-M,-D},{-Hh,-Mm,-Ss}};
            V -> V
        end 
    end;
sort_fun(decimal,Ti,Ci,<<"desc">>) -> sort_fun(number,Ti,Ci,<<"desc">>);
sort_fun(float,Ti,Ci,<<"desc">>) ->   sort_fun(number,Ti,Ci,<<"desc">>);
sort_fun(integer,Ti,Ci,<<"desc">>) -> sort_fun(number,Ti,Ci,<<"desc">>);
sort_fun(ipadr,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case pick(Ci,Ti,X) of 
            {A,B,C,D} when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
                {-A,-B,-C,-D};
            {A,B,C,D,E,F,G,H} when is_integer(A), is_integer(B), is_integer(C), is_integer(D), is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
                {-A,-B,-C,-D,-E,-F,-G,-H};
            V -> V
        end
    end;
sort_fun(number,Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        V = pick(Ci,Ti,X),
        case is_number(V) of
            true ->         (-V);
            false ->        V
        end 
    end;
sort_fun(string,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case pick(Ci,Ti,X) of 
            [H|T] when is_integer(H) ->
                [ -Item || Item <- [H|T]] ++ [?MaxChar];
            V -> V
        end
    end;
sort_fun(timestamp,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case pick(Ci,Ti,X) of 
            {Meg,Sec,Micro} when is_integer(Meg), is_integer(Sec), is_integer(Micro)->
                {-Meg,-Sec,-Micro};
            V -> V
        end    
    end;
sort_fun({atom,atom},Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        case pick(Ci,Ti,X) of 
            {T,A} when is_atom(T), is_atom(A) ->
                {[ -ItemT || ItemT <- atom_to_list(T)] ++ [?MaxChar]
                ,[ -ItemA || ItemA <- atom_to_list(A)] ++ [?MaxChar]
                };
            V -> V
        end    
    end;
sort_fun({atom,integer},Ti,Ci,<<"desc">>) -> sort_fun({atom,number},Ti,Ci,<<"desc">>);
sort_fun({atom,decimal},Ti,Ci,<<"desc">>) -> sort_fun({atom,number},Ti,Ci,<<"desc">>);
sort_fun({atom,float},Ti,Ci,<<"desc">>) -> sort_fun({atom,number},Ti,Ci,<<"desc">>);
sort_fun({atom,userid},Ti,Ci,<<"desc">>) -> sort_fun({atom,number},Ti,Ci,<<"desc">>);
sort_fun({atom,number},Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        case pick(Ci,Ti,X) of 
            {T,N} when is_atom(T), is_number(N) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-N};
            V -> V
        end    
    end;
sort_fun({atom,ipaddr},Ti,Ci,<<"desc">>) ->
    fun(X) -> 
        case pick(Ci,Ti,X) of 
            {T,{A,B,C,D}} when is_atom(T), is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-A,-B,-C,-D};
            {T,{A,B,C,D,E,F,G,H}} when is_atom(T),is_integer(A), is_integer(B), is_integer(C), is_integer(D), is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
                {[ -Item || Item <- atom_to_list(T)] ++ [?MaxChar],-A,-B,-C,-D,-E,-F,-G,-H};
            V -> V   
        end    
    end;
sort_fun(tuple,Ti,Ci,<<"desc">>) -> 
    fun(X) -> 
        case pick(Ci,Ti,X) of 
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
sort_fun(userid,Ti,Ci,<<"desc">>) ->   sort_fun(number,Ti,Ci,<<"desc">>);
sort_fun(Type,_Ti,_Ci,<<"desc">>) ->
    ?SystemException({"Unsupported datatype for sort desc", Type});
sort_fun(_Type,Ti,Ci,_) -> 
    % ?Debug("Sort ~p  : ~p ~p~n", [_Type, Ti,Ci]), 
    fun(X) -> pick(Ci,Ti,X) end.

pick(Ci,Ti,X) -> pick(Ci,element(Ti,X)).

pick(_,undefined) -> ?nav;
pick(Ci,Tuple) -> element(Ci,Tuple).

re_compile(S) when is_list(S);is_binary(S) ->
    case (catch re:compile(S))  of
        {ok, MP} -> MP;
        _ ->        never_match
    end;
re_compile(_) ->    never_match.

like_compile(S) -> like_compile(S, <<>>).

like_compile(S, Esc) when is_list(S); is_binary(S)  -> re_compile(transform_like(S, Esc));
like_compile(_,_)                                   -> never_match.

transform_like(S, Esc) ->
    E = if
        Esc =:= "" ->       "";
        Esc =:= <<>> ->     "";
        is_list(Esc) ->     Esc;
        is_binary(Esc) ->   binary_to_list(Esc);
        true ->             ""
    end,
    Escape = if E =:= "" -> ""; true -> "["++E++"]" end,
    NotEscape = if E =:= "" -> ""; true -> "([^"++E++"])" end,
    S0 = re:replace(S, "([\\\\^$.\\[\\]|()?*+\\-{}])", "\\\\\\1", [global, {return, binary}]),
    S1 = re:replace(S0, NotEscape++"%", "\\1.*", [global, {return, binary}]),
    S2 = re:replace(S1, NotEscape++"_", "\\1.", [global, {return, binary}]),
    S3 = re:replace(S2, Escape++"%", "%", [global, {return, binary}]),
    S4 = re:replace(S3, Escape++"_", "_", [global, {return, binary}]),
    list_to_binary(["^",S4,"$"]).

re_match(never_match, _) -> false;
re_match(RE, S) when is_list(S);is_binary(S) ->
    case re:run(S, RE) of
        nomatch ->  false;
        _ ->        true
    end;
re_match(RE, S) ->
    case re:run(io_lib:format("~p", [S]), RE) of
        nomatch ->  false;
        _ ->        true
    end.

%% Constant tuple expressions
make_expr_fun(_, {const, A}, _) when is_tuple(A) -> A;
%% Comparison expressions
make_expr_fun(_, {'==', Same, Same}, _) -> true;
make_expr_fun(_, {'/=', Same, Same}, _) -> false;
make_expr_fun(Ctx, {Op, A, B}, FBinds) when Op=='==';Op=='>';Op=='>=';Op=='<';Op=='=<';Op=='/=' ->
    make_comp_fun(Ctx, {Op, A, B}, FBinds); 
%% Mathematical expressions    
make_expr_fun(_, {'pi'}, _) -> math:pi();
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='+';Op=='-' ->
    make_math_fun(Ctx, {Op, A}, FBinds); 
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='sqrt';Op=='log';Op=='log10';Op=='exp';Op=='erf';Op=='erfc' ->
    make_module_fun(Ctx, 'math', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='sin';Op=='cos';Op=='tan';Op=='asin';Op=='acos';Op=='atan' ->
    make_module_fun(Ctx, 'math', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='sinh';Op=='cosh';Op=='tanh';Op=='asinh';Op=='acosh';Op=='atanh' ->
    make_module_fun(Ctx, 'math', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A, B}, FBinds) when Op=='+';Op=='-';Op=='*';Op=='/';Op=='div';Op=='rem' ->
    make_math_fun(Ctx, {Op, A, B}, FBinds);
make_expr_fun(Ctx, {Op, A, B}, FBinds) when Op=='pow';Op=='atan2' ->
    make_module_fun(Ctx, 'math', {Op, A, B}, FBinds);
%% Erlang module
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='abs';Op=='length';Op=='hd';Op=='tl';Op=='size';Op=='tuple_size';Op=='round';Op=='trunc' ->
    make_module_fun(Ctx, 'erlang', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='atom_to_list';Op=='binary_to_float';Op=='binary_to_integer';Op=='binary_to_list' ->
    make_module_fun(Ctx, 'erlang', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='bitstring_to_list';Op=='binary_to_term';Op=='bit_size';Op=='byte_size';Op=='crc32' ->
    make_module_fun(Ctx, 'erlang', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='float';Op=='float_to_binary';Op=='float_to_list';Op=='fun_to_list';Op=='tuple_to_list' ->
    make_module_fun(Ctx, 'erlang', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='integer_to_binary';Op=='integer_to_list';Op=='fun_to_list';Op=='list_to_float' ->
    make_module_fun(Ctx, 'erlang', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='list_to_integer';Op=='list_to_pid';Op=='list_to_tuple';Op=='phash2';Op=='pid_to_list' ->
    make_module_fun(Ctx, 'erlang', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='is_atom';Op=='is_binary';Op=='is_bitstring';Op=='is_boolean' ->
    make_module_fun(Ctx, 'erlang', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='is_float';Op=='is_function';Op=='is_integer';Op=='is_list';Op=='is_number' ->
    make_module_fun(Ctx, 'erlang', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='is_pid';Op=='is_port';Op=='is_reference';Op=='is_tuple' ->
    make_module_fun(Ctx, 'erlang', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A, B}, FBinds) when Op=='is_function';Op=='is_record';Op=='atom_to_binary';Op=='binary_part' ->
    make_module_fun(Ctx, 'erlang', {Op, A, B}, FBinds);
make_expr_fun(Ctx, {Op, A, B}, FBinds) when  Op=='integer_to_binary';Op=='integer_to_list';Op=='list_to_binary';Op=='list_to_bitstring' ->
    make_module_fun(Ctx, 'erlang', {Op, A, B}, FBinds);
make_expr_fun(Ctx, {Op, A, B}, FBinds) when  Op=='list_to_integer';Op=='max';Op=='min';Op=='phash2' ->
    make_module_fun(Ctx, 'erlang', {Op, A, B}, FBinds);
make_expr_fun(Ctx, {Op, A, B}, FBinds) when Op=='crc32';Op=='float_to_binary';Op=='float_to_list' ->
    make_module_fun(Ctx, 'erlang', {Op, A, B}, FBinds);
make_expr_fun(Ctx, {Op, A, B}, FBinds) when Op=='atom_to_binary';Op=='binary_to_integer';Op=='binary_to_integer';Op=='binary_to_term' ->
    make_module_fun(Ctx, 'erlang', {Op, A, B}, FBinds);
%% Lists module
make_expr_fun(Ctx, {Op, A}, FBinds) when Op=='last';Op=='reverse';Op=='sort';Op=='usort' ->
    make_module_fun(Ctx, 'lists', {Op, A}, FBinds);
make_expr_fun(Ctx, {Op, A, B}, FBinds) when Op=='nth';Op=='member';Op=='merge';Op=='nthtail';Op=='seq';Op=='sublist';Op=='subtract';Op=='usort' ->
    make_module_fun(Ctx, 'lists', {Op, A, B}, FBinds);
%% Logical expressions
make_expr_fun(Ctx, {'not', A}, FBinds) ->
    case make_expr_fun(Ctx, A, FBinds) of
        ?nav ->                     ?nav;
        true ->                     false;
        false ->                    true;
        F when is_function(F) ->    fun(X) ->   Abound=F(X), 
                                                case Abound of 
                                                    ?nav -> ?nav; 
                                                    true -> false;
                                                    false -> true
                                                end
                                    end
    end;                       
make_expr_fun(Ctx, {'and', A, B}, FBinds) ->
    Fa = make_expr_fun(Ctx,A,FBinds),
    Fb = make_expr_fun(Ctx,B,FBinds),
    case {Fa,Fb} of
        {true,true} ->  true;
        {false,_} ->    false;
        {_,false} ->    false;
        {true,_} ->     Fb;         %% may be ?nav or a fun evaluating to ?nav
        {_,true} ->     Fa;         %% may be ?nav or a fun evaluating to ?nav
        {_,_} ->        fun(X) -> (Fa(X) and Fb(X)) end
    end;
make_expr_fun(Ctx, {'or', A, B}, FBinds) ->
    Fa = make_expr_fun(Ctx,A,FBinds),
    Fb = make_expr_fun(Ctx,B,FBinds),
    case {Fa,Fb} of
        {false,false}-> false;
        {true,_} ->     true;
        {_,true} ->     true;
        {false,_} ->    Fb;         %% may be ?nav or a fun evaluating to ?nav
        {_,false} ->    Fa;         %% may be ?nav or a fun evaluating to ?nav
        {_,_} ->        fun(X) -> (Fa(X) or Fb(X)) end
    end;
%% Custom filters
make_expr_fun(Ctx, {Op, A, B}, FBinds) when Op=='is_member';Op=='is_like';Op=='is_regexp_like';Op=='element' ->
    make_filter_fun(Ctx,{Op, A, B}, FBinds);
make_expr_fun(Ctx, {'safe', A}, FBinds) ->
    make_safe_fun(Ctx, A, FBinds);
make_expr_fun(_Ctx, {Op, A}, _FBinds) ->
    ?UnimplementedException({"Unsupported expression operator", {Op, A}});
make_expr_fun(_Ctx, {Op, A, B}, _FBinds) ->
    ?UnimplementedException({"Unsupported expression operator", {Op, A, B}});
make_expr_fun(_Ctx, Value, _FBinds)  -> Value.


bind_action(P,_FBinds) when is_function(P) -> true;     %% parameter already bound to function
bind_action(P,FBinds) -> lists:keyfind(P,1,FBinds).     %% bind {'$x',Ti,Xi} or false for value prameter 


make_safe_fun(Ctx, A, FBinds) ->
    Fa = make_expr_fun(Ctx,A,FBinds),
    make_safe_fun_final(Ctx, Fa, FBinds).

make_safe_fun_final(_Ctx, A, FBinds) ->
    case bind_action(A,FBinds) of 
        false ->            A;        
        true ->             fun(X) -> try A(X) catch _:_ -> ?nav end end;       
        ABind ->            fun(X) -> try ?BoundVal(ABind,X) catch _:_ -> ?nav end end
    end.

make_module_fun(Ctx, Mod, {Op, {const,A}}, FBinds) when is_tuple(A) ->
    make_module_fun_final(Ctx, Mod, {Op, A}, FBinds);
make_module_fun(Ctx, Mod, {Op, A}, FBinds) ->
    make_module_fun_final(Ctx, Mod, {Op, make_expr_fun(Ctx,A,FBinds)}, FBinds);
make_module_fun(Ctx, Mod, {Op, {const,A}, {const,B}}, FBinds) when is_tuple(A),is_tuple(B)->
    make_module_fun_final(Ctx, Mod, {Op, A, B}, FBinds);
make_module_fun(Ctx, Mod, {Op, {const,A}, B}, FBinds) when is_tuple(A) ->
    make_module_fun_final(Ctx, Mod, {Op, A, make_expr_fun(Ctx,B,FBinds)}, FBinds);
make_module_fun(Ctx, Mod, {Op, A, {const,B}}, FBinds) when is_tuple(B) ->
    make_module_fun_final(Ctx, Mod, {Op, make_expr_fun(Ctx,A,FBinds), B}, FBinds);
make_module_fun(Ctx, Mod, {Op, A, B}, FBinds) ->
    Fa = make_expr_fun(Ctx,A,FBinds),
    Fb = make_expr_fun(Ctx,B,FBinds),
    make_module_fun_final(Ctx, Mod, {Op, Fa, Fb}, FBinds).

make_module_fun_final(_Ctx, Mod, {Op, A}, FBinds) -> 
    case bind_action(A,FBinds) of 
        false ->        Mod:Op(A);
        true ->         fun(X) -> Mod:Op(A(X)) end;
        ABind ->        fun(X) -> Mod:Op(?BoundVal(ABind,X)) end
    end;
make_module_fun_final(_Ctx, Mod, {Op, A, B}, FBinds) -> 
    case {bind_action(A,FBinds),bind_action(B,FBinds)} of 
        {false,false} ->        Mod:Op(A,B);
        {false,true} ->         fun(X) -> Mod:Op(A,B(X)) end;
        {false,BBind} ->        fun(X) -> Mod:Op(A,?BoundVal(BBind,X)) end;
        {true,false} ->         fun(X) -> Mod:Op(A(X),B) end;
        {true,true} ->          fun(X) -> Mod:Op(A(X),B(X)) end; 
        {true,BBind} ->         fun(X) -> Mod:Op(A(X),?BoundVal(BBind,X)) end; 
        {ABind,false} ->        fun(X) -> Mod:Op(?BoundVal(ABind,X),B) end; 
        {ABind,true} ->         fun(X) -> Mod:Op(?BoundVal(ABind,X),B(X)) end; 
        {ABind,BBind} ->        fun(X) -> Mod:Op(?BoundVal(ABind,X),?BoundVal(BBind,X)) end 
    end.

make_math_fun(Ctx, {Op, A}, FBinds) ->
    make_math_fun_unary(Ctx, {Op, make_expr_fun(Ctx,A,FBinds)}, FBinds);
make_math_fun(Ctx, {Op, A, B}, FBinds) ->
    Fa = make_expr_fun(Ctx,A,FBinds),
    Fb = make_expr_fun(Ctx,B,FBinds),
    make_math_fun_binary(Ctx, {Op, Fa, Fb}, FBinds).

make_math_fun_unary(_, {'+', A}, FBinds) ->
    case bind_action(A,FBinds) of 
        false ->            A;        
        true ->             A;       
        ABind ->            fun(X) -> ?BoundVal(ABind,X) end
    end;
make_math_fun_unary(_, {'-', A}, FBinds) ->
    case bind_action(A,FBinds) of 
        false ->            A;        
        true ->             fun(X) -> (-A(X)) end;       
        ABind ->            fun(X) -> (-?BoundVal(ABind,X)) end
    end.

-define(MathOpBlockBinary(__Op,__A,__B), 
            case __Op of
                '+' ->      (__A + __B);
                '-' ->      (__A - __B);
                '*' ->      (__A * __B);
                '/' ->      (__A / __B);
                'div' ->    (__A div __B);
                'rem' ->    (__A rem __B)
            end).

make_math_fun_binary(_Ti, {Op, A, B}, FBinds) ->
    case {bind_action(A,FBinds),bind_action(B,FBinds)} of 
        {false,false} ->    ?MathOpBlockBinary(Op,A,B);
        {false,true} ->     fun(X) -> ?MathOpBlockBinary(Op,A,B(X)) end;
        {false,BBind} ->    fun(X) -> ?MathOpBlockBinary(Op,A,?BoundVal(BBind,X)) end;
        {true,false} ->     fun(X) -> ?MathOpBlockBinary(Op,A(X),B) end;
        {true,true} ->      fun(X) -> ?MathOpBlockBinary(Op,A(X),B(X)) end;  
        {true,BBind} ->     fun(X) -> ?MathOpBlockBinary(Op,A(X),?BoundVal(BBind,X)) end;  
        {ABind,false} ->    fun(X) -> ?MathOpBlockBinary(Op,?BoundVal(ABind,X),B) end;  
        {ABind,true} ->     fun(X) -> ?MathOpBlockBinary(Op,?BoundVal(ABind,X),B(X)) end;  
        {ABind,BBind} ->    fun(X) -> ?MathOpBlockBinary(Op,?BoundVal(ABind,X),?BoundVal(BBind,X)) end
    end.

make_comp_fun(Ctx, {Op, {const,A}, {const,B}}, FBinds) when is_tuple(A),is_tuple(B)->
    make_comp_fun_final(Ctx, {Op, A, B}, FBinds);
make_comp_fun(Ctx, {Op, {const,A}, B}, FBinds) when is_tuple(A) ->
    make_comp_fun_final(Ctx, {Op, A, make_expr_fun(Ctx,B,FBinds)}, FBinds);
make_comp_fun(Ctx, {Op, A, {const,B}}, FBinds) when is_tuple(B) ->
    make_comp_fun_final(Ctx, {Op, make_expr_fun(Ctx,A,FBinds), B}, FBinds);
make_comp_fun(Ctx, {Op, A, B}, FBinds) ->
    Fa = make_expr_fun(Ctx,A,FBinds),
    Fb = make_expr_fun(Ctx,B,FBinds),
    make_comp_fun_final(Ctx, {Op, Fa, Fb}, FBinds).


-define(CompOpBlock(__Op,__A,__B), 
        if
            (__A == ?nav) -> ?nav;
            (__B == ?nav) -> ?nav;
            true ->
                case __Op of
                    '==' -> (__A == __B);
                    '>' ->  (__A > __B);
                    '>=' -> (__A >= __B);
                    '<' ->  (__A < __B);
                    '=<' -> (__A =< __B);
                    '/=' -> (__A /= __B)
                end
        end).

make_comp_fun_final(_Ctx, {Op, A, B}, FBinds) ->
    case {bind_action(A,FBinds),bind_action(B,FBinds)} of 
        {false,false} ->    ?CompOpBlock(Op,A,B);
        {false,true} ->     fun(X) -> Bbound=B(X),?CompOpBlock(Op,A,Bbound) end;
        {false,BBind} ->    fun(X) -> Bbound=?BoundVal(BBind,X),?CompOpBlock(Op,A,Bbound) end;
        {true,false} ->     fun(X) -> Abound=A(X),?CompOpBlock(Op,Abound,B) end;
        {true,true} ->      fun(X) -> Abound=A(X),Bbound=B(X),?CompOpBlock(Op,Abound,Bbound) end;  
        {true,BBind} ->     fun(X) -> Abound=A(X),Bbound=?BoundVal(BBind,X),?CompOpBlock(Op,Abound,Bbound) end;  
        {ABind,false} ->    fun(X) -> Abound=?BoundVal(ABind,X),?CompOpBlock(Op,Abound,B) end;  
        {ABind,true} ->     fun(X) -> Abound=?BoundVal(ABind,X),Bbound=?BoundVal(ABind,X),?CompOpBlock(Op,Abound,Bbound) end;  
        {ABind,BBind} ->    fun(X) -> Abound=?BoundVal(ABind,X),Bbound=?BoundVal(BBind,X),?CompOpBlock(Op,Abound,Bbound) end
    end.

make_filter_fun(Ctx, {Op, {const,A}, {const,B}}, FBinds) ->
    make_filter_fun_final(Ctx,{Op, A, B}, FBinds);
make_filter_fun(Ctx, {Op, {const,A}, B}, FBinds) ->
    make_filter_fun_final(Ctx,{Op, A, make_expr_fun(Ctx, B, FBinds)}, FBinds);
make_filter_fun(Ctx, {Op, A, {const,B}}, FBinds) ->
    make_filter_fun_final(Ctx,{Op, make_filter_fun(Ctx, A, FBinds), B}, FBinds);
make_filter_fun(Ctx, {Op, A, B}, FBinds) ->
    FA = make_expr_fun(Ctx, A, FBinds),
    FB = make_expr_fun(Ctx, B, FBinds),
    make_filter_fun_final(Ctx, {Op, FA, FB}, FBinds);
make_filter_fun(_Ti, Value, _FBinds) -> Value.


-define(ElementOpBlock(__A,__B), 
    if 
        (not is_number(__A)) -> ?nav; 
        (not is_tuple(__B)) -> ?nav;
        (not tuple_size(__B) >= __A) -> ?nav;
        true -> element(__A,__B)
    end).

make_filter_fun_final(_Ctx, {'element', A, B}, FBinds)  ->
    case {bind_action(A,FBinds),bind_action(B,FBinds)} of 
        {false,false} ->    ?ElementOpBlock(A,B);
        {false,true} ->     fun(X) -> Bbound=B(X),?ElementOpBlock(A,Bbound) end;
        {false,BBind} ->    fun(X) -> Bbound=?BoundVal(BBind,X),?ElementOpBlock(A,Bbound) end;
        {true,false} ->     fun(X) -> Abound=A(X),?ElementOpBlock(Abound,B) end;
        {true,true} ->      fun(X) -> Abound=A(X),Bbound=B(X),?ElementOpBlock(Abound,Bbound) end;
        {true,BBind} ->     fun(X) -> Abound=A(X),Bbound=?BoundVal(BBind,X),?ElementOpBlock(Abound,Bbound) end;
        {ABind,false} ->    fun(X) -> Abound=?BoundVal(ABind,X),?ElementOpBlock(Abound,B) end;
        {ABind,true} ->     fun(X) -> Abound=?BoundVal(ABind,X),Bbound=B(X),?ElementOpBlock(Abound,Bbound) end;
        {ABind,BBind} ->    fun(X) -> Abound=?BoundVal(ABind,X),Bbound=?BoundVal(BBind,X),?ElementOpBlock(Abound,Bbound) end
    end;
make_filter_fun_final(_, {'is_member', A, '$_'}, FBinds) ->
    case bind_action(A,FBinds) of 
        false ->        
            fun(X) -> 
                lists:member(A,tl(tuple_to_list(element(?MainIdx,X))))
            end;
        true ->        
            fun(X) -> 
                lists:member(A(X),tl(tuple_to_list(element(?MainIdx,X))))
            end;
        ABind ->  
            fun(X) ->
                lists:member(?BoundVal(ABind,X),tl(tuple_to_list(element(?MainIdx,X))))
            end
    end;
make_filter_fun_final(_Ctx, {'is_like', A, B}, FBinds)  ->
    case {bind_action(A,FBinds),bind_action(B,FBinds)} of 
        {false,false} ->    re_match(like_compile(B),A);
        {false,true} ->     fun(X) -> re_match(like_compile(B(X)),A) end;
        {false,BBind} ->    fun(X) -> re_match(like_compile(?BoundVal(BBind,X)),A) end;
        {true,false} ->     RE = like_compile(B), fun(X) -> re_match(RE,A(X)) end;
        {true,true} ->      fun(X) -> re_match(like_compile(B(X)),A(X)) end;
        {true,BBind} ->     fun(X) -> re_match(like_compile(A(X)),?BoundVal(BBind,X)) end;
        {ABind,false} ->    RE = like_compile(B), fun(X) -> re_match(RE,?BoundVal(ABind,X)) end;
        {ABind,true} ->     fun(X) -> re_match(like_compile(B(X)),?BoundVal(ABind,X)) end;
        {ABind,BBind} ->    fun(X) -> re_match(like_compile(?BoundVal(BBind,X)),?BoundVal(ABind,X)) end
    end;
make_filter_fun_final(_Ctx, {'is_regexp_like', A, B}, FBinds)  ->
    case {bind_action(A,FBinds),bind_action(B,FBinds)} of 
        {false,false} ->    re_match(re_compile(B),A);
        {false,true} ->     fun(X) -> re_match(re_compile(B(X)),A) end;
        {false,BBind} ->    fun(X) -> re_match(re_compile(?BoundVal(BBind,X)),A) end;
        {true,false} ->     RE = re_compile(B), fun(X) -> re_match(RE,A(X)) end;
        {true,true} ->      fun(X) -> re_match(re_compile(B(X)),A(X)) end;
        {true,BBind} ->     fun(X) -> re_match(re_compile(A(X)),?BoundVal(BBind,X)) end;
        {ABind,false} ->    RE = re_compile(B), fun(X) -> re_match(RE,?BoundVal(ABind,X)) end;
        {ABind,true} ->     fun(X) -> re_match(re_compile(B(X)),?BoundVal(ABind,X)) end;
        {ABind,BBind} ->    fun(X) -> re_match(re_compile(?BoundVal(BBind,X)),?BoundVal(ABind,X)) end
    end;
make_filter_fun_final(_Ctx, {'is_member', A, B}, FBinds)  ->
    case {bind_action(A,FBinds),bind_action(B,FBinds)} of 
        {false,false} ->        
            if 
                is_list(B) ->   lists:member(A,B);
                is_tuple(B) ->  lists:member(A,tuple_to_list(B));
                true ->         false
            end;
        {false,true} ->
            fun(X) ->
                Bbound = B(X),
                if 
                    is_list(Bbound) ->  lists:member(A,Bbound);
                    is_tuple(Bbound) -> lists:member(A,tuple_to_list(Bbound));
                    true ->             false
                end
            end;
        {false,BBind} ->
            fun(X) ->
                Bbound = ?BoundVal(BBind,X),
                if 
                    is_list(Bbound) ->  lists:member(A,Bbound);
                    is_tuple(Bbound) -> lists:member(A,tuple_to_list(Bbound));
                    true ->             false
                end
            end;
        {true,false} ->  
            fun(X) ->
                Abound = A(X),
                if 
                    is_list(B) ->  lists:member(Abound,B);
                    is_tuple(B) -> lists:member(Abound,tuple_to_list(B));
                    true ->        false
                end
            end;
        {true,true} ->  
            fun(X) ->
                Abound = A(X),
                Bbound = B(X), 
                if 
                    is_list(Bbound) ->  lists:member(Abound,Bbound);
                    is_tuple(Bbound) -> lists:member(Abound,tuple_to_list(Bbound));
                    true ->             false
                end
            end;
        {true,BBind} ->  
            fun(X) ->
                Abound = A(X),
                Bbound = ?BoundVal(BBind,X), 
                if 
                    is_list(Bbound) ->  lists:member(Abound,Bbound);
                    is_tuple(Bbound) -> lists:member(Abound,tuple_to_list(Bbound));
                    true ->             false
                end
            end;
        {ABind,false} ->  
            fun(X) ->
                if 
                    is_list(B) ->  lists:member(?BoundVal(ABind,X),B);
                    is_tuple(B) -> lists:member(?BoundVal(ABind,X),tuple_to_list(B));
                    true ->        false
                end
            end;
        {ABind,true} ->  
            fun(X) ->
                Bbound = B(X), 
                if 
                    is_list(Bbound) ->  lists:member(?BoundVal(ABind,X),Bbound);
                    is_tuple(Bbound) -> lists:member(?BoundVal(ABind,X),tuple_to_list(Bbound));
                    true ->             false
                end
            end;
        {ABind,BBind} ->  
            fun(X) ->
                Bbound = ?BoundVal(BBind,X), 
                if 
                    is_list(Bbound) ->  lists:member(?BoundVal(ABind,X),Bbound);
                    is_tuple(Bbound) -> lists:member(?BoundVal(ABind,X),tuple_to_list(Bbound));
                    true ->             false
                end
            end
    end;
make_filter_fun_final(Ctx,FGuard, FBinds) ->
    ?UnimplementedException({"Unsupported filter function",{Ctx, FGuard, FBinds}}).



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
        ?Info("----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        ?Info("schema ~p~n", [imem_meta:schema()]),
        ?Info("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey = case IsSec of
            true -> ?imem_test_admin_login();
            _ ->    ok
        end,

    %% filter_member
        ?assertEqual(true, filter_member({'is_member', {'+','$2',1}, '$3'})),
        ?assertEqual(false, filter_member({'==', {'+','$2',1}, '$3'})),
        ?assertEqual(true, filter_member({'==', {'safe',{'+','$2',1}}, '$3'})),
        ?assertEqual(false, filter_member({'or', {'==','$2',1}, {'==','$3',1}})),
        ?assertEqual(true, filter_member({'and', {'==','$2',1}, {'is_member',1,'$3'}})),

    %% make_expr_fun
        ?assertEqual(true, make_expr_fun(1, true, [])),
        ?assertEqual(false, make_expr_fun(1, false, [])),
        ?assertEqual(true, make_expr_fun(1, {'not', false}, [])),
        ?assertEqual(false, make_expr_fun(1, {'not', true}, [])),
        ?assertEqual(12, make_expr_fun(1, 12, [])),
        ?assertEqual(a, make_expr_fun(1, a, [])),
        ?assertEqual({a,b}, make_expr_fun(1, {const,{a,b}}, [])),
        ?assertEqual(true, make_expr_fun(1, {'==', 10,10}, [])),
        ?assertEqual(true, make_expr_fun(1, {'==', {const,{a,b}}, {const,{a,b}}}, [])), 
        ?assertEqual(false, make_expr_fun(1, {'==', {const,{a,b}}, {const,{a,1}}}, [])), 
        ?assertEqual(true, make_expr_fun(1,{'is_member', a, [a,b]}, [])),
        ?assertEqual(true, make_expr_fun(1,{'is_member', 1, [a,b,1]}, [])),
        ?assertEqual(false, make_expr_fun(1,{'is_member', 1, [a,b,c]}, [])),
        ?assertEqual(true, make_expr_fun(1,{'is_member', 1, {const,a,b,1}}, [])),
        ?assertEqual(false, make_expr_fun(1,{'is_member', 1, {const,a,b,c}}, [])),
        ?assertEqual(true, make_expr_fun(1,{'is_like', "12345", "%3%"}, [])),
        ?assertEqual(true, make_expr_fun(1,{'is_like', <<"12345">>, "%3__"}, [])),
        ?assertEqual(true, make_expr_fun(1,{'is_like', "12345", <<"1%">>}, [])),
        ?assertEqual(true, make_expr_fun(1,{'is_like', {'+',12300,45}, <<"%45">>}, [])),
        ?assertEqual(false, make_expr_fun(1,{'is_like', "12345", "%6%"}, [])),
        ?assertEqual(false, make_expr_fun(1,{'is_like', <<"12345">>, "%7__"}, [])),
        ?assertEqual(33, make_expr_fun(1, {'*',{'+',10,1},3}, [])),
        ?assertEqual(10, make_expr_fun(1, {'abs',{'-',10,20}}, [])),
        ?Info("success ~p~n", ["make_expr_fun constants"]),

        X1 = {{1,2,3},{2,2,2}},
        F1 = make_expr_fun(1, {'==', '$1','$2'}, [{'$1',1,2},{'$2',1,2}]),
        ?assertEqual(true, F1(X1)),

        F1a = make_expr_fun(1, {'==', '$1','$2'}, [{'$1',1,2},{'$2',2,2}]),
        ?assertEqual(true, F1a(X1)),

        F2 = make_expr_fun(1,{'is_member', a, '$3'}, [{'$3',1,3}]),
        ?assertEqual(false,F2({{1,2,[3,4,5]}})),        
        ?assertEqual(true, F2({{1,2,[c,a,d]}})),        

        F3 = make_expr_fun(1,{'is_member', '$2', '$3'}, [{'$2',1,2},{'$3',1,3}]),
        ?assertEqual(true, F3({{1,d,[c,a,d]}})),        
        ?assertEqual(true, F3({{1,c,[c,a,d]}})),        
        ?assertEqual(true, F3({{1,a,[c,a,d]}})),        
        ?assertEqual(true, F3({{1,3,[3,4,5]}})),        
        ?assertEqual(false,F3({{1,2,[3,4,5]}})),        
        ?assertEqual(false,F3({{1,a,[3,4,5]}})),        
        ?assertEqual(false,F3({{1,[a],[3,4,5]}})),        
        ?assertEqual(false,F3({{1,3,[]}})),        

        F4 = make_expr_fun(1,{'is_member', {'+','$2',1}, '$3'}, [{'$2',1,2},{'$3',1,3}]),
        ?assertEqual(true, F4({{1,2,[3,4,5]}})),        
        ?assertEqual(false,F4({{1,2,[c,4,d]}})),        

        F5 = make_expr_fun(1,{'is_member', a, '$_'}, []),
        ?assertEqual(true, F5({{},{1,a,[c,a,d]}})),        
        ?assertEqual(false, F5({{},{1,d,[c,a,d]}})),        
        ?Info("success ~p~n", ["make_expr_fun with binds"]),

        % ?assert(false),


    %% Like strig to Regex string
        ?assertEqual(<<"^Sm.th$">>, transform_like(<<"Sm_th">>, <<>>)),
        ?assertEqual(<<"^.*Sm.th.*$">>, transform_like(<<"%Sm_th%">>, <<>>)),
        ?assertEqual(<<"^.A.*Sm.th.*$">>, transform_like(<<"_A%Sm_th%">>, <<>>)),
        ?assertEqual(<<"^.A.*S\\$m.t\\*\\[h.*$">>, transform_like(<<"_A%S$m_t*[h%">>, <<>>)),
        ?assertEqual(<<"^.A.*S\\^\\$\\.\\[\\]\\|\\(\\)\\?\\*\\+\\-\\{\\}m.th.*$">>, transform_like(<<"_A%S^$.[]|()?*+-{}m_th%">>, <<>>)),
        ?assertEqual(<<"^Sm_th.$">>, transform_like(<<"Sm@_th_">>, <<"@">>)),
        ?assertEqual(<<"^Sm%th.*$">>, transform_like(<<"Sm@%th%">>, <<"@">>)),
        ?Info("success ~p~n", [transform_like]),

    %% Regular Expressions
        ?Info("testing regular expressions: ~p~n", ["like_compile"]),
        RE1 = like_compile("abc_123%@@"),
        ?assertEqual(true,re_match(RE1,"abc_123%@@")),         
        ?assertEqual(true,re_match(RE1,<<"abc_123jhhsdhjhj@@">>)),         
        ?assertEqual(true,re_match(RE1,"abc_123%%@@@")),         
        ?assertEqual(true,re_match(RE1,"abc0123@@")),         
        ?assertEqual(false,re_match(RE1,"abc_123%@")),         
        ?assertEqual(false,re_match(RE1,"abc_123%@")),         
        ?assertEqual(false,re_match(RE1,"")),         
        ?assertEqual(false,re_match(RE1,<<"">>)),         
        ?assertEqual(false,re_match(RE1,<<"abc_@@">>)),         
        RE2 = like_compile(<<"%@@">>,<<>>),
        ?assertEqual(true,re_match(RE2,"abc_123%@@")),         
        ?assertEqual(true,re_match(RE2,<<"123%@@">>)),         
        ?assertEqual(true,re_match(RE2,"@@")),
        ?assertEqual(true,re_match(RE2,"@@@")),
        ?assertEqual(false,re_match(RE2,"abc_123%@")),         
        ?assertEqual(false,re_match(RE2,"@.@")),         
        ?assertEqual(false,re_match(RE2,"@_@")),         
        RE3 = like_compile(<<"text_in%">>),
        ?assertEqual(true,re_match(RE3,<<"text_in_text">>)),         
        ?assertEqual(true,re_match(RE3,"text_in_text")),         
        ?assertEqual(true,re_match(RE3,<<"text_in_quotes\"">>)),         
        ?assertEqual(false,re_match(RE3,<<"\"text_in_quotes">>)),         
        ?assertEqual(false,re_match(RE3,"\"text_in_quotes\"")),         
        ?assertEqual(false,re_match(RE3,<<"\"text_in_quotes\"">>)),         
        RE4 = like_compile(<<"%12">>),
        ?assertEqual(true,re_match(RE4,12)),         
        ?assertEqual(true,re_match(RE4,112)),         
        ?assertEqual(true,re_match(RE4,012)),         
        ?assertEqual(false,re_match(RE4,122)),         
        ?assertEqual(false,re_match(RE4,1)),         
        ?assertEqual(false,re_match(RE4,11)),         
        RE5 = like_compile(<<"12.5%">>),
        ?assertEqual(true,re_match(RE5,12.51)),         
        ?assertEqual(true,re_match(RE5,12.55)),         
        ?assertEqual(true,re_match(RE5,12.50)),         
        ?assertEqual(false,re_match(RE5,12)),         
        ?assertEqual(false,re_match(RE5,12.4)),         
        ?assertEqual(false,re_match(RE5,12.49999)),         

        %% ToDo: implement and test patterns involving regexp reserved characters

        % ?Info("success ~p~n", [replace_match]),
        % L = {like,'$6',"%5%"},
        % NL = {not_like,'$7',"1%"},
        % ?assertEqual( true, replace_match(L,L)),
        % ?assertEqual( NL, replace_match(NL,L)),
        % ?assertEqual( {'and',true,NL}, replace_match({'and',L,NL},L)),
        % ?assertEqual( {'and',L,true}, replace_match({'and',L,NL},NL)),
        % ?assertEqual( {'and',{'and',true,NL},{a,b,c}}, replace_match({'and',{'and',L,NL},{a,b,c}},L)),
        % ?assertEqual( {'and',{'and',L,{a,b,c}},true}, replace_match({'and',{'and',L,{a,b,c}},NL},NL)),
        % ?assertEqual( {'and',{'and',true,NL},{'and',L,NL}}, replace_match({'and',{'and',L,NL},{'and',L,NL}},L)),
        % ?assertEqual( {'and',NL,{'and',true,NL}}, replace_match({'and',NL,{'and',L,NL}},L)),
        % ?assertEqual( {'and',NL,{'and',NL,NL}}, replace_match({'and',NL,{'and',NL,NL}},L)),
        % ?assertEqual( {'and',NL,{'and',NL,true}}, replace_match({'and',NL,{'and',NL,L}},L)),
        % ?assertEqual( {'and',{'and',{'and',{'=<',5,'$1'},L},true},{'==','$1','$6'}}, replace_match({'and',{'and',{'and',{'=<',5,'$1'},L},NL},{'==','$1','$6'}},NL)),
        % ?assertEqual( {'and',{'and',{'and',{'=<',5,'$1'},true},NL},{'==','$1','$6'}}, replace_match({'and',{'and',{'and',{'=<',5,'$1'},L},NL},{'==','$1','$6'}},L)),


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


        % field names
        ?assertEqual({undefined,undefined,field}, field_qname(<<"field">>)),
        ?assertEqual({undefined,table,field}, field_qname(<<"table.field">>)),
        ?assertEqual({schema,table,field}, field_qname(<<"schema.table.field">>)),

        ?assertEqual("field", field_name(field_qname(<<"field">>))),
        ?assertEqual("table.field", field_name(field_qname(<<"table.field">>))),
        ?assertEqual("schema.table.field", field_name(field_qname(<<"schema.table.field">>))),

        % table names without alias
        ?assertEqual({imem,table,table}, table_qname(<<"table">>)),
        ?assertEqual({schema,table,table}, table_qname(<<"schema.table">>)),
        ?assertEqual({schema,table,alias}, table_qname(<<"schema.table">>, <<"alias">>)),
        ?assertEqual({schema,table,alias}, table_qname(<<"schema.table">>, "alias")),

        % table names with alias
        ?assertEqual({imem,table,alias}, table_qname({<<"table">>,"alias"})),
        ?assertEqual({schema,table,alias}, table_qname({<<"schema.table">>, "alias"})),
        ?assertEqual({imem,table,alias}, table_qname({<<"table">>,<<"alias">>})),
        ?assertEqual({schema,table,alias}, table_qname({<<"schema.table">>, <<"alias">>})),

        ?Info("----TEST--~p:test_mnesia~n", [?MODULE]),

        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?Info("success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        ?Info("success ~p~n", [data_nodes]),

        ?Info("----TEST--~p:test_database_operations~n", [?MODULE]),
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

        Table1 =    {imem, meta_table_1, meta_table_1},
        Table2 =    {undefined, meta_table_2, meta_table_2},
        Table3 =    {undefined, meta_table_3, meta_table_3},
        TableX =    {undefined,meta_table_x, meta_table_1},

        Alias1 =    {undefined, meta_table_1, alias1},
        Alias2 =    {imem, meta_table_1, alias2},

        ?assertException(throw, {ClEr, {"Table does not exist", {imem, meta_table_x}}}, column_map([TableX], [])),
        ?Info("success ~p~n", [table_no_exists]),

        ColsE1=     [ #ddColMap{tag="A1", schema=imem, table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", name=x}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Unknown column name", x}}, column_map([Table1], ColsE1)),
        ?Info("success ~p~n", [unknown_column_name_1]),

        ColsE2=     [ #ddColMap{tag="A1", schema=imem, table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", table=meta_table_x, name=b1}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Unknown column name", {meta_table_x,b1}}}, column_map([Table1, Table2], ColsE2)),
        ?Info("success ~p~n", [unknown_column_name_2]),

        ?assertMatch([_,_,_], column_map([Table1], [])),
        ?Info("success ~p~n", [empty_select_columns]),


        ColsF =     [ #ddColMap{tag="A", tind=1, cind=1, schema=imem, table=meta_table_1, name=a, type=integer, alias= <<"a">>}
                    , #ddColMap{tag="B", tind=1, cind=2, table=meta_table_1, name=b1, type=string, alias= <<"b1">>}
                    , #ddColMap{tag="C", tind=1, cind=3, name=c1, type=ipaddr, alias= <<"c1">>}
                    ],

        ?assertEqual([], filter_spec_where(?NoFilter, ColsF, [])),
        ?assertEqual({wt}, filter_spec_where(?NoFilter, ColsF, {wt})),
        FA1 = {1,[<<"111">>]},
        CA1 = {'=',<<"imem.meta_table_1.a">>,<<"111">>},
        ?assertEqual({'and',CA1,{wt}}, filter_spec_where({'or',[FA1]}, ColsF, {wt})),
        FB2 = {2,[<<"222">>]},
        CB2 = {'=',<<"meta_table_1.b1">>,<<"'222'">>},
        ?assertEqual({'and',{'and',CA1,CB2},{wt}}, filter_spec_where({'and',[FA1,FB2]}, ColsF, {wt})),
        FC3 = {3,[<<"3.1.2.3">>,<<"3.3.2.1">>]},
        CC3 = {'in',<<"c1">>,{'list',[<<"'3.1.2.3'">>,<<"'3.3.2.1'">>]}},
        ?assertEqual({'and',{'or',{'or',CA1,CB2},CC3},{wt}}, filter_spec_where({'or',[FA1,FB2,FC3]}, ColsF, {wt})),
        ?assertEqual({'and',{'and',{'and',CA1,CB2},CC3},{wt}}, filter_spec_where({'and',[FA1,FB2,FC3]}, ColsF, {wt})),

        FB2a = {2,[<<"22'2">>]},
        CB2a = {'=',<<"meta_table_1.b1">>,<<"'22''2'">>},
        ?assertEqual({'and',{'and',CA1,CB2a},{wt}}, filter_spec_where({'and',[FA1,FB2a]}, ColsF, {wt})),

        ?Info("success ~p~n", [filter_spec_where]),

        ?assertEqual([], sort_spec_order([], ColsF, ColsF)),
        SA = {1,1,<<"desc">>},
        OA = {<<"a">>,<<"desc">>},
        ?assertEqual([OA], sort_spec_order([SA], ColsF, ColsF)),
        SB = {1,2,<<"asc">>},
        OB = {<<"b1">>,<<"asc">>},
        ?assertEqual([OB], sort_spec_order([SB], ColsF, ColsF)),
        SC = {1,3,<<"desc">>},
        OC = {<<"c1">>,<<"desc">>},
        ?assertEqual([OC], sort_spec_order([SC], ColsF, ColsF)),
        ?assertEqual([OC,OA], sort_spec_order([SC,SA], ColsF, ColsF)),
        ?assertEqual([OB,OC,OA], sort_spec_order([SB,SC,SA], ColsF, ColsF)),

        ?assertEqual([OC], sort_spec_order([OC], ColsF, ColsF)),
        ?assertEqual([OC,OA], sort_spec_order([OC,SA], ColsF, ColsF)),
        ?assertEqual([OC,OA], sort_spec_order([SC,OA], ColsF, ColsF)),
        ?assertEqual([OB,OC,OA], sort_spec_order([OB,OC,OA], ColsF, ColsF)),

        ?Info("success ~p~n", [sort_spec_order]),

        ColsA =     [ #ddColMap{tag="A1", schema=imem, table=meta_table_1, name=a, alias= <<"a">>}
                    , #ddColMap{tag="A2", table=meta_table_1, name=b1, alias= <<"b1">>}
                    , #ddColMap{tag="A3", name=c1, alias= <<"c1">>}
                    ],

        ?assertException(throw, {ClEr,{"Empty table list", _}}, column_map([], ColsA)),
        ?Info("success ~p~n", [empty_table_list]),

        ?assertException(throw, {ClEr,{"Ambiguous column name", a}}, column_map([Table1, Table3], [#ddColMap{name=a}])),
        ?Info("success ~p~n", [columns_ambiguous_a]),

        ?assertException(throw, {ClEr,{"Ambiguous column name", c1}}, column_map([Table1, Table3], ColsA)),
        ?Info("success ~p~n", [columns_ambiguous_c1]),

        ?assertMatch([_,_,_], column_map([Table1], ColsA)),
        ?Info("success ~p~n", [columns_A]),

        ?assertMatch([_,_,_,_,_,_], column_map([Table1, Table3], [#ddColMap{name='*'}])),
        ?Info("success ~p~n", [columns_13_join]),

        Cmap3 = column_map([Table1, Table2, Table3], [#ddColMap{name='*'}]),
        % ?Info("ColMap3 ~p~n", [Cmap3]),        
        ?assertMatch([_,_,_,_,_,_,_,_], Cmap3),
        ?assertEqual(lists:sort(Cmap3), Cmap3),
        ?Info("success ~p~n", [columns_123_join]),

        ?assertMatch([_,_,_,_,_,_,_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{name='*'}])),
        ?Info("success ~p~n", [alias_113_join]),

        ?assertMatch([_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=meta_table_3, name='*'}])),
        ?Info("success ~p~n", [columns_113_star1]),

        ?assertMatch([_,_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=alias1, name='*'},#ddColMap{table=meta_table_3, name='a'}])),
        ?Info("success ~p~n", [columns_alias_1]),

        ?assertMatch([_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=alias1, name=a},#ddColMap{table=alias2, name='a'}])),
        ?Info("success ~p~n", [columns_alias_2]),

        ?assertMatch([_,_], column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{name=sysdate}])),
        ?Info("success ~p~n", [sysdate]),

        ?assertException(throw, {ClEr,{"Unknown column name", {any,sysdate}}}, column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{table=any, name=sysdate}])),
        ?Info("success ~p~n", [sysdate_reject]),

        ?assertEqual(["imem.alias1.a","undefined.undefined.user"], column_map_items(column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{name=user}]),qname)),
        ?Info("success ~p~n", [user]),

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

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

-endif.
