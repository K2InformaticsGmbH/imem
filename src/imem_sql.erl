-module(imem_sql).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-export([ exec/5
        , parse/1
        ]).

-export([ field_qname/1
        , table_qname/1
        , table_qname/2
        , column_map_items/2
        , column_map/2
        , simplify_guard/1
        , create_scan_spec/4
        , operand_member/2
        ]).

parse(Statement) when is_list(Statement) ->
    Sql =
    case [lists:last(string:strip(Statement))] of
        ";" -> Statement;
        _ -> Statement ++ ";"
    end,
    case (catch sql_lex:string(Sql)) of
        {ok, Tokens, _} ->
            case (catch sql_parse:parse(Tokens)) of
                {ok, [ParseTree|_]} ->  ParseTree;
                {'EXIT', Error} ->      ?ClientError({"SQL parser error", Error});
                Error ->                ?ClientError({"SQL parser error", Error})
            end;
        {'EXIT', Error} -> ?ClientError({"SQL lexer error", Error})
    end.

exec(SKey, Statement, BlockSize, Schema, IsSec) when is_list(Statement) ->
    Sql =
    case [lists:last(string:strip(Statement))] of
        ";" -> Statement;
        _ -> Statement ++ ";"
    end,
    case (catch sql_lex:string(Sql)) of
        {ok, Tokens, _} ->
            case (catch sql_parse:parse(Tokens)) of
                {ok, [ParseTree|_]} -> 
                    exec(SKey, element(1,ParseTree), ParseTree, 
                        #statement{stmtStr=Statement, stmtParse=ParseTree, blockSize=BlockSize}, 
                        Schema, IsSec);
                {'EXIT', Error} -> 
                    ?ClientError({"SQL parser error", Error});
                Error -> 
                    ?ClientError({"SQL parser error", Error})
            end;
        {'EXIT', Error} -> ?ClientError({"SQL lexer error", Error})
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
    
exec(SKey, 'create table', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, 'drop table', ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, Command, _ParseTree, _Stmt, _Schema, _IsSec) ->
    ?UnimplementedException({"SQL command unimplemented", {SKey, Command}}).


field_qname(B) when is_binary(B) ->
    field_qname(binary_to_list(B));
field_qname(Str) when is_list(Str) ->
    case string:tokens(Str, ".") of
        [A] ->      {undefined, undefined, list_to_atom(A)};
        [T,A] ->    {undefined, list_to_atom(T), list_to_atom(A)};
        [S,T,A] ->  {list_to_atom(S), list_to_atom(T), list_to_atom(A)};
        _ ->        {}
        % _ ->        ?ClientError({"Invalid field name", Str})
    end;
field_qname(S) ->
    ?ClientError({"Invalid field name", S}).

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
    [lists:flatten(io_lib:format("~p(~p,~p)", [C#ddColMap.type,C#ddColMap.length,C#ddColMap.precision])) || C <- Map];
column_map_items(Map, tind) ->
    [C#ddColMap.tind || C <- Map];
column_map_items(Map, cind) ->
    [C#ddColMap.cind || C <- Map];
column_map_items(Map, type) ->
    [C#ddColMap.type || C <- Map];
column_map_items(Map, length) ->
    [C#ddColMap.length || C <- Map];
column_map_items(Map, precision) ->
    [C#ddColMap.precision || C <- Map];
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
    Cols = case imem_meta:read(ddTable,{Schema,Table}) of
        [#ddTable{columns=C}] ->    C;
        [] ->                       ?ClientError({"Table does not exist",{Schema,Table}})
    end,
    L = [{Tindex, Cindex, Schema, Alias, Cinfo#ddColumn.name, Cinfo} || {Cindex, Cinfo} <- lists:zip(lists:seq(2,length(Cols)+1), Cols)],
    % io:format(user, "column_map lookup ~p~n", [Lookup++L]),
    column_map(Tables, Columns, Tindex+1, Lookup++L, Meta, Acc);

column_map([], [#ddColMap{schema=undefined, table=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    Cmaps = [ Cmap0#ddColMap{schema=S, table=T, tind=Ti, cind=Ci, type=Type, length=Len, precision=P, name=N} || {Ti, Ci, S, T, N, #ddColumn{type=Type, length=Len, precision=P}} <- Lookup],
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Meta, Acc);
column_map([], [#ddColMap{schema=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    column_map([], [Cmap0#ddColMap{schema=imem_meta:schema()}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name='*'}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    Cmaps = [ Cmap0#ddColMap{schema=S, table=T, tind=Ti, cind=Ci, type=Type, length=Len, precision=P, name=N} || {Ti, Ci, S, T, N, #ddColumn{type=Type, length=Len, precision=P}} <- Lookup, S==Schema, T==Table],
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Meta, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name=Name}=Cmap0|Columns], Tindex, Lookup, Meta, Acc) ->
    Pred = fun(L) ->
        (Name == element(5, L)) andalso
        ((Table == undefined) or (Table == element(4, L))) andalso
        ((Schema == undefined) or (Schema == element(3, L)))
    end,
    Lmatch = lists:filter(Pred, Lookup),
    %% io:format(user, "column_map matching tables ~p~n", [Lmatch]),
    Tcount = length(lists:usort([{element(3, X), element(4, X)} || X <- Lmatch])),
    %% io:format(user, "column_map matching table count ~p~n", [Tcount]),
    MetaField = imem_meta:meta_field(Name),
    if 
        (Tcount==0) andalso (Schema==undefined) andalso (Table==undefined) andalso MetaField ->
            {Cindex, Meta1} = case index_of(Name, Meta) of
                false ->    {length(Meta)+1, Meta ++ [Name]};
                Index ->    {Index, Meta}
            end, 
            #ddColumn{type=Type, length=Len, precision=P} = imem_meta:meta_field_info(Name),
            Cmap1 = Cmap0#ddColMap{tind=Tindex, cind=Cindex, type=Type, length=Len, precision=P},
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
            {Ti, Ci, S, T, Name, #ddColumn{type=Type, length=Len, precision=P, default=D}} = hd(Lmatch),
            R = ((Ti > 1) or (Ci < 3)),
            Cmap1 = Cmap0#ddColMap{schema=S, table=T, tind=Ti, cind=Ci, type=Type, length=Len, precision=P, default=D, readonly=R},
            column_map([], Columns, Tindex, Lookup, Meta, [Cmap1|Acc])
    end;
column_map([], [{'fun',Fname,[Name]}|Columns], Tindex, Lookup, Meta, Acc) ->
    {S,T,N} = field_qname(Name),
    column_map([], [#ddColMap{schema=S, table=T, name=N, func=Fname}|Columns], Tindex, Lookup, Meta, Acc);    
column_map([], [{as, {'fun',Fname,[Name]}, BAlias}|Columns], Tindex, Lookup, Meta, Acc) ->
    Alias = ?binary_to_atom(BAlias),
    {S,T,N} = field_qname(Name),
    column_map([], [#ddColMap{schema=S, table=T, name=N, func=Fname, alias=Alias}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [{as, Name, BAlias}|Columns], Tindex, Lookup, Meta, Acc) ->
    Alias = ?binary_to_atom(BAlias),
    {S,T,N} = field_qname(Name),
    column_map([], [#ddColMap{schema=S, table=T, name=N, alias=Alias}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [Name|Columns], Tindex, Lookup, Meta, Acc) when is_binary(Name)->
    {S,T,N} = field_qname(Name),
    column_map([], [#ddColMap{schema=S, table=T, name=N, alias=N}|Columns], Tindex, Lookup, Meta, Acc);
column_map([], [Expression|_], _Tindex, _Lookup, _Meta, _Acc)->
    ?UnimplementedException({"Expressions not supported", Expression});
column_map([], [], _Tindex, _Lookup, _Meta, Acc) ->
    lists:reverse(Acc);
column_map(Tables, Columns, Tmax, Lookup, Meta, Acc) ->
    io:format(user, "column_map error Tables ~p~n", [Tables]),
    io:format(user, "column_map error Columns ~p~n", [Columns]),
    io:format(user, "column_map error Tmax ~p~n", [Tmax]),
    io:format(user, "column_map error Lookup ~p~n", [Lookup]),
    io:format(user, "column_map error Meta ~p~n", [Meta]),
    io:format(user, "column_map error Acc ~p~n", [Acc]),
    ?ClientError({"Column map invalid parameter",{Tables,Columns}}).


index_of(Item, List) -> index_of(Item, List, 1).

index_of(_, [], _)  -> false;
index_of(Item, [Item|_], Index) -> Index;
index_of(Item, [_|Tl], Index) -> index_of(Item, Tl, Index+1).

simplify_guard(Term) ->
    case  simplify_once(Term) of
        Term -> Term;
        T ->    simplify_guard(T)
    end.

simplify_once({'or', true, _}) -> true; 
simplify_once({'or', _, true}) -> true; 
simplify_once({'or', false, false}) -> false; 
simplify_once({'or', Left, false}) -> simplify_once(Left); 
simplify_once({'or', false, Right}) -> simplify_once(Right); 
simplify_once({'or', Same, Same}) -> simplify_once(Same); 
simplify_once({'and', false, _}) -> false; 
simplify_once({'and', _, false}) -> false; 
simplify_once({'and', true, true}) -> true; 
simplify_once({'and', Left, true}) -> simplify_once(Left); 
simplify_once({'and', true, Right}) -> simplify_once(Right); 
simplify_once({'and', Same, Same}) -> simplify_once(Same); 
simplify_once({ '+', Left, Right}) when is_number(Left), is_number(Right) -> Left + Right;
simplify_once({ '-', Left, Right}) when is_number(Left), is_number(Right) -> Left - Right;
simplify_once({ '*', Left, Right}) when is_number(Left), is_number(Right) -> Left * Right;
simplify_once({ '/', Left, Right}) when is_number(Left), is_number(Right) -> Left / Right;
simplify_once({ 'div', Left, Right}) when is_number(Left), is_number(Right) -> Left div Right;
simplify_once({ '>', Left, Right}) when is_number(Left), is_number(Right) -> (Left > Right);
simplify_once({ '>=', Left, Right}) when is_number(Left), is_number(Right) -> (Left >= Right);
simplify_once({ '<', Left, Right}) when is_number(Left), is_number(Right) -> (Left < Right);
simplify_once({ '=<', Left, Right}) when is_number(Left), is_number(Right) -> (Left =< Right);
simplify_once({ '==', Left, Right}) when is_number(Left), is_number(Right) -> (Left == Right);
simplify_once({ '/=', Left, Right}) when is_number(Left), is_number(Right) -> (Left /= Right);
simplify_once({ Op, Left, Right}) -> {Op, simplify_once(Left), simplify_once(Right)};
simplify_once({'not', true}) -> false; 
simplify_once({'not', false}) -> true; 
simplify_once({'not', Result}) -> {'not', simplify_once(Result)};
simplify_once({ Op, Result}) -> {Op, Result};
simplify_once(Result) -> Result.


create_scan_spec(_Tmax,Ti,FullMap,[]) ->
    MatchHead = list_to_tuple(['_'|[Tag || #ddColMap{tag=Tag, tind=Tind} <- FullMap, Tind==Ti]]),
    #scanSpec{sspec=[{MatchHead, [], ['$_']}]};
create_scan_spec(_Tmax,Ti,FullMap,[SGuard0]) ->
    % io:format(user, "SGuard0 ~p~n", [SGuard0]),
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
    % io:format(user, "MatchHead (~p) ~p~n", [Ti,MatchHead]),
    SGuard1 = simplify_guard(replace_rownum(SGuard0)),
    % io:format(user, "SGuard1 ~p~n", [SGuard1]),
    FGuard = case operator_match('is_member',SGuard1) of
        false ->    true;   %% no filtering needed
        F ->        F
    end,
    SGuard2 = simplify_guard(replace_is_member(SGuard1)),
    SSpec = [{MatchHead, [SGuard2], ['$_']}],
    % io:format(user, "FGuard ~p~n", [FGuard]),
    SBinds = binds([{Tag,Tind,Ci} || #ddColMap{tag=Tag, tind=Tind, cind=Ci} <- FullMap, (Tind/=Ti)], [SGuard2],[]),
    % io:format(user, "SBinds ~p~n", [SBinds]),
    MBinds = binds([{Tag,Tind,Ci} || #ddColMap{tag=Tag, tind=Tind, cind=Ci} <- FullMap, (Tind/=Ti)], [FGuard],[]),
    % io:format(user, "MBinds ~p~n", [MBinds]),
    FBinds = binds([{Tag,Tind,Ci} || #ddColMap{tag=Tag, tind=Tind, cind=Ci} <- FullMap, (Tind==Ti)], [FGuard],[]),
    % io:format(user, "FBinds ~p~n", [FBinds]),
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

replace_is_member({is_member,_Left,_Right})->    true;
replace_is_member({Op,Left,Right})->    {Op,replace_is_member(Left),replace_is_member(Right)};
replace_is_member({Op,Result}) ->       {Op,replace_is_member(Result)};
replace_is_member(Result) ->            Result.

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
        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        io:format(user, "schema ~p~n", [imem_meta:schema()]),
        io:format(user, "data nodes ~p~n", [imem_meta:data_nodes()]),
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

        io:format(user, "----TEST--~p:test_mnesia~n", [?MODULE]),

        ?assertEqual(true, is_atom(imem_meta:schema())),
        io:format(user, "success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        io:format(user, "success ~p~n", [data_nodes]),

        io:format(user, "----TEST--~p:test_database_operations~n", [?MODULE]),
        _Types1 =    [ #ddColumn{name=a, type=char, length=1}     %% key
                    , #ddColumn{name=b1, type=char, length=1}    %% value 1
                    , #ddColumn{name=c1, type=char, length=1}    %% value 2
                    ],
        _Types2 =    [ #ddColumn{name=a, type=integer, length=10}    %% key
                    , #ddColumn{name=b2, type=float, length=8, precision=3}   %% value
                    ],

        ?assertEqual(ok, exec(SKey, "create table meta_table_1 (a char, b1 char, c1 char);", 0, "Imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, meta_table_1])),    

        ?assertEqual(ok, exec(SKey, "create table meta_table_2 (a integer, b2 float);", 0, "Imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, meta_table_2])),    

        ?assertEqual(ok, exec(SKey, "create table meta_table_3 (a char, b3 integer, c1 char);", 0, "Imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, meta_table_1])),    
        io:format(user, "success ~p~n", [create_tables]),

        Table1 =    {'Imem', meta_table_1, meta_table_1},
        Table2 =    {undefined, meta_table_2, meta_table_2},
        Table3 =    {undefined, meta_table_3, meta_table_3},
        TableX =    {undefined,meta_table_x, meta_table_1},

        Alias1 =    {undefined, meta_table_1, alias1},
        Alias2 =    {'Imem', meta_table_1, alias2},

        ?assertException(throw, {ClEr, {"Table does not exist", {'Imem', meta_table_x}}}, column_map([TableX], [])),
        io:format(user, "success ~p~n", [table_no_exists]),

        ColsE1=     [ #ddColMap{tag="A1", schema='Imem', table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", name=x}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Unknown column name", x}}, column_map([Table1], ColsE1)),
        io:format(user, "success ~p~n", [unknown_column_name_1]),

        ColsE2=     [ #ddColMap{tag="A1", schema='Imem', table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", table=meta_table_x, name=b1}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Unknown column name", {meta_table_x,b1}}}, column_map([Table1, Table2], ColsE2)),
        io:format(user, "success ~p~n", [unknown_column_name_2]),

        ?assertMatch([_,_,_], column_map([Table1], [])),
        io:format(user, "success ~p~n", [empty_select_columns]),


        ColsA =     [ #ddColMap{tag="A1", schema='Imem', table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", table=meta_table_1, name=b1}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Empty table list", _}}, column_map([], ColsA)),
        io:format(user, "success ~p~n", [empty_table_list]),

        ?assertException(throw, {ClEr,{"Ambiguous column name", a}}, column_map([Table1, Table3], [#ddColMap{name=a}])),
        io:format(user, "success ~p~n", [columns_ambiguous_a]),

        ?assertException(throw, {ClEr,{"Ambiguous column name", c1}}, column_map([Table1, Table3], ColsA)),
        io:format(user, "success ~p~n", [columns_ambiguous_c1]),

        ?assertMatch([_,_,_], column_map([Table1], ColsA)),
        io:format(user, "success ~p~n", [columns_A]),

        ?assertMatch([_,_,_,_,_,_], column_map([Table1, Table3], [#ddColMap{name='*'}])),
        io:format(user, "success ~p~n", [columns_13_join]),

        Cmap3 = column_map([Table1, Table2, Table3], [#ddColMap{name='*'}]),
        % io:format(user, "ColMap3 ~p~n", [Cmap3]),        
        ?assertMatch([_,_,_,_,_,_,_,_], Cmap3),
        ?assertEqual(lists:sort(Cmap3), Cmap3),
        io:format(user, "success ~p~n", [columns_123_join]),

        ?assertMatch([_,_,_,_,_,_,_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{name='*'}])),
        io:format(user, "success ~p~n", [alias_113_join]),

        ?assertMatch([_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=meta_table_3, name='*'}])),
        io:format(user, "success ~p~n", [columns_113_star1]),

        ?assertMatch([_,_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=alias1, name='*'},#ddColMap{table=meta_table_3, name='a'}])),
        io:format(user, "success ~p~n", [columns_alias_1]),

        ?assertMatch([_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=alias1, name=a},#ddColMap{table=alias2, name='a'}])),
        io:format(user, "success ~p~n", [columns_alias_2]),

        ?assertMatch([_,_], column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{name=sysdate}])),
        io:format(user, "success ~p~n", [sysdate]),

        ?assertException(throw, {ClEr,{"Unknown column name", {any,sysdate}}}, column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{table=any, name=sysdate}])),
        io:format(user, "success ~p~n", [sysdate_reject]),

        ?assertEqual(["'Imem'.alias1.a","undefined.undefined.user"], column_map_items(column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{name=user}]),qname)),
        io:format(user, "success ~p~n", [user]),

        ?assertEqual(ok, imem_meta:drop_table(meta_table_3)),
        ?assertEqual(ok, imem_meta:drop_table(meta_table_2)),
        ?assertEqual(ok, imem_meta:drop_table(meta_table_1)),
        io:format(user, "success ~p~n", [drop_tables]),

        case IsSec of
            true -> ?imem_logout(SKey);
            _ ->    ok
        end
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

