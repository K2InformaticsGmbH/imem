-module(imem_sql_table).

-include("imem_seco.hrl").

-export([ exec/5
        ]).
    
exec(_SKey, {'drop table', {tables, []}, _Exists, _RestrictCascade}, _Stmt, _Schema, _IsSec) -> ok;
exec(SKey, {'drop table', {tables, [TableName|Tables]}, Exists, RestrictCascade}=_ParseTree, Stmt, Schema, IsSec) ->
    % ?Log("ParseTree ~p~n", [_ParseTree]),
    QName = imem_sql:table_qname(TableName),      %% ?binary_to_existing_atom(Table),
    if_call_mfa(IsSec, 'drop_table', [SKey,QName]),
    exec(SKey, {'drop table', {tables, Tables}, Exists, RestrictCascade}, Stmt, Schema, IsSec);
exec(SKey, {'truncate table', TableName, {}, {}}=_ParseTree, _Stmt, _Schema, IsSec) ->
    % ?Log("Parse Tree ~p~n", [_ParseTree]),
    if_call_mfa(IsSec, 'truncate_table', [SKey, imem_sql:table_qname(TableName)]);

exec(SKey, {'create table', TableName, Columns, TOpts}=_ParseTree, _Stmt, _Schema, IsSec) ->
    % ?Log("Parse Tree ~p~n", [_ParseTree]),
    create_table(SKey, imem_sql:table_qname(TableName), TOpts, Columns, IsSec, []).

create_table(SKey, Table, TOpts, [], IsSec, ColMap) ->
    if_call_mfa(IsSec, 'create_table', [SKey, Table, lists:reverse(ColMap), TOpts]);
create_table(SKey, Table, TOpts, [{Name, Type, COpts}|Columns], IsSec, ColMap) ->
    {T,L,P} = case Type of
        B when is_binary(B) ->      {imem_datatype:io_to_term(imem_datatype:strip_squotes(binary_to_list(B))),0,0};
        A when is_atom(A) ->        {imem_datatype:imem_type(A),0,0};
        {float,SPrec} ->            {float,0,list_to_integer(SPrec)};
        {timestamp,SPrec} ->        {timestamp,0,list_to_integer(SPrec)};
        {Typ,SLen} ->               {imem_datatype:imem_type(Typ),list_to_integer(SLen),0};
        {Typ,SLen,SPrec} ->         {imem_datatype:imem_type(Typ),list_to_integer(SLen),list_to_integer(SPrec)};
        Else ->                     ?SystemException({"Unexpected parse tree structure",Else})
    end,
    {Default,Opts} = case lists:keyfind(default, 1, COpts) of
        false ->
            case lists:member('not null', COpts) of
                true ->     {?nav,lists:delete('not null',COpts)};
                false ->    {undefined,COpts}
            end;
        {_,Bin} ->  
            Str = binary_to_list(Bin),
            case re:run(Str, "fun[ \(\)\-\>]*(.*)end[ ]*.", [global, {capture, [1], list}]) of
                {match,[Body]} ->
                    try 
                        % ?Log("body ~p~n", [Body]),
                        {imem_datatype:io_to_term(Body),lists:keydelete(default, 1, COpts)}
                    catch
                        _:_ ->  try
                                    % ?Log("str ~p~n", [Str]),
                                    Fun = imem_datatype:io_to_fun(Str,0),
                                    {Fun(),lists:keydelete(default, 1, COpts)}
                                catch
                                    _:Reason -> ?ClientError({"Default evaluation fails",{Str,Reason}})
                                end
                    end;
                nomatch ->  
                    {imem_datatype:io_to_term(Str),lists:keydelete(default, 1, COpts)}
            end
    end,
    C = #ddColumn{  name=?binary_to_atom(Name)
                  , type=T
                  , len=L
                  , prec=P
                  , default=Default
                  , opts = Opts
                  },
    create_table(SKey, Table, TOpts, Columns, IsSec, [C|ColMap]).

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec, Fun, Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) -> 
    catch imem_meta:drop_table(key_test),
    catch imem_meta:drop_table(truncate_test),
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
        ?Log("----TEST--- ~p ----Security ~p~n", [?MODULE, IsSec]),

        ?Log("schema ~p~n", [imem_meta:schema()]),
        ?Log("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),
        Sql1 = "create table def (col1 varchar2(10) not null, col2 integer default 12, col3 list default fun() -> [] end.);",
        Expected = 
                [   {ddColumn,col1,string,10,0,?nav,[]},
                    {ddColumn,col2,integer,0,0,12,[]},
                    {ddColumn,col3,list,0,0,[],[]}
                ],
        ?assertEqual(ok, imem_sql:exec(SKey, Sql1, 0, 'Imem', IsSec)),
        [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {'Imem',def}]),
        ?Log("Meta table~n~p~n", [Meta]),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, def])),
        ?assertEqual(Expected,element(3,Meta)),    

        ?assertEqual(ok, imem_sql:exec(SKey, 
            "create table truncate_test (col1 integer, col2 string);", 0, 'Imem', IsSec)),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,1,""}]),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,2,"abc"}]),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,3,"123"}]),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,4,undefined}]),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,5,[]}]),
        ?assertEqual(5,  if_call_mfa(IsSec, table_size, [SKey, truncate_test])),
        ?assertEqual(ok, imem_sql:exec(SKey, 
            "truncate table truncate_test;", 0, 'Imem', IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, truncate_test])),
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table truncate_test;", 0, 'Imem', IsSec)),

        Sql30 = "create table key_test (col1 '{atom,integer}', col2 '{string,binstr}');",
        ?Log("Sql30: ~p~n", [Sql30]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql30, 0, 'Imem', IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, key_test])),
        TableDef = if_call_mfa(IsSec, read, [SKey, ddTable, {imem_meta:schema(),key_test}]),
        ?Log("TableDef: ~p~n", [TableDef]),




        Sql97 = "drop table key_test;",
        ?Log("Sql97: ~p~n", [Sql97]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql97 , 0, 'Imem', IsSec)),


        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, 'Imem', IsSec)),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  if_call_mfa(IsSec, table_size, [SKey, def])),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  imem_sql:exec(SKey, "drop table def;", 0, 'Imem', IsSec))
    catch
        Class:Reason ->  ?Log("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
