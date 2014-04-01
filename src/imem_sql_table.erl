-module(imem_sql_table).

-include("imem_seco.hrl").

-export([ exec/5
        ]).
    
exec(_SKey, {'drop table', {tables, []}, _Exists, _RestrictCascade}, _Stmt, _Opts, _IsSec) -> ok;
exec(SKey, {'drop table', {tables, [TableName|Tables]}, Exists, RestrictCascade}=_ParseTree, Stmt, Opts, IsSec) ->
    % ?LogDebug("Drop Table ParseTree~n~p~n", [_ParseTree]),
    QName = imem_meta:qualified_table_name(TableName), 
    % ?LogDebug("Drop Table QName~n~p~n", [QName]),
    if_call_mfa(IsSec, 'drop_table', [SKey,QName]),
    exec(SKey, {'drop table', {tables, Tables}, Exists, RestrictCascade}, Stmt, Opts, IsSec);
exec(SKey, {'truncate table', TableName, {}, {}}=_ParseTree, _Stmt, _Opts, IsSec) ->
    % ?Info("Parse Tree ~p~n", [_ParseTree]),
    if_call_mfa(IsSec, 'truncate_table', [SKey, imem_meta:qualified_table_name(TableName)]);

exec(SKey, {'create table', TableName, Columns, TOpts}=_ParseTree, _Stmt, _Opts, IsSec) ->
    % ?Info("Parse Tree ~p~n", [_ParseTree]),
    create_table(SKey, imem_sql_expr:binstr_to_qname2(TableName), TOpts, Columns, IsSec, []).

create_table(SKey, Table, TOpts, [], IsSec, ColMap) ->
    if_call_mfa(IsSec, 'create_table', [SKey, Table, lists:reverse(ColMap), TOpts]);        %% {Schema,Table} not converted to atom yet !!!
create_table(SKey, Table, TOpts, [{Name, Type, COpts}|Columns], IsSec, ColMap) when is_binary(Name) ->
    % ?LogDebug("Create table column ~p of type ~p~n",[Name,Type]),
    {T,L,P} = case Type of
        B when B==decimal;B==number ->          {decimal,38,0};
        {B,SLen} when B==decimal;B==number ->   {decimal,list_to_integer(SLen),0};
        B when is_binary(B) ->      {imem_datatype:io_to_term(imem_datatype:strip_squotes(binary_to_list(B))),undefined,undefined};
        A when is_atom(A) ->        {imem_datatype:imem_type(A),undefined,undefined};
        {float,SPrec} ->            {float,undefined,list_to_integer(SPrec)};
        {timestamp,SPrec} ->        {timestamp,undefined,list_to_integer(SPrec)};
        {Typ,SLen} ->               {imem_datatype:imem_type(Typ),list_to_integer(SLen),undefined};
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
                        % ?Info("body ~p~n", [Body]),
                        {imem_datatype:io_to_term(Body),lists:keydelete(default, 1, COpts)}
                    catch
                        _:_ ->  try
                                    % ?Info("str ~p~n", [Str]),
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
    C = #ddColumn{  name=Name           %% not converted to atom yet !!!
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
        ?Info("----TEST--- ~p ----Security ~p~n", [?MODULE, IsSec]),

        ?Info("schema ~p~n", [imem_meta:schema()]),
        ?Info("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),
        Sql1 = "create table def (col1 varchar2(10) not null, col2 integer default 12, col3 list default fun() -> [] end.);",
        Expected = 
                [   {ddColumn,col1,binstr,10,undefined,?nav,[]},
                    {ddColumn,col2,integer,undefined,undefined,12,[]},
                    {ddColumn,col3,list,undefined,undefined,[],[]}
                ],
        ?assertEqual(ok, imem_sql:exec(SKey, Sql1, 0, imem, IsSec)),
        [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,def}]),
        ?Info("Meta table~n~p~n", [Meta]),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, def])),
        ?assertEqual(Expected,element(3,Meta)),    

        ?assertEqual(ok, imem_sql:exec(SKey, 
            "create table truncate_test (col1 integer, col2 string);", 0, imem, IsSec)),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,1,""}]),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,2,"abc"}]),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,3,"123"}]),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,4,undefined}]),
        if_call_mfa(IsSec, write,[SKey,truncate_test,{truncate_test,5,[]}]),
        ?assertEqual(5,  if_call_mfa(IsSec, table_size, [SKey, truncate_test])),
        ?assertEqual(ok, imem_sql:exec(SKey, 
            "truncate table truncate_test;", 0, imem, IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, truncate_test])),
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table truncate_test;", 0, imem, IsSec)),

        Sql30 = "create table key_test (col1 '{atom,integer}', col2 '{string,binstr}');",
        ?Info("Sql30: ~p~n", [Sql30]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql30, 0, imem, IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, key_test])),
        TableDef = if_call_mfa(IsSec, read, [SKey, ddTable, {imem_meta:schema(),key_test}]),
        ?Info("TableDef: ~p~n", [TableDef]),




        Sql97 = "drop table key_test;",
        ?Info("Sql97: ~p~n", [Sql97]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql97 , 0, imem, IsSec)),


        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, imem, IsSec)),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  if_call_mfa(IsSec, table_size, [SKey, def])),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  imem_sql:exec(SKey, "drop table def;", 0, imem, IsSec))
    catch
        Class:Reason ->  ?Info("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
