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
    % ?LogDebug("Parse Tree ~p", [_ParseTree]),
    % ?LogDebug("TOpts ~p", [TOpts]),
    create_table(SKey, imem_sql_expr:binstr_to_qname2(TableName), TOpts, Columns, IsSec, []).

create_table(SKey, Table, TOpts, [], IsSec, ColMap) ->
    % ?LogDebug("create_table ~p ~p", [Table,TOpts]),
    if_call_mfa(IsSec, 'create_table', [SKey, Table, lists:reverse(ColMap), TOpts]);        %% {Schema,Table} not converted to atom yet !!!
create_table(SKey, Table, TOpts, [{Name, Type, COpts}|Columns], IsSec, ColMap) when is_binary(Name) ->
    % ?LogDebug("Create table column ~p of type ~p~n",[Name,Type]),
    {T,L,P} = case Type of
        B when B==<<"decimal">>;B==<<"number">> ->          {decimal,38,0};
        {B,SLen} when B==<<"decimal">>;B==<<"number">> ->   {decimal,binary_to_integer(SLen),0};
        B when is_binary(B) ->      
            case imem_datatype:imem_type(imem_datatype:io_to_term(imem_datatype:strip_squotes(binary_to_list(B)))) of
                Bterm when is_binary(Bterm) ->  ?ClientError({"Unknown datatype",Bterm});
                Term ->                         {Term,undefined,undefined}
            end;
        {<<"float">>,SPrec} ->      {float,undefined,binary_to_integer(SPrec)};
        {<<"timestamp">>,SPrec} ->  {timestamp,undefined,binary_to_integer(SPrec)};
        {Typ,SLen} ->               {imem_datatype:imem_type(binary_to_existing_atom(Typ, utf8)),binary_to_integer(SLen),undefined};
        {Typ,SLen,SPrec} ->         {imem_datatype:imem_type(binary_to_existing_atom(Typ, utf8)),binary_to_integer(SLen),binary_to_integer(SPrec)};
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
            FT = case re:run(Str, "fun\\((.*)\\)[ ]*\->(.*)end.", [global, {capture, [1,2], list}]) of
                {match,[[_Params,Body]]} ->
                    % ?LogDebug("Str ~p~n", [Str]),
                    % ?LogDebug("Params ~p~n", [_Params]),
                    % ?LogDebug("Body ~p~n", [Body]),
                    try 
                        imem_datatype:io_to_term(Body)
                    catch _:_ -> 
                        try 
                            imem_datatype:io_to_fun(Str,undefined)
                        catch _:Reason -> 
                            ?ClientError({"Bad default fun",Reason})
                        end
                    end;
                nomatch ->  
                    imem_datatype:io_to_term(Str)
            end,
            {FT,lists:keydelete(default, 1, COpts)}
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
    ?imem_test_setup.

teardown(_) -> 
    catch imem_meta:drop_table(key_test),
    catch imem_meta:drop_table(truncate_test),
    catch imem_meta:drop_table(def),
    ?imem_test_teardown.

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
        ?LogDebug("---TEST--- ~p ----Security ~p~n", [?MODULE, IsSec]),

        ?LogDebug("schema ~p~n", [imem_meta:schema()]),
        ?LogDebug("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),

        Sql0 = "create table def (col1 varchar2(10) not null, col2 integer default 12, col3 list default fun() -> [/] end.);",
        ?assertException(throw, {ClEr,{"Bad default fun",_}}, imem_sql:exec(SKey, Sql0, 0, imem, IsSec)),

        Sql1 = "create table def (col1 varchar2(10) not null, col2 integer default 12, col3 list default fun() -> [] end.);",
        Expected = 
                [   {ddColumn,col1,binstr,10,undefined,?nav,[]},
                    {ddColumn,col2,integer,undefined,undefined,12,[]},
                    {ddColumn,col3,list,undefined,undefined,[],[]}
                ],
        ?assertEqual(ok, imem_sql:exec(SKey, Sql1, 0, imem, IsSec)),
        [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,def}]),
        ?LogDebug("Meta table~n~p~n", [Meta]),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, def])),
        ?assertEqual(Expected,element(3,Meta)),    

        ?assertEqual(ok, imem_sql:exec(SKey, 
            "create cluster table truncate_test (col1 integer, col2 string);", 0, imem, IsSec)),
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

        Sql30 = "create loCal SeT table key_test (col1 '{atom,integer}', col2 '{string,binstr}');",
        ?LogDebug("Sql30: ~p~n", [Sql30]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql30, 0, imem, IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, key_test])),
        TableDef = if_call_mfa(IsSec, read, [SKey, ddTable, {imem_meta:schema(),key_test}]),
        % ?LogDebug("TableDef: ~p~n", [TableDef]),

        Sql40 = "create someType table def (col1 varchar2(10) not null, col2 integer);",
        ?assertException(throw, {ClEr,{"Unsupported table option",{type,<<"someType">>}}}, imem_sql:exec(SKey, Sql40, 0, imem, IsSec)),
        Sql41 = "create imem_meta table skvhTEST();",
        ?assertException(throw, {ClEr,{"Invalid module name for table type",{type,imem_meta}}}, imem_sql:exec(SKey, Sql41, 0, imem, IsSec)),

        Sql97 = "drop table key_test;",
        ?LogDebug("Sql97: ~p~n", [Sql97]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql97 , 0, imem, IsSec)),

        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, imem, IsSec)),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  if_call_mfa(IsSec, table_size, [SKey, def])),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  imem_sql:exec(SKey, "drop table def;", 0, imem, IsSec))
    catch
        Class:Reason ->  ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
