-module(imem_sql_table).

-include("imem_seco.hrl").

-export([ exec/5
        ]).
    
exec(_SKey, {drop_table, {tables, []}, _Exists, _RestrictCascade}, _Stmt, _Schema, _IsSec) -> ok;
exec(SKey, {drop_table, {tables, [Table|Tables]}, Exists, RestrictCascade}, Stmt, Schema, IsSec) ->
    Tab = ?binary_to_existing_atom(Table),
    io:format(user,"drop_table ~p~n", [Tab]),
    if_call_mfa(IsSec, drop_table, [SKey,Tab]),
    exec(SKey, {drop_table, {tables, Tables}, Exists, RestrictCascade}, Stmt, Schema, IsSec);
exec(SKey, {create_table, TableName, Columns, TOpts}, _Stmt, _Schema, IsSec) ->
    create_table(SKey, ?binary_to_atom(TableName), TOpts, Columns, IsSec, []).

create_table(SKey, Table, TOpts, [], IsSec, ColMap) ->
    if_call_mfa(IsSec, create_table, [SKey, Table, lists:reverse(ColMap), TOpts]);
create_table(SKey, Table, TOpts, [{Name, Type, COpts}|Columns], IsSec, ColMap) ->
    {T,L,P} = case Type of
        A when is_atom(A) ->        {A,0,0};
        {double,SPrec} ->           {double,0,list_to_integer(SPrec)};
        {float,SPrec} ->            {float,0,list_to_integer(SPrec)};
        {timestamp,SPrec} ->        {timestamp,0,list_to_integer(SPrec)};
        {time,SPrec} ->             {time,0,list_to_integer(SPrec),0};
        {Typ,SLen} ->               {Typ,list_to_integer(SLen),0};
        {Typ,SLen,SPrec} ->         {Typ,list_to_integer(SLen),list_to_integer(SPrec)};
        Else ->                     ?SystemException({"Unexpected parse tree structure",Else})
    end,
    Default = case lists:keyfind(default, 1, COpts) of
        false ->    
            undefined;
        {_,Bin} ->  
            Str = binary_to_list(Bin),
            case re:run(Str, "fun[ \(\)\-\>]*(.*)end[ ]*.", [global, {capture, [1], list}]) of
                {match,[Body]} ->
                    try 
                        io:format(user,"body ~p~n", [Body]),
                        imem_datatype:string_to_eterm(Body)
                    catch
                        _:_ ->  try
                                    io:format(user,"str ~p~n", [Str]),
                                    Fun = imem_datatype:string_to_fun(Str,0),
                                    Fun()
                                catch
                                    _:Reason -> ?ClientError({"Default evaluation fails",{Str,Reason}})
                                end
                    end;
                nomatch ->  
                    imem_datatype:string_to_eterm(Str)
            end
    end,
    C = #ddColumn{  name=?binary_to_atom(Name)
                  , type=T
                  , length=L
                  , precision=P
                  , default=Default
                  , opts = lists:keydelete(default, 1, COpts)
                  },
    create_table(SKey, Table, TOpts, Columns, IsSec, [C|ColMap]).

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec, Fun, Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% TESTS ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) -> 
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
        io:format(user, "----TEST--- ~p ----Security ~p~n", [?MODULE, IsSec]),

        io:format(user, "schema ~p~n", [imem_meta:schema()]),
        io:format(user, "data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),
        Sql1 = "create table def (col1 varchar2(10), col2 integer default 12, col3 elist default fun() -> [] end.);",
        Expected = 
                [   {ddColumn,col1,varchar,10,0,undefined,[]},
                    {ddColumn,col2,int,0,0,12,[]},
                    {ddColumn,col3,elist,0,0,[],[]}
                ],
        ?assertEqual(ok, imem_sql:exec(SKey, Sql1, 0, "Imem", IsSec)),
        [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {'Imem',def}]),
        io:format(user, "Meta table~n~p~n", [Meta]),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, def])),
        ?assertEqual(Expected,element(3,Meta)),    
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec)),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  if_call_mfa(IsSec, table_size, [SKey, def])),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec))
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

