-module(imem_sql_table).

-include("imem_seco.hrl").

-export([ exec/5
        ]).
    
exec(SeCo, {create_table, TableName, Columns}, _Stmt, _Schema, IsSec) ->
    Tab = ?binary_to_atom(TableName),
    Cols = [?binary_to_atom(X) || {X, _} <- Columns],
    io:format(user,"create ~p columns ~p~n", [Tab, Cols]),
    if_call_mfa(IsSec, create_table, [SeCo,Tab,Cols,[local]]);

exec(_SeCo, {drop_table, {tables, []}, _Exists, _RestrictCascade}, _Stmt, _Schema, _IsSec) -> ok;
exec(SeCo, {drop_table, {tables, [Table|Tables]}, Exists, RestrictCascade}, Stmt, Schema, IsSec) ->
    Tab = ?binary_to_existing_atom(Table),
    io:format(user,"drop_table ~p~n", [Tab]),
    if_call_mfa(IsSec, drop_table, [SeCo,Tab]),
    exec(SeCo, {drop_table, {tables, Tables}, Exists, RestrictCascade}, Stmt, Schema, IsSec).

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
        SKey=?imem_test_admin_login(),
        ?assertEqual(ok, imem_sql:exec(SKey, "create table def (col1 int, col2 char);", 0, "Imem", IsSec)),
        ?assertEqual(0,  if_call_mfa(IsSec, table_size, [SKey, def])),    
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec)),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  if_call_mfa(IsSec, table_size, [SKey, def])),
        ?assertException(throw, {ClEr,{"Table does not exist",def}},  imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec))
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

