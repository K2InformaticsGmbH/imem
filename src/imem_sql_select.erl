-module(imem_sql_select).

-include("imem_sql.hrl").

-export([ exec/5
        ]).

exec(SeCo, {select, Params}, Stmt, _Schema, IsSec) ->
    Columns = case lists:keyfind(fields, 1, Params) of
        false -> [];
        {_, Cols} -> Cols
    end,
    TableName = case lists:keyfind(from, 1, Params) of
        {_, Tabs} when length(Tabs) == 1 -> ?binary_to_atom(lists:nth(1, Tabs));
        _ -> undefined
    end,
    case TableName of
        undefined -> {error, "Only single valid names are supported"};
        _ ->
            Clms = case Columns of
                [<<"*">>] -> if_call_mfa(IsSec,table_columns,[SeCo,TableName]);
                _ -> Columns
            end,
            Statement = Stmt#statement {
                table = TableName
                , cols = Clms
                , matchspec = ?MatchAllKeys
            },
            {ok, StmtRef} = imem_statement:create_stmt(Statement, SeCo, IsSec),
            io:format(user,"select params ~p in ~p~n", [{Columns, Clms}, TableName]),
            {ok, Clms, StmtRef}
    end.

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
        % ClEr = 'ClientError',
        % SeEx = 'SecurityException',
        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),
        SKey=?imem_test_admin_login(),

        Result0 = if_call_mfa(IsSec,select,[SKey, ddTable, ?MatchAllRecords, 5]),
        io:format(user, "ddTable result~n~p~n", [Result0]),
        {List0, false} = Result0,
        ?assertEqual(5, length(List0)),

        Result1 = if_call_mfa(IsSec,select,[SKey, all_tables, ?MatchAllKeys]),
        io:format(user, "all_tables result~n~p~n", [Result1]),
        {List1, true} = Result1,
        AllTableCount = length(List1),
        ?assert(AllTableCount > 5),

        ?assertEqual(ok, imem_sql:exec(SKey, "create table def (col1 integer, col2 integer);", 0, "Imem", IsSec)),
        ?assertEqual(ok, insert_range(SKey, 10, "def", "Imem", IsSec)),
 
        {ok, _Clm2, StmtRef2} = imem_sql:exec(SKey, "select col1 from def;", 100, "Imem", IsSec),
        ?assertEqual(ok, imem_statement:read_block(SKey, StmtRef2, self(), IsSec)),
        Result2 = receive 
            R2 ->    binary_to_term(R2)
        end,
        io:format(user, "read_block result~n~p~n", [Result2]),
        {List2, true} = Result2,
        ?assertEqual(10, length(List2)),

        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec)),

        {ok, _Clm3, StmtRef3} = imem_sql:exec(SKey, "select qname from all_tables;", 100, "Imem", IsSec),
        ?assertEqual(ok, imem_statement:select(SKey, StmtRef3, self(), IsSec)),
        Result3 = receive 
            R3 ->    binary_to_term(R3)
        end,
        io:format(user, "select result~n~p~n", [Result3]),
        {List3, true} = Result3,
        ?assertEqual(AllTableCount, length(List3))

    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 



insert_range(_SKey, 0, _TableName, _Schema, _IsSec) -> ok;
insert_range(SKey, N, TableName, Schema, IsSec) when is_integer(N), N > 0 ->
    imem_sql:exec(SKey, "insert into " ++ TableName ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');", 0, Schema, IsSec),
    insert_range(SKey, N-1, TableName, Schema, IsSec).
