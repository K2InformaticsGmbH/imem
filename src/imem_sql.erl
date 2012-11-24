-module(imem_sql).

-include("imem_sql.hrl").

-export([ exec/5
        , field_qname/1
        , table_qname/1
        , table_qname/2        
        ]).

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
                        #statement{stmt_str=Statement, stmt_parse=ParseTree, block_size=BlockSize}, 
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
exec(SKey, insert, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_insert:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, create_user, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, alter_user, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, drop_user, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_account:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, create_table, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, drop_table, ParseTree, Stmt, Schema, IsSec) ->
    imem_sql_table:exec(SKey, ParseTree, Stmt, Schema, IsSec);
exec(SKey, Command, _ParseTree, _Stmt, _Schema, _IsSec) ->
    ?UnimplementedException({"SQL command unimplemented", {SKey, Command}}).


field_qname(A) when is_atom(A) ->
    {undefined, undefined, A};
field_qname({T, A}) when is_atom(T), is_atom(A) ->
    {undefined, T, A};
field_qname({S, T, A}) when is_atom(S), is_atom(T), is_atom(A) ->
    {S, T, A};
field_qname(B) when is_binary(B) ->
    field_qname(binary_to_list(B));
field_qname(Str) when is_list(Str) ->
    case string:tokens(Str, ".") of
        [A] ->      {undefined, undefined, list_to_atom(A)};
        [T,A] ->    {undefined, list_to_atom(T), list_to_atom(A)};
        [S,T,A] ->  {list_to_atom(S), list_to_atom(T), list_to_atom(A)};
        _ ->        ?ClientError({"Invalid field name", Str})
    end;
field_qname(S) ->
    ?ClientError({"Invalid field name", S}).

table_qname(B) when is_binary(B) ->
    table_qname(binary_to_list(B));
table_qname(T) when is_atom(T) ->
    {undefined, T, T};
table_qname(Str) when is_list(Str) ->
    case string:tokens(Str, ".") of
        [T] ->      {undefined, list_to_atom(T), list_to_atom(T)};
        [S,T] ->    {list_to_atom(S), list_to_atom(T), list_to_atom(T)};
        _ ->        ?ClientError({"Invalid table name", Str})
    end;
table_qname({S, T}) when is_atom(S), is_atom(T) ->
    {S, T, T};
table_qname({S, T, A}) when is_atom(S), is_atom(T), is_atom(A) ->
    {S, T, A};
table_qname(N) -> 
    ?ClientError({"Invalid table name", N}).

table_qname(T, A) when is_binary(A) ->    
    table_qname(T, binary_to_list(A));
table_qname(T, S) when is_list(S) ->    
    table_qname(T, list_to_atom(S));
table_qname(T, A) when is_atom(T), is_atom(A) ->
    {undefined, T, A};
table_qname({S, T}, A) when is_atom(S), is_atom(T), is_atom(A) ->
    {S, T, A};
table_qname(B , A) when is_binary(B), is_atom(A) ->
    table_qname(binary_to_list(B), A);
table_qname(Str, A) when is_list(Str), is_atom(A) ->
    case string:tokens(Str, ".") of
        [T] ->      {undefined, list_to_atom(T), A};
        [S,T] ->    {list_to_atom(S), list_to_atom(T), A};
        _ ->        ?ClientError({"Invalid table name", Str})
    end;
table_qname(S, _A) ->
    ?ClientError({"Invalid table name", S}).


%% TESTS ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) -> 
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
        SKey = case IsSec of
            true -> ?imem_test_admin_login();
            _ ->    ok
        end,
        ?assertEqual({undefined,undefined,field}, field_qname(<<"field">>)),
        ?assertEqual({undefined,table,field}, field_qname(<<"table.field">>)),
        ?assertEqual({schema,table,field}, field_qname(<<"schema.table.field">>)),

        ?assertEqual({undefined,table,table}, table_qname(<<"table">>)),
        ?assertEqual({schema,table,table}, table_qname(<<"schema.table">>)),
        ?assertEqual({schema,table,alias}, table_qname(<<"schema.table">>, <<"alias">>)),
        case IsSec of
            true -> ?imem_logout(SKey);
            _ ->    ok
        end
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

