-module(imem_sql).

-include("imem_seco.hrl").

-export([ exec/5
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
        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec])
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

