-module(imem_sql).
-export([exec/2]).

exec(Statement, Schema) when is_list(Statement) ->
    Sql =
    case [lists:last(string:strip(Statement))] of
        ";" -> Statement;
        _ -> Statement ++ ";"
    end,
    io:format(user, "got sql ~p~n", [Sql]),
    case (catch sql_lex:string(Sql)) of
        {ok, Tokens, _} ->
            case (catch sql_parse:parse(Tokens)) of
                {ok, [ParseTree|_]} -> exec(ParseTree, Schema);
                {'EXIT', Error} -> {error, Error};
                Error -> {error, Error}
            end;
        {'EXIT', Error} -> {error, Error}
    end;
exec({create_table, TableName, Columns}, _Schema) ->
    Cols = [erlang:binary_to_atom(X, utf8) || {X, _} <- Columns],
    imem_if:create_table(TableName,Cols,[]);
exec({select, Params}, Schema) ->
    Columns = case lists:keyfind(fields, 1, Params) of
        false -> [];
        Cols -> Cols
    end,
    TableName = case lists:keyfind(from, 1, Params) of
        Tabs when length(Tabs) == 1 -> erlang:binary_to_atom(lists:nth(1, Tabs));
        _ -> undefined
    end,
    case TableName of
        tables -> imem_if:all_tables();
        imem_nodes -> imem_if:find_imem_nodes(Schema);
        undefined -> {error, "Only single valid names are supported"};
        _ ->
            Clms = case Columns of
                [<<"*">>] -> imem_if:columns(TableName);
                _ -> Columns
            end
    end.
