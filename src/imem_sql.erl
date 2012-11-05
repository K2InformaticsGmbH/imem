-module(imem_sql).
-export([exec/2]).

exec(Statement, Schema) when is_list(Statement) ->
    Sql =
    case [lists:last(string:strip(Statement))] of
        ";" -> Statement;
        _ -> Statement ++ ";"
    end,
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
    Tab = binary_to_atom(TableName),
    Cols = [binary_to_atom(X) || {X, _} <- Columns],
    io:format(user,"create ~p columns ~p~n", [Tab, Cols]),
    imem_if:create_table(Tab,Cols,[local]);
exec({select, Params}, Schema) ->
    Columns = case lists:keyfind(fields, 1, Params) of
        false -> [];
        {_, Cols} -> Cols
    end,
    TableName = case lists:keyfind(from, 1, Params) of
        {_, Tabs} when length(Tabs) == 1 -> binary_to_atom(lists:nth(1, Tabs));
        _ -> undefined
    end,
    io:format(user,"select params ~p in ~p~n", [Columns, TableName]),
    case TableName of
        tables -> imem_if:all_tables();
        imem_nodes -> imem_if:find_imem_nodes(Schema);
        undefined -> {error, "Only single valid names are supported"};
        _ ->
            Clms = case Columns of
                [<<"*">>] -> imem_if:table_columns(TableName);
                _ -> Columns
            end,
            {Clms, imem_if:table_size(TableName)}
    end.

binary_to_atom(Bin) when is_binary(Bin) -> list_to_atom(binary_to_list(Bin)).
