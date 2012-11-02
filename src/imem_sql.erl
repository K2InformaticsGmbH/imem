-module(imem_sql).
-export([exec/1]).

exec(Statement) when is_list(Statement) ->
    Sql =
    case [lists:last(string:strip(Statement))] of
        ";" -> Statement;
        _ -> Statement ++ ";"
    end,
    case (catch sql_lex:string(Sql)) of
        {ok, Tokens, _} ->
            case (catch sql_parse:parse(Tokens)) of
                {ok, [ParseTree|_]} -> exec(ParseTree);
                {'EXIT', Error} -> {error, Error};
                Error -> {error, Error}
            end;
        {'EXIT', Error} -> {error, Error}
    end;
exec(ParseTree) when is_tuple(ParseTree) ->

    ok.
