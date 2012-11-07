-module(imem_statement).
-behaviour(gen_server).

-export([ exec/3
        , read_block/1
        ]).

-record(statement, {
        table = undefined
        , block_size = 0
        , key = '$start_of_table'
        , stmt_str = ""
        , stmt_parse = undefined
        , cols = []
    }).

exec(Statement, BlockSize, Schema) when is_list(Statement) ->
    Sql =
    case [lists:last(string:strip(Statement))] of
        ";" -> Statement;
        _ -> Statement ++ ";"
    end,
    case (catch sql_lex:string(Sql)) of
        {ok, Tokens, _} ->
            case (catch sql_parse:parse(Tokens)) of
                {ok, [ParseTree|_]} -> exec(ParseTree, #statement{stmt_str=Statement
                                                                , stmt_parse=ParseTree
                                                                , block_size=BlockSize}, Schema);
                {'EXIT', Error} -> {error, Error};
                Error -> {error, Error}
            end;
        {'EXIT', Error} -> {error, Error}
    end;

exec({create_table, TableName, Columns}, _Stmt, _Schema) ->
    Tab = binary_to_atom(TableName),
    Cols = [binary_to_atom(X) || {X, _} <- Columns],
    io:format(user,"create ~p columns ~p~n", [Tab, Cols]),
    imem_if:create_table(Tab,Cols,[local]);

exec({insert, TableName, {_, Columns}, {_, Values}}, _Stmt, _Schema) ->
    Tab = binary_to_atom(TableName),
    io:format(user,"insert ~p ~p in ~p~n", [Columns, Values, Tab]),
    Vs = [binary_to_list(V) || V <- Values],
    imem_if:insert(Tab, Vs);

exec({select, Params}, Stmt, _Schema) ->
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
        undefined -> {error, "Only single valid names are supported"};
        _ ->
            Clms = case Columns of
                [<<"*">>] -> imem_if:table_columns(TableName);
                _ -> Columns
            end,
            Statement = Stmt#statement {
                table = TableName
                , cols = Clms
            },
            StmtRef = imem_server:create_stmt(Statement),
            {ok, Clms, StmtRef}
    end.

read_block(Ref) when is_reference(Ref) ->
    {ok, #statement {
            table = TableName
            , key = Key
            , block_size = BlockSize
    } = Stmt} = imem_server:get_stmt(Ref),
    {NewKey, Rows} = imem_if:read_block(TableName, Key, BlockSize),
    imem_server:update_stmt(Ref, Stmt#statement{key=NewKey}),
    {ok, Rows}.

binary_to_atom(Bin) when is_binary(Bin) -> list_to_atom(binary_to_list(Bin)).
