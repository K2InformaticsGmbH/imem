-module(imem_if).

-export([add_attribute/2
        , find_imem_nodes/1
		, create_table/2
        , create_table/3
		, drop_table/1
        , update_opts/2
        , read_all/1
        , select/2
        , read/2                 
        , insert/2    
        , write/2
        , delete/2
        , all_tables/0
        , columns/1
		]).

add_attribute(A, Opts) -> update_opts({attributes,A}, Opts).

update_opts({K,_} = T, Opts) when is_atom(K) -> lists:keystore(K, 1, Opts, T).

create_table(TableName,Columns,Opts) ->
    case lists:member(loacl, Opts) of
        true -> create_table(local, TableName, Columns, Opts -- local);
        _ ->    create_table(cluster, TableName, Columns, Opts)
    end.

create_table(local,TableName,Columns,Opts) ->
    Cols = [list_to_atom(lists:flatten(io_lib:format("~p", [X]))) || X <- Columns],
    CompleteOpts = add_attribute(Cols, Opts),
    create_table(TableName, CompleteOpts);
create_table(cluster,TableName,Columns,Opts) ->
    DiscNodes = mnesia:table_info(schema, disc_copies),
    RamNodes = mnesia:table_info(schema, ram_copies),
    create_table(TableName,Columns,[{ram_copies, RamNodes}, {disc_copies, DiscNodes}|Opts]).

create_table(Table, Opts) when is_list(Table) ->
    create_table(list_to_atom(Table), Opts);    
create_table(Table, Opts) when is_atom(Table) ->
   	case mnesia:create_table(Table, Opts) of
        {aborted, {already_exists, Table}} ->
            %% table exists on local node.
            {aborted, {already_exists, Table}};
        {aborted, {already_exists, Table, _}} ->
            %% table exists on remote node(s)
            %% io:format("waiting for table '~p' ...~n", [Table]),
            mnesia:wait_for_tables([Table], 30000),
            %% io:format("copying table '~p' ...~n", [Table]),
            mnesia:add_table_copy(Table, node(), ram_copies);
		{aborted, Details} ->
            %% other table creation problems
			{aborted, Details};
		%%_ ->
			%% io:format("table '~p' created...~n", [Table]),
            %% ToDo: Check if this is needed.
			%% mnesia:clear_table(Table)
        Result -> Result
	end.

drop_table(Table) when is_atom(Table) ->
    mnesia:delete_table(Table).

insert(TableName, Row) when is_atom(TableName), is_tuple(Row) ->
    Row1 = case element(1, Row) of
        TableName ->
            [_|R] = tuple_to_list(Row),
            R;
        _ -> tuple_to_list(Row)
    end,
    insert(TableName, Row1);
insert(TableName, Row) when is_atom(TableName), is_list(Row) ->
    RowLen = length(Row),
    TableRowLen = length(mnesia:table_info(TableName, attributes)),
    if TableRowLen =:= RowLen ->
        mnesia:dirty_write(TableName, list_to_tuple([TableName|Row]));
        true -> {error, {"schema mismatch {table_row_len, insert_row_len} ", TableRowLen, RowLen, Row}}
    end.

read(TableName, Key) ->
    mnesia:dirty_read(TableName, Key).

write(TableName, Row) when is_atom(TableName), is_tuple(Row) ->
    mnesia:dirty_write(TableName, Row).

delete(TableName, Key) ->
    mnesia:dirty_delete({TableName, Key}).

read_all(TableName) ->
    {_, Keys} = mnesia:transaction(fun() -> mnesia:all_keys(TableName) end),
    [lists:nthtail(1, tuple_to_list(lists:nth(1, mnesia:dirty_read(TableName, X)))) || X <- Keys].

select(TableName, MatchSpec) ->
    mnesia:dirty_select(TableName, MatchSpec).

find_imem_nodes(Schema) when is_list(Schema) ->
    [lists:foldl(
            fun(N, Acc) ->
                    case lists:keyfind(imem, 1, rpc:call(N, application, loaded_applications, [])) of
                        false -> Acc;
                        _ ->
                            case re:split(filename:basename(
                                    rpc:call(N, mnesia, system_info, [directory])
                                ), "[.]", [{return, list}]) of
                                [Schema|_] -> [N|Acc];
                                _ -> Acc
                            end
                    end
            end
            , []
            , [node() | nodes()])].


all_tables() ->
    lists:delete(schema, mnesia:system_info(tables)).

columns(TableName) ->
    mnesia:table_info(TableName, attributes).
