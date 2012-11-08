-module(imem_if).

-export([ schema/0
        , schema/1
        , data_nodes/0
        , all_tables/0
        , table_columns/1
        , table_size/1
        , system_table/1
        ]).

-export([ add_attribute/2
        , update_opts/2
        ]).
        
-export([ create_table/3
		, drop_table/1
        , truncate/1
        , select/1
        , select/2
        , select/3
        , read/1
        , read/2                 
        , read_block/3
        , write/2
        , insert/2    
        , delete/2
        ]).


schema() ->
    %% schema identifier of local imem node
    [Schema|_]=re:split(filename:basename(mnesia:system_info(directory)),"[.]",[{return,list}]),
    Schema.

schema(Node) ->
    %% schema identifier of remote imem node in the same erlang cluster
    [Schema|_] = re:split(filename:basename(rpc:call(Node, mnesia, system_info, [directory])), "[.]", [{return, list}]),
    Schema.

add_attribute(A, Opts) -> update_opts({attributes,A}, Opts).

update_opts({K,_} = T, Opts) when is_atom(K) -> lists:keystore(K, 1, Opts, T).

create_table(TableName,Columns,Opts) ->
    case lists:member(local, Opts) of
        true ->     create_local_table(TableName, Columns, Opts -- [local]);
        false ->    create_cluster_table(TableName, Columns, Opts)
    end.

system_table(_) -> false.

create_local_table(TableName,Columns,Opts) ->
    Cols = [list_to_atom(lists:flatten(io_lib:format("~p", [X]))) || X <- Columns],
    CompleteOpts = add_attribute(Cols, Opts),
    create_table(TableName, CompleteOpts).

create_cluster_table(TableName,Columns,Opts) ->
    DiscNodes = mnesia:table_info(schema, disc_copies),
    RamNodes = mnesia:table_info(schema, ram_copies),
    create_local_table(TableName,Columns,[{ram_copies, RamNodes}, {disc_copies, DiscNodes}|Opts]).

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
            ret_ok(mnesia:add_table_copy(Table, node(), ram_copies));
		{aborted, Details} ->
            %% other table creation problems
			{aborted, Details};
		%%_ ->
			%% io:format("table '~p' created...~n", [Table]),
            %% ToDo: Check if this is needed.
			%% mnesia:clear_table(Table)
        Result -> ret_ok(Result)
	end.

drop_table(Table) when is_atom(Table) ->
    ret_ok(mnesia:delete_table(Table)).

truncate(Table) when is_atom(Table) ->
    ret_ok(mnesia:clear_table(Table)).

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

read(TableName) ->
    {_, Keys} = mnesia:transaction(fun() -> mnesia:all_keys(TableName) end),
    [lists:nthtail(1, tuple_to_list(lists:nth(1, mnesia:dirty_read(TableName, X)))) || X <- Keys].

read(TableName, Key) ->
    mnesia:dirty_read(TableName, Key).

write(TableName, Row) when is_atom(TableName), is_tuple(Row) ->
    mnesia:dirty_write(TableName, Row).

delete(TableName, Key) ->
    mnesia:dirty_delete({TableName, Key}).

select(Continuation) ->
    mnesia:dirty_select(Continuation).

select(TableName, MatchSpec) ->
    mnesia:dirty_select(TableName, MatchSpec).

select(TableName, MatchSpec, Limit) ->
    mnesia:dirty_select(TableName, MatchSpec, Limit).

data_nodes() ->
    [lists:foldl(
            fun(N, Acc) ->
                    case lists:keyfind(imem, 1, rpc:call(N, application, which_applications, [])) of
                        false ->    Acc;
                        _ ->        [{schema(N),N}|Acc]
                    end
            end
            , []
            , [node() | nodes()])].

all_tables() ->
    lists:delete(schema, mnesia:system_info(tables)).

table_columns(TableName) ->
    mnesia:table_info(TableName, attributes).

table_size(TableName) ->
    mnesia:table_info(TableName, all),
    mnesia:table_info(TableName, size).

read_block(TableName, Key, BlockSize)                       -> read_block(TableName, Key, BlockSize, []).
read_block(_, '$end_of_table' = Key, _, Acc)                -> {Key, Acc};
read_block(_, Key, BlockSize, Acc) when BlockSize =< 0      -> {Key, Acc};
read_block(TableName, '$start_of_table', BlockSize, Acc)    -> read_block(TableName, mnesia:dirty_first(TableName), BlockSize, Acc);
read_block(TableName, Key, BlockSize, Acc) ->
    Rows = mnesia:dirty_read(TableName, Key),
    read_block(TableName, mnesia:dirty_next(TableName, Key), BlockSize - length(Rows), Acc ++ Rows).

ret_ok({atomic, ok}) -> ok;
ret_ok(Other)        -> Other.
