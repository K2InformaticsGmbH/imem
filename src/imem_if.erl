-module(imem_if).
-behavior(gen_server).

-include("imem_if.hrl").

% gen_server
-record(state, {
        }).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        , start_link/1
        ]).

-export([ schema/0
        , schema/1
        , data_nodes/0
        , all_tables/0
        , table_columns/1
        , table_size/1
        , system_table/1
        , subscribe/1
        , unsubscribe/1
        ]).

-export([ add_attribute/2
        , update_opts/2
        ]).
        
-export([ create_table/3
		, drop_table/1
        , create_index/2
        , drop_index/2
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


return_list(L) when is_list(L) -> L;
return_list(Error) -> ?SystemException(Error).

return_atomic_ok({atomic, ok}) -> ok;
return_atomic_ok(Error)        -> ?SystemException(Error).

mnesia_transaction(Function, Args) ->
    F = fun() ->
                apply(mnesia, Function, Args)
        end,
    mnesia:transaction(F).

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

create_table(Table,Columns,Opts) ->
    case lists:member(local, Opts) of
        true ->     create_local_table(Table, Columns, Opts -- [local]);
        false ->    create_cluster_table(Table, Columns, Opts)
    end.

system_table(_) -> false.

create_local_table(Table,Columns,Opts) ->
    Cols = [list_to_atom(lists:flatten(io_lib:format("~p", [X]))) || X <- Columns],
    CompleteOpts = add_attribute(Cols, Opts),
    create_table(Table, CompleteOpts).

create_cluster_table(Table,Columns,Opts) ->
    DiscNodes = mnesia:table_info(schema, disc_copies),
    RamNodes = mnesia:table_info(schema, ram_copies),
    create_local_table(Table,Columns,[{ram_copies, RamNodes}, {disc_copies, DiscNodes}|Opts]).

create_table(Table, Opts) when is_list(Table) ->
    create_table(list_to_atom(Table), Opts);    
create_table(Table, Opts) when is_atom(Table) ->
   	case mnesia:create_table(Table, Opts) of
        {aborted, {already_exists, Table}} ->
            ?ClientError({"Table already exists", Table});
        {aborted, {already_exists, Table, _}} ->
            %% table exists on remote node(s)
            %% io:format("waiting for table '~p' ...~n", [Table]),
            mnesia:wait_for_tables([Table], 30000),
            %% io:format("copying table '~p' ...~n", [Table]),
            return_atomic_ok(mnesia:add_table_copy(Table, node(), ram_copies));
        Result -> return_atomic_ok(Result)
	end.

drop_table(Table) when is_atom(Table) ->
    return_atomic_ok(mnesia:delete_table(Table)).

create_index(Table, Column) ->
    case mnesia:add_table_index(Table, Column) of
        {aborted, {no_exists, Table}} ->
            ?ClientError({"Table does not exist", Table});
        {aborted, {already_exists, {Table,Column}}} ->
            ?ClientError({"Index already exists", {Table,Column}});
        Result -> return_atomic_ok(Result)
    end.

drop_index(Table, Column) ->
    case mnesia:del_table_index(Table, Column) of
        {aborted, {no_exists, Table}} ->
            ?ClientError({"Table does not exist", Table});
        {aborted, {no_exists, {Table,Column}}} ->
            ?ClientError({"Index does not exist", {Table,Column}});
        Result -> return_atomic_ok(Result)
    end.

truncate(Table) when is_atom(Table) ->
    return_atomic_ok(mnesia:clear_table(Table)).

insert(Table, Row) when is_atom(Table), is_tuple(Row) ->
    Row1 = case element(1, Row) of
        Table ->
            [_|R] = tuple_to_list(Row),
            R;
        _ -> tuple_to_list(Row)
    end,
    insert(Table, Row1);
insert(Table, Row) when is_atom(Table), is_list(Row) ->
    RowLen = length(Row),
    TableRowLen = length(mnesia:table_info(Table, attributes)),
    case TableRowLen of 
        RowLen ->   return_atomic_ok(mnesia_transaction(write,[Table, list_to_tuple([Table|Row])]));
        _ ->        ?ClientError({"Wrong number of columns",RowLen})
    end.

read(Table) ->
    case mnesia_transaction(all_keys,[Table]) of
        {aborted, no_exists} -> 
            ?ClientError({"Table does not exists",Table});    
        {aborted, Reason} -> 
            ?SystemException(Reason);
        {atomic, Keys} ->   
            [lists:nthtail(1, tuple_to_list(lists:nth(1, mnesia_transaction(read,[Table, X])))) || X <- Keys]
    end.

read(Table, Key) ->
    return_list(mnesia_transaction(read,[Table, Key])).

write(Table, Row) when is_atom(Table), is_tuple(Row) ->
    return_atomic_ok(mnesia_transaction(write,[Table, Row, write])).

delete(Table, Key) ->
    return_atomic_ok(mnesia_transaction(delete,[{Table, Key}])).

select(Continuation) ->
    case mnesia_transaction(select,[Continuation]) of
        {L,Cont} when is_list(L)    ->  {L,Cont};
        '$end_of_table' ->              '$end_of_table';
        Error ->                        ?SystemException(Error)
    end.    

select(Table, MatchSpec) ->
    case mnesia_transaction(select,[Table, MatchSpec]) of
        {atomic, L}     ->              L;
        '$end_of_table' ->              '$end_of_table';
        {aborted, no_exists} ->         ?ClientError({"Table does not exists",Table});    
        Error ->                        ?SystemException(Error)        
    end.

select(Table, MatchSpec, Limit) ->
    case mnesia_transaction(select,[Table, MatchSpec, Limit, read]) of
        {L,Cont} when is_list(L)    ->  {L,Cont};
        '$end_of_table' ->              '$end_of_table';
        {aborted, no_exists} ->         ?ClientError({"Table does not exists",Table});    
        Error ->                        ?SystemException(Error)        
    end.    

data_nodes() ->
    lists:flatten([lists:foldl(
            fun(N, Acc) ->
                    case lists:keyfind(imem, 1, rpc:call(N, application, which_applications, [])) of
                        false ->    Acc;
                        _ ->        [{schema(N),N}|Acc]
                    end
            end
            , []
            , [node() | nodes()])]).

all_tables() ->
    lists:delete(schema, mnesia:system_info(tables)).

table_columns(Table) ->
    mnesia:table_info(Table, attributes).

table_size(Table) ->
    try
        mnesia:table_info(Table, all),
        mnesia:table_info(Table, size)
    catch
        exit:{aborted,{no_exists,_,all}} -> ?ClientError({"Table does not exist", Table});
        throw:Error ->                      ?SystemException(Error)
    end.

read_block(Table, Key, BlockSize)                       -> read_block(Table, Key, BlockSize, []).
read_block(_, '$end_of_table' = Key, _, Acc)            -> {Key, Acc};
read_block(_, Key, BlockSize, Acc) when BlockSize =< 0  -> {Key, Acc};
read_block(Table, '$start_of_table', BlockSize, Acc)    -> read_block(Table, mnesia:dirty_first(Table), BlockSize, Acc);
read_block(Table, Key, BlockSize, Acc) ->
    Rows = mnesia:dirty_read(Table, Key),
    read_block(Table, mnesia:dirty_next(Table, Key), BlockSize - length(Rows), Acc ++ Rows).


subscribe({table, Tab, simple}) ->
mnesia:subscribe({table, Tab, simple});
subscribe({table, Tab, detailed}) ->
mnesia:subscribe({table, Tab, detailed});
subscribe(EventCategory) ->
    ?ClientError({"Unsupported event category", EventCategory}).

unsubscribe({table, Tab, simple}) ->
mnesia:unsubscribe({table, Tab, simple});
unsubscribe({table, Tab, detailed}) ->
mnesia:unsubscribe({table, Tab, detailed});
unsubscribe(EventCategory) ->
    ?ClientError({"Unsupported event category", EventCategory}).

%% gen_server
start_link(Params) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

init(Params) ->
    {_, NodeType} = lists:keyfind(node_type,1,Params),
    {_, SchemaName} = lists:keyfind(schema_name,1,Params),
    SchemaDir = SchemaName ++ "." ++ atom_to_list(node()),
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    SleepTime = random:uniform(1000),
    io:format(user, "~p sleeping for ~p ms...~n", [?MODULE, SleepTime]),
    timer:sleep(SleepTime),
    application:set_env(mnesia, dir, SchemaDir),
    ok = mnesia:start(),
    case disc_schema_nodes(SchemaName) of
        [] -> ok;
        [DiscSchemaNode|_] ->
            io:format(user, "~p adding ~p to schema ~p on ~p~n", [?MODULE, node(), SchemaName, DiscSchemaNode]),
            {ok, _} = rpc:call(DiscSchemaNode, mnesia, change_config, [extra_db_nodes, [node()]])
    end,
    case NodeType of
        disc -> mnesia:change_table_copy_type(schema, node(), disc_copies);
        _ -> ok
    end,
	mnesia:subscribe(system),
	io:format("~p started!~n", [?MODULE]),
    {ok,#state{}}.

disc_schema_nodes(Schema) ->
    lists:flatten([lists:foldl(
            fun(N, Acc) ->
                    case lists:keyfind(mnesia, 1, rpc:call(N, application, which_applications, [])) of
                        false -> Acc;
                        _ ->
                            case rpc:call(N,mnesia,table_info,[schema, disc_copies]) of
                                Nodes when length(Nodes) > 0 ->
                                    case schema(N) of
                                        Schema -> [N|Acc];
                                        _ -> Acc
                                    end;
                                _ -> Acc
                            end
                    end
            end
            , []
            , nodes())]).

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(Info, State) ->
	case Info of
		{mnesia_system_event,{mnesia_overload,Details}} ->
			BulkSleepTime0 = get(mnesia_bulk_sleep_time),
			BulkSleepTime = trunc(1.1 * BulkSleepTime0),
			put(mnesia_bulk_sleep_time, BulkSleepTime),
			io:format("Mnesia overload : ~p!~n",[Details]);
		{mnesia_system_event,{Event,Node}} ->
			io:format("Mnesia event ~p from Node ~p!~n",[Event, Node]);
		Error ->
			io:format("Mnesia error : ~p~n",[Error])
	end,
	{noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.
