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
        , table_type/1
        , table_columns/1
        , table_size/1
        , check_table/1
        , check_table_record/2
        , check_table_columns/2
        , system_table/1
        , meta_field_value/1        
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
        , truncate_table/1
        , select/2
        , select/3
        , select_sort/2
        , select_sort/3
        , read/1
        , read/2                 
        , fetch_start/4
        , write/2
        , insert/2    
        , delete/2
        , update_tables/2
        ]).

-export([ transaction/1
        , transaction/2
        , transaction/3
        , return_atomic_list/1
        , return_atomic_ok/1
        , return_atomic/1
        ]).

disc_schema_nodes(Schema) when is_atom(Schema) ->
    lists:flatten([lists:foldl(
            fun(N, Acc) ->
                    case lists:keyfind(imem, 1, rpc:call(N, application, which_applications, [])) of
                        false -> Acc;
                        _ ->
                            case schema(N) of
                                Schema -> [N|Acc];
                                _ -> Acc
                            end
                    end
            end
            , []
            , nodes() -- mnesia:system_info(running_db_nodes) -- [node()])]).


%% ---------- TRANSACTION SUPPORT ------ exported -------------------------------
return_atomic_list({atomic, L}) when is_list(L) -> L;
return_atomic_list({aborted,{throw,{Exception,Reason}}}) -> 
    throw({Exception,Reason});
return_atomic_list({aborted,{exit,{Exception,Reason}}}) -> 
    exit({Exception,Reason});
return_atomic_list(Error) -> ?SystemException(Error).

return_atomic_ok({atomic, ok}) -> ok;
return_atomic_ok({aborted,{throw,{Exception,Reason}}}) -> 
    throw({Exception,Reason});
return_atomic_ok({aborted,{exit,{Exception,Reason}}}) -> 
    exit({Exception,Reason});
return_atomic_ok(Error) -> 
    ?SystemException(Error).

return_atomic({atomic, Result}) -> Result;
return_atomic({aborted, {throw,{Exception, Reason}}}) -> 
    throw({Exception, Reason});
return_atomic({aborted, {exit, {Exception, Reason}}}) -> 
    exit({Exception, Reason});
return_atomic(Error) ->  ?SystemException(Error).


transaction(Function) when is_atom(Function)->
    F = fun() ->
                apply(mnesia, Function, [])
        end,
    mnesia:transaction(F);
transaction(Fun) when is_function(Fun)->
    mnesia:transaction(Fun).

transaction(Function, Args) when is_atom(Function)->
    F = fun() ->
                apply(mnesia, Function, Args)
        end,
    mnesia:transaction(F);
transaction(Fun, Args) when is_function(Fun)->
    mnesia:transaction(Fun, Args).

transaction(Function, Args, Retries) when is_atom(Function)->
    F = fun() ->
                apply(mnesia, Function, Args)
        end,
    mnesia:transaction(F, Retries);
transaction(Fun, Args, Retries) when is_function(Fun)->
    mnesia:transaction(Fun, Args, Retries).

%% ---------- HELPER FUNCTIONS ------ exported -------------------------------

meta_field_value(node) -> node();
meta_field_value(user) -> <<"unknown">>;
meta_field_value(schema) -> schema();
meta_field_value(localtime) -> calendar:local_time();            
meta_field_value(now) -> erlang:now();            
meta_field_value(sysdate) -> calendar:local_time();    
meta_field_value(systimestamp) -> erlang:now();         
meta_field_value(Name) -> ?ClientError({"Undefined meta value",Name}).

schema() ->
    %% schema identifier of local imem node
    [Schema|_]=re:split(filename:basename(mnesia:system_info(directory)),"[.]",[{return,list}]),
    list_to_atom(Schema).

schema(Node) ->
    %% schema identifier of remote imem node in the same erlang cluster
    [Schema|_] = re:split(filename:basename(rpc:call(Node, mnesia, system_info, [directory])), "[.]", [{return, list}]),
    list_to_atom(Schema).

add_attribute(A, Opts) -> update_opts({attributes,A}, Opts).

update_opts({K,_} = T, Opts) when is_atom(K) -> lists:keystore(K, 1, Opts, T).


column_names(ColumnInfos)->
    [list_to_atom(lists:flatten(io_lib:format("~p", [element(2,C)]))) || C <- ColumnInfos].

data_nodes() -> [{schema(N),N} || N <- mnesia:system_info(running_db_nodes)].

all_tables() ->
    lists:delete(schema, mnesia:system_info(tables)).

table_type(Table) ->
    mnesia:table_info(Table, type).

table_columns(Table) ->
    mnesia:table_info(Table, attributes).

table_size(Table) ->
    try
        % mnesia:wait_for_tables([Table], 2000),
        mnesia:table_info(Table, all),
        mnesia:table_info(Table, size)
    catch
        exit:{aborted,{no_exists,_,all}} -> ?ClientError({"Table does not exist", Table});
        throw:Error ->                      ?SystemException(Error)
    end.

check_table(Table) ->
    table_size(Table).

check_table_record(Table, ColumnNames) ->
    TableColumns = table_columns(Table),    
    if
        ColumnNames =:= TableColumns ->
            ok;  
        true ->                 
            ?SystemException({"Record field names do not match table structure",Table})             
    end.

check_table_columns(Table, ColumnInfo) ->
    ColumnNames = column_names(ColumnInfo),
    TableColumns = table_columns(Table),    
    if
        ColumnNames =:= TableColumns ->
            ok;  
        true ->                 
            ?SystemException({"Column info does not match table structure",Table})             
    end.

%% ---------- MNESIA FUNCTIONS ------ exported -------------------------------

create_table(Table,[First|_]=ColumnInfos,Opts) when is_tuple(First) ->
    ColumnNames = column_names(ColumnInfos),
    create_table(Table,ColumnNames,Opts);
create_table(Table,Columns,Opts) ->
    Local = lists:member({scope,local}, Opts),
    Cluster = lists:member({scope,cluster}, Opts),
    if 
        Local ->    create_local_table(Table, Columns, Opts);
        Cluster ->  create_cluster_table(Table, Columns, Opts);
        true ->     create_schema_table(Table, Columns, Opts)
    end.

system_table(_) -> false.

create_local_table(Table,Columns,Opts) when is_atom(Table) ->
    Cols = [list_to_atom(lists:flatten(io_lib:format("~p", [X]))) || X <- Columns],
    CompleteOpts = add_attribute(Cols, Opts) -- [{scope,local}],
    create_table(Table, CompleteOpts).

create_schema_table(Table,Columns,Opts) when is_atom(Table) ->
    DiscNodes = mnesia:table_info(schema, disc_copies),
    RamNodes = mnesia:table_info(schema, ram_copies),
    CompleteOpts = [{ram_copies, RamNodes}, {disc_copies, DiscNodes}|Opts] -- [{scope,schema}],
    create_local_table(Table,Columns,CompleteOpts).

create_cluster_table(Table,Columns,Opts) when is_atom(Table) ->
    DiscNodes = mnesia:table_info(schema, disc_copies),
    RamNodes = mnesia:table_info(schema, ram_copies),
    %% ToDo: may need to pull from another imem schema first and initiate sync
    CompleteOpts = [{ram_copies, RamNodes}, {disc_copies, DiscNodes}|Opts] -- [{scope,cluster}],
    create_local_table(Table,Columns,CompleteOpts).

create_table(Table, Opts) when is_list(Table) ->
    create_table(list_to_atom(Table), Opts);    
create_table(Table, Opts) when is_atom(Table) ->
   	case mnesia:create_table(Table, Opts) of
        {aborted, {already_exists, Table}} ->
            io:format(user, "table ~p locally exists~n", [Table]),
            wait_table_tries([Table], 30),
            ?ClientError({"Table already exists", Table});
        {aborted, {already_exists, Table, Node}} ->
            io:format(user, "table ~p exists at ~p~n", [Table, Node]),
            case mnesia:force_load_table(Table) of
                yes -> ok;
                Error -> ?ClientError({"Loading table(s) timeout~p", Error})
            end,
            ?ClientError({"Table already exists", Table});
            %return_atomic_ok(mnesia:add_table_copy(Table, node(), ram_copies));
        Result -> 
            io:format(user, "create_table ~p for ~p~n", [Result, Table]),
            wait_table_tries([Table], 30),
            return_atomic_ok(Result)
	end.

wait_table_tries(Tables, 0) ->
    ?ClientError({"Loading table(s) timeout~p", Tables});
wait_table_tries(Tables, Count) when is_list(Tables) ->
    case mnesia:wait_for_tables(Tables, 30000) of
        ok -> ok;
        {timeout, BadTabList} ->
            io:format(user, "table ~p load time out attempt ~p~n", [BadTabList, Count]),
            wait_table_tries(Tables, Count-1);
        {error, Reason} -> ?ClientError({"Error loading table~p", Reason})
    end.

drop_table(Table) when is_atom(Table) ->
    case mnesia:delete_table(Table) of
        {atomic,ok} ->                  ok;
        {aborted,{no_exists,Table}} ->  ?ClientError({"Table does not exist",Table});
        Error ->                        ?SystemException(Error)
    end.

create_index(Table, Column) when is_atom(Table) ->
    case mnesia:add_table_index(Table, Column) of
        {aborted, {no_exists, Table}} ->
            ?ClientError({"Table does not exist", Table});
        {aborted, {already_exists, {Table,Column}}} ->
            ?ClientError({"Index already exists", {Table,Column}});
        Result -> return_atomic_ok(Result)
    end.

drop_index(Table, Column) when is_atom(Table) ->
    case mnesia:del_table_index(Table, Column) of
        {aborted, {no_exists, Table}} ->
            ?ClientError({"Table does not exist", Table});
        {aborted, {no_exists, {Table,Column}}} ->
            ?ClientError({"Index does not exist", {Table,Column}});
        Result -> return_atomic_ok(Result)
    end.

truncate_table(Table) when is_atom(Table) ->
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
        RowLen ->   return_atomic_ok(transaction(write,[list_to_tuple([Table|Row])]));
        _ ->        ?ClientError({"Wrong number of columns",RowLen})
    end.

read(Table) when is_atom(Table) ->
    Trans = fun() ->      
        Keys = mnesia:all_keys(Table), 
        [lists:nth(1, mnesia:read(Table, X)) || X <- Keys]
    end,
    case transaction(Trans) of
        {aborted,{no_exists,_}} ->  ?ClientError({"Table does not exist",Table});    
        {atomic, Result} ->         Result;
        Error ->                    ?SystemException(Error)
    end.

read(Table, Key) when is_atom(Table) ->
    Result = case transaction(read,[Table, Key]) of
        {aborted,{no_exists,_}} ->  ?ClientError({"Table does not exist",Table}); 
        Res ->                      Res 
    end,
    return_atomic_list(Result).

write(Table, Row) when is_atom(Table), is_tuple(Row) ->
    Result = case transaction(write,[Table, Row, write]) of
        {aborted,{no_exists,_}} ->  ?ClientError({"Table does not exist",Table}); 
        Res ->                      Res 
    end,
    return_atomic_ok(Result).

delete(Table, Key) when is_atom(Table) ->
    Result = case transaction(delete,[{Table, Key}]) of
        {aborted,{no_exists,_}} ->  ?ClientError({"Table does not exist",Table}); 
        Res ->                      Res 
    end,
    return_atomic_ok(Result).

select(Table, MatchSpec) when is_atom(Table) ->
    case transaction(select,[Table, MatchSpec]) of
        {atomic, L}     ->              {L, true};
        {aborted,{no_exists,_}} ->      ?ClientError({"Table does not exist",Table});    
        Error ->                        ?SystemException(Error)        
    end.

select_sort(Table, MatchSpec) ->
    {L, true} = select(Table, MatchSpec),
    {lists:sort(L), true}.

select(Table, MatchSpec, Limit) when is_atom(Table) ->
    Start = fun(N) ->
        case mnesia:select(Table, MatchSpec, Limit, read) of
            '$end_of_table' ->              {[], true};
            {L, Cont} when is_list(L) ->    N(N, L, Cont);
            Error ->                        Error
        end 
    end,
    Next = fun(N, Acc0, Cont0) ->
        if  length(Acc0) >= Limit ->
                {Acc0, false};
            true ->         
                case mnesia:select(Cont0) of
                    '$end_of_table' ->              {Acc0, true};
                    {L, Cont1} when is_list(L) ->   N(N, [L|Acc0], Cont1);
                    Error ->                        Error
                end 
        end
    end,
    case transaction(Start, [Next]) of
        {aborted,{no_exists,_}} ->              ?ClientError({"Table does not exist",Table});    
        {atomic, {Result, AllRead}} ->          {Result, AllRead};
        Error ->                                ?SystemException(Error)
    end.

select_sort(Table, MatchSpec, Limit) ->
    {Result, AllRead} = select(Table, MatchSpec, Limit),
    {lists:sort(Result), AllRead}.

fetch_start(Pid, Table, MatchSpec, BlockSize) ->
    F =
    fun(F,Contd0) ->
        receive
            abort ->
                io:format(user, "Abort fetch on table ~p~n", [Table]);
            next ->
                case (case Contd0 of
                        undefined ->    mnesia:select(Table, MatchSpec, BlockSize, read);
                        Contd0 ->       mnesia:select(Contd0)
                      end) of
                    '$end_of_table' -> 
                        Pid ! {row, ?eot};
                    {Rows, Contd1} ->
                        Pid ! {row, Rows},
                        Eot = lists:member('$end_of_table', tuple_to_list(Contd1)),
                        if  Eot ->
                                Pid ! {row, ?eot};
                            true ->
                                F(F,Contd1)
                        end
                end
        end
    end,
    spawn(mnesia, transaction, [F, [F,undefined]]).

update_tables(UpdatePlan, Lock) ->
    Update = fun() ->
        [update_xt(Table, Item, Lock, Old, New) || [Table, Item, Old, New] <- UpdatePlan],
        ok
    end,
    return_atomic_ok(transaction(Update)).

update_xt({_Table,bag}, _Item, _Lock, [], []) ->
    ok;
update_xt({Table,bag}, _Item, _Lock, [Old|_], []) when is_atom(Table) ->
    mnesia:delete(Table, element(2, Old), write);
update_xt({Table,bag}, _Item, _Lock, [], New) when is_atom(Table), is_list(New) ->
    [mnesia:write(N) || N <- New] ;
update_xt({Table,bag}, _Item, none, Old, Old) when is_atom(Table), is_list(Old) ->
    ok;
update_xt({Table,bag}, Item, _Lock, [O|_]=Old, Old) when is_atom(Table), is_list(Old) ->
    Current = read(Table, element(2,O)),
    if  
        Current == Old ->   
            ok;
        true ->             
            CurrentSorted = lists:sort(Current),
            OldSorted = lists:sort(Old),
            if  
                CurrentSorted == OldSorted ->    
                    ok;
                true -> 
                    ?ConcurrencyException({"Data is modified by someone else", {Item, {OldSorted, CurrentSorted}}})
            end
    end;
update_xt({Table,bag}, Item, Lock, [O|_]=Old, [N|_]=New) when is_atom(Table) ->
    if
        element(2,O) /= element(2,N) ->
            ?ClientError({"Key update not allowed", {Item, {element(2,O), element(2,N)}}});
        Lock == none ->
            ok;
        true ->
            Current = read(Table, element(2,O)),
            % io:format(user, "current ~p~n", [Current]),
            % io:format(user, "old ~p~n", [Old]),
            if  
                Current == Old ->   
                    ok;
                true ->             
                    CurrentSorted = lists:sort(Current),
                    OldSorted = lists:sort(Old),
                    if  
                        CurrentSorted == OldSorted ->    
                            ok;
                        true -> 
                            ?ConcurrencyException({"Data is modified by someone else", {Item, {OldSorted, CurrentSorted}}})
                    end
            end
    end,
    % io:format(user, "delete ~p~n", [element(2,O)]),
    mnesia:delete({Table, element(2,O)}), 
    % io:format(user, "write ~p~n", [New]),
    [mnesia:write(Y) || Y <- New];

update_xt(_Table, _Item, _Lock, {}, {}) ->
    ok;
update_xt({Table,_}, _Item, _Lock, Old, {}) when is_atom(Table), is_tuple(Old) ->
    mnesia:delete(Table, element(2, Old), write);
update_xt({Table,_}, Item, Lock, {}, New) when is_atom(Table), is_tuple(New) ->
    if
        Lock == none ->
            ok;
        true ->
            case read(Table, element(2,New)) of
                [New] ->    ok;
                [] ->       ok;
                Current ->  ?ConcurrencyException({"Key violation", {Item,{Current, New}}})
            end
    end,
    mnesia:write(New);
update_xt({Table,_}, _Item, none, Old, Old) when is_atom(Table), is_tuple(Old) ->
    ok;    
update_xt({Table,_}, Item, _Lock, Old, Old) when is_atom(Table), is_tuple(Old) ->
    case read(Table, element(2,Old)) of
        [Old] ->    ok;
        [] ->       ?ConcurrencyException({"Data is deleted by someone else", {Item, Old}});
        Current ->  ?ConcurrencyException({"Data is modified by someone else", {Item,{Old, Current}}})
    end;
update_xt({Table,_}, Item, Lock, Old, New) when is_atom(Table), is_tuple(Old), is_tuple(New) ->
    if
        element(2,Old) /= element(2,New) ->
            ?ClientError({"Key update not allowed", {Item, {element(2,Old), element(2,New)}}});
        Lock == none ->
            ok;
        true ->
            case read(Table, element(2,Old)) of
                [Old] ->    ok;
                [] ->       ?ConcurrencyException({"Data is deleted by someone else", {Item, Old}});
                Current ->  ?ConcurrencyException({"Data is modified by someone else", {Item,{Old, Current}}})
            end
    end,
    mnesia:write(New).


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


%% ----- gen_server -------------------------------------------

start_link(Params) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

init(Params) ->
    {_, NodeType} = lists:keyfind(node_type,1,Params),
    {_, SchemaName} = lists:keyfind(schema_name,1,Params),
    SchemaDir = atom_to_list(SchemaName) ++ "." ++ atom_to_list(node()),
    random:seed(now()),
    SleepTime = random:uniform(1000),
    io:format(user, "~p sleeping for ~p ms...~n", [?MODULE, SleepTime]),
    timer:sleep(SleepTime),
    application:set_env(mnesia, dir, SchemaDir),
    ok = mnesia:start(),
    case disc_schema_nodes(SchemaName) of
        [] -> io:format(user, "~p no node found at ~p for schema ~p in erlang cluster ~p~n", [?MODULE, node(), SchemaName, erlang:get_cookie()]);
        [DiscSchemaNode|_] ->
            io:format(user, "~p adding ~p to schema ~p on ~p~n", [?MODULE, node(), SchemaName, DiscSchemaNode]),
            {ok, _} = rpc:call(DiscSchemaNode, mnesia, change_config, [extra_db_nodes, [node()]])
    end,
    case NodeType of
        disc -> mnesia:change_table_copy_type(schema, node(), disc_copies);
        _ -> ok
    end,
    mnesia:subscribe(system),
    io:format("~p started as ~p!~n", [?MODULE, NodeType]),
    {ok,#state{}}.

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

%% ----- TESTS ------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ?imem_test_setup().

teardown(_) ->
    catch drop_table(imem_table_123),
    application:stop(imem).

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun table_operations/1
            %%, fun test_create_account/1
        ]}}.    

table_operations(_) -> 
    try
        ClEr = 'ClientError',
        SyEx = 'SystemException',
        CoEx = 'ConcurrencyException',

        io:format(user, "----TEST--~p:test_mnesia~n", [?MODULE]),

        io:format(user, "schema ~p~n", [imem_meta:schema()]),
        io:format(user, "data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        io:format(user, "----TEST--~p:test_database_operations~n", [?MODULE]),

        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, table_size(non_existing_table)),
        io:format(user, "success ~p~n", [table_size_no_exists]),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, read(non_existing_table)),
        io:format(user, "success ~p~n", [table_read_no_exists]),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, read(non_existing_table, no_key)),
        io:format(user, "success ~p~n", [row_read_no_exists]),
        ?assertException(throw, {SyEx, {aborted,{bad_type,non_existing_table,{},write}}}, write(non_existing_table, {})),
        io:format(user, "success ~p~n", [row_write_no_exists]),
        ?assertEqual(ok, create_table(imem_table_123, [a,b,c], [])),
        io:format(user, "success ~p~n", [create_table]),
        ?assertEqual(0, table_size(imem_table_123)),
        io:format(user, "success ~p~n", [table_size_empty]),
        ?assertEqual(ok, insert(imem_table_123, {"A","B","C"})),
        ?assertEqual(1, table_size(imem_table_123)),
        io:format(user, "success ~p~n", [insert_table]),
        ?assertEqual(ok, insert(imem_table_123, {"AA","BB","CC"})),
        ?assertEqual(2, table_size(imem_table_123)),
        io:format(user, "success ~p~n", [insert_table]),
        ?assertEqual(ok, insert(imem_table_123, {"AA","BB","cc"})),
        ?assertEqual(2, table_size(imem_table_123)),
        io:format(user, "success ~p~n", [insert_table]),
        ?assertEqual(ok, write(imem_table_123, {imem_table_123, "AAA","BB","CC"})),
        ?assertEqual(3, table_size(imem_table_123)),
        io:format(user, "success ~p~n", [write_table]),
        ?assertEqual(ok, write(imem_table_123, {imem_table_123, "AAA","BB","CC"})),
        ?assertEqual(3, table_size(imem_table_123)),
        io:format(user, "success ~p~n", [write_table]),
        ?assertEqual([{imem_table_123,"A","B","C"}], read(imem_table_123,"A")),
        io:format(user, "success ~p~n", [read_table_1]),
        ?assertEqual([{imem_table_123,"AA","BB","cc"}], read(imem_table_123,"AA")),
        io:format(user, "success ~p~n", [read_table_2]),
        ?assertEqual([], read(imem_table_123,"XX")),
        io:format(user, "success ~p~n", [read_table_3]),
        AllRecords=lists:sort([{imem_table_123,"A","B","C"},{imem_table_123,"AA","BB","cc"},{imem_table_123,"AAA","BB","CC"}]),
        AllKeys=["A","AA","AAA"],
        ?assertEqual(AllRecords, lists:sort(read(imem_table_123))),
        io:format(user, "success ~p~n", [read_table_4]),
        ?assertEqual({AllRecords,true}, select_sort(imem_table_123, ?MatchAllRecords)),
        io:format(user, "success ~p~n", [select_all_records]),
        ?assertEqual({AllKeys,true}, select_sort(imem_table_123, ?MatchAllKeys)),
        io:format(user, "success ~p~n", [select_all_keys]),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, select(non_existing_table, ?MatchAllRecords)),
        io:format(user, "success ~p~n", [select_table_no_exists]),
        MatchHead = {'$1','$2','$3','$4'},
        Guard = {'==', '$3', "BB"},
        Result = {{'$3','$4'}},
        DTupResult = lists:sort([{"BB","cc"},{"BB","CC"}]),
        ?assertEqual({DTupResult,true}, select_sort(imem_table_123, [{MatchHead, [Guard], [Result]}])),
        io:format(user, "success ~p~n", [select_some_data1]),
        STupResult = lists:sort(["cc","CC"]),
        ?assertEqual({STupResult,true}, select_sort(imem_table_123, [{MatchHead, [Guard], ['$4']}])),
        io:format(user, "success ~p~n", [select_some_data]),
        NTupResult = lists:sort([{"cc"},{"CC"}]),
        ?assertEqual({NTupResult,true}, select_sort(imem_table_123, [{MatchHead, [Guard], [{{'$4'}}]}])),
        io:format(user, "success ~p~n", [select_some_data2]),
        Limit=10,
        SelRes=select_sort(imem_table_123, [{MatchHead, [Guard], [{{'$4'}}]}], Limit),
        ?assertMatch({[_|_], true}, SelRes),
        {SelList, true} = SelRes,
        ?assertEqual(NTupResult, SelList),
        io:format(user, "success ~p~n", [select_some_data3]),

        io:format(user, "----TEST--~p:test_transactions~n", [?MODULE]),

        io:format(user, "data in table ~p~n~p~n", [imem_table_123, lists:sort(read(imem_table_123))]),
    
        Update1 = fun(X) ->
            update_xt({imem_table_123,set}, 1, optimistic, {imem_table_123, "AAA","BB","CC"}, {imem_table_123, "AAA","11",X}),
            update_xt({imem_table_123,set}, 2, optimistic, {}, {imem_table_123, "XXX","11","22"}),
            update_xt({imem_table_123,set}, 3, optimistic, {imem_table_123, "AA","BB","cc"}, {}),
            lists:sort(read(imem_table_123))
        end,
        UR1 = return_atomic(transaction(Update1, ["99"])),
        io:format(user, "updated data in table ~p~n~p~n", [imem_table_123, UR1]),
        ?assertEqual(UR1, [{imem_table_123,"A","B","C"},{imem_table_123,"AAA","11","99"},{imem_table_123,"XXX","11","22"}]),
        
        ?assertEqual(ok, drop_table(imem_table_123)),
        io:format(user, "success ~p~n", [drop_table]),

        ?assertEqual(ok, create_table(imem_table_123, [a,b,c], [{type, bag}])),
        io:format(user, "success ~p~n", [create_bag_table]),

        ?assertEqual(ok, insert(imem_table_123, {"A","B","C"})),
        ?assertEqual(1, table_size(imem_table_123)),
        ?assertEqual(ok, insert(imem_table_123, {"AA","BB","CC"})),
        ?assertEqual(2, table_size(imem_table_123)),
        ?assertEqual(ok, insert(imem_table_123, {"AA","BB","cc"})),
        ?assertEqual(3, table_size(imem_table_123)),
        ?assertEqual(ok, write(imem_table_123, {imem_table_123, "AAA","BB","CC"})),
        ?assertEqual(4, table_size(imem_table_123)),
        ?assertEqual(ok, write(imem_table_123, {imem_table_123, "AAA","BB","CC"})),
        ?assertEqual(4, table_size(imem_table_123)),
        io:format(user, "success ~p~n", [write_table]),

        Update2 = fun(X) ->
            update_xt({imem_table_123,bag}, 1, optimistic, [{imem_table_123, "AA","BB","cc"},{imem_table_123, "AA","BB","CC"}], [{imem_table_123, "AA","11",X}]),
            update_xt({imem_table_123,bag}, 2, optimistic, [], [{imem_table_123, "XXX","11","22"}]),
            update_xt({imem_table_123,bag}, 3, optimistic, [{imem_table_123, "A","B","C"}], []),
            lists:sort(read(imem_table_123))
        end,
        UR2 = return_atomic(transaction(Update2, ["99"])),
        io:format(user, "updated data in table ~p~n~p~n", [imem_table_123, UR2]),
        ?assertEqual([{imem_table_123,"AA","11","99"},{imem_table_123,"AAA","BB","CC"},{imem_table_123,"XXX","11","22"}], UR2),

        Update3 = fun() ->
            update_xt({imem_table_123,bag}, 1, optimistic, [{imem_table_123, "AA","BB","cc"}], [{imem_table_123, "AA","11","11"}])
        end,
        ?assertException(throw, {CoEx, {"Data is modified by someone else", {1, {[{imem_table_123, "AA","BB","cc"}], [{imem_table_123,"AA","11","99"}]}}}}, return_atomic(transaction(Update3))),

        Update4 = fun() ->
            update_xt({imem_table_123,bag}, 1, optimistic, [{imem_table_123,"AA","11","99"}], [{imem_table_123, "AB","11","11"}])
        end,
        ?assertException(throw, {ClEr, {"Key update not allowed", {1, {"AA", "AB"}}}}, return_atomic(transaction(Update4))),

        ?assertEqual(ok, drop_table(imem_table_123)),
        io:format(user, "success ~p~n", [drop_table])

    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
