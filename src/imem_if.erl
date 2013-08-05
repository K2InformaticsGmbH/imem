-module(imem_if).
-behavior(gen_server).

-include("imem.hrl").
-include("imem_if.hrl").

% gen_server
-record(state, {}).

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
        , system_id/0
        , data_nodes/0
        , all_tables/0
        , is_local_table/1
        , table_type/1
        , table_columns/1
        , table_info/2
        , table_size/1
        , table_memory/1
        , table_record_name/1        
        , check_table/1
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
        , read_hlk/2            %% read using hierarchical list key
        , fetch_start/5
        , write/2
        , dirty_write/2
        , delete/2
        , delete_object/2
        , update_tables/2
        , update_bound_counter/6
        ]).

-export([ transaction/1
        , transaction/2
        , transaction/3
        , return_atomic_list/1
        , return_atomic_ok/1
        , return_atomic/1
        , get_os_memory/0
        ]).

-define(INIT_SNAP(__Table,__Now),
            true = ets:insert(?SNAP_ETS_TAB, #snap_properties{table=__Table, last_write=__Now, last_snap=__Now})
       ).

-define(TOUCH_SNAP(__Table),                  
        (fun(__T) ->
            [__Up] = ets:lookup(?SNAP_ETS_TAB, __T),
            true = ets:insert(?SNAP_ETS_TAB, __Up#snap_properties{last_write = erlang:now()})
        end)(__Table)
       ).


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
return_atomic_list(Error) -> ?SystemExceptionNoLogging(Error).

return_atomic_ok({atomic, ok}) -> ok;
return_atomic_ok({aborted,{throw,{Exception,Reason}}}) ->
    throw({Exception,Reason});
return_atomic_ok({aborted,{exit,{Exception,Reason}}}) ->
    exit({Exception,Reason});
return_atomic_ok(Error) ->
    ?SystemExceptionNoLogging(Error).

return_atomic({atomic, Result}) -> Result;
return_atomic({aborted, {throw,{Exception, Reason}}}) ->
    throw({Exception, Reason});
return_atomic({aborted, {exit, {Exception, Reason}}}) ->
    exit({Exception, Reason});
return_atomic(Error) ->  ?SystemExceptionNoLogging(Error).


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
meta_field_value(username) -> <<"unknown">>;
meta_field_value(schema) -> schema();
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

system_id() ->
    lists:flatten(atom_to_list(schema()) ++ "@",atom_to_list(node())).

add_attribute(A, Opts) -> update_opts({attributes,A}, Opts).

update_opts({K,_} = T, Opts) when is_atom(K) -> lists:keystore(K, 1, Opts, T).

data_nodes() -> [{schema(N),N} || N <- mnesia:system_info(running_db_nodes)].

all_tables() ->
    lists:delete(schema, mnesia:system_info(tables)).

is_local_table(Table) ->
    try
        case mnesia:table_info(Table, storage_type) of
            unknown ->  false;
            _ ->        true
        end
    catch
        _:{aborted,{no_exists,_,_}} ->  ?ClientErrorNoLogging({"Table does not exist", Table});
        _:Error ->                      ?SystemExceptionNoLogging(Error)
    end.  

table_type(Table) ->
    mnesia:table_info(Table, type).

table_columns(Table) ->
    mnesia:table_info(Table, attributes).

table_info(Table, InfoKey) ->
    mnesia:table_info(Table, InfoKey).

table_record_name(Table) ->
    table_info(Table, record_name).
    
table_size(Table) ->
    % record count in Table
    case is_local_table(Table) of
        false ->  ?ClientErrorNoLogging({"Cannot evaluate size of remote table", Table});
        true  ->  mnesia:table_info(Table, size)
                  %% is_local_table() needed to catch non existing table
    end.

table_memory(Table) ->
    % memory in bytes occupied by Table
    case is_local_table(Table) of
        false ->  ?ClientErrorNoLogging({"Cannot evaluate memory of remote table", Table});
        true ->   mnesia:table_info(Table, memory) * erlang:system_info(wordsize)
                  %% is_local_table() needed to catch non existing table
    end.  

check_table(Table) ->
    % return ok for local loaded table, throw exception 
    case is_local_table(Table) of
        false ->  ?ClientErrorNoLogging({"This is a remote table", Table});
        true  ->  ok
    end.

check_table_columns(Table, ColumnNames) ->
    TableColumns = table_columns(Table),
    if
        ColumnNames =:= TableColumns ->
            ok;
        true ->
            ?SystemException({"Column names do not match table structure",Table})
    end.


%% ---------- MNESIA FUNCTIONS ------ exported -------------------------------

create_table(Table, ColumnNames, Opts) ->
    Local = lists:member({scope,local}, Opts),
    Cluster = lists:member({scope,cluster}, Opts),
    if
        Local ->    create_local_table(Table, ColumnNames, Opts);
        Cluster ->  create_cluster_table(Table, ColumnNames, Opts);
        true ->     create_schema_table(Table, ColumnNames, Opts)
    end.

system_table(_) -> false.

create_local_table(Table,ColumnNames,Opts) when is_atom(Table) ->
    Cols = [list_to_atom(lists:flatten(io_lib:format("~p", [X]))) || X <- ColumnNames],
    CompleteOpts = add_attribute(Cols, Opts) -- [{scope,local}],
    create_table(Table, CompleteOpts).

create_schema_table(Table,ColumnNames,Opts) when is_atom(Table) ->
    DiscNodes = mnesia:table_info(schema, disc_copies),
    RamNodes = mnesia:table_info(schema, ram_copies),
    CompleteOpts = [{ram_copies, RamNodes}, {disc_copies, DiscNodes}|Opts] -- [{scope,schema}],
    create_local_table(Table,ColumnNames,CompleteOpts).

create_cluster_table(Table,ColumnNames,Opts) when is_atom(Table) ->
    DiscNodes = mnesia:table_info(schema, disc_copies),
    RamNodes = mnesia:table_info(schema, ram_copies),
    %% ToDo: may need to pull from another imem schema first and initiate sync
    CompleteOpts = [{ram_copies, RamNodes}, {disc_copies, DiscNodes}|Opts] -- [{scope,cluster}],
    create_local_table(Table,ColumnNames,CompleteOpts).

create_table(Table, Opts) when is_list(Table) ->
    create_table(list_to_atom(Table), Opts);
create_table(Table, Opts) when is_atom(Table) ->
    {ok, Conf} = application:get_env(imem, mnesia_wait_table_config),
    Now = erlang:now(),
    case mnesia:create_table(Table, Opts) of
        {aborted, {already_exists, Table}} ->
            % ?Debug("table ~p locally exists~n", [Table]),
            mnesia:add_table_copy(Table, node(), ram_copies),
            yes = mnesia:force_load_table(Table),
            wait_table_tries([Table], Conf),
            ?INIT_SNAP(Table,Now),
            ?ClientErrorNoLogging({"Table already exists", Table});
        {aborted, {already_exists, Table, _Node}} ->
            % ?Debug("table ~p exists at ~p~n", [Table, _Node]),
            case mnesia:force_load_table(Table) of
                yes -> ok;
                Error -> ?ClientErrorNoLogging({"Loading table(s) timeout~p", Error})
            end,
            ?INIT_SNAP(Table,Now),
            ?ClientErrorNoLogging({"Table already exists", Table});
            %return_atomic_ok(mnesia:add_table_copy(Table, node(), ram_copies));
        Result ->
            % ?Debug("create_table ~p for ~p~n", [Result, Table]),
            wait_table_tries([Table], Conf),
            ?INIT_SNAP(Table,Now),
            return_atomic_ok(Result)
    end.

wait_table_tries(Tables, {0, _}) ->
    ?ClientErrorNoLogging({"Loading table(s) timeout~p", Tables});
wait_table_tries(Tables, {Count,Timeout}) when is_list(Tables) ->
    case mnesia:wait_for_tables(Tables, Timeout) of
        ok -> ok;
        {timeout, BadTabList} ->
            ?Debug("table ~p load time out attempt ~p~n", [BadTabList, Count]),
            wait_table_tries(Tables, {Count-1,Timeout});
        {error, Reason} -> ?ClientErrorNoLogging({"Error loading table~p", Reason})
    end.

drop_table(Table) when is_atom(Table) ->
    case mnesia:delete_table(Table) of
        {atomic,ok} ->
            true = ets:delete(?SNAP_ETS_TAB, Table),
            ok;
        {aborted,{no_exists,Table}} ->  ?ClientError({"Table does not exist",Table});
        Error ->                        ?SystemExceptionNoLogging(Error)
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
    case mnesia:clear_table(Table) of
        {atomic,ok} ->
            ?TOUCH_SNAP(Table),                  
            ok;
        {aborted,{no_exists,Table}} ->  ?ClientError({"Table does not exist",Table});
        Error ->                        ?SystemExceptionNoLogging(Error)
    end.

read(Table) when is_atom(Table) ->
    Trans = fun() ->
        Keys = mnesia:all_keys(Table),
        % [lists:nth(1, mnesia:read(Table, X)) || X <- Keys]
        lists:flatten([mnesia:read(Table, X) || X <- Keys])
    end,
    case transaction(Trans) of
        {atomic, Result} ->         Result;
        {aborted,{no_exists,_}} ->  ?ClientError({"Table does not exist",Table});
        Error ->                    ?SystemExceptionNoLogging(Error)
    end.

read(Table, Key) when is_atom(Table) ->
    Result = case transaction(read,[Table, Key]) of
        {aborted,{no_exists,_}} ->  ?ClientError({"Table does not exist",Table});
        Res ->                      Res
    end,
    return_atomic_list(Result).


read_hlk(Table, HListKey) when is_atom(Table), is_list(HListKey) ->
    % read using HierarchicalListKey 
    Trans = fun
        ([],_) ->
            [];
        (HLK,Tra) ->
            case mnesia:read(Table,HLK) of
                [] ->   Tra(lists:sublist(HLK, length(HLK)-1),Tra);
                R ->    R
            end
    end,
    case transaction(Trans,[HListKey,Trans]) of
        {atomic, Result} ->         Result;
        {aborted,{no_exists,_}} ->  ?ClientError({"Table does not exist",Table});
        Error ->                    ?SystemExceptionNoLogging(Error)
    end.


dirty_write(Table, Row) when is_atom(Table), is_tuple(Row) ->
    try
        % ?Debug("mnesia:dirty_write ~p ~p~n", [Table,Row]),
        mnesia_table_write_access(dirty_write, [Table, Row])
    catch
        exit:{aborted, {no_exists,_}} ->    ?ClientErrorNoLogging({"Table does not exist",Table});
        exit:{aborted, {no_exists,_,_}} ->  ?ClientErrorNoLogging({"Table does not exist",Table});
        _:Reason ->                         ?SystemExceptionNoLogging({"Mnesia dirty_write failure",Reason})
    end.

write(Table, Row) when is_atom(Table), is_tuple(Row) ->
    %if Table =:= ddTable -> ?Debug("mnesia:write ~p ~p~n", [Table,Row]); true -> ok end,
    Result = case transaction(write,[Table, Row, write]) of
        {aborted,{no_exists,_}} ->
            % ?Debug("cannot write ~p to ~p~n", [Row,Table]),
            ?ClientErrorNoLogging({"Table does not exist",Table});
        {atomic,ok} ->
            ?TOUCH_SNAP(Table),
            %if Table =:= ddTable -> io:format(user, "ddTable written ~p~n", [Up]); true -> ok end,
            {atomic,ok};
        Error ->
            Error   
    end,
    return_atomic_ok(Result).

delete(Table, Key) when is_atom(Table) ->
    Result = case transaction(delete,[{Table, Key}]) of
        {atomic,ok} ->
            ?TOUCH_SNAP(Table),
            {atomic,ok};
        {aborted,{no_exists,_}} ->          
            ?ClientError({"Table does not exist",Table});
        Res ->
            Res
    end,
    return_atomic_ok(Result).

delete_object(Table, Row) when is_atom(Table) ->
    Result = case transaction(delete_object,[Table, Row, write]) of
        {atomic,ok} ->
            ?TOUCH_SNAP(Table),
            {atomic,ok};
        {aborted,{no_exists,_}} ->          
            ?ClientError({"Table does not exist",Table});
        Res ->                              
            Res
    end,
    return_atomic_ok(Result).

select(Table, MatchSpec) when is_atom(Table) ->
    case transaction(select,[Table, MatchSpec]) of
        {atomic, L}     ->                  {L, true};
        {aborted,{no_exists,_}} ->          ?ClientError({"Table does not exist",Table});
        Error ->                            ?SystemExceptionNoLogging(Error)
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
        {atomic, {Result, AllRead}} ->          {Result, AllRead};
        {aborted,{no_exists,_}} ->              ?ClientError({"Table does not exist",Table});
        Error ->                                ?SystemExceptionNoLogging(Error)
    end.

select_sort(Table, MatchSpec, Limit) ->
    {Result, AllRead} = select(Table, MatchSpec, Limit),
    {lists:sort(Result), AllRead}.

fetch_start(Pid, Table, MatchSpec, BlockSize, Opts) ->
    F =
    fun(F,Contd0) ->
        receive
            abort ->
                % ?Debug("Abort fetch on table ~p~n", [Table]),
                ok;
            next ->
                ?Debug("got start trigger ~p", [Pid]),
                case Contd0 of
                        undefined ->
                            ?Debug("[~p] got starting fetch...~n", [Pid]),
                            case mnesia:select(Table, MatchSpec, BlockSize, read) of
                                '$end_of_table' ->
                                    Pid ! {row, [?sot,?eot]};
                                {Rows, Contd1} ->
                                    ?Debug("First continuation object ~p~n",[Contd1]),
                                    Eot = lists:member('$end_of_table', tuple_to_list(Contd1)),
                                    if  Eot ->
                                            Pid ! {row, [?sot,?eot|Rows]};
                                        true ->
                                            Pid ! {row, [?sot|Rows]},
                                            F(F,Contd1)
                                    end
                            end;
                        Contd0 ->
                            ?Debug("[~p] got continuing fetch...~n", [Pid]),
                            case mnesia:select(Contd0) of
                                '$end_of_table' ->
                                    ?Debug("Last continuation object ~p~n",[Contd0]),
                                    Pid ! {row, ?eot};
                                {Rows, Contd1} ->
                                    Eot = lists:member('$end_of_table', tuple_to_list(Contd1)),
                                    if  Eot ->
                                            Pid ! {row, [?eot|Rows]};
                                        true ->
                                            Pid ! {row, Rows},
                                            F(F,Contd1)
                                    end
                            end
                end
        end
    end,
    case lists:keyfind(access, 1, Opts) of
        {_,Access} ->   spawn(mnesia, Access, [F, [F,undefined]]);
        false ->        spawn(mnesia, async_dirty, [F, [F,undefined]])
    end.

update_tables(UpdatePlan, Lock) ->
    Update = fun() ->
        [update_xt(Table, Item, Lock, Old, New) || [Table, Item, Old, New] <- UpdatePlan]
    end,
    return_atomic(transaction(Update)).

% Field as column name into tuple
update_bound_counter(Table, Field, Key, Incr, LimitMin, LimitMax)
    when is_atom(Table),
         is_atom(Field),
         is_number(Incr),
         is_number(LimitMin),
         is_number(LimitMax) ->
    Attrs = mnesia:table_info(Table, attributes),
    case lists:keyfind(Field, 1, lists:zip(Attrs, lists:seq(1,length(Attrs)))) of
        {Field, FieldIdx} -> update_bound_counter(Table, FieldIdx, Key, Incr, LimitMin, LimitMax);
        _ -> field_not_found
    end;
% Field as index into the tuple
update_bound_counter(Table, Field, Key, Incr, LimitMin, LimitMax)
    when is_atom(Table),
         is_integer(Field),
         is_number(Incr),
         is_number(LimitMin),
         is_number(LimitMax) ->
    mnesia:transaction(fun() ->
        case mnesia:read(Table, Key) of
            [Row|_] ->
                N = element(Field+1, Row),
                if
                    ((N + Incr) =< LimitMax) andalso is_number(N) ->
                        ok = mnesia:write(setelement(Field+1, Row, N + Incr)),
                        Incr;
                    ((N + Incr) > LimitMax) andalso is_number(N) ->
                        ok = mnesia:write(setelement(Field+1, Row, LimitMax)),
                        LimitMax - N;
                    true -> {field_not_number, N}
                end;
            _ -> no_rows
        end
    end).

update_xt({_Table,bag}, _Item, _Lock, {}, {}) ->
    ok;
update_xt({Table,bag}, Item, Lock, Old, {}) when is_atom(Table) ->
    Current = mnesia:read(Table, element(2,Old)),
    Exists = lists:member(Old,Current),
    if
        Exists ->
            %mnesia:delete_object(Table, Old, write),
            mnesia_table_write_access(delete_object, [Table, Old, write]),
            {Item,{}};
        Lock == none ->
            {Item,{}};
        true ->
            ?ConcurrencyException({"Data is modified by someone else", {Item, Old}})
    end;
update_xt({Table,bag}, Item, Lock, {}, New) when is_atom(Table) ->
    Current = mnesia:read(Table, element(2,New)),  %% may be expensive
    Exists = lists:member(New,Current),
    if
        (Exists and (Lock==none)) ->
            {Item,New};
        Exists ->
            ?ConcurrencyException({"Record already exists", {Item, New}});
        true ->
            %mnesia:write(Table, New, write),
            mnesia_table_write_access(write, [Table, New, write]),
            {Item,New}
    end;
update_xt({Table,bag}, Item, Lock, Old, Old) when is_atom(Table) ->
    Current = mnesia:read(Table, element(2,Old)),  %% may be expensive
    Exists = lists:member(Old,Current),
    if
        Exists ->
            {Item,Old};
        Lock == none ->
            {Item,Old};
        true ->
            ?ConcurrencyException({"Data is modified by someone else", {Item, Old}})
    end;
update_xt({Table,bag}, Item, Lock, Old, New) when is_atom(Table) ->
    update_xt({Table,bag}, Item, Lock, Old, {}),
    update_xt({Table,bag}, Item, Lock, {}, New);

update_xt({_Table,_}, _Item, _Lock, {}, {}) ->
    ok;
update_xt({Table,_}, Item, _Lock, Old, {}) when is_atom(Table), is_tuple(Old) ->
    %mnesia:delete(Table, element(2, Old), write),
    mnesia_table_write_access(delete, [Table, element(2, Old), write]),
    {Item,{}};
update_xt({Table,_}, Item, Lock, {}, New) when is_atom(Table), is_tuple(New) ->
    if
        Lock == none ->
            ok;
        true ->
            case mnesia:read(Table, element(2,New)) of
                [New] ->    ok;
                [] ->       ok;
                Current ->  ?ConcurrencyException({"Key violation", {Item,{Current, New}}})
            end
    end,
    %mnesia:write(Table,New,write),
    mnesia_table_write_access(write, [Table, New, write]),
    {Item,New};
update_xt({Table,_}, Item, none, Old, Old) when is_atom(Table), is_tuple(Old) ->
    {Item,Old};
update_xt({Table,_}, Item, _Lock, Old, Old) when is_atom(Table), is_tuple(Old) ->
    case mnesia:read(Table, element(2,Old)) of
        [Old] ->    {Item,Old};
        [] ->       ?ConcurrencyException({"Data is deleted by someone else", {Item, Old}});
        Current ->  ?ConcurrencyException({"Data is modified by someone else", {Item,{Old, Current}}})
    end;
update_xt({Table,_}, Item, Lock, Old, New) when is_atom(Table), is_tuple(Old), is_tuple(New) ->
    OldKey=element(2,Old),
    if
        Lock == none ->
            ok;
        true ->
            case mnesia:read(Table, OldKey) of
                [Old] ->    ok;
                [] ->       ?ConcurrencyException({"Data is deleted by someone else", {Item, Old}});
                Curr1 ->    ?ConcurrencyException({"Data is modified by someone else", {Item,{Old, Curr1}}})
            end
    end,
    NewKey = element(2,New),
    case NewKey of
        OldKey ->
            %mnesia:write(Table,New,write),
            mnesia_table_write_access(write, [Table, New, write]),
            {Item,New};
        NewKey ->
            case mnesia:read(Table, NewKey) of
                [New] ->    %mnesia:delete(Table,OldKey,write),
                            mnesia_table_write_access(delete, [Table, OldKey, write]),
                            %mnesia:write(Table,New,write),
                            mnesia_table_write_access(write, [Table, New, write]),
                            {Item,New};
                [] ->       %mnesia:delete(Table,OldKey,write),
                            mnesia_table_write_access(delete, [Table, OldKey, write]),
                            %mnesia:write(Table,New,write),
                            mnesia_table_write_access(write, [Table, New, write]),
                            {Item,New};
                Curr2 ->    ?ConcurrencyException({"Modified key already exists", {Item,Curr2}})
            end
    end.

subscribe({table, Tab, simple}) ->
    {ok,_} = mnesia:subscribe({table, Tab, simple}),
    ok;
subscribe({table, Tab, detailed}) ->
    {ok,_} = mnesia:subscribe({table, Tab, detailed}),
    ok;
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
    ets:new(?SNAP_ETS_TAB, [public, named_table, {keypos,2}]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

init(_) ->
    ?Info("~p starting...~n", [?MODULE]),
    {ok, SchemaName} = application:get_env(mnesia_schema_name),
    {ok, NodeType} = application:get_env(mnesia_node_type),
    ?Info("mnesia node type is '~p'~n", [NodeType]),
    SDir = atom_to_list(SchemaName) ++ "." ++ atom_to_list(node()),
    {ok, Cwd} = file:get_cwd(),
    LastFolder = lists:last(filename:split(Cwd)),
    SchemaDir = if LastFolder =:= ".eunit" -> filename:join([Cwd, "..", SDir]); true ->  filename:join([Cwd, SDir]) end,
    ?Info("SchemaDir ~p~n", [SchemaDir]),
    random:seed(now()),
    SleepTime = random:uniform(1000),
    ?Info("~p sleeping for ~p ms...~n", [?MODULE, SleepTime]),
    timer:sleep(SleepTime),
    application:set_env(mnesia, dir, SchemaDir),
    ok = mnesia:start(),
    case disc_schema_nodes(SchemaName) of
        [] -> ?Warn("~p no node found at ~p for schema ~p in erlang cluster ~p~n", [?MODULE, node(), SchemaName, erlang:get_cookie()]);
        [DiscSchemaNode|_] ->
            ?Info("~p adding ~p to schema ~p on ~p~n", [?MODULE, node(), SchemaName, DiscSchemaNode]),
            {ok, _} = rpc:call(DiscSchemaNode, mnesia, change_config, [extra_db_nodes, [node()]])
    end,
    case NodeType of
        disc -> mnesia:change_table_copy_type(schema, node(), disc_copies);
        _ -> ok
    end,
    mnesia:subscribe(system),
    ?Info("~p started!~n", [?MODULE]),
    {ok,#state{}}.

handle_call(Request, From, State) ->
    ?Info("Unknown request ~p from ~p!", [Request, From]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?Info("Unknown cast ~p!", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    case Info of
        {mnesia_system_event,{mnesia_overload,Details}} ->
            % BulkSleepTime0 = get(mnesia_bulk_sleep_time),
            % BulkSleepTime = trunc(1.1 * BulkSleepTime0),
            % put(mnesia_bulk_sleep_time, BulkSleepTime),
            ?Warn("Mnesia overload : ~p!~n",[Details]);
        {mnesia_system_event,{Event,Node}} ->
            ?Info("Mnesia event ~p from Node ~p!~n",[Event, Node]);
        Error ->
            ?Error("Mnesia error : ~p~n",[Error])
    end,
    case lists:keyfind(mnesia, 1, application:which_applications()) of
        {mnesia,_,_} -> {noreply, State};
        false ->
            ?Error("Mnesia down!~n"),
            {stop, mnesia_down, State}
    end.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

-spec get_os_memory() -> {any(), integer(), integer()}.
get_os_memory() ->
    case os:type() of
        {win32, _} = Win ->
            {Win
            , list_to_integer(re:replace(os:cmd("wmic OS get FreePhysicalMemory")
                                        ,"[[:space:]]+",""
                                        ,[global,{return,list}])
                             -- "FreePhysicalMemory") * 1024
            , list_to_integer(re:replace(os:cmd("wmic ComputerSystem get TotalPhysicalMemory")
                                        ,"[[:space:]]+",""
                                        ,[global,{return,list}])
                                -- "TotalPhysicalMemory")
            };
        {unix, _} = Unix ->
            {Unix
            , list_to_integer(re:replace(os:cmd("free -b | sed -n 2p | awk '{print $4}'")
                                        ,"[[:space:]]+","",[global,{return,list}]))
            , list_to_integer(re:replace(os:cmd("free -b | sed -n 2p | awk '{print $2}'")
                                        ,"[[:space:]]+","",[global,{return,list}]))
            };
        Unknown ->
		       {Unknown, 1, 1}
    end.

%% ----- Private functions ------------------------------------
mnesia_table_write_access(Fun, Args) when is_atom(Fun), is_list(Args) ->
    case apply(mnesia, Fun, Args) of
        {atomic,ok} ->
            ?TOUCH_SNAP(hd(Args)),
            {atomic,ok};
        Error ->
            Error   
    end.

%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ?imem_test_setup().

teardown(_) ->
    catch drop_table(imem_table_bag),
    catch drop_table(imem_table_123),
    imem:stop().

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

        ?Log("----TEST--~p:test_mnesia~n", [?MODULE]),

        ?Log("schema ~p~n", [imem_meta:schema()]),
        ?Log("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        ?Log("----TEST--~p:test_database_operations~n", [?MODULE]),

        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, table_size(non_existing_table)),
        ?Log("success ~p~n", [table_size_no_exists]),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, table_memory(non_existing_table)),
        ?Log("success ~p~n", [table_memory_no_exists]),

        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, read(non_existing_table)),
        ?Log("success ~p~n", [table_read_no_exists]),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, read(non_existing_table, no_key)),
        ?Log("success ~p~n", [row_read_no_exists]),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, write(non_existing_table, {non_existing_table, "AAA","BB","CC"})),
        ?Log("success ~p~n", [row_write_no_exists]),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, dirty_write(non_existing_table, {non_existing_table, "AAA","BB","CC"})),
        ?Log("success ~p~n", [row_dirty_write_no_exists]),
        ?assertException(throw, {SyEx, {aborted,{bad_type,non_existing_table,{},write}}}, write(non_existing_table, {})),
        ?Log("success ~p~n", [row_write_bad_type]),
        ?assertEqual(ok, create_table(imem_table_123, [a,b,c], [])),
        ?Log("success ~p~n", [create_set_table]),
        ?assertEqual(0, table_size(imem_table_123)),
        ?Log("success ~p~n", [table_size_empty]),
        BaseMemory = table_memory(imem_table_123),
        ?assert(BaseMemory < 4000),    %% got value of 303 words x 8 bytes on 10.05.2013
        ?Log("success ~p ~p~n", [table_memory_empty, BaseMemory]),
        ?assertEqual(ok, write(imem_table_123, {imem_table_123,"A","B","C"})),
        ?assertEqual(1, table_size(imem_table_123)),
        ?Log("success ~p~n", [write_table]),
        ?assertEqual(ok, write(imem_table_123, {imem_table_123,"AA","BB","CC"})),
        ?assertEqual(2, table_size(imem_table_123)),
        ?Log("success ~p~n", [write_table]),
        ?assertEqual(ok, write(imem_table_123, {imem_table_123,"AA","BB","cc"})),
        ?assertEqual(2, table_size(imem_table_123)),
        ?Log("success ~p~n", [write_table]),
        ?assertEqual(ok, write(imem_table_123, {imem_table_123, "AAA","BB","CC"})),
        ?assertEqual(3, table_size(imem_table_123)),
        ?Log("success ~p~n", [write_table]),
        ?assertEqual(ok, dirty_write(imem_table_123, {imem_table_123, "AAA","BB","CC"})),
        ?assertEqual(3, table_size(imem_table_123)),
        ?Log("success ~p~n", [write_table]),
        FullMemory = table_memory(imem_table_123),
        ?assert(FullMemory > BaseMemory),
        ?assert(FullMemory < BaseMemory + 800),  %% got 362 words on 10.5.2013
        ?Log("success ~p ~p~n", [table_memory_full, FullMemory]),
        ?assertEqual([{imem_table_123,"A","B","C"}], read(imem_table_123,"A")),
        ?Log("success ~p~n", [read_table_1]),
        ?assertEqual([{imem_table_123,"AA","BB","cc"}], read(imem_table_123,"AA")),
        ?Log("success ~p~n", [read_table_2]),
        ?assertEqual([], read(imem_table_123,"XX")),
        ?Log("success ~p~n", [read_table_3]),
        AllRecords=lists:sort([{imem_table_123,"A","B","C"},{imem_table_123,"AA","BB","cc"},{imem_table_123,"AAA","BB","CC"}]),
        AllKeys=["A","AA","AAA"],
        ?assertEqual(AllRecords, lists:sort(read(imem_table_123))),
        ?Log("success ~p~n", [read_table_4]),
        ?assertEqual({AllRecords,true}, select_sort(imem_table_123, ?MatchAllRecords)),
        ?Log("success ~p~n", [select_all_records]),
        ?assertEqual({AllKeys,true}, select_sort(imem_table_123, ?MatchAllKeys)),
        ?Log("success ~p~n", [select_all_keys]),
        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, select(non_existing_table, ?MatchAllRecords)),
        ?Log("success ~p~n", [select_table_no_exists]),
        MatchHead = {'$1','$2','$3','$4'},
        Guard = {'==', '$3', "BB"},
        Result = {{'$3','$4'}},
        DTupResult = lists:sort([{"BB","cc"},{"BB","CC"}]),
        ?assertEqual({DTupResult,true}, select_sort(imem_table_123, [{MatchHead, [Guard], [Result]}])),
        ?Log("success ~p~n", [select_some_data1]),
        STupResult = lists:sort(["cc","CC"]),
        ?assertEqual({STupResult,true}, select_sort(imem_table_123, [{MatchHead, [Guard], ['$4']}])),
        ?Log("success ~p~n", [select_some_data]),
        NTupResult = lists:sort([{"cc"},{"CC"}]),
        ?assertEqual({NTupResult,true}, select_sort(imem_table_123, [{MatchHead, [Guard], [{{'$4'}}]}])),
        ?Log("success ~p~n", [select_some_data2]),
        Limit=10,
        SelRes=select_sort(imem_table_123, [{MatchHead, [Guard], [{{'$4'}}]}], Limit),
        ?assertMatch({[_|_], true}, SelRes),
        {SelList, true} = SelRes,
        ?assertEqual(NTupResult, SelList),
        ?Log("success ~p~n", [select_some_data3]),

        ?Log("----TEST--~p:test_transactions~n", [?MODULE]),

        ?Log("data in table ~p~n~p~n", [imem_table_123, lists:sort(read(imem_table_123))]),

        Update1 = fun(X) ->
            update_xt({imem_table_123,set}, 1, optimistic, {imem_table_123, "AAA","BB","CC"}, {imem_table_123, "AAA","11",X}),
            update_xt({imem_table_123,set}, 2, optimistic, {}, {imem_table_123, "XXX","11","22"}),
            update_xt({imem_table_123,set}, 3, optimistic, {imem_table_123, "AA","BB","cc"}, {}),
            lists:sort(read(imem_table_123))
        end,
        UR1 = return_atomic(transaction(Update1, ["99"])),
        ?Log("updated data in table ~p~n~p~n", [imem_table_123, UR1]),
        ?assertEqual(UR1, [{imem_table_123,"A","B","C"},{imem_table_123,"AAA","11","99"},{imem_table_123,"XXX","11","22"}]),

        Update1a = fun(X) ->
            update_xt({imem_table_123,set}, 1, optimistic, {imem_table_123, "AAA","11","99"}, {imem_table_123, "AAA","BB",X})
        end,
        UR1a = return_atomic(transaction(Update1a, ["xx"])),
        ?Log("updated key ~p~n", [UR1a]),
        ?assertEqual({1,{imem_table_123, "AAA","BB","xx"}},UR1a),


        ?assertEqual(ok, truncate_table(imem_table_123)),
        ?assertEqual(0,table_size(imem_table_123)),
        ?assertEqual(BaseMemory, table_memory(imem_table_123)),

        ?assertEqual(ok, drop_table(imem_table_123)),
        ?Log("success ~p~n", [drop_table]),

        ?assertEqual(ok, create_table(imem_table_bag, [a,b,c], [{type, bag}])),
        ?Log("success ~p~n", [create_bag_table]),

        ?assertEqual(ok, write(imem_table_bag, {imem_table_bag,"A","B","C"})),
        ?assertEqual(1, table_size(imem_table_bag)),
        ?assertEqual(ok, write(imem_table_bag, {imem_table_bag,"AA","BB","CC"})),
        ?assertEqual(2, table_size(imem_table_bag)),
        ?assertEqual(ok, write(imem_table_bag, {imem_table_bag,"AA","BB","cc"})),
        ?assertEqual(3, table_size(imem_table_bag)),
        ?assertEqual(ok, write(imem_table_bag, {imem_table_bag, "AAA","BB","CC"})),
        ?assertEqual(4, table_size(imem_table_bag)),
        ?assertEqual(ok, write(imem_table_bag, {imem_table_bag, "AAA","BB","CC"})),
        ?assertEqual(bag, table_info(imem_table_bag, type)),
        ?assertEqual(4, table_size(imem_table_bag)),
        ?Log("data in table ~p~n~p~n", [imem_table_bag, lists:sort(read(imem_table_bag))]),
        ?Log("success ~p~n", [write_table]),

        Update2 = fun(X) ->
            update_xt({imem_table_bag,bag}, 1, optimistic, {imem_table_bag, "AA","BB","cc"}, {imem_table_bag, "AA","11",X}),
            update_xt({imem_table_bag,bag}, 2, optimistic, {}, {imem_table_bag, "XXX","11","22"}),
            update_xt({imem_table_bag,bag}, 3, optimistic, {imem_table_bag, "A","B","C"}, {}),
            lists:sort(read(imem_table_bag))
        end,
        UR2 = return_atomic(transaction(Update2, ["99"])),
        ?Log("updated data in table ~p~n~p~n", [imem_table_bag, UR2]),
        ?assertEqual([{imem_table_bag,"AA","11","99"},{imem_table_bag,"AA","BB","CC"},{imem_table_bag,"AAA","BB","CC"},{imem_table_bag,"XXX","11","22"}], UR2),

        Update3 = fun() ->
            update_xt({imem_table_bag,bag}, 1, optimistic, {imem_table_bag, "AA","BB","cc"}, {imem_table_bag, "AA","11","11"})
        end,
        ?assertException(throw, {CoEx, {"Data is modified by someone else", {1, {imem_table_bag, "AA","BB","cc"}}}}, return_atomic(transaction(Update3))),

        Update4 = fun() ->
            update_xt({imem_table_bag,bag}, 1, optimistic, {imem_table_bag,"AA","11","99"}, {imem_table_bag, "AB","11","11"})
        end,
        ?assertEqual({1, {imem_table_bag, "AB","11","11"}}, return_atomic(transaction(Update4))),

        ?assertEqual(ok, drop_table(imem_table_bag)),
        ?Log("success ~p~n", [drop_table]),

        ?assertEqual(ok, create_table(imem_table_123, [hlk,val], [])),
        ?Log("success ~p~n", [imem_table_123]),
        ?assertEqual([], read_hlk(imem_table_123, [some_key])),
        HlkR1 = {imem_table_123, [some_key],some_value},
        ?assertEqual(ok, write(imem_table_123, HlkR1)),
        ?assertEqual([HlkR1], read_hlk(imem_table_123, [some_key])),
        ?assertEqual([HlkR1], read_hlk(imem_table_123, [some_key,some_context])),
        ?assertEqual([], read_hlk(imem_table_123, [some_wrong_key])),
        HlkR2 = {imem_table_123, [some_key],some_value},        
        ?assertEqual(ok, write(imem_table_123, HlkR2)),
        ?assertEqual([HlkR2], read_hlk(imem_table_123, [some_key,over_context])),
        ?assertEqual([], read_hlk(imem_table_123, [])),

        ?assertException(throw, {ClEr, {"Table does not exist", non_existing_table}}, read_hlk(non_existing_table, [some_key,over_context])),

        ?assertEqual(ok, drop_table(imem_table_123))

    catch
        Class:Reason ->  ?Log("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.

-endif.
