-module(imem_if_mnesia).
-behavior(gen_server).

-include("imem.hrl").
-include("imem_if.hrl").

-define(TABLE_INFO_RPC_TIMEOUT,10000).

% -define(RecIdx, 1).                                       %% Record name position in records
% -define(FirstIdx, 2).                                     %% First field position in records
-define(KeyIdx, 2).                                       %% Key position in records

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
        , data_nodes/0
        , all_tables/0
        , is_readable_table/1
        , table_type/1
        , table_columns/1
        , table_info/2
        , table_size/1
        , table_memory/1
        , table_record_name/1        
        , check_table/1
        , check_local_table_copy/1
        , is_system_table/1
        , meta_field_value/1
        , subscribe/1
        , unsubscribe/1
        ]).

-export([ create_table/3
        , drop_table/1
        , create_index/2
        , create_or_replace_index/2
        , drop_index/2
        , truncate_table/1
        , select/2
        , dirty_select/2
        , select/3
        , select_sort/2
        , select_sort/3
        , read/1
        , read/2
        , dirty_read/1
        , dirty_read/2
        , dirty_index_read/3
        , read_hlk/2            %% read using hierarchical list key
        , fetch_start/5
        , write/2
        , dirty_write/2
        , delete/2
        , dirty_delete/2
        , delete_object/2
        , update_tables/2
        , update_bound_counter/6
        , write_table_property/2
        , write_table_property_in_transaction/2
        , read_table_property/2
        , delete_table_property/2
        ]).

-export([ transaction/1
        , transaction/2
        , transaction/3
        , ets/2
        , select_count/2
        , return_atomic_list/1
        , return_atomic_ok/1
        , return_atomic/1
        , lock/2
        , abort/1
        , integer_uid/0
        , time_uid/0
        , timestamp/0
        , timestamp_diff/2
        ]).

-export([ first/1
        , dirty_first/1
        , next/2
        , dirty_next/2
        , last/1
        , dirty_last/1
        , prev/2
        , dirty_prev/2
        , foldl/3
        ]).

-export([ field_pick/2
        ]).

-export([ epmd_register/0
        ]).

% Functions applied with Common Test
-export([update_xt/8]).

-safe([epmd_register/0]).

-define(TOUCH_SNAP(__Table),                  
            case ets:lookup(?SNAP_ETS_TAB, __Table) of
                [__Up] ->   
                    true = ets:insert(?SNAP_ETS_TAB, __Up#snap_properties{last_write=?TIMESTAMP}),
                    ok;
                [] ->
                    __Now = ?TIMESTAMP,
                    true = ets:insert(?SNAP_ETS_TAB, #snap_properties{table=__Table, last_write=__Now, last_snap=__Now}),
                    ok
            end
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

% Monotonic, adapted, non-unique timestamp
% microsecond resolution and OS-dependent precision
% referenced with macro ?TIMESTAMP
-spec timestamp() -> ddTimestamp().
timestamp() -> 
    SystemTime = erlang:system_time(1000000),
    {SystemTime div 1000000, SystemTime rem 1000000}.

% Monotonic, adapted, non-unique timestamp difference
% microsecond resolution and OS-dependent precision
% referenced with macro ?TIMESTAMP_DIFF
-spec timestamp_diff(ddTimestamp(),ddTimestamp()) -> integer().
timestamp_diff({Sec1, Micro1}, {Sec2, Micro2}) -> 1000000*(Sec2-Sec1)+Micro2-Micro1.

% Monotonic, adapted, unique timestamp
% microsecond resolution and OS-dependent precision
% referenced with macro ?TIME_UID
-spec time_uid() -> ddTimeUID(). 
time_uid() -> 
    {Secs, Micros} = timestamp(),
    {Secs, Micros, node(), erlang:unique_integer([monotonic, positive])}.

% Unique integer per imem node (VM)
% referenced with macro ?INTEGER_UID
-spec integer_uid() -> integer().
integer_uid() -> erlang:unique_integer([monotonic, positive]).


return_atomic_list({atomic, L}) when is_list(L) -> L;
return_atomic_list({aborted,{throw,{Exception,Reason}}}) -> throw({Exception,Reason});
return_atomic_list({aborted,{exit,{Exception,Reason}}}) -> exit({Exception,Reason});
return_atomic_list({aborted,Error}) ->      ?SystemExceptionNoLogging(Error);
return_atomic_list(L) when is_list(L) ->    L;
return_atomic_list(Error) ->                ?SystemExceptionNoLogging(Error).

return_atomic_ok({atomic, ok}) ->           ok;
return_atomic_ok({aborted,{throw,{Exception,Reason}}}) -> throw({Exception,Reason});
return_atomic_ok({aborted,{exit,{Exception,Reason}}}) -> exit({Exception,Reason});
return_atomic_ok({aborted,Error}) ->        ?SystemExceptionNoLogging(Error);
return_atomic_ok(ok) ->                     ok.

return_atomic({atomic, Result}) ->          Result;
return_atomic({aborted, {throw,{Exception, Reason}}}) -> throw({Exception, Reason});
return_atomic({aborted, {exit, {Exception, Reason}}}) -> exit({Exception, Reason});
return_atomic({aborted, Error}) ->          ?SystemExceptionNoLogging(Error);
return_atomic(Other) ->                     Other.

abort(Reason) -> mnesia:abort(Reason).

% init and store transaction time
trans_time_init() ->
    erlang:put(?TRANS_TIME_NAME,?TIME_UID).

transaction(Function) when is_atom(Function) ->
    case mnesia:is_transaction() of
        false ->    F = fun() -> apply(mnesia, Function, []) end,
                    trans_time_init(),
                    mnesia:transaction(F);
        true ->     mnesia:Function()
    end;
transaction(Fun) when is_function(Fun)->
    case mnesia:is_transaction() of
        false ->    trans_time_init(),
                    mnesia:transaction(Fun);
        true ->     Fun()
    end.

transaction(Function, Args) when is_atom(Function)->
    case mnesia:is_transaction() of
        false ->    F = fun() -> apply(mnesia, Function, Args) end,
                    trans_time_init(),
                    mnesia:transaction(F);
        true ->     apply(mnesia, Function, Args)
    end;
transaction(Fun, Args) when is_function(Fun)->
    case mnesia:is_transaction() of
        false ->    trans_time_init(),
                    mnesia:transaction(Fun, Args);
        true ->     apply(Fun, Args)        
    end.

transaction(Function, Args, Retries) when is_atom(Function)->
    case mnesia:is_transaction() of
        false ->    F = fun() -> apply(mnesia, Function, Args) end,
                    trans_time_init(),
                    mnesia:transaction(F, Retries);
        true ->     apply(mnesia, Function, Args)
    end;
transaction(Fun, Args, Retries) when is_function(Fun)->
    case mnesia:is_transaction() of
        false ->    trans_time_init(),
                    mnesia:transaction(Fun, Args, Retries);
        true ->     ?ClientErrorNoLogging({"Cannot specify retries in nested transaction"})
    end.

%% ---------- HELPER FUNCTIONS ------ exported -------------------------------

%% @doc Picks a set of chosen elements out of a list of tuples.
%% Integer pattern encodes element positions.
%% Valid single digit positions 1..9
%% use () for positions after 9
%% 12(12)3 -> 1, 2, 12, 3.
-spec field_pick(list(tuple()),list()) -> list(tuple()).
field_pick(ListOfRecords,Pattern) ->
    Pointers = field_pick_pointers(Pattern, []),
    [field_pick_mapped(Tup,Pointers) || Tup <- ListOfRecords].

field_pick_pointers([], Acc) -> lists:reverse(Acc); 
field_pick_pointers([$(,A,$)|Rest], Acc) ->
    field_pick_pointers(Rest, [(A-48)|Acc]);
field_pick_pointers([$(,A,B,$)|Rest], Acc) ->
    field_pick_pointers(Rest, [10*(A-48)+(B-48)|Acc]);
field_pick_pointers([$(,A,B,C,$)|Rest], Acc) ->
    field_pick_pointers(Rest, [100*(A-48)+10*(B-48)+(C-48)|Acc]);
field_pick_pointers([A|Rest], Acc) ->
    field_pick_pointers(Rest, [(A-48)|Acc]).

field_pick_mapped(Tup,Pointers) when is_tuple(Tup) ->
    EL = tuple_to_list(Tup),                %% tuple as list
    catch list_to_tuple([E || P <- Pointers, {I,E} <- lists:zip(lists:seq(1,length(EL)),EL),P==I]);    
field_pick_mapped(_,_) -> {}.

meta_field_value(<<"systimestamp">>) -> ?TIME_UID;
meta_field_value(systimestamp) -> ?TIME_UID;
meta_field_value(<<"user">>) -> <<"unknown">>;
meta_field_value(user) -> <<"unknown">>;
meta_field_value(<<"sysdate">>) -> calendar:local_time();
meta_field_value(sysdate) -> calendar:local_time();
meta_field_value(<<"username">>) -> <<"unknown">>;
meta_field_value(username) -> <<"unknown">>;
meta_field_value(<<"schema">>) -> schema();
meta_field_value(schema) -> schema();
meta_field_value(<<"node">>) -> node();
meta_field_value(node) -> node();
meta_field_value(Name) -> ?ClientErrorNoLogging({"Undefined meta value",Name}).

schema() ->
    %% schema identifier of local imem node
    [Schema|_]=re:split(filename:basename(mnesia:system_info(directory)),"[.]",[{return,list}]),
    list_to_atom(Schema).

schema(Node) ->
    %% schema identifier of remote imem node in the same erlang cluster
    case rpc:call(Node, mnesia, system_info, [directory], 1500) of
        {badrpc, _} = Error -> Error;
        MnesiaDirectory ->
            [Schema|_] = re:split(filename:basename(MnesiaDirectory), "[.]", [{return, list}]),
            list_to_atom(Schema)
    end.

add_attribute(A, Opts) -> update_opts({attributes,A}, Opts).

update_opts({K,_} = T, Opts) when is_atom(K) -> lists:keystore(K, 1, Opts, T).

-spec data_nodes() -> list({Schema::atom,Node::atom()}).
data_nodes() ->
    data_nodes(mnesia:system_info(running_db_nodes), []).
data_nodes([], Acc) -> lists:reverse(Acc);
data_nodes([Node|Nodes], Acc) ->
    case schema(Node) of
        {badrpc, _} -> data_nodes(Nodes, Acc);
        Schema ->
            data_nodes(Nodes, [{Schema, Node} | Acc])
    end.

all_tables() ->
    lists:delete(schema, mnesia:system_info(tables)).

is_readable_table(Table) ->
    try
        case mnesia:table_info(Table, where_to_read) of
            nowhere ->  false;
            _ ->        true
        end
    catch
        exit:{aborted,{no_exists,_,_}} ->  false
    end.  

table_type(Table) ->
    table_info(Table, type).

table_columns(Table) ->
    table_info(Table, attributes).

table_info(Table, InfoKey) ->
    Node = node(),
    try
        case mnesia:table_info(Table, where_to_read) of
            nowhere ->  
                ?ClientErrorNoLogging({"Table info cannot be read", {Table,InfoKey}});
            Node ->     
                mnesia:table_info(Table, InfoKey);
            Other ->    
                case rpc:call(Other,mnesia,table_info,[Table, InfoKey], ?TABLE_INFO_RPC_TIMEOUT) of
                    {badrpc,Reason} ->
                        ?ClientErrorNoLogging({"Table info is not accessible by rpc", Reason});
                    Result ->
                        Result    
                end
        end
    catch
        exit:{aborted,{no_exists,_,_}} ->  ?ClientErrorNoLogging({"Table does not exist", Table})
    end.  

table_record_name(Table) ->
    table_info(Table, record_name).
    
table_size(Table) ->
    table_info(Table, size).

table_memory(Table) ->
    table_info(Table, memory) * erlang:system_info(wordsize).  

check_table(Table) ->
    % return ok for readable table, throw exception otherwise 
    case is_readable_table(Table) of
        false ->  ?ClientErrorNoLogging({"This table is not readable", Table});
        true  ->  ok
    end.

check_local_table_copy(Table) ->
    try 
        case mnesia:table_info(Table, storage_type) of
            unknown -> ?ClientErrorNoLogging({"This table does not reside locally", Table});
            _ -> ok
        end
    catch
        exit:{aborted,{no_exists,_,_}} -> ?ClientErrorNoLogging({"Table does not exist", Table})
    end.

%% ---------- MNESIA FUNCTIONS ------ exported -------------------------------

-spec create_table(ddMnesiaTable(), ddColumnList(), ddOptions()) -> ok.
create_table(Table, ColumnNames, Opts) when is_atom(Table) ->
    Local = lists:member({scope,local}, Opts),
    Cluster = lists:member({scope,cluster}, Opts),
    if
        Local ->    create_local_table(Table, ColumnNames, Opts);
        Cluster ->  create_cluster_table(Table, ColumnNames, Opts);
        true ->     create_schema_table(Table, ColumnNames, Opts)
    end.

is_system_table(_) -> false.

-spec create_local_table(ddMnesiaTable(), ddColumnList(), ddOptions()) -> ok.
create_local_table(Table, ColumnNames, Opts) when is_atom(Table) ->
    Cols = [list_to_atom(lists:flatten(io_lib:format("~p", [X]))) || X <- ColumnNames],
    CompleteOpts = add_attribute(Cols, Opts) -- [{scope,local}],
    create_table(Table, CompleteOpts).

-spec create_schema_table(ddMnesiaTable(), ddColumnList(), ddOptions()) -> ok.
create_schema_table(Table, ColumnNames, Opts) when is_atom(Table) ->
    DiscNodes = mnesia:table_info(schema, disc_copies),
    RamNodes = mnesia:table_info(schema, ram_copies),
    CompleteOpts = [{ram_copies, RamNodes}, {disc_copies, DiscNodes}|Opts] -- [{scope,schema}],
    create_local_table(Table,ColumnNames,CompleteOpts).

-spec create_cluster_table(ddMnesiaTable(), ddColumnList(), ddOptions()) -> ok.
create_cluster_table(Table, ColumnNames, Opts) when is_atom(Table) ->
    DiscNodes = mnesia:table_info(schema, disc_copies),
    RamNodes = mnesia:table_info(schema, ram_copies),
    %% ToDo: may need to pull from another imem schema first and initiate sync
    CompleteOpts = [{ram_copies, RamNodes}, {disc_copies, DiscNodes}|Opts] -- [{scope,cluster}],
    create_local_table(Table,ColumnNames,CompleteOpts).

-spec create_table(ddMnesiaTable(), ddOptions()) -> ok.
create_table(Table, Opts) when is_atom(Table) ->
    % ?LogDebug("imem_if_mnesia create table ~p ~p",[Table,Opts]),
    {ok, Conf} = application:get_env(imem, mnesia_wait_table_config),
    case mnesia:create_table(Table, Opts) of
        {aborted, {already_exists, Table}} ->
            % ?Debug("table ~p locally exists~n", [Table]),
            mnesia:add_table_copy(Table, node(), ram_copies),
            yes = mnesia:force_load_table(Table),
            wait_table_tries([Table], Conf),
            ?ClientErrorNoLogging({"Table already exists", Table});
        {aborted, {already_exists, Table, _Node}} ->
            % ?Debug("table ~p exists at ~p~n", [Table, _Node]),
            case mnesia:force_load_table(Table) of
                yes -> ok;
                Error -> ?ClientErrorNoLogging({"Loading table(s) timeout~p", Error})
            end,
            ?ClientErrorNoLogging({"Table already exists", Table});
            %return_atomic_ok(mnesia:add_table_copy(Table, node(), ram_copies));
        Result ->
            % ?LogDebug("create_table ~p for ~p~n", [Result, Table]),
            wait_table_tries([Table], Conf),
            return_atomic_ok(Result)
    end.

-spec wait_table_tries([ddMnesiaTable()], {integer(), integer()}) -> ok.
wait_table_tries(Tables, {0, _}) ->
    ?ClientErrorNoLogging({"Loading table(s) timeout~p", Tables});
wait_table_tries(Tables, {Count,Timeout}) when is_list(Tables) ->
    case mnesia:wait_for_tables(Tables, Timeout) of
        ok ->                           ok;
        {timeout, _BadTabList} ->       ?Debug("table ~p load time out attempt ~p~n", [_BadTabList, Count]),
                                        wait_table_tries(Tables, {Count-1,Timeout});
        {error, Reason} ->              ?ClientErrorNoLogging({"Error loading table~p", Reason})
    end.

-spec drop_table(ddMnesiaTable()) -> ok.
drop_table(Table) when is_atom(Table) ->
    case imem:spawn_sync_mfa(mnesia, delete_table, [Table]) of
        ok ->                           
            true = ets:delete(?SNAP_ETS_TAB, Table),
            ok;
        {atomic,ok} ->                  
            true = ets:delete(?SNAP_ETS_TAB, Table),
            ok;
        {aborted,{no_exists, Table}} ->  
            ?ClientErrorNoLogging({"Table does not exist",Table});
        Error ->                        
            ?SystemExceptionNoLogging(Error)
    end.

-spec create_index(ddMnesiaTable(), ddColumnName()) -> ok.
create_index(Table, Column) when is_atom(Table) ->
    case mnesia:add_table_index(Table, Column) of
        {aborted, {no_exists, Table}} ->
            ?ClientErrorNoLogging({"Table does not exist", Table});
        {aborted, {already_exists, Table, _ }} ->
            ?ClientErrorNoLogging({"Index already exists", {Table,Column}});
        Result ->                       
            return_atomic_ok(Result)
    end.

create_or_replace_index(Table, Column) when is_atom(Table) ->
    case mnesia:add_table_index(Table, Column) of
        {aborted, {no_exists, Table}} ->   
            ?ClientErrorNoLogging({"Table does not exist", Table});
        {aborted, {already_exists, Table, _ }} ->   
            ok;
        Result ->
            return_atomic_ok(Result)
    end.

drop_index(Table, Column) when is_atom(Table) ->
    case mnesia:del_table_index(Table, Column) of
        {aborted, {no_exists, Table}} ->
            ?ClientErrorNoLogging({"Table does not exist", Table});
        {aborted, {no_exists, Table, _ }} ->   
            ?ClientErrorNoLogging({"Index does not exist", {Table,Column}});
        Result ->                       
            return_atomic_ok(Result)
    end.

truncate_table(Table) when is_atom(Table) ->
    case imem:spawn_sync_mfa(mnesia,clear_table,[Table]) of
        {atomic,ok} ->                  ?TOUCH_SNAP(Table);
        {aborted,{no_exists,Table}} ->  ?ClientErrorNoLogging({"Table does not exist",Table});
        Result ->                       return_atomic_ok(Result)
    end.

read(Table) when is_atom(Table) ->
    Trans = fun() ->
        Keys = mnesia:all_keys(Table),
        % [lists:nth(1, mnesia:read(Table, X)) || X <- Keys]
        lists:flatten([mnesia:read(Table, X) || X <- Keys])
    end,
    case transaction(Trans) of
        {aborted,{no_exists,_}} ->  ?ClientErrorNoLogging({"Table does not exist",Table});
        Result ->                   return_atomic_list(Result)
    end.

read(Table, Key) when is_atom(Table) ->
    case transaction(read,[Table, Key]) of
        {aborted,{no_exists,_}} ->  ?ClientErrorNoLogging({"Table does not exist",Table});
        Result ->                   return_atomic_list(Result)
    end.

dirty_read(Table) -> mnesia:dirty_select(Table, [{'_',[],['$_']}]).

dirty_read(Table, Key) when is_atom(Table) ->
    try
        mnesia:dirty_read(Table, Key)
    catch
        exit:{aborted, {no_exists,_}} ->    ?ClientErrorNoLogging({"Table does not exist",Table});
        exit:{aborted, {no_exists,_,_}} ->  ?ClientErrorNoLogging({"Table does not exist",Table});
        throw:Reason ->                     ?SystemExceptionNoLogging({"Mnesia dirty_read failure",Reason})
    end.

dirty_index_read(Table, SecKey, Index) when is_atom(Table) ->
    try
        mnesia:dirty_index_read(Table, SecKey, Index)
    catch
        exit:{aborted, {no_exists,_}} ->    ?ClientErrorNoLogging({"Table does not exist",Table});
        exit:{aborted, {no_exists,_,_}} ->  ?ClientErrorNoLogging({"Table does not exist",Table});
        exit:{badarg,[Table,SecKey,Index]} -> ?ClientErrorNoLogging({"Index does not exist",{Table,Index}});
        throw:Reason ->                     ?SystemExceptionNoLogging({"Mnesia dirty_index_read failure",Reason})
    end.

read_hlk(_, []) -> [];
read_hlk(Table, HListKey) when is_atom(Table), is_list(HListKey) ->
    % read using HierarchicalListKey
    try 
        case mnesia:dirty_read(Table, HListKey) of
            [] ->   read_hlk(Table, lists:sublist(HListKey, length(HListKey)-1));
            R ->    R
        end
    catch
        exit:{aborted, {no_exists,_}} ->    ?ClientErrorNoLogging({"Table does not exist",Table});
        exit:{aborted, {no_exists,_,_}} ->  ?ClientErrorNoLogging({"Table does not exist",Table});
        throw:Reason ->                     ?SystemExceptionNoLogging({"Mnesia dirty_index_read failure",Reason})
    end.

dirty_write(Table, Row) when is_atom(Table), is_tuple(Row) ->
    try mnesia:dirty_write(Table, Row) of
        ok -> ?TOUCH_SNAP(Table)
    catch
        exit:{aborted, {no_exists,_}} ->    ?ClientErrorNoLogging({"Table does not exist",Table});
        exit:{aborted, {no_exists,_,_}} ->  ?ClientErrorNoLogging({"Table does not exist",Table});
        throw:Reason ->                     ?SystemExceptionNoLogging({"Mnesia dirty_write failure",Reason})
    end.

write(Table, Row) when is_atom(Table), is_tuple(Row) ->
    %if Table =:= ddTable -> ?Debug("mnesia:write ~p ~p~n", [Table,Row]); true -> ok end,
    case transaction(write,[Table, Row, write]) of
        ok ->                               ?TOUCH_SNAP(Table);
        {atomic,ok} ->                      ?TOUCH_SNAP(Table);
        {aborted,{no_exists,_}} ->          ?ClientErrorNoLogging({"Table does not exist",Table});
        Result ->                           return_atomic_ok(Result)  
    end.

delete(Table, Key) when is_atom(Table) ->
    case transaction(delete,[{Table, Key}]) of
        ok ->                               ?TOUCH_SNAP(Table);
        {atomic,ok} ->                      ?TOUCH_SNAP(Table);
        {aborted,{no_exists,_}} ->          ?ClientErrorNoLogging({"Table does not exist",Table});
        Result ->                           return_atomic_ok(Result)
    end.

delete_object(Table, Row) when is_atom(Table) ->
    case transaction(delete_object,[Table, Row, write]) of
        ok ->                               ?TOUCH_SNAP(Table);
        {atomic,ok} ->                      ?TOUCH_SNAP(Table);
        {aborted,{no_exists,_}} ->          ?ClientErrorNoLogging({"Table does not exist",Table});
        Result ->                           return_atomic_ok(Result)
    end.

dirty_select(Table, MatchSpec) when is_atom(Table) ->
    mnesia:dirty_select(Table, MatchSpec).

dirty_delete(Table, Key) when is_atom(Table) ->
    mnesia:dirty_delete(Table, Key).

select(Table, MatchSpec) when is_atom(Table) ->
    case transaction(select,[Table, MatchSpec]) of
        {atomic, L}     ->                  {L, true};
        L when is_list(L)     ->            {L, true};
        {aborted,{no_exists,_}} ->          ?ClientErrorNoLogging({"Table does not exist",Table});
        Result ->                           return_atomic_list(Result)
    end.

select_sort(Table, MatchSpec) when is_atom(Table) ->
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
        {aborted,{no_exists,_}} ->              ?ClientErrorNoLogging({"Table does not exist",Table});
        {aborted,Error} ->                      ?SystemExceptionNoLogging(Error);
        {Result, AllRead} ->                    {Result, AllRead};
        Error ->                                ?SystemExceptionNoLogging(Error)
    end.

select_sort(Table, MatchSpec, Limit) when is_atom(Table) ->
    {Result, AllRead} = select(Table, MatchSpec, Limit),
    {lists:sort(Result), AllRead}.

fetch_start(Pid, Table, MatchSpec, BlockSize, Opts) when is_atom(Table) ->
    F =
    fun(F,Contd0) ->
        receive
            abort ->
                % ?Info("[~p] got abort on ~p~n", [Pid, Table]),
                ok;
            next ->
                case Contd0 of
                        undefined ->
                            % ?Info("[~p] got MatchSpec ~p for ~p limit ~p~n", [Pid,MatchSpec,Table,BlockSize]),
                            case mnesia:select(Table, MatchSpec, BlockSize, read) of
                                '$end_of_table' ->
                                    % ?Info("[~p] got empty table~n", [Pid]),
                                    Pid ! {row, [?sot,?eot]};
                                {aborted, Reason} ->
                                    exit(Reason);
                                {Rows, Contd1} ->
                                    % ?LogDebug("[~p] select got rows~n~p~n~p",[Pid,Rows,Contd1]),
                                    Eot = lists:member('$end_of_table', tuple_to_list(Contd1)),
                                    if  Eot ->
                                           Pid ! {row, [?sot,?eot|Rows]};
                                       true ->
                                            Pid ! {row, [?sot|Rows]},
                                            F(F,Contd1)
                                    end
                            end;
                        Contd0 ->
                            % ?Info("[~p] got continuing fetch...~n", [Pid]),
                            case mnesia:select(Contd0) of
                                '$end_of_table' ->
                                    % ?Info("[~p] complete after ~n",[Pid,Contd0]),
                                    Pid ! {row, ?eot};
                                {aborted, Reason} ->
                                    exit(Reason);
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
        [update_xt(Table, Item, Lock, Old, New, Trigger, User, TrOpts) || [Table, Item, Old, New, Trigger, User, TrOpts] <- UpdatePlan]
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
    transaction(fun() ->
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

write_table_property(Table, Prop)       -> mnesia:write_table_property(Table, Prop).
read_table_property(Table, PropName)    -> mnesia:read_table_property(Table, PropName).
delete_table_property(Table, PropName)  -> mnesia:delete_table_property(Table, PropName).

write_table_property_in_transaction(Table, Prop) -> 
    S = self(), 
    spawn(fun() -> S ! mnesia:write_table_property(Table,Prop) end), 
    receive R -> R end.

update_xt({_Table,bag}, _Item, _Lock, {}, {}, _, _, _) ->
    ok;
update_xt({Table,bag}, Item, Lock, Old, {}, Trigger, User, TrOpts) when is_atom(Table) ->
    Current = mnesia:read(Table, element(?KeyIdx,Old)),
    Exists = lists:member(Old,Current),
    if
        Exists ->
            mnesia_atomic(delete_object, [Table, Old, write]),
            Trigger(Old, {}, Table, User, TrOpts),
            {Item,{}};
        Lock == none ->
            {Item,{}};
        true ->
            ?ConcurrencyExceptionNoLogging({"Data is modified by someone else", {Item, Old}})
    end;
update_xt({Table,bag}, Item, Lock, {}, New, Trigger, User, TrOpts) when is_atom(Table) ->
    Current = mnesia:read(Table, element(?KeyIdx,New)),  %% may be expensive
    Exists = lists:member(New,Current),
    if
        (Exists and (Lock==none)) ->
            {Item,New};
        Exists ->
            ?ConcurrencyExceptionNoLogging({"Record already exists", {Item, New}});
        true ->
            mnesia_atomic(write, [Table, New, write]),
            Trigger({}, New, Table, User, TrOpts),
            {Item,New}
    end;
update_xt({Table,bag}, Item, Lock, Old, Old, Trigger, User, TrOpts) when is_atom(Table) ->
    Current = mnesia:read(Table, element(?KeyIdx,Old)),  %% may be expensive
    Exists = lists:member(Old,Current),
    if
        Exists ->
            Trigger(Old, Old, Table, User, TrOpts),
            {Item,Old};
        Lock == none ->
            mnesia_atomic(write, [Table, Old, write]),
            Trigger(?NoRec, Old, Table, User, TrOpts),
            {Item,Old};
        true ->
            ?ConcurrencyExceptionNoLogging({"Data is modified by someone else", {Item, Old}})
    end;
update_xt({Table,bag}, Item, Lock, Old, New, Trigger, User, TrOpts) when is_atom(Table) ->
    update_xt({Table,bag}, Item, Lock, Old, ?NoRec, Trigger, User, TrOpts),
    update_xt({Table,bag}, Item, Lock, ?NoRec, New, Trigger, User, TrOpts);

update_xt({_Table,_}, _Item, _Lock, ?NoRec, ?NoRec, _, _, _) ->
    ok;
update_xt({Table,_}, Item, Lock, Old, ?NoRec, Trigger, User, TrOpts) when is_atom(Table), is_tuple(Old) ->
    case mnesia:read(Table, element(?KeyIdx,Old)) of
        [Old] ->    
            mnesia_atomic(delete, [Table, element(?KeyIdx, Old), write]),
            Trigger(Old, ?NoRec, Table, User, TrOpts),
            {Item,?NoRec};
        [] ->       
            case Lock of
                none -> {Item,?NoRec};
                _ ->    ?ConcurrencyExceptionNoLogging({"Missing key", {Item,Old}})
            end;
        [Current] ->  
            case Lock of
                none -> mnesia_atomic(delete, [Table, element(?KeyIdx, Old), write]),
                        Trigger(Current, ?NoRec, Table, User, TrOpts),
                        {Item,?NoRec};
                _ ->    ?ConcurrencyExceptionNoLogging({"Key violation", {Item,{Old,Current}}})
            end
    end;
update_xt({Table,_}, Item, Lock, ?NoRec, New, Trigger, User, TrOpts) when is_atom(Table), is_tuple(New) ->
    case mnesia:read(Table, element(?KeyIdx,New)) of
        [] ->       mnesia_atomic(write, [Table, New, write]),
                    Trigger(?NoRec, New, Table, User, TrOpts),
                    {Item,New};
        [New] ->    Trigger(New, New, Table, User, TrOpts),
                    {Item,New};
        [Current] ->  
            case Lock of
                none -> mnesia_atomic(write, [Table, New, write]),
                        Trigger(Current, New, Table, User, TrOpts),
                        {Item,New};
                _ ->    ?ConcurrencyExceptionNoLogging({"Key already exists", {Item,Current}})
            end
    end;
update_xt({Table,_}, Item, Lock, Old, Old, Trigger, User, TrOpts) when is_atom(Table), is_tuple(Old) ->
    case mnesia:read(Table, element(?KeyIdx,Old)) of
        [Old] ->    Trigger(Old, Old, Table, User, TrOpts),
                    {Item,Old};
        [] ->       
            case Lock of
                none -> mnesia_atomic(write, [Table, Old, write]),
                        Trigger(?NoRec, Old, Table, User, TrOpts),
                        {Item,Old};
                _ ->    ?ConcurrencyExceptionNoLogging({"Data is deleted by someone else", {Item, Old}})
            end;
        [Current] ->  
            case Lock of
                none -> mnesia_atomic(write, [Table, Old, write]),
                        Trigger(Current, Old, Table, User, TrOpts),
                        {Item,Old};
                _ ->    ?ConcurrencyExceptionNoLogging({"Data is modified by someone else", {Item,{Old, Current}}})
            end
    end;
update_xt({Table,_}, Item, Lock, Old, New, Trigger, User, TrOpts) when is_atom(Table), is_tuple(Old), is_tuple(New) ->
    OldKey=element(?KeyIdx,Old),
    NewKey = element(?KeyIdx,New),
    case {mnesia:read(Table, OldKey),(OldKey==NewKey)} of
        {[Old],true} ->    
            mnesia_atomic(write, [Table, New, write]),
            Trigger(Old, New, Table, User, TrOpts),
            {Item,New};
        {[Old],false} ->
            mnesia_atomic(delete, [Table, OldKey, write]),    
            mnesia_atomic(write, [Table, New, write]),
            Trigger(Old, New, Table, User, TrOpts),
            {Item,New};
        {[],_} ->       
            case Lock of
                none -> mnesia_atomic(write, [Table, New, write]),
                        Trigger(?NoRec, New, Table, User, TrOpts),
                        {Item,New};
                _ ->    ?ConcurrencyExceptionNoLogging({"Data is deleted by someone else", {Item, Old}})
            end;
        {[Current],true} ->  
            case Lock of
                none -> mnesia_atomic(write, [Table, New, write]),
                        Trigger(Current, New, Table, User, TrOpts),
                        {Item,New};
                _ ->    ?ConcurrencyExceptionNoLogging({"Data is modified by someone else", {Item,{Old, Current}}})
            end;
        {[Current],false} ->  
            case Lock of
                none -> mnesia_atomic(delete, [Table, OldKey, write]),
                        mnesia_atomic(write, [Table, New, write]),
                        Trigger(Current, New, Table, User, TrOpts),
                        {Item,New};
                _ ->    ?ConcurrencyExceptionNoLogging({"Data is modified by someone else", {Item,{Old, Current}}})
            end
    end.

subscribe({table, ddTrace, _}) -> imem_tracer:subscribe();
subscribe({table, Tab, simple}) ->
    {ok,_} = mnesia:subscribe({table, Tab, simple}),
    ok;
subscribe({table, Tab, detailed}) ->
    {ok,_} = mnesia:subscribe({table, Tab, detailed}),
    ok;
subscribe({table, schema}) ->
    {ok,_} = mnesia:subscribe({table,schema}),
    ok;
subscribe(system) ->
    {ok,_} = mnesia:subscribe(system),
    ok;
subscribe(EventCategory) ->
    ?ClientErrorNoLogging({"Unsupported event category subscription", EventCategory}).

unsubscribe({table, ddTrace, _})    -> imem_tracer:unsubscribe();
unsubscribe({table, Tab, simple})   -> mnesia:unsubscribe({table, Tab, simple});
unsubscribe({table, Tab, detailed}) -> mnesia:unsubscribe({table, Tab, detailed});
unsubscribe({table,schema})         -> mnesia:unsubscribe({table, schema});
unsubscribe(system)                 -> mnesia:unsubscribe(system);
unsubscribe(EventCategory) ->
    ?ClientErrorNoLogging({"Unsupported event category unsubscription", EventCategory}).

first(Table) ->             mnesia:first(Table).

dirty_first(Table) ->       mnesia:dirty_first(Table).

next(Table,Key) ->          mnesia:next(Table,Key).

dirty_next(Table,Key) ->    mnesia:dirty_next(Table,Key).

last(Table) ->              mnesia:last(Table).

dirty_last(Table) ->        mnesia:dirty_last(Table).

prev(Table,Key) ->          mnesia:prev(Table,Key).

dirty_prev(Table,Key) ->    mnesia:dirty_prev(Table,Key).

foldl(FoldFun, InputAcc, Table) ->
    return_atomic(transaction(fun mnesia:foldl/3, [FoldFun, InputAcc, Table])).

lock(LockItem, LockKind) -> mnesia:lock(LockItem, LockKind).

%% ----- gen_server -------------------------------------------

start_link(Params) ->
    ?Info("~p starting...~n", [?MODULE]),
    ets:new(?SNAP_ETS_TAB, [public, named_table, {keypos,2}]),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]) of
        {ok, _} = Success ->
            ?Info("~p started!~n", [?MODULE]),
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [?MODULE, Error]),
            Error
    end.

init(_) ->
    {ok, SchemaName} = application:get_env(mnesia_schema_name),
    {ok, ClusterManagers} = application:get_env(erl_cluster_mgrs),
    case disc_schema_nodes(SchemaName) of
        [] ->   
            case node() of
                nonode@nohost ->    
                    ok;
                _ ->                
                    ?Warn ("no node found at ~p for schema ~p cluster ~p~n",
                           [node(), SchemaName, erlang:get_cookie()]),
                    {ok, _} = mnesia:change_config(
                                extra_db_nodes, [node() | ClusterManagers])
            end;
        [DiscSchemaNode|_] ->
            ?Info("adding ~p to schema ~p on ~p~n",
                  [node(), SchemaName, DiscSchemaNode]),
            MnesiaNodes = [node() | ClusterManagers],
            {ok, _} = mnesia:change_config(extra_db_nodes, MnesiaNodes),
            {ok, _} = rpc:call(DiscSchemaNode, mnesia, change_config,
                               [extra_db_nodes, MnesiaNodes])
    end,
    {ok, NodeType} = application:get_env(mnesia_node_type),
    ?Info("mnesia node type is '~p'~n", [NodeType]),
    case NodeType of
        disc -> mnesia:change_table_copy_type(schema, node(), disc_copies);
        _ -> ok
    end,
    mnesia:subscribe(system),

    % For application gracefull shutdown cleanup
    process_flag(trap_exit, true),

    % Start periodic EPMD check
    case is_pid(whereis(imem_inet_tcp_dist)) of
        true -> self() ! check_epmd;
        _ ->
            ?Warn("imem_inet_tcp_dist not started, possibly missing -proto_dist"
                  " erlang VM start parameter")
    end,

    {ok,#state{}}.

handle_call(_Request, _From, State) ->
    ?Info("Unknown request ~p from ~p!", [_Request, _From]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    ?Info("Unknown cast ~p!", [_Request]),
    {noreply, State}.

handle_info(check_epmd, State) ->
    case epmd_register() of
        {error, {already_registered, _OldPort}} ->
            ?Debug("EPMD has registration on ~p", [_OldPort]);
        ok -> ?Info("Registration entered in EPMD");
        {error, Reason} -> ?Warn("EPMD registration error ~p", [Reason])
    end,
    erlang:send_after(30000, self(), check_epmd),
    {noreply, State};
handle_info(Info, State) ->
    case Info of
        {mnesia_system_event,{mnesia_overload,_Details}} ->
            % ?Warn("Mnesia overload : ~p!",[_Details]),
            {noreply, State};
        {mnesia_system_event,{mnesia_down,Node}} ->
            case node() of 
                Node -> 
                    ?Error("Mnesia node down ~p!",[Node]),
                    mnesia_down_stop_if_not_testing(State);
                _ -> 
                    ?Info("Mnesia node down ~p!", [Node]),
                    {noreply, State}
            end;
        {mnesia_system_event,{_Event,_Node}} ->
            ?Info("Mnesia event ~p from Node ~p!",[_Event, _Node]),
            {noreply, State};
        Error ->
            ?Error("Mnesia error : ~p",[Error]),
            {noreply, State}
    end.

-ifdef(TEST).
mnesia_down_stop_if_not_testing(State) ->
    {noreply, State}.
-else.
mnesia_down_stop_if_not_testing(State) ->
    ?Error("Mnesia down!"),
    {stop, mnesia_down, State}.
-endif.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, _Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

% @doc reregisters an disconnected node back to epmd. Requires the node to be
% initially started with '-proto_dist imem_inet_tcp' as command line option and
% there is a running instance of epmd process in the local system. If
% unsuccessful {error, Reason} is returned. This function never generates any
% exception.
-spec epmd_register() -> ok | {error, Reason :: any()}.
epmd_register() ->
    try
        case erl_epmd:names() of
            {error, address} ->
                spawn(
                  fun() ->
                          open_port({spawn_executable,
                                     os:find_executable("epmd")},
                                    [{args, ["-daemon"]}]),
                          ?Info("Started epmd, retrying after 1s"),
                          timer:sleep(1000),
                          epmd_register()
                  end),
                {error, not_running};
            {ok, RegisteredNodes} ->
                {Node,{_Ip,Port},_Host} = imem_inet_tcp_dist:reg_info(),
                NodeName = atom_to_list(Node),
                case proplists:get_value(NodeName, RegisteredNodes) of
                    undefined ->
                        {ok, _} = erl_epmd:register_node(Node, Port),
                        ?Info("Registered in EPMD on port ~p", [Port]),
                        ok;
                    OldPort ->
                        {error, {already_registered, OldPort}}
                end
        end
    catch
        _:Reason -> {error, Reason}
    end.

%% ----- Private functions ------------------------------------
mnesia_atomic(Fun, [_|_] = Args)
  when Fun == write; Fun == delete; Fun == delete_object ->
    case apply(mnesia, Fun, Args) of
        ok -> ?TOUCH_SNAP(hd(Args)),
              ok;
        Error -> Error
    end.

ets(Fun, Args) ->
    mnesia:ets(Fun, Args).

select_count(Tab, MatchSpec) ->
    ets(fun ets:select_count/2, [Tab, MatchSpec]).
