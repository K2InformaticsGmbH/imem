%% -*- coding: utf-8 -*-
-module(imem_meta).

%% @doc == imem metadata and table management ==
%% Naming conventions for sharded/partitioned tables
%% Table creation / Index table creation
%% Triggers and validator funs
%% Virtual tables  


-include("imem.hrl").
-include("imem_meta.hrl").
-include("imem_if_csv.hrl").

-include_lib("kernel/include/file.hrl").

%% HARD CODED CONFIGURATIONS

-define(DDNODE_TIMEOUT, 3000).       % RPC timeout for ddNode evaluation

-define(META_TABLES, [?CACHE_TABLE, ?LOG_TABLE, ?MONITOR_TABLE, ?CONFIG_TABLE, dual, ddNode, ddSnap, ddSchema, ddSize, ddAlias, ddTable]).
-define(META_FIELDS, [?META_ROWNUM, ?META_NODE, <<"systimestamp">>,<<"user">>,<<"username">>,<<"sysdate">>,<<"schema">>]). 
-define(META_OPTS, [purge_delay, trigger]). % table options only used in imem_meta and above
-define(VIRTUAL_TABLE_ROW_LIMIT,?GET_CONFIG(virtualTableRowLimit, [] , 1000, "Maximum number of rows which can be generated in first (and only) result block")).

-define(LOG_TABLE_OPTS,     [{record_name,ddLog}
                            ,{type,ordered_set}
                            ,{purge_delay,430000}        %% 430000 = 5 Days - 2000 sec
                            ]).          

-define(DD_TRIGGER_ARITY,   5).
-define(ddTableTrigger,     <<"fun(__OR,__NR,__T,__U,__O) -> imem_meta:dictionary_trigger(__OR,__NR,__T,__U,__O) end.">> ).
-define(DD_ALIAS_OPTS,      [{trigger,?ddTableTrigger}]).          
-define(DD_TABLE_OPTS,      [{trigger,?ddTableTrigger}]).
-define(DD_CACHE_OPTS,      [{scope,local}
                            ,{local_content,true}
                            ,{record_name,ddCache}
                            ]).          
-define(DD_INDEX_OPTS,      [{record_name,ddIndex}
                            ,{type,ordered_set}         %% ,{purge_delay,430000}  %% inherit from parent table
                            ]).          

-define(INDEX_TABLE(__MasterTableName), __MasterTableName++"Idx").    % ToDo: Implement also for sharded and partitioned tables 

-define(BAD_NAME_CHARACTERS,"!?#*:+-.\\<|>/").  %% invalid chars for tables and columns

% -define(RecIdx, 1).       %% Record name position in records
-define(FirstIdx, 2).       %% First field position in records (matching to hd(ColumnInfos))
-define(KeyIdx, 2).         %% Key position in records

-define(EmptyJPStr, <<>>).  %% placeholder for JSON path pointing to the whole document (record position)    
-define(EmptyJP, {}).       %% placeholder for compiled JSON path pointing to the whole document (record position)    

-define(PartEndDigits,10).  % Number of digits representing the partition end (seconds since epoch) in partition names

-define(GET_SNAP_NAME_TRANS(__TABLE),
        ?GET_CONFIG(snapNameTrans,[__TABLE], <<"fun(N) -> atom_to_list(N) end">> , "Name translation for snapshot files, '.bkp' added")
       ).
-define(GET_SNAP_PROP_TRANS(__TABLE),
        ?GET_CONFIG(snapPropTrans,[__TABLE], <<"fun(P) -> P end">> , "Table property translation for snapshot files")
       ).
-define(GET_SNAP_ROW_TRANS(__TABLE),
        ?GET_CONFIG(snapRowTrans,[__TABLE], <<"fun(R) -> R end">> , "Row translation for snapshot files")
       ).

%% DEFAULT CONFIGURATIONS ( overridden in table ddConfig)

-behavior(gen_server).

-record(state, {}).

-export([ start_link/1
        ]).

% gen_server behavior callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        ]).


-export([ drop_meta_tables/0
        , drop_system_table/1
        , drop_system_table/2
        , fail/1
        , get_tables_count/0
        , sql_jp_bind/1
        , sql_bind_jp_values/2
        ]).

-export([ schema/0
        , schema/1
        , data_nodes/0
        , host_fqdn/1
        , host_name/1
        , node_name/1
        , node_hash/1
        , clean_host_name/1
        , record_hash/2
        , nodes/0
        , integer_uid/0
        , integer_uid/1
        , time_uid/0
        , time_uid/1
        , time/0
        , time/1
        , seconds_since_epoch/1
        , all_aliases/0
        , all_tables/0
        , tables_starting_with/1
        , tables_ending_with/1
        , node_shard/0
        , node_shards/0
        , qualified_table_name/1
        , qualified_table_names/1
        , qualified_new_table_name/1
        , qualified_alias/1
        , physical_table_name/1
        , physical_table_name/2
        , physical_table_names/1
        , cluster_table_names/1
        , partitioned_table_name_str/2
        , partitioned_table_name/2
        , parse_table_name/1
        , index_table/1
        , index_table_name/1
        , is_system_table/1
        , is_readable_table/1
        , is_virtual_table/1
        , is_time_partitioned_alias/1
        , is_time_partitioned_table/1
        , is_local_time_partitioned_table/1
        , is_schema_time_partitioned_table/1
        , is_local_or_schema_time_partitioned_table/1
        , is_local_alias/1
        , is_node_sharded_alias/1
        , is_local_node_sharded_table/1
        , time_of_partition_expiry/1
        , time_to_partition_expiry/1
        , table_type/1
        , table_columns/1
        , table_size/1
        , table_memory/1
        , table_record_name/1
        , table_opts/1
        , table_opts/2        
        , table_alias_opts/1
        , table_alias_opts/2        
        , trigger_infos/1
        , dictionary_trigger/5
        , check_table/1
        , check_local_table_copy/1
        , meta_field_list/0        
        , meta_field/1
        , meta_field_info/1
        , meta_field_value/1
        , column_infos/1
        , from_column_infos/1
        ]).

-export([ log_to_db/5
        , log_to_db/6
        , log_to_db/7
        , log_slow_process/6
        , failing_function/1
        , get_config_hlk/6
        , put_config_hlk/6
        , put_config_hlk/7
        , admin_exec/3
        ]).

-export([ init_create_table/3
        , init_create_table/4
        , init_create_check_table/3
        , init_create_check_table/4
        , init_create_trigger/2
        , init_create_or_replace_trigger/2
        , init_create_index/2
        , init_create_or_replace_index/2
        , create_table/3
        , create_table/4
        , create_partitioned_table/2
        , create_partitioned_table_sync/2
        , create_check_table/3
        , create_check_table/4
        , create_trigger/2
        , get_trigger/1
        , create_or_replace_trigger/2
        , create_index/2
        , create_or_replace_index/2
        , create_sys_conf/1
        , drop_table/1
        , drop_table/2
        , drop_trigger/1
        , drop_index/1
        , drop_index/2
        , purge_table/1
        , purge_table/2
        , truncate_table/1
        , truncate_table/2
        , snapshot_table/1      %% dump local table to snapshot directory
        , restore_table/1       %% replace local table by version in snapshot directory
        , restore_table_as/2    %% replace/create local table by version in snapshot directory
        , read/1                %% read whole table, only use for small tables 
        , read/2                %% read by key
        , read_hlk/2            %% read using hierarchical list key
        , select/2              %% select without limit, only use for small result sets
        , dirty_select/2        %% select dirty without any trynsaction locks
        , select_virtual/3      %% select virtual table without limit, only use for small result sets
        , select/3              %% select with limit
        , select_sort/2
        , select_sort/3
        , ets/2
        , select_count/2
        , modify/8              %% parameterized insert/update/merge/remove
        , modify/9              %% parameterized insert/update/merge/remove with trigger options
        , insert/2              %% apply defaults, write row if key does not exist, apply trigger
        , insert/3              %% apply defaults, write row if key does not exist, apply trigger
        , insert/4              %% apply defaults, write row if key does not exist, apply trigger with trigger options
        , update/2              %% apply defaults, write row if key exists, apply trigger (bags not supported)
        , update/3              %% apply defaults, write row if key exists, apply trigger (bags not supported)
        , update/4              %% apply defaults, write row if key exists, apply trigger with trigger options (bags not supported) 
        , merge/2               %% apply defaults, write row, apply trigger (bags not supported)
        , merge/3               %% apply defaults, write row, apply trigger (bags not supported)
        , merge/4               %% apply defaults, write row, apply trigger (bags not supported) with trigger options
        , remove/2              %% delete row if key exists (if bag row exists), apply trigger
        , remove/3              %% delete row if key exists (if bag row exists), apply trigger
        , remove/4              %% delete row if key exists (if bag row exists), apply trigger with trigger options
        , write/2               %% write row for single key, no defaults applied, no trigger applied
        , write_log/1
        , dirty_read/1
        , dirty_read/2
        , dirty_index_read/3    
        , dirty_write/2
        , delete/2              %% delete row by key
        , dirty_delete/2        %% delete row by key
        , delete_object/2       %% delete single row in bag table 
        , abort/1               %% abort transaction with a message
        ]).

-export([ update_prepare/3          %% stateless creation of update plan from change list
        , update_cursor_prepare/2   %% take change list and generate update plan (stored in state)
        , update_cursor_execute/2   %% take update plan from state and execute it (fetch aborted first)
        , apply_defaults/2          %% apply arity/0 funs of default record to ?nav values of current record
        , apply_validators/3        %% apply any arity funs of default record to current record
        , apply_validators/4        %% apply any arity funs of default record to current record
        , fetch_recs/3
        , fetch_recs_sort/3 
        , fetch_recs_async/2        
        , fetch_recs_async/3 
        , filter_and_sort/3       
        , filter_and_sort/4       
        , fetch_close/1
        , exec/3
        , close/1
        ]).

-export([ fetch_start/5
        , fetch_start_virtual/6
        , update_tables/2
        , update_index/6            %% (Old,New,Tab,User,TrOpts,IdxDef)   
        , fill_index/2              %% (Table,IndexDefinition)
        , update_bound_counter/6
        , subscribe/1
        , unsubscribe/1
        ]).

-export([ transaction/1
        , transaction/2
        , transaction/3
        , return_atomic_list/1
        , return_atomic_ok/1
        , return_atomic/1
        , lock/2
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

-export([ merge_diff/3      %% merge two data tables into a bigger one, presenting the differences side by side
        , merge_diff/4      %% merge two data tables into a bigger one, presenting the differences side by side
        , merge_diff/5      %% merge two data tables into a bigger one, presenting the differences side by side
        , term_diff/6       %% take (LeftType, LeftData, RightType, RightData, Opts, User) and produce data for a side-by-side view 
        ]).

% Functions applied with Common Test
-export([ check_table_meta/2
        ]).

-export([simple_or_local_node_sharded_tables/1]).

-safe([log_to_db,update_index,dictionary_trigger,data_nodes,schema/0,node_shard,
       physical_table_name,get_tables_count,record_hash,integer_uid,time_uid,
       merge_diff,term_diff]).

start_link(Params) ->
    ?Info("~p starting...~n", [?MODULE]),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]) of
        {ok, _} = Success ->
            ?Info("~p started!~n", [?MODULE]),
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [?MODULE, Error]),
            Error
    end.

-spec init_create_table(ddTable(), ddTableMeta(), ddOptions()) -> ok | ddError().
% Checks if a matching table and current partition already exists.
% Returns an error tuple if so.
% Create table/partition if it does not exist.
% Returns current physical table name.
init_create_table(TableName, RecDef, Opts) ->
    init_create_table(TableName, RecDef, Opts, #ddTable{}#ddTable.owner).

-spec init_create_table(ddTable(), ddTableMeta(), ddOptions(), ddEntityId()) -> ok | ddError().
init_create_table(TableName, RecDef, Opts, Owner) ->
    case (catch create_table(TableName, RecDef, Opts, Owner)) of
        {ok, PTN} -> 
            ?Info("creating ~p results in ~p", [PTN, ok]),
            ok;
        {'ClientError',{"Table already exists", _}} = R ->   
            ?Info("creating ~p results in ~p", [TableName, "Table already exists"]),
            R;
        {'ClientError',{"Table does not exist",ddTable}} = R ->   
            ?Info("creating ~p results in ~p", [TableName, "Created without dictionary"]),
            R;
        {'ClientError',{"Table does not exist",_Table}} = R ->   
            ?Info("creating ~p results in \"Table ~p does not exist\"", [TableName, _Table]),
            R;
        {'ClientError',_Reason}=R ->   
            ?Info("creating ~p results in ~p", [TableName, _Reason]),
            R;
        R ->                   
            ?Info("creating ~p results in ~p", [TableName, R]),
            R
    end.

-spec init_create_check_table(ddTable(), ddTableMeta(), ddOptions()) -> ok | ddError().
init_create_check_table(TableName, RecDef, Opts) ->
    init_create_check_table(TableName, RecDef, Opts, #ddTable{}#ddTable.owner).

-spec init_create_check_table(ddTable(), ddTableMeta(), ddOptions(), ddEntityId()) -> ok | ddError().
init_create_check_table(TableName, RecDef, Opts, Owner) ->
    case (catch create_check_table(TableName, RecDef, Opts, Owner)) of
        {ok, PartitionName} ->   
            ?Info("table ~p created", [PartitionName]),
            ok;
        {'ClientError',{"Table already exists", _}} ->   
            ?Info("table ~p already exists", [TableName]),
            ok;
        {'ClientError',_Reason}=Res ->   
            ?Warn("creating ~p results in ~p", [TableName,_Reason]),
            Res;
        Result ->                   
            ?Warn("creating ~p results in ~p", [TableName,Result]),
            Result
    end.

-spec init_create_trigger(ddTable(), ddString()) -> ok | ddError().
init_create_trigger(TableName, TriggerStr) ->
    case (catch create_trigger(TableName, TriggerStr)) of
        ok ->   
            ?Info("trigger for ~p created", [TableName]),
            ok;
        {'ClientError',{"Trigger already exists",{_Table,_}}} = Res -> 
            ?Warn("creating trigger for ~p results in ~p", [_Table,"Trigger exists in different version"]),
            Res;
        {'ClientError',{"Trigger already exists", _Table}} ->   
            ?Info("creating trigger for ~p results in ~p", [_Table,"Trigger already exists"]),
            ok;
        Result ->                   
            ?Warn("creating trigger for ~p results in ~p", [TableName,Result]),
            Result
    end.


init_create_or_replace_trigger(TableName,TriggerStr) ->
    case (catch create_or_replace_trigger(TableName,TriggerStr)) of
        Result ->                   
            ?Info("creating trigger for ~p results in ~p", [TableName,Result]),
            Result
    end.

init_create_index(TableName,IndexDefinition) when is_list(IndexDefinition) ->
    case (catch create_index(TableName,IndexDefinition)) of
        ok ->
            ?Info("index for ~p created", [TableName]),
            ok;
        {'ClientError',{"Index already exists",{_Table,_}}} = Res ->
            ?Warn("index for ~p results in different version", [_Table]),
            Res;
        {'ClientError',{"Index already exists", _Table}} ->   
            ?Info("index for ~p already exists", [_Table]),
            ok;
        Result ->                   
            ?Warn("creating index for ~p results in ~p", [TableName,Result]),
            Result
    end.

init_create_or_replace_index(TableName,IndexDefinition) when is_list(IndexDefinition) ->
    case (catch create_or_replace_index(TableName,IndexDefinition)) of
        Result ->                   
            ?Info("creating index for ~p results in ~p", [TableName,Result]),
            Result
    end.

init(_Args) ->
    Result = try
        application:set_env(imem, node_shard, node_shard()), % read node_shard config, evaluate and write back (cache)
        init_create_table(ddAlias, {record_info(fields, ddAlias), ?ddAlias, #ddAlias{}}, ?DD_ALIAS_OPTS, system),         %% may not be able to register in ddTable
        init_create_table(?CACHE_TABLE, {record_info(fields, ddCache), ?ddCache, #ddCache{}}, ?DD_CACHE_OPTS, system),  %% may not be able to register in ddTable
        init_create_table(ddTable, {record_info(fields, ddTable), ?ddTable, #ddTable{}}, ?DD_TABLE_OPTS, system),
        init_create_table(?CACHE_TABLE, {record_info(fields, ddCache), ?ddCache, #ddCache{}}, ?DD_CACHE_OPTS, system),  %% register in ddTable if not done yet
        init_create_table(ddAlias, {record_info(fields, ddAlias), ?ddAlias, #ddAlias{}}, ?DD_ALIAS_OPTS, system),         %% register in ddTable if not done yet 
        catch check_table(ddTable),
        catch check_table_meta(ddTable, {record_info(fields, ddTable), ?ddTable, #ddTable{}}),

        init_create_check_table(ddNode, {record_info(fields, ddNode), ?ddNode, #ddNode{}}, [], system),    
        init_create_check_table(ddSnap, {record_info(fields, ddSnap), ?ddSnap, #ddSnap{}}, [], system),
        init_create_check_table(ddSchema, {record_info(fields, ddSchema), ?ddSchema, #ddSchema{}}, [], system),    
        init_create_check_table(ddSize, {record_info(fields, ddSize), ?ddSize, #ddSize{}}, [], system),
        init_create_check_table(?LOG_TABLE, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system),
        init_create_check_table(ddVersion, {record_info(fields, ddVersion), ?ddVersion, #ddVersion{}}, [], system),

        imem_tracer:init(),
        init_create_table(dual, {record_info(fields, dual), ?dual, #dual{}}, [], system),
        write(dual,#dual{}),

        init_create_trigger(ddTable, ?ddTableTrigger),

        process_flag(trap_exit, true),
        {ok,#state{}}
    catch
        _Class:Reason -> {stop, {Reason,erlang:get_stacktrace()}} 
    end,
    Result.

-spec create_partitioned_table_sync(ddMnesiaTable(), ddMnesiaTable()) -> ok | ddError().
create_partitioned_table_sync(TableAlias, TableName) when is_atom(TableAlias), is_atom(TableName) ->
    ImemMetaPid = erlang:whereis(?MODULE),
    case self() of
        ImemMetaPid ->
            {error, recursive_call};   %% cannot call myself
        _ ->
            gen_server:call(?MODULE, {create_partitioned_table, TableAlias, TableName},35000)
    end. 

-spec create_partitioned_table(ddMnesiaTable(), ddMnesiaTable()) -> ok | ddError().
create_partitioned_table(TableAlias, TableName) when is_atom(TableName) ->
    try 
        case imem_if_mnesia:read(ddTable,{schema(), TableName}) of
            [#ddTable{}] ->
                % Table seems to exist, may need to load it
                case catch(check_table(TableName)) of
                    ok ->   ok;
                    {'ClientError',{"Table does not exist",TableName}} ->
                        create_nonexisting_partitioned_table(TableAlias, TableName);
                    _Res ->
                        ?Info("Waiting for partitioned table ~p needed because of ~p", [TableName, _Res]),
                        case mnesia:wait_for_tables([TableName], 30000) of
                            ok ->   ok;   
                            Error ->            
                                ?Error("Waiting for partitioned table failed with ~p", [Error]),
                                {error, Error}
                        end
                end;
            [] ->
                % Table does not exist in ddTable, must create it similar to existing
                create_nonexisting_partitioned_table(TableAlias, TableName)   
        end
    catch
        _:Reason1 ->
            ?Error("Create partitioned table failed with ~p", [Reason1]),
            {error, Reason1}
    end.

-spec create_nonexisting_partitioned_table(ddMnesiaTable(), ddMnesiaTable()) -> ok | ddError().
create_nonexisting_partitioned_table(TableAlias, TableName) ->
    % find out ColumnsInfos, Opts, Owner from ddAlias
    case imem_if_mnesia:read(ddAlias,{schema(), TableAlias}) of
        [] ->
            ?Error("Table template not found in ddAlias ~p", [TableAlias]),   
            {error, {"Table template not found in ddAlias", TableAlias}}; 
        [#ddAlias{columns=ColumnInfos, opts=Opts, owner=Owner}] ->
            try
                {ok, _} = create_table(TableName, ColumnInfos, Opts, Owner),
                ok
            catch
                _:Reason2 -> {error, Reason2}
            end
    end.

handle_call({create_partitioned_table, TableAlias, TableName}, _From, State) ->
    {reply, create_partitioned_table(TableAlias,TableName), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, _Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

-spec fail(any()) -> no_return().
fail(Reason) ->    throw(Reason).

-spec dictionary_trigger(#ddAlias{} | #ddTable{}, #ddAlias{} | #ddTable{}, ddMnesiaTable(), ddEntityId(), ddOptions()) -> ok.
dictionary_trigger(OldRec, NewRec, T, _User, _TrOpts) when T==ddTable; T==ddAlias ->
    %% clears cached trigger information when ddTable is 
    %% modified in GUI or with insert/update/merge/remove functions
    case {OldRec,NewRec} of
        {{},{}} ->  %% truncate ddTable/ddAlias should never happen, allow for recovery operations
            ok;          
        {{},_}  ->  %% write new rec (maybe fixing something)
            {S,TN} = element(2,NewRec),
            imem_cache:clear({?MODULE, trigger, S, TN});
        {_,{}}  ->  %% drop dictionary row
            {S,TO} = element(2,OldRec),
            imem_cache:clear({?MODULE, trigger, S, TO});
        {_,_}  ->  %% update dict rec (maybe fixing something)
            {S,TO} = element(2,OldRec),
            %% ToDo: check for changed index definition
            %% ToDo: rebuild index table if index definition changed
            imem_if_mnesia:write_table_property_in_transaction(TO, NewRec),
            imem_cache:clear({?MODULE, trigger, S, TO})
    end.

%% ------ META implementation -------------------------------------------------------


% Monotonic, unique per node restart integer
-spec integer_uid() -> integer().
integer_uid() -> ?INTEGER_UID.

-spec integer_uid(any()) -> integer().
integer_uid(_Dummy) -> ?INTEGER_UID.

% Monotonic, adapted, unique timestamp with microsecond resolution and OS-dependent precision
-spec time_uid() -> ddTimeUID().
time_uid() -> ?TIME_UID.

-spec time_uid(any()) -> ddTimeUID().
time_uid(_Dummy) -> ?TIME_UID.

% Monotonic, adapted timestamp with microsecond resolution and OS-dependent precision
-spec time() -> ddTimestamp().
time() -> ?TIMESTAMP.

-spec time(any()) -> ddTimestamp().
time(_Dummy) -> ?TIMESTAMP.

-spec seconds_since_epoch(ddTimestamp() | ddTimeUID() | ddDatetime() | {integer(),integer(),integer()}) -> undefined | integer().
seconds_since_epoch(Time) -> imem_datatype:seconds_since_epoch(Time).

% is_system_table({_S,Table,_A}) -> is_system_table(Table);   % TODO: May depend on Schema
is_system_table({_,Table}) -> 
    is_system_table(Table);       % TODO: May depend on Schema
is_system_table(Table) when is_atom(Table) ->
    case lists:member(Table,?META_TABLES) of
        true ->     true;
        false ->    imem_if_mnesia:is_system_table(Table)
    end;
is_system_table(Table) when is_binary(Table) ->
    try
        {S,T} = imem_sql_expr:binstr_to_qname2(Table), 
        is_system_table({?binary_to_existing_atom(S),?binary_to_existing_atom(T)})
    catch
        _:_ -> false
    end.

-spec check_table(ddTable()) -> ok.
check_table({ddSysConf, _Table}) -> 
    ok;
check_table({Schema, Table}) ->
    case schema() of
        Schema -> check_table(Table);
        _ ->        ?UnimplementedException({"Check table in foreign schema",{Schema,Table}})
    end;
check_table(Table) when is_atom(Table);is_binary(Table);is_list(Table) ->
    imem_if_mnesia:table_size(physical_table_name(Table)),
    ok.

-spec check_local_table_copy(ddTable()) -> ok.
check_local_table_copy({ddSysConf, _Table}) -> ok;
check_local_table_copy(Table) -> imem_if_mnesia:check_local_table_copy(physical_table_name(Table)).

-spec check_table_meta(ddTable(), ddTableMeta()) -> ok.
check_table_meta({ddSysConf, _}, _) -> ok;
check_table_meta({Schema, Table}, Meta) ->
    case schema() of
        Schema -> check_table_meta(Table, Meta);
        _ ->      ?UnimplementedException({"Check table meta in foreign schema", {Schema, Table}})
    end;
check_table_meta(TableAlias, {Names, Types, DefaultRecord}) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(Names, Types, Defaults),
    PTN = physical_table_name(TableAlias),
    case imem_if_mnesia:read(ddTable,{schema(), PTN}) of
        [] ->   ?SystemException({"Missing table metadata",PTN}); 
        [#ddTable{columns=CI}] ->
            case column_info_items(CI, name) of
                Names ->    ok;
                OldN ->     ?SystemException({"Column names do not match ddTable dictionary",{PTN, Names, OldN}})
            end,
            case column_info_items(ColumnInfos, type) of
                Types ->    ok;
                OldT ->     ?SystemException({"Column types do not match ddTable dictionary",{PTN, Types, OldT}})
            end,
            case column_info_items(ColumnInfos, default) of
                Defaults -> ok;
                OldD ->     ?SystemException({"Column defaults do not match ddTable dictionary",{PTN, Defaults, OldD}})
            end
    end;  
check_table_meta(TableAlias, [CI|_]=ColumnInfo) when is_tuple(CI) ->
    Names = column_info_items(ColumnInfo, name),
    Types = column_info_items(ColumnInfo, type),
    Defaults = column_info_items(ColumnInfo, default),
    check_table_meta(TableAlias, {Names, Types, Defaults});
check_table_meta(TableAlias, Names) ->
    PTN = physical_table_name(TableAlias),
    case imem_if_mnesia:read(ddTable,{schema(), PTN}) of
        [] ->   ?SystemException({"Missing table metadata",PTN}); 
        [#ddTable{columns=CI}] ->
            case column_info_items(CI, name) of
                Names ->    ok;
                OldN ->     ?SystemException({"Column names do not match ddTable dictionary", {PTN, Names, OldN}})
            end
    end.

drop_meta_tables() ->
    drop_meta_tables(?META_TABLES).

drop_meta_tables([]) -> ok;
drop_meta_tables([TableAlias|Tables]) ->
    drop_system_table(TableAlias),
    drop_meta_tables(Tables).

meta_field_list() -> ?META_FIELDS.

meta_field(Name) when is_atom(Name) ->
    meta_field(?atom_to_binary(Name));
meta_field(Name) ->
    lists:member(Name,?META_FIELDS).

meta_field_info(Name) when is_atom(Name) ->
    meta_field_info(?atom_to_binary(Name));
meta_field_info(<<"sysdate">>=N) ->
    #ddColumn{name=N, type='datetime', len=20, prec=0};
meta_field_info(<<"systimestamp">>=N) ->
    #ddColumn{name=N, type='timestamp', len=20, prec=0};
meta_field_info(<<"schema">>=N) ->
    #ddColumn{name=N, type='atom', len=10, prec=0};
meta_field_info(?META_NODE=N) ->
    #ddColumn{name=N, type='atom', len=100, prec=0};
meta_field_info(<<"user">>=N) ->
    #ddColumn{name=N, type='userid', len=20, prec=0};
meta_field_info(<<"username">>=N) ->
    #ddColumn{name=N, type='binstr', len=20, prec=0};
meta_field_info(?META_ROWNUM=N) ->
    #ddColumn{name=N, type='integer', len=10, prec=0};
meta_field_info(Name) ->
    ?ClientError({"Unknown meta column",Name}). 

meta_field_value(?META_ROWNUM) ->   1; 
%meta_field_value(rownum) ->         1; 
meta_field_value(<<"username">>) -> <<"unknown">>; 
%meta_field_value(username) ->       <<"unknown">>; 
meta_field_value(<<"user">>) ->     unknown; 
%meta_field_value(user) ->           unknown; 
meta_field_value(Name) ->
    imem_if_mnesia:meta_field_value(Name). 

column_info_items(Info, name) ->
    [C#ddColumn.name || C <- Info];
column_info_items(Info, type) ->
    [C#ddColumn.type || C <- Info];
column_info_items(Info, default) ->
    [C#ddColumn.default || C <- Info];
column_info_items(Info, default_fun) ->
    lists:map(fun imem_datatype:to_term_or_fun/1, [C#ddColumn.default || C <- Info]);
column_info_items(Info, len) ->
    [C#ddColumn.len || C <- Info];
column_info_items(Info, prec) ->
    [C#ddColumn.prec || C <- Info];
column_info_items(Info, opts) ->
    [C#ddColumn.opts || C <- Info];
column_info_items(_Info, Item) ->
    ?ClientError({"Invalid item",Item}).

column_names(Infos)->
    [list_to_atom(lists:flatten(io_lib:format("~p", [N]))) || #ddColumn{name=N} <- Infos].

-spec column_infos(ddTable()) -> ddTableMeta().
column_infos(TableAlias) when is_atom(TableAlias) ->
    column_infos({schema(),TableAlias});    
column_infos({_Node,Schema,TableAlias}) ->
    column_infos({Schema,TableAlias});
column_infos({?CSV_SCHEMA_PATTERN=S, FileName}) when is_binary(FileName) ->
    [ #ddColumn{name=N,type=T,default=D} || {N,T,D} <- ?CSV_DEFAULT_INFO]
    ++[#ddColumn{name=N,type=binstr,default= <<>>} || N <- imem_if_csv:column_names({S,FileName})];
column_infos({Schema,TableAlias}) when is_binary(Schema), is_binary(TableAlias) ->
    S= try 
        ?binary_to_existing_atom(Schema)
    catch 
        _:_ -> ?ClientError({"Schema does not exist", Schema})
    end,
    T = try 
        ?binary_to_existing_atom(TableAlias)
    catch 
        _:_ -> ?ClientError({"Table does not exist", TableAlias})
    end,        
    column_infos({S,T});
column_infos({Schema, TableAlias}) when is_atom(Schema), is_atom(TableAlias) ->
    case lists:member(TableAlias, ?DataTypes) of
        true -> 
            [#ddColumn{name=item, type=TableAlias, len=0, prec=0, default=undefined}
            ,#ddColumn{name=itemkey, type=binstr, len=0, prec=0, default=undefined}
            ];
        false ->
            case imem_if_mnesia:read(ddTable, {Schema, physical_table_name(TableAlias)}) of
                [] ->                       ?ClientError({"Table does not exist", {Schema, TableAlias}}); 
                [#ddTable{columns=CI}] ->   CI
            end
    end;  
column_infos(Names) when is_list(Names)->
    [#ddColumn{name=list_to_atom(lists:flatten(io_lib:format("~p", [N])))} || N <- Names].

column_infos(Names, Types, Defaults)->
    NamesLength = length(Names),
    TypesLength = length(Types),
    DefaultsLength = length(Defaults),
    if (NamesLength =/= TypesLength)
       orelse (NamesLength =/= DefaultsLength)
       orelse (TypesLength =/= DefaultsLength) ->
        ?ClientError({"Column definition params length mismatch", { {"Names", NamesLength}
                                                             , {"Types", TypesLength}
                                                             , {"Defaults", DefaultsLength}}});
    true -> ok
    end,
    [#ddColumn{name=list_to_atom(lists:flatten(io_lib:format("~p", [N]))), type=T, default=D} || {N,T,D} <- lists:zip3(Names, Types, Defaults)].

from_column_infos([#ddColumn{}|_] = ColumnInfos) ->
    ColumnNames = column_info_items(ColumnInfos, name),
    ColumnTypes = column_info_items(ColumnInfos, type),
    DefaultRecord = list_to_tuple([rec|column_info_items(ColumnInfos, default)]),
    {ColumnNames, ColumnTypes, DefaultRecord}.

-spec create_table(ddTable(), ddTableMeta(), ddOptions()) -> {ok, ddQualifiedTable()}.
create_table(TableAlias, Columns, Opts) ->
    create_table(TableAlias, Columns, Opts, #ddTable{}#ddTable.owner).

-spec create_table(ddTable(), ddTableMeta(), ddOptions(), ddEntityId()) -> {ok, ddQualifiedTable()} | ddError().
create_table(TableAlias, {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(ColumnNames, ColumnTypes, Defaults),
    create_physical_table(TableAlias, ColumnInfos, Opts, Owner);
create_table(TableAlias, [#ddColumn{}|_]=ColumnInfos, Opts, Owner) ->   
    Conv = fun(X) ->
        case X#ddColumn.name of
            A when is_atom(A) -> X; 
            B -> X#ddColumn{name=?binary_to_atom(B)} 
        end
    end,
    create_physical_table(qualified_new_table_name(TableAlias), lists:map(Conv, ColumnInfos), Opts, Owner);
create_table(TableAlias, ColumnNames, Opts, Owner) ->
    ColumnInfos = column_infos(ColumnNames),
    create_physical_table(qualified_new_table_name(TableAlias), ColumnInfos, Opts, Owner).

-spec create_check_table(ddTable(), ddTableMeta(), ddOptions()) -> {ok, ddQualifiedTable()}.
create_check_table(TableAlias, Columns, Opts) ->
    create_check_table(TableAlias, Columns, Opts, (#ddTable{})#ddTable.owner).

-spec create_check_table(ddTable(), ddTableMeta(), ddOptions(), ddEntityId()) -> {ok, ddQualifiedTable()}.
create_check_table(TableAlias, [#ddColumn{}|_]=ColumnInfos, Opts, Owner) ->
    Conv = fun(X) ->
        case X#ddColumn.name of
            A when is_atom(A) -> X; 
            B -> X#ddColumn{name=binary_to_atom(B, utf8)} 
        end
    end,
    {ColumnNames, ColumnTypes, DefaultRecord} = from_column_infos(lists:map(Conv,ColumnInfos)),
    create_check_table(qualified_new_table_name(TableAlias), {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner);
create_check_table(TableAlias, {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(ColumnNames, ColumnTypes, Defaults),
    {ok, QName} = create_check_physical_table(TableAlias, ColumnInfos, Opts, Owner),
    % ?LogDebug("QName ~p",[QName]),
    check_table(QName), 
    check_table_meta(QName, {ColumnNames, ColumnTypes, DefaultRecord}),
    {ok, QName}.

create_sys_conf(Path) ->
    imem_if_sys_conf:create_sys_conf(Path).    

-spec create_check_physical_table(ddTable(), ddTableMeta(), ddOptions(), ddEntityId()) -> {ok, ddQualifiedTable()}.
create_check_physical_table(TableAlias, ColumnInfos, Opts, Owner) when is_atom(TableAlias) ->
    create_check_physical_table({schema(), TableAlias}, ColumnInfos, Opts, Owner);    
create_check_physical_table(TableAlias, ColumnInfos, Opts, Owner) when is_binary(TableAlias) ->
    create_check_physical_table({schema(), binary_to_atom(TableAlias, utf8)}, ColumnInfos, Opts, Owner);    
create_check_physical_table(TableAlias, ColumnInfos, Opts, Owner) when is_list(TableAlias) ->
    create_check_physical_table({schema(), binary_to_list(TableAlias)}, ColumnInfos, Opts, Owner);    
create_check_physical_table({Schema, TableAlias}, ColumnInfos, Opts0, Owner) when is_atom(TableAlias), is_atom(Schema) ->
    Opts1 = norm_opts(Opts0),
    case lists:member(Schema, [schema(), ddSysConf]) of
        true ->
            PTN = physical_table_name(TableAlias),
            case read(ddTable, {Schema, PTN}) of 
                [] ->
                    create_physical_table({Schema, TableAlias}, ColumnInfos, Opts1, Owner);
                [#ddTable{opts=Opts1, owner=Owner}] ->
                    catch create_physical_table({Schema, TableAlias}, ColumnInfos, Opts1, Owner),
                    {ok, {Schema, PTN}};
                [#ddTable{opts=Old, owner=Owner}] ->
                    OldOpts = lists:sort(lists:keydelete(purge_delay, 1, Old)) -- [{record_name,TableAlias}],
                    NewOpts = lists:sort(lists:keydelete(purge_delay, 1, Opts1)) -- [{record_name,TableAlias}],
                    case NewOpts of
                        OldOpts ->
                            catch create_physical_table({Schema, TableAlias}, ColumnInfos, Opts1, Owner),
                            {ok, {Schema, PTN}};
                        _ ->
                            catch create_physical_table({Schema, TableAlias}, ColumnInfos, Opts1, Owner),
                            Diff = (OldOpts -- NewOpts) ++(NewOpts -- OldOpts),
                            ?SystemException({"Wrong table options", {TableAlias, Diff}})
                    end;        
                [#ddTable{owner=Own}] ->
                    catch create_physical_table({Schema, TableAlias}, ColumnInfos, Opts1, Owner),
                    ?SystemException({"Wrong table owner", {TableAlias, [Own, Owner]}})        
            end;
        _ ->        
            ?UnimplementedException({"Create/check table in foreign schema", {Schema, TableAlias}})
    end.

-spec create_physical_table(ddTable(), ddTableMeta(), ddOptions(), ddEntityId()) -> {ok, ddQualifiedTable()}.
create_physical_table({Schema, TableAlias, _Alias}, ColumnInfos, Opts, Owner) ->
    create_physical_table({Schema, TableAlias}, ColumnInfos, Opts, Owner);
create_physical_table({Schema, TableAlias}, ColumnInfos, Opts, Owner) ->
    MySchema = schema(),
    case Schema of
        MySchema ->  create_physical_table(TableAlias, ColumnInfos, Opts, Owner);
        ddSysConf -> create_table_sys_conf(TableAlias, ColumnInfos, norm_opts(Opts), Owner);
        _ ->    ?UnimplementedException({"Create table in foreign schema", {Schema, TableAlias}})
    end;
create_physical_table(TableAlias, ColInfos, Opts0, Owner) ->
    case is_valid_table_name(TableAlias) of
        true ->     ok;
        false ->    ?ClientError({"Invalid character(s) in table name", TableAlias})
    end,    
    case is_reserved_for_tables(TableAlias) of
        false ->    ok;
        true ->     ?Info("Reserved table name detected", [TableAlias]),
                    ok
                    %% ?ClientError({"Reserved table name", TableAlias})
    end,
    Opts1 = norm_opts(Opts0),
    TypeMod = module_from_type_opts(Opts1),
    case {TypeMod, length(ColInfos)} of
        {undefined, 0} ->   ?ClientError({"No columns given in create table", TableAlias});
        {undefined, 1} ->   ?ClientError({"No value column given in create table, add dummy value column", TableAlias});
        _ ->                ok % no such check for module defined tables
    end,
    AllNames = [Name || Name <- column_info_items(ColInfos, name)],
    DistinctNames = lists:usort(AllNames),
    case (AllNames -- DistinctNames) of
        [] ->    ok;
        [DupN|_] -> ?ClientError({"Duplicate column name",DupN})
    end,
    CharsCheck = [{is_valid_column_name(Name),Name} || Name <- column_info_items(ColInfos, name)],
    case lists:keyfind(false, 1, CharsCheck) of
        false ->    ok;
        {_,BadN} -> ?ClientError({"Invalid character(s) in column name",BadN})
    end,
    ReservedCheck = [{is_reserved_for_columns(Name),Name} || Name <- column_info_items(ColInfos, name)],
    case lists:keyfind(true, 1, ReservedCheck) of
        false ->    ok;
        {_,BadC} -> ?Info("Reserved column name '~p' detected", [BadC]),
                    ok
                    %% ?ClientError({"Reserved column name",BadC})
    end,
    TypeCheck = [{imem_datatype:is_datatype(Type),Type} || Type <- column_info_items(ColInfos, type)],
    case lists:keyfind(false, 1, TypeCheck) of
        false ->    ok;
        {_,BadT} -> ?ClientError({"Invalid data type",BadT})
    end,
    FunCheck = [{imem_datatype:is_term_or_fun_text(Def),Def} || Def <- column_info_items(ColInfos, default)],
    case lists:keyfind(false, 1, FunCheck) of
        false ->    ok;
        {_,BadDef} -> ?ClientError({"Invalid default fun",BadDef})
    end,
    case TypeMod of 
        undefined ->    create_mnesia_table(TableAlias, ColInfos, Opts1, Owner);
        _ ->            TypeMod:create_table(TableAlias, ColInfos, Opts1, Owner)
    end.

module_from_type_opts(Opts) ->
    case lists:keyfind(type, 1, Opts) of
        false ->                                        undefined;  % normal table
        {type,T} when T==set;T==ordered_set;T==bag ->   undefined;  % normal table
        {type,?MODULE} ->                               ?ClientError({"Invalid module name for table type",{type,?MODULE}});
        {type,M} when is_atom(M) ->                     module_with_table_api(M);
        {type,B} when is_binary(B) ->
            case catch ?binary_to_existing_atom(B) of
                M when is_atom(M) ->                    module_with_table_api(M);
                _ ->                                    ?ClientError({"Invalid module name for table type",{type,B}})
            end
    end.

module_with_table_api(M) ->
    case catch M:module_info(exports) of
        {'EXIT',_} ->
            ?ClientError({"Unknown module name for table type",{type,M}});
        Exports -> 
            case lists:member({create_table,4},Exports) of
                true ->     M;
                false ->    ?ClientError({"Bad module name for table type",{type,M}})
            end
    end.

-spec create_mnesia_table(ddSimpleTable(), ddTableMeta(), ddOptions(), ddEntityId()) -> {ok, ddQualifiedTable()}.
create_mnesia_table(TableAlias, ColInfos, Opts0, Owner) when is_binary(TableAlias) ->
    create_mnesia_table(binary_to_atom(TableAlias, utf8), ColInfos, Opts0, Owner);
create_mnesia_table(TableAlias, ColInfos, Opts0, Owner) when is_list(TableAlias) ->
    create_mnesia_table(list_to_atom(TableAlias), ColInfos, Opts0, Owner);
create_mnesia_table(TableAlias, ColInfos, Opts0, Owner) when is_atom(TableAlias) ->
    MySchema = schema(),
    PTN = physical_table_name(TableAlias),
    case parse_table_name(TableAlias) of
        [_,_,_,"","","",""] when PTN==ddAlias ->
            % create it, even if ddTable is not yet there, register it if missing)
            DDTableRow = #ddTable{qname={MySchema, PTN}, columns=ColInfos, opts=Opts0, owner=Owner},
            case (catch imem_if_mnesia:read(ddTable, {MySchema, PTN})) of
                [] ->   % ddTable exists but has no ddAlias entry, ddAlias may or may not (due to errors) exist 
                    imem_if_mnesia:write(ddTable, DDTableRow), % register ddAlias table in dictionary
                    catch imem_if_mnesia:create_table(PTN, column_names(ColInfos), if_opts(Opts0)++[{user_properties, [DDTableRow]}]);
                _ ->    % dictionary exists or ddTable does not exist yet
                    imem_if_mnesia:create_table(PTN, column_names(ColInfos), if_opts(Opts0)++[{user_properties, [DDTableRow]}])
            end,
            imem_cache:clear({?MODULE, trigger, MySchema, PTN}),
            {ok, {MySchema, PTN}};
        [_,_,_,"","","",""] when PTN==?CACHE_TABLE ->
            % create it, even if ddTable is not yet there, register it if missing)
            DDTableRow = #ddTable{qname={MySchema, PTN}, columns=ColInfos, opts=Opts0, owner=Owner},
            case (catch imem_if_mnesia:read(ddTable, {MySchema, PTN})) of
                [] ->   % ddTable exists but has no ddCache@ entry, ddCache@ may or may not (due to errors) exist 
                    imem_if_mnesia:write(ddTable, DDTableRow),
                    catch imem_if_mnesia:create_table(PTN, column_names(ColInfos), if_opts(Opts0)++[{user_properties, [DDTableRow]}]);
                _ ->    % entry exists or ddTable does not exist yet
                    imem_if_mnesia:create_table(PTN, column_names(ColInfos), if_opts(Opts0)++[{user_properties, [DDTableRow]}])
            end,
            imem_cache:clear({?MODULE, trigger, MySchema, PTN}),
            {ok, {MySchema, PTN}};
        [_,_,_,"","","",""] ->
            %% not a time or node sharded alias
            DDTableRow = #ddTable{qname={MySchema, PTN}, columns=ColInfos, opts=Opts0, owner=Owner},
            try
                imem_if_mnesia:create_table(PTN, column_names(ColInfos), if_opts(Opts0)++[{user_properties, [DDTableRow]}]),
                imem_if_mnesia:write(ddTable, DDTableRow)
            catch
                throw:{'ClientError',{"Table already exists", PTN}} = Reason ->
                    case imem_if_mnesia:read(ddTable, {MySchema, PTN}) of
                        [] ->   imem_if_mnesia:write(ddTable, DDTableRow); % ddTable dictionary data was missing
                        _ ->    ok
                    end,
                    throw(Reason)
            end,
            imem_cache:clear({?MODULE, trigger, MySchema, PTN}),
            {ok, {MySchema, PTN}};
        [_,_,BaseName,_,_,_,_] ->
            %% Time or node sharded alias
            %% check if ddAlias parameters match (if alias record already exists)
            case [parse_table_name(TA) || #ddAlias{qname={S,TA}} <- imem_if_mnesia:read(ddAlias), S==MySchema] of
                [] -> ok; % No matching aliases registered so far for MySchema 
                ParsedTNs ->
                    case length([ BB || [_,_,BB,_,_,_,_] <- ParsedTNs, BB==BaseName]) of
                        0 ->    ok;
                        _ ->    ?ClientError({"Name conflict (different rolling period) in ddAlias", TableAlias})
                    end   
            end,
            Opts = case imem_if_mnesia:read(ddAlias, {MySchema, TableAlias}) of
                [#ddAlias{columns=ColInfos, opts=Opts1, owner=Owner}] ->
                    Opts1;  %% ddAlias options override the given values for new partitions
                [#ddAlias{}] ->
                    ?ClientError({"Create table fails because columns or owner do not match with ddAlias", TableAlias});
                [] ->
                    case lists:keyfind(record_name, 1, Opts0) of
                        false ->    Opts0++[{record_name, list_to_atom(BaseName)}];
                        _ ->        Opts0
                    end
            end,
            DDAliasRow = #ddAlias{qname={MySchema, TableAlias}, columns=ColInfos, opts=Opts, owner=Owner},
            DDTableRow = #ddTable{qname={MySchema, PTN}, columns=ColInfos, opts=Opts, owner=Owner},
            try        
                imem_if_mnesia:create_table(PTN, column_names(ColInfos), if_opts(Opts)++[{user_properties, [DDTableRow]}]),
                imem_if_mnesia:write(ddTable, DDTableRow),
                imem_if_mnesia:write(ddAlias, DDAliasRow)
            catch
                throw:{'ClientError',{"Table already exists", PTN}} = Reason ->
                    case imem_if_mnesia:read(ddTable, {MySchema, PTN}) of
                        [] ->   imem_if_mnesia:write(ddTable, DDTableRow); % ddTable meta data was missing
                        _ ->    ok
                    end,
                    case imem_if_mnesia:read(ddAlias, {MySchema, TableAlias}) of
                        [] ->   imem_if_mnesia:write(ddAlias, DDAliasRow); % ddAlias meta data was missing
                        _ ->    ok
                    end,
                    throw(Reason)
            end,
            imem_cache:clear({?MODULE, trigger, MySchema, TableAlias}),
            imem_cache:clear({?MODULE, trigger, MySchema, PTN}),
            {ok, {MySchema, PTN}}
    end.

-spec create_table_sys_conf(ddMnesiaTable(), ddTableMeta(), ddOptions(), ddEntityId()) -> {ok, ddQualifiedTable()}.
create_table_sys_conf(TableName, ColumnInfos, Opts, Owner) when is_atom(TableName) ->
    DDTableRow = #ddTable{qname={ddSysConf, TableName}, columns=ColumnInfos, opts=Opts, owner=Owner},
    return_atomic_ok(imem_if_mnesia:write(ddTable, DDTableRow)),
    {ok, {ddSysConf, TableName}}.


-spec get_trigger(ddTable()) -> undefined | ddString().
get_trigger({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> get_trigger(Table);
        _ ->        ?UnimplementedException({"Get Trigger in foreign schema", {Schema, Table}})
    end;
get_trigger(Table) when is_binary(Table) ->
    get_trigger(binary_to_atom(Table, utf8));
get_trigger(Table) when is_list(Table) ->
    get_trigger(list_to_atom(Table));
get_trigger(Table) when is_atom(Table) ->
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=D] ->
            case lists:keysearch(trigger, 1, D#ddTable.opts) of
                false -> undefined;
                {value,{_, TFunStr}} ->  TFunStr
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for", Table})
    end.

-spec create_trigger(ddTable(), ddString() | ddBinStr()) -> ok.
create_trigger({Schema, Table}, TFun) ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_trigger(Table, TFun);
        _ ->        ?UnimplementedException({"Create Trigger in foreign schema",{Schema, Table}})
    end;
create_trigger(Table, TFunStr) when is_binary(Table) ->
    create_trigger(binary_to_atom(Table, utf8), TFunStr);
create_trigger(Table, TFunStr) when is_list(Table) ->
    create_trigger(list_to_atom(Table), TFunStr);
create_trigger(Table, TFunStr) when is_atom(Table) ->
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=D] -> 
            case lists:keysearch(trigger, 1, D#ddTable.opts) of
                false ->            
                    create_or_replace_trigger(Table, TFunStr);
                {value,{_,TFunStr}} ->  
                    ?ClientError({"Trigger already exists", {Table}});
                {value,{_,Trig}} ->     
                    try 
                        imem_datatype:io_to_fun(Trig,?DD_TRIGGER_ARITY),
                        ?ClientError({"Trigger already exists", {Table,Trig}})
                    catch 
                        throw:{'ClientError',_} -> 
                            ?Warn("migrating trigger on table ~p results in ~p", [Table, create_or_replace_trigger(Table, TFunStr)])
                    end
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for", Table})
    end.

-spec create_or_replace_trigger(ddTable(), ddString() | ddBinStr()) -> ok.
create_or_replace_trigger({Schema, Table}, TFun) ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_or_replace_trigger(Table, TFun);
        _ ->        ?UnimplementedException({"Create Trigger in foreign schema", {Schema, Table}})
    end;
create_or_replace_trigger(Table, TFun) when is_binary(Table) ->
    create_or_replace_trigger(binary_to_atom(Table, utf8), TFun);
create_or_replace_trigger(Table, TFun) when is_list(Table) ->
    create_or_replace_trigger(list_to_atom(Table), TFun);
create_or_replace_trigger(Table, TFun) when is_atom(Table) ->
    imem_datatype:io_to_fun(TFun, ?DD_TRIGGER_ARITY),
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=D] -> 
            case lists:keysearch(trigger, 1, D#ddTable.opts) of
                {value,{trigger,TFun}} ->  
                    ok;
                _ -> 
                    Opts = lists:keydelete(trigger, 1, D#ddTable.opts)++[{trigger,TFun}],
                    Trans = fun() ->
                        write(ddTable, D#ddTable{opts=Opts}),                       
                        imem_cache:clear({?MODULE, trigger, schema(), Table})
                    end,
                    return_atomic_ok(transaction(Trans))                
            end; 
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end;   
create_or_replace_trigger(Table,_) when is_atom(Table) ->
    ?ClientError({"Bad fun for create_or_replace_trigger, expecting arity 4", Table}).


-spec create_index_table(ddTable(), ddOptions(), ddEntityId()) -> ok.
%% Create external index or MNESIA internal index
create_index_table(IndexTable, ParentOpts, Owner) ->
    IndexOpts = case lists:keysearch(purge_delay, 1, ParentOpts) of
                false ->        ?DD_INDEX_OPTS;
                {value,PD} ->   ?DD_INDEX_OPTS++[{purge_delay,PD}]
    end,
    init_create_table(IndexTable, {record_info(fields, ddIndex), ?ddIndex, #ddIndex{}}, IndexOpts, Owner). 

-spec create_index(ddTable(), list() | atom()) -> ok.
create_index({Schema, Table}, IndexDefinition) ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_index(Table, IndexDefinition);
        _ ->        ?UnimplementedException({"Create Index in foreign schema",{Schema,Table}})
    end;
create_index(Table, ColName) when is_atom(Table), is_atom(ColName) ->
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=_D] ->  imem_if_mnesia:create_index(Table,ColName);
        [] ->               ?ClientError({"Table dictionary does not exist for",Table})
    end;
create_index(Table, IndexDefinition) when is_atom(Table), is_list(IndexDefinition) ->
    IndexTable = index_table(Table),
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=D] -> 
            case lists:keysearch(index, 1, D#ddTable.opts) of
                false ->
                    create_or_replace_index(Table,IndexDefinition);
                {value,{index,IndexDefinition}} ->
                    create_index_table(IndexTable,D#ddTable.opts,D#ddTable.owner),
                    ?ClientError({"Index already exists",{Table}});
                {value,{index,IDL}} ->
                    create_index_table(IndexTable,D#ddTable.opts,D#ddTable.owner),
                    ?ClientError({"Index already exists",{Table,IDL}})
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end.

%% @doc Create or replace an external index or a MNESIA internal index
-spec create_or_replace_index(ddTable(), list() | atom()) -> ok.
create_or_replace_index({Schema, Table}, IndexDefinition)  ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_or_replace_index(Table, IndexDefinition);
        _ ->        ?UnimplementedException({"Create Index in foreign schema", {Schema, Table}})
    end;
create_or_replace_index(Table, ColName) when is_atom(Table), is_atom(ColName) ->
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=_D] ->  imem_if_mnesia:create_or_replace_index(Table,ColName);
        [] ->               ?ClientError({"Table dictionary does not exist for", Table})
    end;
create_or_replace_index(Table, IndexDefinition) when is_atom(Table), is_list(IndexDefinition) ->
    Schema = schema(),
    case read(ddTable,{Schema, Table}) of
        [#ddTable{}=D] -> 
            Opts = lists:keydelete(index, 1, D#ddTable.opts)++[{index, IndexDefinition}],
            IndexTable = index_table(Table),
            case (catch check_table(IndexTable)) of
                ok ->   
                    Trans = fun() ->
                        lock({table, Table}, write),
                        write(ddTable, D#ddTable{opts=Opts}),                       
                        imem_cache:clear({?MODULE, trigger, Schema, Table}),
                        imem_if_mnesia:truncate_table(IndexTable),
                        fill_index(Table, IndexDefinition)
                    end,
                    return_atomic_ok(transaction(Trans));
                _ ->
                    create_index_table(IndexTable,D#ddTable.opts,D#ddTable.owner),
                    Trans = fun() ->
                        lock({table, Table}, write),
                        write(ddTable, D#ddTable{opts=Opts}),                       
                        imem_cache:clear({?MODULE, trigger, Schema, Table}),
                        fill_index(Table, IndexDefinition)
                    end,
                    return_atomic_ok(transaction(Trans))
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for", Table})
    end.

%% @Doc Fill application index for a table with data
-spec fill_index(ddMnesiaTable(), list()) -> ok.
fill_index(Table, IndexDefinition) when is_atom(Table), is_list(IndexDefinition) ->
    Schema = schema(),
    case imem_if_mnesia:read(ddTable,{Schema, Table}) of
        [] ->
            ?ClientError({"Table does not exist", {Schema, Table}}); 
        [#ddTable{columns=ColumnInfos}] ->
            IdxPlan = compiled_index_plan(IndexDefinition, ColumnInfos),
            FoldFun = fun(Row,_) -> imem_meta:update_index({}, Row, Table, system, [], IdxPlan), ok end,
            foldl(FoldFun, ok, Table)
    end.

%% @Doc Drop application index (binary name or integer id) for a table or MNESIA internal index (atom name)
-spec drop_index(ddTable(), ddIndex())  -> ok.
drop_index({Schema, Table}, Index) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_index(Table, Index);
        _ ->        ?UnimplementedException({"Drop Index in foreign schema", {Schema, Table}})
    end;
drop_index(Table, ColName) when is_atom(ColName) ->
    PTN = physical_table_name(Table),
    case read(ddTable, {schema(), PTN}) of
        [#ddTable{}=_D] ->  imem_if_mnesia:drop_index(PTN, ColName);
        [] ->               ?ClientError({"Table dictionary does not exist for", PTN})
    end;
drop_index(Table, Index) ->
    PTN = physical_table_name(Table),
    case read(ddTable,{schema(), PTN}) of
        [#ddTable{}=D] ->
            case lists:keyfind(index, 1, D#ddTable.opts) of
                {index, Indices} ->
                    {IndexToDrop, RestIndices} =
                    lists:foldl(
                      fun(I, {Drop, Rest}) ->
                              case Index of
                                  Index
                                    when is_binary(Index)
                                         andalso I#ddIdxDef.name =:= Index ->
                                      {I, Rest};
                                  Index
                                    when is_integer(Index)
                                         andalso I#ddIdxDef.id =:= Index ->
                                      {I, Rest};
                                  _ ->
                                      {Drop, [I|Rest]}
                              end
                      end
                      , {undefined, []}
                      , lists:reverse(Indices)),
                    case IndexToDrop of
                        undefined ->
                            ?ClientError({"Index does not exist for", PTN, Index});
                        _ ->
                            if 
                                RestIndices =:= [] ->
                                    drop_index(PTN);
                                true ->
                                    create_or_replace_index(PTN, RestIndices)
                            end
                    end;
                false ->
                    drop_index(PTN)
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for", PTN})
    end.

%% @Doc Drop all application indexes for a table (but not MNESIA internal indexes)
-spec drop_index(ddTable()) -> ok.
drop_index({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_index(Table);
        _ ->        ?UnimplementedException({"Drop Index in foreign schema",{Schema,Table}})
    end;
drop_index(Table) when is_binary(Table) ->
    drop_index(binary_to_atom(Table, utf8));
drop_index(Table) when is_list(Table) ->
    drop_index(list_to_atom(Table));    
drop_index(Table) when is_atom(Table) ->
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=D] -> 
            Opts = lists:keydelete(index, 1, D#ddTable.opts),
            Trans = fun() ->
                write(ddTable, D#ddTable{opts=Opts}),                       
                imem_cache:clear({?MODULE, trigger, schema(), Table})
            end,
            ok = return_atomic_ok(transaction(Trans)),
            catch drop_table(index_table(Table));
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end.

%% @Doc Drop a trigger from a table (ignore if it does not exist)
-spec drop_trigger(ddTable()) -> ok.
drop_trigger({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_trigger(Table);
        _ ->        ?UnimplementedException({"Drop Trigger in foreign schema",{Schema,Table}})
    end;
drop_trigger(Table) when is_binary(Table) ->
    drop_trigger(binary_to_atom(Table, utf8));
drop_trigger(Table) when is_list(Table) ->
    drop_trigger(list_to_atom(Table));
drop_trigger(Table) when is_atom(Table) ->
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=D] -> 
            Opts = lists:keydelete(trigger, 1, D#ddTable.opts),
            Trans = fun() ->
                write(ddTable, D#ddTable{opts=Opts}),                       
                imem_cache:clear({?MODULE, trigger, schema(), Table})
            end,
            return_atomic_ok(transaction(Trans));
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end.

-spec is_valid_table_name(ddTable()) -> boolean().
is_valid_table_name(Table) when is_atom(Table) ->
    is_valid_table_name(atom_to_list(Table));
is_valid_table_name(Table) when is_binary(Table) ->
    is_valid_table_name(binary_to_list(Table));
is_valid_table_name(Table) when is_list(Table) ->
    [H|_] = Table,
    L = lists:last(Table),
    if
        H == $" andalso L == $" -> true;
        true -> (length(Table) == length(Table -- ?BAD_NAME_CHARACTERS))
    end.

-spec is_valid_column_name(ddColumnName()) -> boolean().
is_valid_column_name(Column) -> is_valid_table_name(Column).

% @Doc Normalize option datatypes (convert binstr values to atom) and do some checks 
-spec norm_opts(ddOptions()) -> ddOptions().
norm_opts(Opts) ->  [norm_opt(O) || O <- Opts].

-spec norm_opt(ddOption()) -> ddOption().
norm_opt({trigger,V}) -> {trigger,V};   % trigger is the only binary option for now
norm_opt({K, V}) when is_binary(V) ->
    case (catch ?binary_to_existing_atom(V)) of
        A when is_atom(A) -> {K, A};
        _ ->   ?ClientError({"Unsupported option", {K, V}})
    end;
norm_opt(Opt) -> Opt.

% @Doc Remove imem_meta table options which are not recognized by imem_if
-spec if_opts(ddOptions()) -> ddOptions().
if_opts(Opts) ->    if_opts(Opts, ?META_OPTS).

-spec if_opts(ddOptions(), ddOptions()) -> ddOptions().
if_opts([],_) -> [];
if_opts(Opts,[]) -> Opts;
if_opts(Opts,[MO|Others]) ->
    if_opts(lists:keydelete(MO, 1, Opts),Others).

-spec truncate_table(ddTable()) -> ok.
truncate_table(TableAlias) ->
    truncate_table(TableAlias, meta_field_value(user)).

-spec truncate_table(ddTable(), ddEntityId()) -> ok.
truncate_table({Schema, TableAlias, _Alias}, User) ->
    truncate_table({Schema, TableAlias}, User);    
truncate_table({Schema, TableAlias}, User) ->
    MySchema = schema(),
    case Schema of
        MySchema -> truncate_table(TableAlias, User);
        _ ->        ?UnimplementedException({"Truncate table in foreign schema", {Schema, TableAlias}})
    end;
truncate_table(TableAlias, User) when is_atom(TableAlias) ->
    log_to_db(debug, ?MODULE, truncate_table, [{table, TableAlias}], "truncate table"),
    truncate_partitioned_tables(lists:sort(simple_or_local_node_sharded_tables(TableAlias)), User);
truncate_table(TableAlias, User) ->
    truncate_table(qualified_table_name(TableAlias), User).

truncate_partitioned_tables([],_) -> ok;
truncate_partitioned_tables([TableName|TableNames], User) ->
    {_, _, Trigger} =  trigger_infos(TableName),
    IndexTable = index_table(TableName),
    Trans = case imem_if_mnesia:is_readable_table(IndexTable) of
        true -> 
            fun() ->
                Trigger({},{},TableName,User,[]),
                imem_if_mnesia:truncate_table(IndexTable),
                imem_if_mnesia:truncate_table(TableName)
            end;
        false ->
            fun() ->
                Trigger({},{},TableName,User,[]),
                imem_if_mnesia:truncate_table(TableName)
            end
    end,
    return_atomic_ok(transaction(Trans)),
    truncate_partitioned_tables(TableNames,User).

snapshot_table({_Schema,Table,_Alias}) ->
    snapshot_table({_Schema,Table});    
snapshot_table({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> snapshot_table(Table);
        _ ->        ?UnimplementedException({"Snapshot table in foreign schema",{Schema,Table}})
    end;
snapshot_table(Alias) when is_atom(Alias) ->
    log_to_db(debug, ?MODULE, snapshot_table, [{table, Alias}], "snapshot table"),
    case lists:sort(simple_or_local_node_sharded_tables(Alias)) of
        [] ->   
            ?ClientError({"Table does not exist", Alias});
        PTNs -> 
            case lists:usort([check_table(T) || T <- PTNs]) of
                [ok] -> 
                    snapshot_partitioned_tables(
                          imem_datatype:io_to_fun(?GET_SNAP_NAME_TRANS(Alias),1)
                        , imem_datatype:io_to_fun(?GET_SNAP_PROP_TRANS(Alias),1)
                        , imem_datatype:io_to_fun(?GET_SNAP_ROW_TRANS(Alias),1)            
                        , PTNs
                    );
                _ ->    
                    ?ClientError({"Table does not exist",Alias})
            end
    end;
snapshot_table(TableName) ->
    snapshot_table(qualified_table_name(TableName)).

snapshot_partitioned_tables(_,_,_,[]) -> ok;
snapshot_partitioned_tables(NTrans,PTrans,RTrans,[TableName|TableNames]) ->
    imem_snap:take(#{table=>TableName,nTrans=>NTrans, pTrans=>PTrans, rTrans=>RTrans}),
    snapshot_partitioned_tables(NTrans,PTrans,RTrans,TableNames).

restore_table({_Schema,Table,_Alias}) ->
    restore_table({_Schema,Table});
restore_table({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> restore_table(Table);
        _ ->        ?UnimplementedException({"Restore table in foreign schema",{Schema,Table}})
    end;
restore_table(Alias) when is_atom(Alias) ->
    log_to_db(debug, ?MODULE, restore_table, [{table, Alias}], "restore table"),
    case lists:sort(simple_or_local_node_sharded_tables(Alias)) of
        [] ->   
            ?ClientError({"Table does not exist",Alias});
        PTNs -> 
            case imem_snap:restore(bkp, PTNs, destroy, false) of
                [{_, {_, _, _}}]  ->    ok;
                Error ->
                    E = case Error of 
                        [{_, Err}] -> Err; 
                        Error -> Error 
                    end,
                    ?SystemException({"Restore table failed with",E})
            end
    end;    
restore_table(TableName) ->
    restore_table(qualified_table_name(TableName)).

restore_table_as({_Schema,Table,_Alias}, NewTable) ->
    restore_table_as({_Schema,Table}, NewTable);
restore_table_as(Oldtable, {_Schema,Table,_Alias}) ->
    restore_table_as(Oldtable, {_Schema,Table});
restore_table_as({Schema,Table},{NewSchema,NewTable}) ->
    MySchema = schema(),
    case {Schema, NewSchema} of
        {MySchema, MySchema} -> restore_table_as(Table, NewTable);
        _ -> ?UnimplementedException({"Restore table as in foreign schema",{Schema,NewSchema,Table}})
    end;
restore_table_as(Alias, NewAlias) when is_atom(Alias) ->
    log_to_db(debug, ?MODULE, restore_table_as, [{table, Alias}, {newTable, NewAlias}], "restore table as"),
    case lists:sort(simple_or_local_node_sharded_tables(Alias)) of
        [] ->   ?ClientError({"Table does not exist", Alias});
        [PTN] ->
            NewPTN = case catch binary_to_atom(NewAlias, utf8) of    % _existing
                          {'EXIT',_} -> NewAlias;
                          {'ClientError',_} -> NewAlias;
                          NewAliasAtom ->
                              case lists:sort(simple_or_local_node_sharded_tables(NewAliasAtom)) of
                                  [] -> NewAliasAtom;
                                  [NPTN] -> NPTN;
                                  NPTNs ->
                                      ?ClientError({"Too many target tables", NPTNs})
                              end
                      end,
            case imem_snap:restore_as(bkp,PTN,NewPTN,destroy,false) of
                ok -> ok;
                E -> ?SystemException({"Restore table as failed with",E})
            end;
        PTNs ->
            ?ClientError({"Too many source tables", PTNs})
    end;    
restore_table_as(TableName,NewTableName) ->
    MySchema = schema(),
    MySchemaBin = atom_to_binary(MySchema, utf8),
    case imem_sql_expr:binstr_to_qname2(NewTableName) of
        {MySchemaBin, Table} ->
            restore_table_as(qualified_table_name(TableName), {MySchema, Table});
        {Schema, Table} ->
            ?UnimplementedException({"Restore table as in foreign schema",{MySchema,Schema,Table}})
    end.

drop_table(Table) -> drop_table(Table, []).

drop_table({Schema,TableAlias},TableTypeOpts) when is_atom(Schema), is_atom(TableAlias), is_list(TableTypeOpts) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_table(TableAlias,TableTypeOpts);
        _ ->        ?UnimplementedException({"Drop table in foreign schema",{Schema,TableAlias}})
    end;
drop_table(TableAlias,TableTypeOpts) when is_atom(TableAlias), is_list(TableTypeOpts) ->
    case is_system_table(TableAlias) of
        true -> ?ClientError({"Cannot drop system table",TableAlias});
        false-> drop_tables_and_infos(TableAlias,lists:sort(simple_or_local_node_sharded_tables(TableAlias)),TableTypeOpts)
    end;
drop_table(TableAlias,TableTypeOpts) when is_binary(TableAlias), is_list(TableTypeOpts) ->
    drop_table(qualified_table_name(TableAlias),TableTypeOpts).

drop_system_table(TableAlias) -> drop_system_table(TableAlias,[]).

drop_system_table(TableAlias, TableTypeOpts) when is_atom(TableAlias), is_list(TableTypeOpts) ->
    case is_system_table(TableAlias) of
        false -> ?ClientError({"Not a system table",TableAlias});
        true ->  drop_tables_and_infos(TableAlias,lists:sort(simple_or_local_node_sharded_tables(TableAlias)), TableTypeOpts)
    end.

drop_tables_and_infos(TableName,[TableName],TableTypeOpts) ->
    drop_table_and_info(TableName,TableTypeOpts);
drop_tables_and_infos(TableAlias, TableList,TableTypeOpts) -> 
     imem_if_mnesia:delete(ddAlias, {schema(),TableAlias}),
     drop_partitions_and_infos(TableList,TableTypeOpts).

drop_partitions_and_infos([],_TableTypeOpts) -> ok;
drop_partitions_and_infos([TableName|TableNames],TableTypeOpts) ->
    drop_table_and_info(TableName,TableTypeOpts),
    drop_partitions_and_infos(TableNames,TableTypeOpts).

drop_table_and_info(TableName, []) ->
    try
        imem_if_mnesia:drop_table(TableName),
        imem_cache:clear({?MODULE, trigger, schema(), TableName}),
        case TableName of
            ddTable ->  ok;
            ddAlias ->  ok;
            _ ->
                ?Info("Dropping Table ~p.~p", [schema(), TableName]),        
                imem_if_mnesia:delete(ddTable, {schema(), TableName})
        end
    catch
        throw:{'ClientError',{"Table does not exist", Table}} ->
            catch imem_if_mnesia:delete(ddTable, {schema(), TableName}),
            throw({'ClientError',{"Table does not exist",Table}})
    end;       
drop_table_and_info(TableName, Opts) ->
    case module_from_type_opts(Opts) of
        undefined ->    
            drop_table_and_info(TableName, []);
        TypeMod ->
            ?Info("Dropping Table ~p.~p", [TypeMod, TableName]),      
            TypeMod:drop_table(TableName)
    end.

-spec purge_table(ddTable()) -> integer().
purge_table(TableAlias) -> purge_table(TableAlias, []).

-spec purge_table(ddTable(), ddOptions()) -> integer().
purge_table({Schema, TableAlias, _Alias}, Opts) -> 
    purge_table({Schema,TableAlias}, Opts);
purge_table({Schema,TableAlias}, Opts) ->
    MySchema = schema(),
    case Schema of
        MySchema -> purge_table(TableAlias, Opts);
        _ ->        ?UnimplementedException({"Purge table in foreign schema", {Schema, TableAlias}})
    end;
purge_table(TableAlias, Opts) ->
    case is_time_partitioned_alias(TableAlias) of
        false ->    
            ?UnimplementedException({"Purge not supported on this table type", TableAlias});
        true ->
            purge_time_partitioned_table(TableAlias, Opts)
    end.

%% @Doc Purge older partitions (according to purge delay in ddTable) for a time sharded alias
%% Output: Number of bytes purged in memory (evantually) 
-spec purge_time_partitioned_table(ddSimpleTable(), ddOptions()) -> integer().
purge_time_partitioned_table(TableAlias, Opts) ->
    case lists:sort(simple_or_local_node_sharded_tables(TableAlias)) of
        [] ->
            ?ClientError({"Table to be purged does not exist", TableAlias});
        [TableName|_] ->
            KeepTime = case proplists:get_value(purge_delay, Opts) of
                undefined ->    ?TIMESTAMP;
                Seconds ->      {Secs, Micro} = ?TIMESTAMP,
                                {Secs-Seconds, Micro}
            end,
            KeepName = partitioned_table_name(TableAlias, KeepTime),
            if  
                TableName >= KeepName ->
                    0;      %% no memory could be freed       
                true ->
                    FreedMemory = table_memory(TableName),
                    ?Info("Purge time partition ~p", [TableName]),
                    drop_table_and_info(TableName, Opts),
                    FreedMemory
            end
    end.

-spec simple_or_local_node_sharded_tables(ddSimpleTable()) -> [ddMnesiaTable()].
simple_or_local_node_sharded_tables(TableAlias) ->    
    case is_node_sharded_alias(TableAlias) of
        true ->
            case is_time_partitioned_alias(TableAlias) of
                true ->
                    Tail = lists:reverse("@"++node_shard()),
                    Pred = fun(TN) -> lists:prefix(Tail, lists:reverse(atom_to_list(TN))) end,
                    lists:filter(Pred, physical_table_names(TableAlias));
                false ->
                    [physical_table_name(TableAlias)]
            end;        
        false ->
            physical_table_names(TableAlias)
    end.

-spec is_local_alias(ddSimpleTable() | ddQualifiedTable()) -> boolean().
is_local_alias({_Schema,TableAlias}) -> 
    is_local_alias(TableAlias);
is_local_alias(TableAlias) when is_atom(TableAlias) -> 
    is_local_alias(atom_to_list(TableAlias));
is_local_alias(TableAlias) when is_binary(TableAlias) -> 
    is_local_alias(binary_to_list(TableAlias));
is_local_alias(TableAlias) when is_list(TableAlias) ->
    NS = node_shard(),
    case parse_table_name(TableAlias) of
        [_,_,_,_,_,"@","_"] ->      true;
        [_,_,_,_,_,"@","local"] ->  true;
        [_,_,_,_,_,"@",NS] ->       true;
        [_,_,_,_,_,"@",_] ->        false;
        _ ->                        true
    end.

-spec is_node_sharded_alias(ddSimpleTable() | ddQualifiedTable()) -> boolean().
is_node_sharded_alias({Schema,TableAlias})  ->
    case schema() of
        Schema ->   is_node_sharded_alias(TableAlias);
        _ ->        false
    end;
is_node_sharded_alias(TableAlias) when is_atom(TableAlias) -> 
    is_node_sharded_alias(atom_to_list(TableAlias));
is_node_sharded_alias(TableAlias) when is_binary(TableAlias) -> 
    is_node_sharded_alias(binary_to_list(TableAlias));
is_node_sharded_alias(TableAlias) when is_list(TableAlias) -> 
    (lists:last(TableAlias) == $@).

-spec is_time_partitioned_alias(ddSimpleTable() | ddQualifiedTable()) -> boolean().
is_time_partitioned_alias({Schema,TableAlias}) ->
    case schema() of
        Schema ->   is_time_partitioned_alias(TableAlias);
        _ ->        false
    end;
is_time_partitioned_alias(TableAlias) when is_atom(TableAlias) ->
    is_time_partitioned_alias(atom_to_list(TableAlias));
is_time_partitioned_alias(TableAlias) when is_binary(TableAlias) ->
    is_time_partitioned_alias(binary_to_list(TableAlias));
is_time_partitioned_alias(TableAlias) when is_list(TableAlias) ->
    case parse_table_name(TableAlias) of
        [_,_,_,_,"",_,_] ->                     false;
        [_,_,_,_,T,"@",_] when length(T) < ?PartEndDigits ->  true;
        _ ->                                    false
    end.

-spec is_local_node_sharded_table(ddSimpleTable()) -> boolean.
is_local_node_sharded_table(Name) when is_atom(Name) -> 
    is_local_node_sharded_table(atom_to_list(Name));
is_local_node_sharded_table(Name) when is_binary(Name) -> 
    is_local_node_sharded_table(binary_to_list(Name));
is_local_node_sharded_table(Name) when is_list(Name) -> 
    lists:suffix([$@|node_shard()],Name).

-spec is_local_time_partitioned_table(ddSimpleTable()) -> boolean.
is_local_time_partitioned_table(Name) when is_atom(Name) ->
    is_local_time_partitioned_table(atom_to_list(Name));
is_local_time_partitioned_table(Name) when is_binary(Name) ->
    is_local_time_partitioned_table(binary_to_list(Name));
is_local_time_partitioned_table(Name) when is_list(Name) ->
    case is_local_node_sharded_table(Name) of
        false ->    false;
        true ->     is_time_partitioned_table(Name)
    end.

-spec is_time_partitioned_table(ddSimpleTable()) -> boolean.
is_time_partitioned_table(Name) when is_atom(Name) ->
    is_time_partitioned_table(atom_to_list(Name));
is_time_partitioned_table(Name) when is_binary(Name) ->
    is_time_partitioned_table(binary_to_list(Name));
is_time_partitioned_table(Name) when is_list(Name) ->
    case parse_table_name(Name) of
        [_,_,_,_,"",_,_] ->     false;
        _ ->                    true
    end.

-spec is_schema_time_partitioned_table(ddSimpleTable()) -> boolean.
is_schema_time_partitioned_table(Name) when is_atom(Name) ->
    is_schema_time_partitioned_table(atom_to_list(Name));
is_schema_time_partitioned_table(Name) when is_binary(Name) ->
    is_schema_time_partitioned_table(binary_to_list(Name));
is_schema_time_partitioned_table(Name) when is_list(Name) ->
    case parse_table_name(Name) of
        [_,_,_,_,"",_,_] ->     false;
        [_,_,_,_,_,"@","_"] ->  true;
        _ ->                    false
    end.

-spec is_local_or_schema_time_partitioned_table(ddSimpleTable()) -> boolean.
is_local_or_schema_time_partitioned_table(Name) when is_atom(Name) ->
    is_local_or_schema_time_partitioned_table(atom_to_list(Name));
is_local_or_schema_time_partitioned_table(Name) when is_binary(Name) ->
    is_local_or_schema_time_partitioned_table(binary_to_list(Name));
is_local_or_schema_time_partitioned_table(Name) when is_list(Name) ->
    NS = node_shard(),
    case parse_table_name(Name) of
        [_,_,_,_,"",_,_] ->         false;
        [_,_,_,_,_,"@","_"] ->      true;
        [_,_,_,_,_,"@","local"] ->  true;
        [_,_,_,_,_,"@",NS] ->       true;
        _ ->                        false
    end.

-spec is_reserved_for_tables(ddSimpleTable()) -> boolean.
is_reserved_for_tables(TableAlias) -> sqlparse:is_reserved(TableAlias).

-spec is_reserved_for_columns(ddColumnName()) -> boolean.
is_reserved_for_columns(Name) -> sqlparse:is_reserved(Name).

-spec parse_table_name(ddSimpleTable()) -> [ddString()].
%% TableName -> [Schema, ".", Name, "_", Period, "@", Node]
%% list of string items, all optional ("") except Name
parse_table_name(TableName) when is_atom(TableName) -> 
    parse_table_name(atom_to_list(TableName));
parse_table_name(TableName) when is_list(TableName) ->
    case imem_sql:parse_sql_name(TableName) of
        {"",N} ->       ["",""|parse_simple_name(N)];
        {S,N} ->        [S,"."|parse_simple_name(N)]
    end;
parse_table_name(TableName) when is_binary(TableName) ->
    parse_table_name(binary_to_list(TableName)).

-spec parse_simple_name(ddString()) -> [ddString()].
parse_simple_name(TableName) when is_list(TableName) ->
    %% TableName -> [Name,"_",Period,"@",Node] all strings , all optional except Name        
    case string:tokens(TableName, "@") of
        [TableName] ->    
            [TableName,"","","",""];
        [Name] ->    
            case string:tokens(Name, "_") of  
                [Name] ->  
                    [Name,"","","@",""];
                BL ->     
                    case catch list_to_integer(lists:last(BL)) of
                        I when is_integer(I) ->
                            [string:join(lists:sublist(BL,length(BL)-1),"."),"_",lists:last(BL),"@",""];
                        _ ->
                            [Name,"","","@",""]
                    end
            end;
        [Name,Node] ->
            case string:tokens(Name, "_") of  
                [Name] ->  
                    [Name,"","","@",Node];
                BL ->     
                    case catch list_to_integer(lists:last(BL)) of
                        I when is_integer(I) ->
                            [string:join(lists:sublist(BL,length(BL)-1),"."),"_",lists:last(BL),"@",Node];
                        _ ->
                            [Name,"","","@",Node]
                    end
            end;
        _ ->
            [TableName,"","","",""]
    end.

-spec index_table_name(ddSimpleTable()) -> ddBinStr().
index_table_name(Table) when is_atom(Table) ->
    index_table_name(atom_to_list(Table));
index_table_name(Table) when is_binary(Table) ->
    index_table_name(binary_to_list(Table));
index_table_name(Table) when is_list(Table) ->  list_to_binary(?INDEX_TABLE(Table)).

-spec index_table(ddSimpleTable()) -> ddMnesiaTable().
index_table(Table) -> binary_to_atom(index_table_name(Table), utf8).

-spec time_to_partition_expiry(ddSimpleTable() | ddString()) -> integer().
time_to_partition_expiry(Table) when is_atom(Table) ->
    time_to_partition_expiry(atom_to_list(Table));
time_to_partition_expiry(Table) when is_binary(Table) ->
    time_to_partition_expiry(binary_to_list(Table));
time_to_partition_expiry(Table) when is_list(Table) ->
    case parse_table_name(Table) of
        [_Schema,_Dot,_BaseName,_,"",_Aterate,_Shard] ->
            ?ClientError({"Not a time partitioned table",Table});     
        [_Schema,_Dot,_BaseName,"_",Number,_Aterate,_Shard] ->
            {Secs,_} = ?TIMESTAMP,
            list_to_integer(Number) - Secs
    end.

-spec time_of_partition_expiry(ddSimpleTable() | ddString()) -> ddTimestamp().
time_of_partition_expiry(Table) when is_atom(Table) ->
    time_of_partition_expiry(atom_to_list(Table));
time_of_partition_expiry(Table) when is_binary(Table) ->
    time_of_partition_expiry(binary_to_list(Table));
time_of_partition_expiry(Table) when is_list(Table) ->
    case parse_table_name(Table) of
        [_Schema,_Dot,_BaseName,_,"",_Aterate,_Shard] ->
            ?ClientError({"Not a time partitioned table",Table});     
        [_Schema,_Dot,_BaseName,"_",N,_Aterate,_Shard] ->
            {list_to_integer(N), 0}
    end.

-spec physical_table_name(ddTable()) -> ddMnesiaTable().
physical_table_name({_Node,Schema,N}) -> physical_table_name({Schema,N});
physical_table_name({_Schema,N}) -> physical_table_name(N);
physical_table_name(dba_tables) -> ddTable;
physical_table_name(all_tables) -> ddTable;
physical_table_name(all_aliases) -> ddAlias;
physical_table_name(user_tables) -> ddTable;
physical_table_name(TableAlias) when is_atom(TableAlias) ->
    case lists:member(TableAlias, ?DataTypes) of
        true ->     TableAlias;
        false ->    physical_table_name(atom_to_list(TableAlias))
    end;
physical_table_name(TableAlias) when is_binary(TableAlias) ->
    physical_table_name(binary_to_list(TableAlias));
physical_table_name(TableAlias) when is_list(TableAlias) ->
    case lists:reverse(TableAlias) of
        [$@|_] ->       
            partitioned_table_name(TableAlias, ?TIMESTAMP);     % node sharded alias, maybe time sharded
        [$_,$@|_] ->    
            partitioned_table_name(TableAlias, ?TIMESTAMP);     % time sharded only
        _ ->
            case lists:member($@,TableAlias) of            
                false ->    list_to_atom(TableAlias);           % simple table
                true ->     partitioned_table_name(TableAlias, ?TIMESTAMP)     % maybe node sharded alias
            end
    end.

-spec physical_table_name(ddTable(), any()) -> ddMnesiaTable().     % must work for any key
physical_table_name({_Node,Schema,N}, Key) -> physical_table_name({Schema,N}, Key);
physical_table_name({_Schema, N}, Key) -> physical_table_name(N, Key);
physical_table_name(dba_tables, _) -> ddTable;
physical_table_name(all_tables, _) -> ddTable;
physical_table_name(all_aliases, _) -> ddAlias;
physical_table_name(user_tables, _) -> ddTable;
physical_table_name(TableAlias, Key) when is_atom(TableAlias) ->
    case lists:member(TableAlias, ?DataTypes) of
        true ->     TableAlias;
        false ->    physical_table_name(atom_to_list(TableAlias), Key)
    end;
physical_table_name(TableAlias, Key) when is_binary(TableAlias) ->
    physical_table_name(binary_to_list(TableAlias), Key);
physical_table_name(TableAlias, Key) when is_list(TableAlias) ->
    case lists:reverse(TableAlias) of
        [$@|_] ->       
            partitioned_table_name(TableAlias, Key);    % node sharded, maybe time sharded
        [$_,$@|_] ->    
            partitioned_table_name(TableAlias, Key);    % time sharded only
        _ ->
            case lists:member($@, TableAlias) of            
                false ->    list_to_atom(TableAlias);               % simple table
                true ->     partitioned_table_name(TableAlias, Key)  % maybe node sharded alias
            end
    end.

table_alias_opts(TableAlias) ->
    table_alias_opts(schema(), TableAlias).

table_alias_opts("", TableAlias) when is_list(TableAlias) ->
    try 
        table_alias_opts(schema(), list_to_existing_atom(TableAlias))
    catch _:_ -> undefined
    end;
table_alias_opts(Schema, TableAlias) when is_list(TableAlias) ->
    try 
        table_alias_opts(list_to_existing_atom(Schema), list_to_existing_atom(TableAlias))
    catch _:_ -> undefined
    end;
table_alias_opts(Schema, TableName) when is_binary(TableName) ->
    table_alias_opts(binary_to_list(Schema), binary_to_list(TableName));
table_alias_opts(Schema, TableAlias) ->
    case imem_if_mnesia:read(ddAlias,{Schema, TableAlias}) of
        [] ->                       undefined;
        [#ddAlias{opts=Opts}] ->    Opts
    end.

table_opts(TableName) ->
    table_opts(schema(), TableName).

table_opts("", TableName) when is_list(TableName) ->
    try 
        table_opts(schema(), list_to_existing_atom(TableName))
    catch _:_ -> undefined
    end;
table_opts(Schema, TableName) when is_list(Schema), is_list(TableName) ->
    try 
        table_opts(list_to_existing_atom(Schema), list_to_existing_atom(TableName))
    catch _:_ -> undefined
    end;
table_opts(Schema, TableName) when is_binary(TableName) ->
    table_opts(binary_to_list(Schema), binary_to_list(TableName));
table_opts(Schema, TableName) ->
    case lists:member(TableName, ?DataTypes) of
        true ->
            [];
        false ->    
            case imem_if_mnesia:read(ddTable,{Schema, TableName}) of
                [] ->                       undefined; 
                [#ddTable{opts=Opts}] ->    Opts
            end
    end.

-spec qualified_alias(ddTable()) -> [ddQualifiedTable()].
qualified_alias(TableAlias) when is_binary(TableAlias) ->
    qualified_alias(binary_to_list(TableAlias));
qualified_alias(TableAlias) when is_list(TableAlias) ->
    NS = node_shard(),
    SN = atom_to_list(schema()),     
    {QS,QN} = case parse_table_name(TableAlias) of
        % [Schema, ".", Name, "_", Period, "@", Node]
        ["","",N, "","","@","_"] ->     
            {SN, N++"@_"};              % plain shared table (e.g. audit)
        [S,".",N, "","","@","_"] ->     
            {S, N++"@_"};               % plain shared table (e.g. audit)
        ["","",N,"_",PT,"@","_"] ->     
            {SN, N++"_"++PT++"@_"};     % particular shared time partition 
        [S,".",N,"_",PT,"@","_"] ->     
            {S, N++"_"++PT++"@_"};      % particular shared time partition 
        [[$c,$s,$v,$$|Rest],_,N,"","","@","local"] ->
            {[$c,$s,$v,$$|Rest], N};
        [S,_,N,"","","@","local"] when S=="";S==SN ->
            {SN, N++"@"++NS};
        ["",_,N,"_",PT,"@","local"] -> 
            {SN, N++"_"++PT++"@"++NS};
        [SN,_,N,"_",PT,"@","local"] -> 
            {SN, N++"_"++PT++"@"++NS};
        [[$c,$s,$v,$$|Rest],_,N,"" ,"","@",_] ->
            {[$c,$s,$v,$$|Rest], N};
        [S,_,N,"" ,"","@",Shard] ->
            % handle invisible remote tables with {scope,local},{local_content,true} and optional @
            cluster_alias_name(S, N, Shard);
        [S,_,N,"_",PT,"@",""] when S=="";S==SN -> 
            {SN, N++"_"++PT++"@"};
        [S,_,N,"_",PT,"@",Shard] when S=="";S==SN -> 
            {SN, N++"_"++PT++"@"++Shard};
        ["","",N,"","","",""] -> 
            {SN, N};
        [[$c,$s,$v,$$|Rest],_,N,"","","",""] ->
            {[$c,$s,$v,$$|Rest], N};
        [S,".",N,"","","",""] -> 
            {S, N}
    end,
    {list_to_binary(QS), list_to_binary(QN)}.

cluster_alias_name(Schema, BaseName, Shard) ->
    case table_opts(Schema, BaseName++"@"++ node_shard()) of
        undefined ->
            case table_opts(Schema, BaseName) of 
                undefined -> [];
                _Opts1 ->           % table exists locally or is a virtual type table
                    {default_schema_str(Schema), BaseName}
            end;
        _Opts2 ->       % table exists locally or is a virtual type table
            {default_schema_str(Schema), BaseName++"@"++Shard}
    end.

-spec physical_table_names(ddTable()) -> [ddMnesiaTable()].
physical_table_names({_S,N}) -> physical_table_names(N);
physical_table_names(dba_tables) -> [ddTable];
physical_table_names(all_tables) -> [ddTable];
physical_table_names(all_aliases) -> [ddAlias];
physical_table_names(user_tables) -> [ddTable];
physical_table_names(TableAlias) when is_atom(TableAlias) ->
    case lists:member(TableAlias,?DataTypes) of
        true ->     [TableAlias];
        false ->    physical_table_names(atom_to_list(TableAlias))
    end;
physical_table_names(TableAlias) when is_binary(TableAlias) ->
    physical_table_names(binary_to_list(TableAlias));
physical_table_names(TableAlias) when is_list(TableAlias) ->
    NS = node_shard(),
    case parse_table_name(TableAlias) of
        % [Schema, ".", Name, "_", Period, "@", Node]
        [_,_,N, "","","@","_"] ->     
            [list_to_atom(N++"@_")];               % plain shared table (e.g. audit)
        [_,_,N,"_",PT,"@","_"] when length(PT) >= ?PartEndDigits ->     
            [list_to_atom(N++"_"++PT++"@_")];  % particular shared time partition 
        [_,_,N,"_",_P,"@","_"] ->
            NameLen = length(N) + 3 + ?PartEndDigits,
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)==NameLen andalso lists:suffix("@_",TNS))
            end,
            lists:filter(Pred, tables_starting_with(N++"_"));
        [_,_,N,"","","@","local"] ->
            [list_to_atom(N++"@"++NS)];
        [_,_,N,"_",PT,"@","local"] when length(PT) >= ?PartEndDigits -> 
            [list_to_atom(N++"_"++PT++"@"++NS)];
        [_,_,N,"_",_P,"@","local"] ->
            NameLen = length(N) + 2 + ?PartEndDigits + length(NS),
            Suffix = "@"++NS,
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)==NameLen andalso lists:suffix(Suffix, TNS))
            end,
            lists:filter(Pred, tables_starting_with(N++"_"));
        [_,_,N,"" ,"","@",Shard] ->
            tables_starting_with(N++"@"++Shard);
        [_,_,N,"_",PT,"@",""] when length(PT) >= ?PartEndDigits -> 
            tables_starting_with(N++"_"++PT++"@");
        [_,_,N,"_",_P,"@",""] -> 
            AtPos = length(N) + 2 + ?PartEndDigits,
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)>AtPos andalso lists:nth(AtPos,TNS)==$@)
            end,
            lists:filter(Pred, tables_starting_with(N++"_"));
        [_,_,N,"_",PT,"@",Shard] when length(PT) >= ?PartEndDigits -> 
           [list_to_atom(N++"_"++PT++"@"++Shard)]; 
        [_,_,N,"_",_P,"@",Shard] -> 
            NameLen = length(N) + 2 + ?PartEndDigits + length(Shard),
            Suffix = "@"++Shard,
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)==NameLen andalso lists:suffix(Suffix, TNS))
            end,
            lists:filter(Pred, tables_starting_with(N++"_"));
        [[$c,$s,$v,$$|Rest],_,N,"","","",""] ->
            [{node(),list_to_binary([$c,$s,$v,$$|Rest]),list_to_binary(N)}];
        [_,_,N,"","","",""] -> 
            [list_to_atom(N)]
    end.

-spec cluster_table_names(ddTable()) -> [{Node::atom(), Schema::atom(), ddMnesiaTable()}].
cluster_table_names(TableAlias) when is_atom(TableAlias) -> cluster_table_names(atom_to_list(TableAlias));
cluster_table_names(TableAlias) when is_binary(TableAlias) -> cluster_table_names(binary_to_list(TableAlias));
cluster_table_names(TableAlias) when is_list(TableAlias) ->
    NS = node_shard(),
    SN = atom_to_list(schema()),     
    case parse_table_name(TableAlias) of
        % [Schema, ".", Name, "_", Period, "@", Node]
        ["","",N, "","","@","_"] ->     
            [{node(),schema(),list_to_atom(N++"@_")}];                  % plain shared table (e.g. audit)
        [S,".",N, "","","@","_"] ->     
            [{node(),list_to_atom(S),list_to_atom(N++"@_")}];           % plain shared table (e.g. audit)
        ["","",N,"_",PT,"@","_"] when length(PT) >= ?PartEndDigits ->     
            [{node(),schema(),list_to_atom(N++"_"++PT++"@_")}];         % particular shared time partition 
        [S,".",N,"_",PT,"@","_"] when length(PT) >= ?PartEndDigits ->     
            [{node(),list_to_atom(S),list_to_atom(N++"_"++PT++"@_")}];  % particular shared time partition 
        [S,_,N,"_",_P,"@","_"] when S=="";S==SN ->
            NameLen = length(N) + 3 + ?PartEndDigits,
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)==NameLen andalso lists:suffix("@_",TNS))
            end,
            [{node(), schema(), T} || T <- lists:filter(Pred, tables_starting_with(N++"_"))];
        [[$c,$s,$v,$$|Rest],_,N,"","","@","local"] ->
            [{node(),list_to_binary([$c,$s,$v,$$|Rest]),list_to_binary(N)}];
        [S,_,N,"","","@","local"] when S=="";S==SN ->
            [{node(),schema(),list_to_atom(N++"@"++NS)}];
        ["",_,N,"_",PT,"@","local"] when length(PT) >= ?PartEndDigits -> 
            [{node(),schema(),list_to_atom(N++"_"++PT++"@"++NS)}];
        [SN,_,N,"_",PT,"@","local"] when length(PT) >= ?PartEndDigits -> 
            [{node(),schema(),list_to_atom(N++"_"++PT++"@"++NS)}];
        [S,_,N,"_",_P,"@","local"] when S=="";S==SN ->
            NameLen = length(N) + 2 + ?PartEndDigits + length(NS),
            Suffix = "@"++NS,
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)==NameLen andalso lists:suffix(Suffix, TNS))
            end,
            [{node(), schema(), T} || T <- lists:filter(Pred, tables_starting_with(N++"_"))];
        [[$c,$s,$v,$$|Rest],_,N,"" ,"","@",Shard] ->
            [{node_from_shard(Shard),list_to_binary([$c,$s,$v,$$|Rest]),list_to_binary(N)}];
        [S,_,N,"" ,"","@",Shard] ->
            % handle invisible remote tables with {scope,local},{local_content,true} and optional @
            cluster_table_names(S, N, Shard);
        ["","",N,"_",PT,"@",""] when length(PT) >= ?PartEndDigits -> 
            [{node_from_table(T),schema(),T} || T <- tables_starting_with(N++"_"++PT++"@")];
        [SN,".",N,"_",PT,"@",""] when length(PT) >= ?PartEndDigits -> 
            [{node_from_table(T),schema(),T} || T <- tables_starting_with(N++"_"++PT++"@")];
        [S,_,N,"_",_P,"@",""] when S=="";S==SN -> 
            AtPos = length(N) + 2 + ?PartEndDigits,
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)>AtPos andalso lists:nth(AtPos,TNS)==$@)
            end,
            [{node_from_table(T),schema(),T} || T <- lists:filter(Pred, tables_starting_with(N++"_"))];
        ["","",N,"_",PT,"@",Shard] when length(PT) >= ?PartEndDigits -> 
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)==length(N)+length(PT)+length(Shard)+2)
            end,
            [{node_from_table(T),schema(),T} || T <- lists:filter(Pred, tables_starting_with(N++"_"++PT++"@"++Shard))];
        [SN,".",N,"_",PT,"@",Shard] when length(PT) >= ?PartEndDigits -> 
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)==length(N)+length(PT)+length(Shard)+2)
            end,
            [{node_from_table(T),schema(),T} || T <- lists:filter(Pred, tables_starting_with(N++"_"++PT++"@"++Shard))];
        [S,_,N,"_",_P,"@",Shard] when S=="";S==SN -> 
            NameLen = length(N) + 2 + ?PartEndDigits + length(Shard),
            Suffix = "@"++Shard,
            Pred = fun(TN) ->
                TNS = atom_to_list(TN),
                (length(TNS)==NameLen andalso lists:suffix(Suffix, TNS))
            end,
            [{node_from_table(T),schema(),T} || T <- lists:filter(Pred, tables_starting_with(N++"_"))];
        ["","",N,"","","",""] -> 
            [{node(),schema(),list_to_atom(N)}];
        [[$c,$s,$v,$$|Rest],_,N,"","","",""] ->
            [{node(),list_to_binary([$c,$s,$v,$$|Rest]),list_to_binary(N)}];
        [S,".",N,"","","",""] -> 
            [{node(),list_to_atom(S),list_to_atom(N)}]
    end.

node_from_table(Table) ->
    [_,_,_,_,_,_,Shard] = parse_table_name(Table),
    node_from_shard(Shard).

node_from_shard("") -> node();
node_from_shard("local") -> node();
node_from_shard(Shard) ->
    case node_shard() of
        Shard ->   
            node();
        _ ->
            Pred = fun({Sh,_Nd}) -> (Sh==Shard) end,
            case lists:filter(Pred, [{rpc:call(N, imem_meta, node_shard, []),N} || {_S,N} <- data_nodes(), N=/=node()]) of
                [] -> ?ClientError({"Node not found for shard",Shard});
                [{Shard,Node}] -> Node;
                Other ->  ?ClientError({"Multiple nodes found",Other})
            end
    end.

cluster_table_names("", BaseName, "") ->
    [{N,S,TN} || {N,S,TN,_} <- cluster_table_names("", BaseName)];
cluster_table_names(Schema, BaseName, "") ->
    Pred = fun({_Nd,S,_Name,_Sh}) -> (S==list_to_existing_atom(Schema)) end,
    [{N,S,TN} || {N,S,TN,_} <- lists:filter(Pred, cluster_table_names(Schema, BaseName))];
cluster_table_names("", BaseName, Shard) ->
    Pred = fun({_Nd,_S,_Name,Sh}) -> (Sh==Shard) end,
    [{N,S,TN} || {N,S,TN,_} <- lists:filter(Pred, cluster_table_names("", BaseName))];
cluster_table_names(Schema, BaseName, Shard) ->
    Pred = fun({_Nd,S,_Name,Sh}) -> (Sh==Shard andalso S==list_to_existing_atom(Schema)) end,
    [{N,S,TN} || {N,S,TN,_} <- lists:filter(Pred, cluster_table_names(Schema, BaseName))].

cluster_table_names(Schema, BaseName) ->
    case table_opts(Schema, BaseName++"@"++ node_shard()) of
        undefined ->
            case table_opts(Schema, BaseName) of 
                undefined -> [];
                _Opts1 ->           % table exists locally or is a virtual type table
                    [{node(), default_schema(Schema), list_to_atom(BaseName), node_shard()} | 
                        [remote_non_sharded(N, S, BaseName) || {S,N} <- data_nodes(), N=/=node()]
                    ]
            end;
        _Opts2 ->       % table exists locally or is a virtual type table
            [{node(), default_schema(Schema), list_to_atom(BaseName++"@"++node_shard()), node_shard()} | 
                [remote_sharded(N, S, BaseName) || {S,N} <- data_nodes(), N=/=node()]
            ]
    end.

default_schema("") -> schema();
default_schema(Schema) -> list_to_existing_atom(Schema).

default_schema_str("") -> atom_to_list(schema());
default_schema_str(Schema) -> Schema.

remote_non_sharded(Node, Schema, BaseName) -> 
    Shard = rpc:call(Node, imem_meta, node_shard, []),
    {Node, Schema, list_to_atom(BaseName), Shard}.

remote_sharded(Node, Schema, BaseName) -> 
    Shard = rpc:call(Node, imem_meta, node_shard, []),
    {Node, Schema, list_to_atom(BaseName++"@"++Shard), Shard}.

-spec partitioned_table_name(ddTable(), any()) -> [ddMnesiaTable()].
partitioned_table_name(TableAlias, Key) ->
    list_to_atom(partitioned_table_name_str(TableAlias, Key)).

-spec partitioned_table_name_str(ddTable(), any()) -> ddString().
partitioned_table_name_str(TableAlias, Key) when is_atom(TableAlias) ->
    partitioned_table_name_str(atom_to_list(TableAlias), Key);
partitioned_table_name_str(TableAlias, Key) when is_list(TableAlias) ->
    TableAliasRev = lists:reverse(TableAlias),
    case TableAliasRev of
        [$@|_] ->       
            [[$@|RN]|_] = string:tokens(TableAliasRev, "_"),
            PL = length(RN), 
            case catch list_to_integer(lists:reverse(RN)) of
                P  when is_integer(P), P > 0, PL < ?PartEndDigits ->
                    % timestamp partitiond and node sharded table alias
                    PartitionEnd=integer_to_list(P*(seconds_since_epoch(Key) div P + 1)),    % {Sec,...}
                    Prefix = lists:duplicate(?PartEndDigits-length(PartitionEnd),$0),
                    {BaseName,_} = lists:split(length(TableAlias)-length(RN)-1, TableAlias),
                    lists:flatten(BaseName++Prefix++PartitionEnd++"@"++node_shard());
                _ ->
                    % unpartitiond but node sharded table alias
                    lists:flatten(TableAlias++node_shard())
            end;
        [$_,$@|_] ->       
            [[$@|RN]|_] = string:tokens(tl(TableAliasRev), "_"),
            PL = length(RN),             
            case catch list_to_integer(lists:reverse(RN)) of
                P  when is_integer(P), P > 0, PL < ?PartEndDigits ->
                    % timestamp partitioned global (not node sharded) table alias
                    PartitionEnd=integer_to_list(P*(seconds_since_epoch(Key) div P + 1)),    % {Sec,...}
                    Prefix = lists:duplicate(?PartEndDigits-length(PartitionEnd),$0),
                    {BaseName,_} = lists:split(length(TableAlias)-length(RN)-2, TableAlias),
                    lists:flatten(BaseName++Prefix++PartitionEnd++"@_" );
                _ ->
                    % unpartitiond global table alias (a normal table)
                    TableAlias
            end;
        _ ->
            case string:tokens(TableAlias, "@") of 
                [_] -> 
                    TableAlias;     % simple table name
                [N,S] ->
                    case lists:member(S,node_shards()) of
                        false ->    
                            TableAlias;
                        true ->
                            [RN|_] = string:tokens(lists:reverse(N), "_"),
                            PL = length(RN), 
                            case catch list_to_integer(lists:reverse(RN)) of
                                P  when is_integer(P), P > 0, PL < ?PartEndDigits ->
                                    % timestamp partitiond and node sharded table alias
                                    PartitionEnd=integer_to_list(P*(seconds_since_epoch(Key) div P + 1)),    % {Sec,...}
                                    Prefix = lists:duplicate(?PartEndDigits-length(PartitionEnd),$0),
                                    {BaseName,_} = lists:split(length(N)-length(RN), N),
                                    lists:flatten(BaseName++Prefix++PartitionEnd++"@"++S);
                                _ ->
                                    TableAlias      % physical node sharded table name
                            end
                    end
            end
    end.

-spec qualified_table_names(ddTable()) -> [ddQualifiedTable()].
qualified_table_names(Table) ->
    case qualified_table_name(Table) of
        {?CSV_SCHEMA_PATTERN = S,T} when is_binary(T) -> [{S,T}];
        {Schema,Alias} -> [{Schema,PTN} || PTN <- physical_table_names(Alias)]
    end.

-spec qualified_table_name(ddTable()) -> ddQualifiedTable().
qualified_table_name({_Node,Schema,Table}) ->
    qualified_table_name({Schema,Table}) ;
qualified_table_name({undefined,Table}) when is_atom(Table) ->              {schema(), Table};
qualified_table_name(Table) when is_atom(Table) ->                          {schema(), Table};
qualified_table_name({Schema,Table}) when is_atom(Schema),is_atom(Table) -> {Schema, Table};
qualified_table_name({undefined,T}) when is_binary(T) ->
    try
        {schema(),?binary_to_existing_atom(T)}
    catch
        _:_ -> ?ClientError({"Unknown Table name",T})
    end;
qualified_table_name({?CSV_SCHEMA_PATTERN = S,T}) when is_binary(T) ->
    {S,T};
qualified_table_name({S,T}) when is_binary(S),is_binary(T) ->
    try
        {?binary_to_existing_atom(S),?binary_to_existing_atom(T)}
    catch
        _:_ -> ?ClientError({"Unknown Schema or Table name",{S,T}})
    end;
qualified_table_name(Table) when is_binary(Table) ->                        
    qualified_table_name(imem_sql_expr:binstr_to_qname2(Table)).

-spec qualified_new_table_name(ddTable()) -> ddQualifiedTable().
qualified_new_table_name({_Node,Schema, Table}) ->
    qualified_new_table_name({Schema, Table});
qualified_new_table_name({undefined, Table}) when is_atom(Table) ->               {schema(), Table};
qualified_new_table_name({undefined, Table}) when is_binary(Table) ->             {schema(), binary_to_atom(Table, utf8)};
qualified_new_table_name({Schema, Table}) when is_atom(Schema), is_atom(Table) -> {Schema, Table};
qualified_new_table_name({S, T}) when is_binary(S), is_binary(T) ->               {binary_to_atom(S, utf8), binary_to_atom(T, utf8)};
qualified_new_table_name(Table) when is_atom(Table) ->                            {schema(), Table};
qualified_new_table_name(Table) when is_binary(Table) ->
    qualified_new_table_name(imem_sql_expr:binstr_to_qname2(Table)).

-spec tables_starting_with(ddSimpleTable()) -> [ddMnesiaTable()].
tables_starting_with(Prefix) when is_atom(Prefix) ->
    tables_starting_with(atom_to_list(Prefix));
tables_starting_with(Prefix) when is_binary(Prefix) ->
    tables_starting_with(binary_to_list(Prefix));
tables_starting_with(Prefix) when is_list(Prefix) ->
    atoms_starting_with(Prefix, all_tables()).

-spec atoms_starting_with(ddString(), [atom()]) -> [atom()].
atoms_starting_with(Prefix, Atoms) ->
    atoms_starting_with(Prefix, Atoms, []). 

-spec atoms_starting_with(ddString(), [atom()], [atom()]) -> [atom()].
atoms_starting_with(_, [], Acc) -> lists:sort(Acc);
atoms_starting_with(Prefix, [A|Atoms], Acc) ->
    case lists:prefix(Prefix,atom_to_list(A)) of
        true ->     atoms_starting_with(Prefix,Atoms,[A|Acc]);
        false ->    atoms_starting_with(Prefix,Atoms,Acc)
    end.

-spec tables_ending_with(ddTable()) -> [atom()].
tables_ending_with(Suffix) when is_atom(Suffix) ->
    tables_ending_with(atom_to_list(Suffix));
tables_ending_with(Suffix) when is_binary(Suffix) ->
    tables_ending_with(binary_to_list(Suffix));
tables_ending_with(Suffix) when is_list(Suffix) ->
    atoms_ending_with(Suffix,all_tables()).

-spec atoms_ending_with(ddString(), [atom()]) -> [atom()].
atoms_ending_with(Suffix, Atoms) ->
    atoms_ending_with(Suffix, Atoms, []).

-spec atoms_ending_with(ddString(), [atom()], [atom()]) -> [atom()].
atoms_ending_with(_,[],Acc) -> lists:sort(Acc);
atoms_ending_with(Suffix,[A|Atoms],Acc) ->
    case lists:suffix(Suffix,atom_to_list(A)) of
        true ->     atoms_ending_with(Suffix, Atoms, [A|Acc]);
        false ->    atoms_ending_with(Suffix, Atoms, Acc)
    end.


%% one to one from imme_if -------------- HELPER FUNCTIONS ------


abort(Reason) ->
    imem_if_mnesia:abort(Reason).

schema() ->
    imem_if_mnesia:schema().

schema(Node) ->
    imem_if_mnesia:schema(Node).

failing_function([]) -> 
    {undefined,undefined, 0};
failing_function([{imem_meta,throw_exception,_,_}|STrace]) -> 
    failing_function(STrace);
failing_function([{M,N,_,FileInfo}|STrace]) ->
    case lists:prefix("imem",atom_to_list(M)) of
        true ->
            NAsBin = atom_to_binary(N, utf8),
            Line = proplists:get_value(line, FileInfo, 0),
            case re:run(NAsBin, <<"-(.+)/">>, [{capture, all_but_first, binary}]) of
                nomatch ->
                    {M, N, Line};
                {match, [FunNameBin]} ->
                    {M, binary_to_atom(FunNameBin, utf8), Line}
            end;
        false ->
            failing_function(STrace)
    end;
failing_function(_Other) ->
    ?Debug("unexpected stack trace ~p~n", [_Other]),
    {undefined,undefined, 0}.

log_to_db(Level,Module,Function,Fields,Message)  ->
    log_to_db(Level,Module,Function,Fields,Message,[]).

log_to_db(Level,Module,Function,Fields,Message,Stacktrace) ->
    BinStr = try 
        list_to_binary(Message)
    catch
        _:_ ->  list_to_binary(lists:flatten(io_lib:format("~tp",[Message])))
    end,
    log_to_db(Level,Module,Function,0,Fields,BinStr,Stacktrace).

log_to_db(Level,Module,Function,Line,Fields,Message,StackTrace)
when is_atom(Level)
    , is_atom(Module)
    , is_atom(Function)
    , is_integer(Line)
    , is_list(Fields)
    , is_binary(Message)
    , is_list(StackTrace) ->
    LogRec = #ddLog{logTime=?TIME_UID,logLevel=Level,pid=self()
                    ,module=Module,function=Function,line=Line,node=node()
                    ,fields=Fields,message=Message,stacktrace=StackTrace
                    },
    dirty_write(?LOG_TABLE, LogRec).

-spec log_slow_process(atom(), atom(), ddTimestamp(), integer(), integer(), list()) -> ok.
log_slow_process(Module, Function, StartTimestamp, LimitWarning, LimitError, Fields) ->
    DurationMs = imem_datatype:msec_diff(StartTimestamp),
    if 
        DurationMs < LimitWarning ->    ok;
        DurationMs < LimitError ->      log_to_db(warning,Module,Function,Fields,"slow_process",[]);
        true ->                         log_to_db(error,Module,Function,Fields,"slow_process",[])
    end.

%% imem_if but security context added --- META INFORMATION ------

data_nodes() ->
    imem_if_mnesia:data_nodes().

all_tables() ->
    imem_if_mnesia:all_tables().

all_aliases() ->
    MySchema = schema(),
    [A || #ddAlias{qname={S,A}} <- imem_if_mnesia:read(ddAlias),S==MySchema].

is_readable_table({_Node,_Schema,Table}) ->
    is_readable_table(Table);   %% ToDo: may depend on schema
is_readable_table({_Schema,Table}) ->
    is_readable_table(Table);   %% ToDo: may depend on schema
is_readable_table(Table) ->
    imem_if_mnesia:is_readable_table(Table).

is_virtual_table({_Node,_Schema,Table}) ->
    is_virtual_table(Table);   %% ToDo: may depend on schema
is_virtual_table({_Schema,Table}) ->
    is_virtual_table(Table);   %% ToDo: may depend on schema
is_virtual_table(Table) ->
    lists:member(Table,?VirtualTables).

node_shard() ->
    case application:get_env(imem, node_shard) of
        {ok,NS} when is_list(NS) ->      NS;
        {ok,NI} when is_integer(NI) ->   integer_to_list(NI);
        undefined ->                     node_hash(node());    
        {ok,node_shard_fun} ->  
            try 
                node_shard_value(application:get_env(imem, node_shard_fun),node())
            catch
                _:_ ->  ?Error("bad config parameter ~p~n", [node_shard_fun]),
                        "nohost"
            end;
        {ok,host_name} ->                host_name(node());    
        {ok,host_fqdn} ->                host_fqdn(node());    
        {ok,node_name} ->                node_name(node());    
        {ok,node_hash} ->                node_hash(node());    
        {ok,NA} when is_atom(NA) ->      atom_to_list(NA);
        _Else ->    ?Error("bad config parameter ~p ~p~n", [node_shard, _Else]),
                    node_hash(node())
    end.

node_shards() ->
    DataNodes = [DN || {_Schema,DN} <- data_nodes()],
    case application:get_env(imem, node_shard) of
        undefined ->         [node_hash(N1) || N1 <- DataNodes];    
        {ok,node_shard_fun} ->  
            try 
                [node_shard_value(application:get_env(imem, node_shard_fun),N2) || N2 <- DataNodes]
            catch
                _:_ ->  ?Error("bad config parameter ~p~n", [node_shard_fun]),
                        ["nohost"]
            end;
        {ok,host_name} ->   [host_name(N3) || N3 <- DataNodes];    
        {ok,host_fqdn} ->   [host_fqdn(N4) || N4 <- DataNodes];    
        {ok,node_name} ->   [node_name(N5) || N5 <- DataNodes];    
        {ok,node_hash} ->   [node_hash(N6) || N6 <- DataNodes];    
        {ok,O} when is_list(O);is_integer(O);is_atom(O) -> 
                            [rpc:call(N7, imem_meta, node_shard, []) || N7 <- DataNodes];
        _Else ->            ?Error("bad config parameter ~p ~p~n", [node_shard, _Else]),
                            [node_hash(N8) || N8 <- DataNodes]
    end.

node_shard_value({ok,FunStr},Node) ->
    % ?Debug("node_shard calculated for ~p~n", [FunStr]),
    Code = case [lists:last(string:strip(FunStr))] of
        "." -> FunStr;
        _ -> FunStr++"."
    end,
    {ok,ErlTokens,_}=erl_scan:string(Code),    
    {ok,ErlAbsForm}=erl_parse:parse_exprs(ErlTokens),    
    {value,Value,_}=erl_eval:exprs(ErlAbsForm,[]),    
    Result = Value(Node),
    % ?Debug("node_shard_value ~p~n", [Result]),
    Result.

host_fqdn(Node) when is_atom(Node) -> 
    NodeStr = atom_to_list(Node),
    [_,Fqdn] = string:tokens(NodeStr, "@"),
    Fqdn.

host_name(Node) when is_atom(Node) -> 
    [Host|_] = string:tokens(host_fqdn(Node), "."),
    Host.

clean_host_name(Node) when is_atom(Node) -> 
    clean_host_name(host_name(Node),[]).

clean_host_name([], Acc) -> lists:reverse(Acc);
clean_host_name([Ch|Name], Acc) when Ch >= $@, Ch=<$Z ->
    clean_host_name(Name, [Ch|Acc]);
clean_host_name([Ch|Name], Acc) when Ch >= $a, Ch=<$z ->
    clean_host_name(Name, [Ch|Acc]);
clean_host_name([Ch|Name], Acc) when Ch >= $0, Ch=<$9 ->
    clean_host_name(Name, [Ch|Acc]);
clean_host_name([$_|Name], Acc) ->
    clean_host_name(Name, [$_|Acc]);
clean_host_name([_|Name], Acc) ->
    clean_host_name(Name, Acc).

node_name(Node) when is_atom(Node) -> 
    NodeStr = atom_to_list(Node),
    [Name,_] = string:tokens(NodeStr, "@"),
    Name.

node_hash(Node) when is_atom(Node) ->
    io_lib:format("~6.6.0w",[erlang:phash2(Node, 1000000)]).

nodes() ->
    lists:filter(
      fun(Node) ->
              case rpc:call(Node, erlang, system_info, [version], 1500) of
                  {badrpc, _} -> false;
                  _ -> true
              end
      end, erlang:nodes()).

record_hash(Rec,PosList) when is_tuple(Rec), is_list(PosList) ->
    TupleToHash = list_to_tuple([element(N,Rec) || N <- PosList]),
    list_to_binary(io_lib:format("~.36B",[erlang:phash2(TupleToHash)])).

-spec trigger_infos(atom()|{atom(),atom()}) -> {TableType :: atom(), DefaultRecord :: tuple(), TriggerFun :: function()}.
trigger_infos(Table) when is_atom(Table) ->
    trigger_infos({schema(),Table});
trigger_infos({Schema,Table}) when is_atom(Schema),is_atom(Table) ->
    Key = {?MODULE,trigger,Schema,Table},
    case imem_cache:read(Key) of 
        [] ->
            case imem_if_mnesia:read(ddTable,{Schema, Table}) of
                [] ->
                    ?ClientError({"Table does not exist",{Schema, Table}}); 
                [#ddTable{columns=ColumnInfos,opts=Opts}] ->
                    TableType = case lists:keyfind(type,1,Opts) of
                        false ->    set;
                        {_,Type} -> Type
                    end,
                    RecordName = case lists:keyfind(record_name,1,Opts) of
                        false ->    Table;
                        {_,Name} -> Name
                    end,
                    DefRec = [RecordName|column_info_items(ColumnInfos, default_fun)],
                    IdxPlan = case lists:keyfind(index,1,Opts) of
                        false ->    #ddIdxPlan{};
                        {_,Def} ->  compiled_index_plan(Def,ColumnInfos)
                    end,
                    Trigger = case {IdxPlan,lists:keyfind(trigger,1,Opts)} of
                        {#ddIdxPlan{def=[]},false} ->     %% no trigger actions needed
                            fun(_Old,_New,_Tab,_User,_TrOpts) -> ok end;
                        {_,false} ->                %% only index maintenance trigger needed
                            fun(Old,New,Tab,User,_TrOpts) -> imem_meta:update_index(Old,New,Tab,User,[],IdxPlan) end;
                        {_,{_,TFun}} ->             %% custom trigger and index maintenance needed
                            TriggerWithIndexing = trigger_with_indexing(TFun,<<"imem_meta:update_index">>,<<"IdxPlan">>),
                            imem_datatype:io_to_fun(TriggerWithIndexing,?DD_TRIGGER_ARITY,[{'IdxPlan',IdxPlan}])    % 
                    end,
                    Result = {TableType, DefRec, Trigger},
                    imem_cache:write(Key,Result),
                    % ?LogDebug("trigger_infos ~p",[Result]),
                    Result
            end;
        [{TT, DR, TR}] -> {TT, DR, TR};
        Other -> ?SystemException({"Unexpected trigger format", Other})
    end.

compiled_index_plan(IdxDef,ColumnInfos) ->
    FieldMap = [ {list_to_binary(atom_to_list(Name)),Pos+?FirstIdx-1} 
                 || {#ddColumn{name=Name},Pos} <- lists:zip(ColumnInfos,lists:seq(1,length(ColumnInfos)))
               ],
    % ?Info("FieldMap ~n~p",[FieldMap]),
    Def = [D#ddIdxDef{ pl=compile_path_list(PL,FieldMap) 
                     , vnf=compile_or_generate(Vnf)
                     , iff=compile_or_generate(Iff)
                     } 
           || #ddIdxDef{pl=PL,vnf=Vnf,iff=Iff}=D <- IdxDef
          ],
    JPos = lists:usort(lists:flatten([json_pos_list(PL) || #ddIdxDef{pl=PL} <- Def])),
    #ddIdxPlan{def=Def,jpos=JPos}.

-spec compile_or_generate(binary()|function()) -> function().
compile_or_generate(Source) when is_binary(Source) ->
    compile_or_generate(imem_datatype:io_to_fun(Source));   %% compile the source code
compile_or_generate(IdxFun) when is_function(IdxFun,1) ->
    IdxFun;                                                 %% return the arity 1 function for vnf/1
compile_or_generate(IdxFun) when is_function(IdxFun,2) ->
    IdxFun;                                                 %% return the arity 2 function for iff/2 or vnf/2
compile_or_generate(IdxFun) when is_function(IdxFun,0) ->
    IdxFun();                                               %% arity 0 function is a function generator
compile_or_generate(Bad) ->
    ?ClientError({"Bad index fun",Bad}).

json_pos_list(PathList) -> 
    json_pos_list(PathList,[]).

json_pos_list([],Acc) -> Acc;
json_pos_list([{_ParseTreeTuple,FM}|Rest],Acc) -> json_pos_list(Rest,Acc++[Pos || {_Name,Pos} <- FM]);
json_pos_list([_|Rest],Acc) ->  json_pos_list(Rest,Acc).

compile_path_list([],_FieldMap) -> [];
compile_path_list([JsonPath|PL],FieldMap) ->
    [compile_json_path(JsonPath,FieldMap) | compile_path_list(PL,FieldMap)].

compile_json_path(JsonPath,FieldMap) when is_binary(JsonPath) ->
    JPath = case jpparse:parsetree(JsonPath) of
        {ok,ParseTreeBinary} when is_binary(ParseTreeBinary) -> {jp, ParseTreeBinary};
        {ok,ParseTreeTuple} when is_tuple(ParseTreeTuple) -> {jp, ParseTreeTuple};
        [{parse_error,Reason}] ->   ?ClientError({"Cannot parse JSON path",{JsonPath,Reason}})
    end,
    compile_json_path(JPath, FieldMap);
compile_json_path({jp, ParseTreeBinary}, FieldMap) when is_binary(ParseTreeBinary) ->
    %% this is a simple record field name, not a parse tree, return index position
    case lists:keyfind(ParseTreeBinary,1,FieldMap) of
        false ->    ?ClientError({"Unknown column name",ParseTreeBinary});
        {_,Pos} ->  Pos             %% represent field as record index (integer)
    end;
compile_json_path({jp, ParseTreeTuple}, FieldMap) when is_tuple(ParseTreeTuple) ->
    %% this is a json path, return {ParseTreeTuple,UsedFieldMap}
    {ok, FieldList} = jpparse_fold:roots(ParseTreeTuple),
    Pred = fun({Name,_Pos}) -> lists:member(Name,FieldList) end,
    case lists:filter(Pred, FieldMap) of
        [] ->
            {ok, JsonPath} = jpparse_fold:string(ParseTreeTuple),
            ?ClientError({"Unknown JSON document name in ", JsonPath});
        FM ->
            {ParseTreeTuple,FM}
    end;
compile_json_path(JsonPath,FieldMap) when is_tuple(JsonPath) ->
    compile_json_path({jp, JsonPath}, FieldMap).

trigger_with_indexing(TFun,MF,Var) ->
    case re:run(TFun, "fun\\((.*)\\)[ ]*\->(.*)end.", [global, {capture, [1,2], binary}, dotall]) of
        {match,[[Params,Body0]]} ->
            case binary:match(Body0,MF) of
                nomatch ->    <<"fun(",Params/binary,") ->",Body0/binary,", ",MF/binary,"(",Params/binary,",",Var/binary,") end." >>;
                {_,_} ->     TFun
            end
    end.

-spec table_type(ddTable()) -> atom().
table_type({ddSysConf, Table}) ->               imem_if_sys_conf:table_type(Table);
table_type({_Node,_Schema,Table}) ->            table_type(Table);                          %% ToDo: may depend on schema
table_type({_Schema,Table}) ->                  table_type(Table);                          %% ToDo: may depend on schema
table_type(Table)  ->                           imem_if_mnesia:table_type(physical_table_name(Table)).

-spec table_record_name(ddTable()) -> atom().
table_record_name({ddSysConf,Table}) ->         imem_if_sys_conf:table_record_name(Table);  %% ToDo: may depend on schema
table_record_name({?CSV_SCHEMA_PATTERN,_Table}) -> ?CSV_RECORD_NAME;
table_record_name({_Node,?CSV_SCHEMA_PATTERN,_Table}) -> ?CSV_RECORD_NAME;
table_record_name({_Schema,Table}) ->           table_record_name(Table);                   %% ToDo: may depend on schema
table_record_name({_Node,_Schema,Table}) ->     table_record_name(Table);                   %% ToDo: may depend on schema
table_record_name(ddNode)  ->                   ddNode;
table_record_name(ddSnap)  ->                   ddSnap;
table_record_name(ddSchema)  ->                 ddSchema;
table_record_name(ddSize)  ->                   ddSize;
table_record_name(ddVersion)  ->                ddVersion;
table_record_name(Table) ->
    PTN = physical_table_name(Table),
    case is_virtual_table(PTN) of
        true ->     Table; 
        false ->    imem_if_mnesia:table_record_name(PTN)
    end.

-spec table_columns(ddTable()) -> integer().
table_columns({ddSysConf,Table}) ->             imem_if_sys_conf:table_columns(Table);
table_columns({_Node,_Schema,Table}) ->         table_columns(Table);       %% ToDo: may depend on schema
table_columns({_Schema,Table}) ->               table_columns(Table);       %% ToDo: may depend on schema
table_columns(Table) ->                         imem_if_mnesia:table_columns(physical_table_name(Table)).


-spec table_size(ddTable()) -> integer().
table_size({ddSysConf,_Table}) ->               0; %% ToDo: implement imem_if_sys_conf:table_size(Table)
table_size({_Node,_Schema,Table}) ->            table_size(Table);      %% ToDo: may depend on schema
table_size({_Schema,Table}) ->                  table_size(Table);      %% ToDo: may depend on schema
table_size(ddNode) ->                           length(read(ddNode));
table_size(ddSnap) ->                           imem_snap:snap_file_count();
table_size(ddSchema) ->                         length(read(ddSchema));
table_size(ddSize) ->                           1;
table_size(ddVersion) ->                        0;
table_size(Table) ->                            %% ToDo: for an Alias, sum should be returned for all local time partitions
                                                imem_if_mnesia:table_size(physical_table_name(Table)).

-spec table_memory(ddTable()) ->                integer().
table_memory({ddSysConf,_Table}) ->             0;          %% ToDo: implement imem_if_sys_conf:table_memory(Table)                  
table_memory({_Node,_Schema,Table}) ->          table_memory(Table);   %% ToDo: may depend on schema
table_memory({_Schema,Table}) ->                table_memory(Table);   %% ToDo: may depend on schema
table_memory(Table) ->                          imem_if_mnesia:table_memory(physical_table_name(Table)). %% ToDo: sum should be returned for all local time partitions

exec(Statement, BlockSize, Schema) ->
    imem_sql:exec(none, Statement, BlockSize, Schema, false).   

fetch_recs(Pid, Sock, Timeout) ->
    imem_statement:fetch_recs(none, Pid, Sock, Timeout, false).

fetch_recs_sort(Pid, Sock, Timeout) ->
    imem_statement:fetch_recs_sort(none, Pid, Sock, Timeout, false).

fetch_recs_async(Pid, Sock) ->
    imem_statement:fetch_recs_async(none, Pid, Sock, false).

fetch_recs_async(Opts, Pid, Sock) ->
    imem_statement:fetch_recs_async(none, Pid, Sock, Opts, false).

filter_and_sort(Pid, FilterSpec, SortSpec) ->
    imem_statement:filter_and_sort(none, Pid, FilterSpec, SortSpec, false).

filter_and_sort(Pid, FilterSpec, SortSpec, Cols) ->
    imem_statement:filter_and_sort(none, Pid, FilterSpec, SortSpec, Cols, false).

fetch_close(Pid) ->
    imem_statement:fetch_close(none, Pid, false).

update_prepare(Tables, ColMap, ChangeList) ->
    imem_statement:update_prepare(false, none, Tables, ColMap, ChangeList).

update_cursor_prepare(Pid, ChangeList) ->
    imem_statement:update_cursor_prepare(none, Pid, false, ChangeList).

update_cursor_execute(Pid, Lock) ->
    imem_statement:update_cursor_execute(none, Pid, false, Lock).

apply_defaults(DefRec, Rec) when is_tuple(DefRec) ->
    apply_defaults(tuple_to_list(DefRec), Rec);
apply_defaults(DefRec, Rec) when is_list(DefRec), is_tuple(Rec) ->
    apply_defaults(DefRec, Rec, 1).

apply_defaults([], Rec, _) -> Rec;
apply_defaults([D|DefRec], Rec0, N) ->
    Rec1 = case {element(N,Rec0),is_function(D),is_function(D,0)} of
        {?nav,true,true} ->     setelement(N,Rec0,D());
        {?nav,false,false} ->   setelement(N,Rec0,D);
        _ ->                    Rec0
    end,
    apply_defaults(DefRec, Rec1, N+1).

apply_validators(DefRec, Rec, Table) ->
    apply_validators(DefRec, Rec, Table, meta_field_value(user)).

apply_validators(DefRec, Rec, Table, User) when is_tuple(DefRec) ->
    apply_validators(tuple_to_list(DefRec), Rec, Table, User);
apply_validators(DefRec, Rec, Table, User) when is_list(DefRec), is_tuple(Rec) ->
    apply_validators(DefRec, Rec, Table, User, 1).

apply_validators([], Rec, _, _, _) -> Rec;
apply_validators([D|DefRec], Rec0, Table, User, N) ->
    Rec1 = if 
        is_function(D,1) -> setelement(N,Rec0,D(element(N,Rec0)));  %% Params=[Field]
        is_function(D,2) -> setelement(N,Rec0,D(element(N,Rec0),Rec0));  %% Params=[Field,Rec]
        is_function(D,3) -> setelement(N,Rec0,D(element(N,Rec0),Rec0,Table));  %% Params=[Field,Rec,Table]
        is_function(D,4) -> setelement(N,Rec0,D(element(N,Rec0),Rec0,Table,User));  %% Params=[Field,Rec,Table,User]
        true ->             Rec0
    end,
    apply_validators(DefRec, Rec1, Table, User, N+1).

fetch_start(Pid, {ddSysConf,Table}, MatchSpec, BlockSize, Opts) ->
    imem_if_sys_conf:fetch_start(Pid, Table, MatchSpec, BlockSize, Opts);
fetch_start(Pid, {_Node,?CSV_SCHEMA_PATTERN = S,FileName}, MatchSpec, BlockSize, Opts) ->
    imem_if_csv:fetch_start(Pid, {S,FileName}, MatchSpec, BlockSize, Opts);
fetch_start(Pid, {?CSV_SCHEMA_PATTERN = S,FileName}, MatchSpec, BlockSize, Opts) ->
    imem_if_csv:fetch_start(Pid, {S,FileName}, MatchSpec, BlockSize, Opts);
fetch_start(Pid, {_Node,Schema,Table}, MatchSpec, BlockSize, Opts) ->
    fetch_start(Pid, {Schema,Table}, MatchSpec, BlockSize, Opts); 
fetch_start(Pid, {_Schema,Table}, MatchSpec, BlockSize, Opts) ->
    fetch_start(Pid, Table, MatchSpec, BlockSize, Opts);          %% ToDo: may depend on schema
fetch_start(Pid, Tab, MatchSpec, BlockSize, Opts) when 
        Tab==ddNode;Tab==ddSnap;Tab==ddSchema;Tab==ddSize;Tab==ddVersion  -> 
    fetch_start_calculated(Pid, Tab, MatchSpec, BlockSize, Opts);
fetch_start(Pid, Table, MatchSpec, BlockSize, Opts) ->
    imem_if_mnesia:fetch_start(Pid, physical_table_name(Table), MatchSpec, BlockSize, Opts).

fetch_start_calculated(Pid, VTable, MatchSpec, _BlockSize, _Opts) ->
    Limit = ?VIRTUAL_TABLE_ROW_LIMIT,
    % ?LogDebug("fetch_start_calculated matchspec ~n~p",[MatchSpec]),
    % ?LogDebug("fetch_start_calculated limit ~n~p",[Limit]),
    {Rows,true} = select(VTable, MatchSpec, Limit),
    spawn(
        fun() ->
            receive
                abort ->    ok;
                next ->     Pid ! {row, [?sot,?eot|Rows]}
            end
        end
    ).

fetch_start_virtual(Pid, _Table, Rows, _BlockSize, _Limit, _Opts) ->
    % ?LogDebug("fetch_start_virtual table rows ~p:~n~p",[Table, Rows]),
    % ?LogDebug("fetch_start_virtual limit = ~p",[_Limit]),
    spawn(
        fun() ->
            receive
                abort ->    ok;
                next ->     Pid ! {row, [?sot,?eot|Rows]}
            end
        end
    ).


close(Pid) ->
    imem_statement:close(none, Pid).

read({ddSysConf,_Table}) -> 
    % imem_if_sys_conf:read(physical_table_name(Table));
    ?UnimplementedException({"Cannot read from ddSysConf schema, use DDerl GUI instead"});
read({_Node,Schema,Table}) -> 
    read({Schema,Table});
read({_Schema,Table}) -> 
    read(Table);                %% ToDo: may depend on schema
read(ddNode) ->
    lists:flatten([read(ddNode,Node) || Node <- [node()|erlang:nodes()]]);
read(ddSnap) ->
    {bkp, BkpInfo} = imem_snap:info(bkp),
    {zip, ZipInfo} = imem_snap:info(zip),    
    [#ddSnap{file = list_to_binary(N), type = bkp, size = S, lastModified = D}
     || {N,S,D} <- proplists:get_value(snaptables, BkpInfo)]
    ++
    [begin
         {ok, #file_info{size=S, mtime=D}} = file:read_file_info(N),
         #ddSnap{file = list_to_binary(filename:basename(N)), type = zip, size = S, lastModified = D}
     end || {N,_} <- ZipInfo];
read(ddSchema) ->
    [{ddSchema,{Schema,Node},[]} || {Schema,Node} <- data_nodes()];
read(ddSize) ->
    [hd(read(ddSize,Name)) || Name <- all_tables()];
read(ddVersion) ->
    imem:all_apps_version_info();
read(Table) ->
    imem_if_mnesia:read(physical_table_name(Table)).

read({ddSysConf,Table}, _Key) -> 
    % imem_if_sys_conf:read(physical_table_name(Table),Key);
    ?UnimplementedException({"Cannot read from ddSysConf schema, use DDerl GUI instead",Table});
read({_Node,Schema,Table}, Key) ->
    read({Schema,Table}, Key);
read({_Schema,Table}, Key) ->
    read(Table, Key);           %% ToDo: may depend on schema
read(ddNode, Node) when is_atom(Node) ->
    case rpc:call(Node, erlang, statistics, [wall_clock], ?DDNODE_TIMEOUT) of
        {WC, WCDiff} when is_integer(WC), is_integer(WCDiff) ->
            case rpc:call(Node, imem_meta, time, [], ?DDNODE_TIMEOUT) of
                {Sec, Mic} when is_integer(Sec), is_integer(Mic) ->                        
                    [#ddNode{ name=Node
                             , wall_clock=WC
                             , time={Sec, Mic}
                             , extra=[]     
                             }       
                    ];
                _ ->    
                    []
            end;
         _ -> 
            []
    end;
read(ddNode,_) -> [];
read(ddSchema,Key) when is_tuple(Key) ->
    [ S || #ddSchema{schemaNode=K} = S <- read(ddSchema), K==Key];
read(ddSchema,_) -> [];
read(ddVersion, App) ->
    imem:all_apps_version_info([{apps,[App]}]);
read(ddSize,Table) ->
    PTN =  physical_table_name(Table),
    case is_time_partitioned_table(PTN) of 
        true ->
            case (catch {table_size(PTN),table_memory(PTN), time_of_partition_expiry(PTN),time_to_partition_expiry(PTN)}) of
                {S,M,E,T} when is_integer(S),is_integer(M) -> 
                    [{ddSize,PTN,S,M,E,T}];
                _ ->                    
                    [{ddSize,PTN,undefined,undefined,undefined,undefined}]
            end;
        false ->
            case (catch {table_size(PTN),table_memory(PTN)}) of
                {S,M} when is_integer(S),is_integer(M) -> 
                    [{ddSize,PTN,S,M,undefined,undefined}];
                _ ->                    
                    [{ddSize,PTN,undefined,undefined,undefined,undefined}]
            end
    end;            
read(Table, Key) -> 
    imem_if_mnesia:read(physical_table_name(Table), Key).

dirty_read(Table) -> imem_if_mnesia:dirty_read(physical_table_name(Table)).

dirty_read({ddSysConf,Table}, Key) ->       read({ddSysConf,Table}, Key);
dirty_read({_Node,Schema,Table}, Key) ->    dirty_read({Schema,Table}, Key);
dirty_read({_Schema,Table}, Key) ->         dirty_read(Table, Key); %% ToDo: may depend on schema
dirty_read(ddNode,Node) ->                  read(ddNode,Node); 
dirty_read(ddSchema,Key) ->                 read(ddSchema,Key);
dirty_read(ddSize,Table) ->                 read(ddSize,Table);
dirty_read(ddVersion,App) ->                read(ddVersion,App);
dirty_read(Table, Key) ->                   imem_if_mnesia:dirty_read(physical_table_name(Table), Key).

dirty_index_read({_Node,Schema,Table}, SecKey,Index) -> 
    dirty_index_read({Schema,Table}, SecKey, Index);
dirty_index_read({_Schema,Table}, SecKey,Index) -> 
    dirty_index_read(Table, SecKey, Index);     %% ToDo: may depend on schema
dirty_index_read(Table, SecKey, Index) ->   
    imem_if_mnesia:dirty_index_read(physical_table_name(Table), SecKey, Index).

read_hlk({_Node,Schema,Table}, HListKey) -> 
    read_hlk({Schema,Table}, HListKey);
read_hlk({_Schema,Table}, HListKey) -> 
    read_hlk(Table, HListKey);                              %% ToDo: may depend on schema
read_hlk(Table,HListKey) ->
    imem_if_mnesia:read_hlk(Table,HListKey).


get_config_hlk(Table, Key, Owner, Context, Default, _Documentation) ->
    get_config_hlk(Table, Key, Owner, Context, Default).
get_config_hlk({_Node,Schema,Table}, Key, Owner, Context, Default) ->
    get_config_hlk({Schema,Table}, Key, Owner, Context, Default);
get_config_hlk({_Schema,Table}, Key, Owner, Context, Default) ->
    get_config_hlk(Table, Key, Owner, Context, Default);    %% ToDo: may depend on schema
get_config_hlk(Table, Key, Owner, Context, Default) when is_atom(Table), is_list(Context), is_atom(Owner) ->
    Remark = list_to_binary(["auto_provisioned from ",io_lib:format("~p",[Context])]),
    case (catch read_hlk(Table, [Key|Context])) of
        [] ->                                   
            %% no value found, create global config with default value
            catch put_config_hlk(Table, Key, Owner, [], Default, Remark),
            Default;
        [#ddConfig{val=Default, hkl=[Key]}] ->    
            %% global config is relevant and matches default
            Default;
        [#ddConfig{val=OldVal, hkl=[Key], remark=R, owner=DefOwner}] ->
            %% global config is relevant and differs from default
            case binary:longest_common_prefix([R,<<"auto_provisioned">>]) of
                16 ->
                    %% comment starts with default comment may be overwrite
                    case {DefOwner, Owner} of
                        _ when
                                  ((?MODULE     =:= DefOwner)
                            orelse (Owner       =:= DefOwner)                            
                            orelse (undefined   =:= DefOwner)) ->
                            %% was created by imem_meta and/or same module
                            %% overwrite the default
                            catch put_config_hlk(Table, Key, Owner, [], Default, Remark),
                            Default;
                        _ ->
                            %% being accessed by non creator, protect creator's config value
                            OldVal
                    end;
                _ ->    
                    %% comment was changed by user, protect his config value
                    OldVal
            end;
        [#ddConfig{val=Val}] ->
            %% config value is overridden by user, return that value
            Val;
        _ ->
            %% fallback in case ddConf is deleted in a running system
            Default
    end.

put_config_hlk(Table, Key, Owner, Context, Value, Remark, _Documentation) ->
    put_config_hlk(Table, Key, Owner, Context, Value, Remark).
put_config_hlk({_Node,Schema,Table}, Key, Owner, Context, Value, Remark) ->
    put_config_hlk({Schema,Table}, Key, Owner, Context, Value, Remark);
put_config_hlk({_Schema,Table}, Key, Owner, Context, Value, Remark) ->
    put_config_hlk(Table, Key, Owner, Context, Value, Remark);  %% ToDo: may depend on schema
put_config_hlk(Table, Key, Owner, Context, Value, Remark) when is_atom(Table), is_list(Context), is_binary(Remark) ->
    dirty_write(Table,#ddConfig{hkl=[Key|Context], val=Value, remark=Remark, owner=Owner}).

select({ddSysConf,Table}, _MatchSpec) ->
    ?UnimplementedException({"Cannot select from ddSysConf schema, use DDerl GUI instead",Table});
select({_Node,Schema,Table}, MatchSpec) ->
    select({Schema,Table}, MatchSpec); 
select({_Schema,Table}, MatchSpec) ->
    select(Table, MatchSpec);           %% ToDo: may depend on schema
select(Table, MatchSpec) ->
    imem_if_mnesia:select(physical_table_name(Table), MatchSpec).

ets(Fun, Args) ->
    imem_if_mnesia:ets(Fun, Args).

select_count({ddSysConf,Table}, _MatchSpec) ->
    % imem_if_sys_conf:select_count(physical_table_name(Table), MatchSpec);
    ?UnimplementedException({"Cannot select_count from ddSysConf schema, use DDerl GUI instead",Table});
select_count({_Node,Schema,Table}, MatchSpec) ->
    select_count({Schema,Table}, MatchSpec);
select_count({_Schema,Table}, MatchSpec) ->
    select_count(Table, MatchSpec);           %% ToDo: may depend on schema
select_count(Table, MatchSpec) ->
    imem_if_mnesia:select_count(physical_table_name(Table), MatchSpec).

dirty_select({_Node,Schema,Table}, MatchSpec) ->
    dirty_select({Schema,Table}, MatchSpec);
dirty_select({_Schema,Table}, MatchSpec) ->
    dirty_select(Table, MatchSpec);           %% ToDo: may depend on schema
dirty_select(Table, MatchSpec) ->
    imem_if_mnesia:dirty_select(physical_table_name(Table), MatchSpec).

select(Table, MatchSpec, 0) ->
    select(Table, MatchSpec);
select({ddSysConf,Table}, _MatchSpec, _Limit) ->
    ?UnimplementedException({"Cannot select from ddSysConf schema, use DDerl GUI instead",Table});
select({_Node,Schema,Table}, MatchSpec, Limit) ->
    select({Schema,Table}, MatchSpec, Limit);
select({_Schema,Table}, MatchSpec, Limit) ->
    select(Table, MatchSpec, Limit);        %% ToDo: may depend on schema
select(Tab, MatchSpec, Limit) when
        Tab==ddNode;Tab==ddSnap;Tab==ddSchema;Tab==ddSize;Tab==ddVersion;Tab==integer ->
    select_virtual(Tab, MatchSpec, Limit);
select(Table, MatchSpec, Limit) ->
    imem_if_mnesia:select(physical_table_name(Table), MatchSpec, Limit).

select_integer_range(Min,Max,Limit) ->
    {[{integer,I} || I <- lists:seq(Min,erlang:min(Max,Min+Limit-1))],true}.

select_virtual(_Table, [{_,[false],['$_']}],_Limit) ->
    {[],true};
select_virtual(integer, [{{'_','$22','$23'},[{'and',{'>=','$22',Min},{'=<','$22',Max}}],['$_']}],Limit) ->
    select_integer_range(Min,Max,Limit);
select_virtual(integer, [{{'_','$22','$23'},[{'and',{'>','$22',Min},{'=<','$22',Max}}],['$_']}],Limit) ->
    select_integer_range(Min+1,Max,Limit);
select_virtual(integer, [{{'_','$22','$23'},[{'and',{'>=','$22',Min},{'<','$22',Max}}],['$_']}],Limit) ->
    select_integer_range(Min,Max-1,Limit);
select_virtual(integer, [{{'_','$22','$23'},[{'and',{'>','$22',Min},{'<','$22',Max}}],['$_']}],Limit) ->
    select_integer_range(Min+1,Max-1,Limit);
select_virtual(Table, [{_,[true],['$_']}],_Limit) ->
    {read(Table),true};                 %% used in select * from virtual_table
select_virtual(Table, [{_,[],['$_']}],_Limit) ->
    {read(Table),true};                 %% used in select * from virtual_table
select_virtual(Table, [{MatchHead, [Guard], ['$_']}]=MatchSpec,_Limit) ->
    Tag = element(2,MatchHead),
    % ?Info("Virtual Select Tag / MatchSpec: ~p / ~p~n", [Tag,MatchSpec]),
    Candidates = case operand_match(Tag,Guard) of
        false ->                        read(Table);
        {'==',Tag,{element,N,Tup1}} ->  % ?Info("Virtual Select Key : ~p~n", [element(N,Tup1)]),
                                        read(Table,element(N,Tup1));
        {'==',{element,N,Tup2},Tag} ->  % ?Info("Virtual Select Key : ~p~n", [element(N,Tup2)]),
                                        read(Table,element(N,Tup2));
        {'==',Tag,Val1} ->              % ?Info("Virtual Select Key : ~p~n", [Val1]),
                                        read(Table,Val1);
        {'==',Val2,Tag} ->              % ?Info("Virtual Select Key : ~p~n", [Val2]),
                                        read(Table,Val2);
        _ ->                            read(Table)
    end,
    % ?Info("Virtual Select Candidates  : ~p~n", [Candidates]),
    MS = ets:match_spec_compile(MatchSpec),
    Result = ets:match_spec_run(Candidates,MS),
    % ?Debug("Virtual Select Result  : ~p~n", [Result]),    
    {Result, true}.

%% Does this guard use the operand Tx?      TODO: Generalize from guard tree to expression tree
operand_match(Tx,{_,Tx}=C0) ->      C0;
operand_match(Tx,{_,R}) ->          operand_match(Tx,R);
operand_match(Tx,{_,Tx,_}=C1) ->    C1;
operand_match(Tx,{_,_,Tx}=C2) ->    C2;
operand_match(Tx,{_,L,R}) ->        case operand_match(Tx,L) of
                                        false ->    operand_match(Tx,R);
                                        Else ->     Else
                                    end;    
operand_match(Tx,Tx) ->             Tx;
operand_match(_,_) ->               false.

select_sort(Table, MatchSpec)->
    {L, true} = select(Table, MatchSpec),
    {lists:sort(L), true}.

select_sort(Table, MatchSpec, Limit) ->
    {Result, AllRead} = select(Table, MatchSpec, Limit),
    {lists:sort(Result), AllRead}.

write_log(Record) -> write(?LOG_TABLE, Record#ddLog{logTime=?TIME_UID}).

write({ddSysConf,TableAlias}, _Record) -> 
    % imem_if_sys_conf:write(TableAlias, Record);
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead",TableAlias});
write({_Node,Schema,TableAlias}, Record) ->
    write({Schema,TableAlias}, Record);
write({_Schema, TableAlias}, Record) ->
    write(TableAlias, Record);           %% ToDo: may depend on schema 
write(TableAlias, Record) ->
    % log_to_db(debug,?MODULE,write,[{table,TableAlias},{rec,Record}],"write"), 
    PTN = physical_table_name(TableAlias, element(?KeyIdx, Record)),
    try
        imem_if_mnesia:write(PTN, Record)
    catch
        throw:{'ClientError',{"Table does not exist",T}} ->
            % ToDo: instruct imem_meta gen_server to create the table
            case is_time_partitioned_alias(TableAlias) of
                true ->
                    case create_partitioned_table_sync(TableAlias, PTN) of
                        ok ->   
                            imem_if_mnesia:write(PTN, Record);
                        {error,recursive_call} ->
                            ok; %% cannot create a new partition now, skip logging to database
                        E ->
                            ?ClientError({"Table partition cannot be created", {PTN, E}})
                    end;        
                false ->
                    ?ClientError({"Table does not exist",T})
            end
    end. 

dirty_write({ddSysConf, TableAlias}, _Record) -> 
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead", TableAlias});
dirty_write({_Node,Schema,TableAlias}, Record) -> 
    dirty_write({Schema,TableAlias}, Record); 
dirty_write({_Schema,TableAlias}, Record) -> 
    dirty_write(TableAlias, Record);           %% ToDo: may depend on schema 
dirty_write(TableAlias, Record) -> 
    % log_to_db(debug,?MODULE,dirty_write,[{table,TableAlias},{rec,Record}],"dirty_write"), 
    PTN = physical_table_name(TableAlias, element(?KeyIdx, Record)),
    try
        imem_if_mnesia:dirty_write(PTN, Record)
    catch
        throw:{'ClientError',{"Table does not exist",T}} ->
            case is_time_partitioned_alias(TableAlias) of
                true ->
                    case create_partitioned_table_sync(TableAlias, PTN) of
                        ok ->   
                            imem_if_mnesia:dirty_write(PTN, Record);
                        {error,recursive_call} ->
                            ok; %% cannot create a new partition now, skip logging to database
                        E ->
                            ?ClientError({"Table partition cannot be created",{PTN,E}})
                    end;        
                false ->
                    ?ClientError({"Table does not exist",T})
            end
    end. 

insert(TableAlias, Row) ->
    insert(TableAlias,Row,meta_field_value(user),[]).

insert(TableAlias, Row, TrOpts) when is_list(TrOpts) ->
    insert(TableAlias, Row, meta_field_value(user), TrOpts);
insert(TableAlias, Row, User) ->
    insert(TableAlias, Row, User, []).

insert({ddSysConf,TableAlias}, _Row, _User, _TrOpts) ->
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead",TableAlias});
insert({_Node,Schema,TableAlias}, Row, User, TrOpts) ->
    insert({Schema,TableAlias}, Row, User, TrOpts);
insert({_Schema,TableAlias}, Row, User, TrOpts) ->
    insert(TableAlias, Row, User, TrOpts);               %% ToDo: may depend on schema
insert(TableAlias, Row, User, TrOpts) when is_atom(TableAlias), is_tuple(Row) ->
    {TableType, DefRec, Trigger} =  trigger_infos(TableAlias),
    modify(insert, TableType, TableAlias, DefRec, Trigger, {}, Row, User, TrOpts).

update(TableAlias, Row) ->
    update(TableAlias, Row, meta_field_value(user), []).

update(TableAlias, Row, TrOpts) when is_list(TrOpts) ->
    update(TableAlias, Row, meta_field_value(user), TrOpts);
update(TableAlias, Row, User) ->
    update(TableAlias, Row, User, []).

update({ddSysConf,TableAlias}, _Row, _User, _TrOpts) ->
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead",TableAlias});    
update({_Node,Schema,TableAlias}, Row, User, TrOpts) ->
    update({Schema,TableAlias}, Row, User, TrOpts);
update({_Schema,TableAlias}, Row, User, TrOpts) ->
    update(TableAlias, Row, User, TrOpts);               %% ToDo: may depend on schema
update(TableAlias, {ORow,NRow}, User, TrOpts) when is_atom(TableAlias), is_tuple(ORow), is_tuple(NRow) ->
    {TableType, DefRec, Trigger} =  trigger_infos(TableAlias),
    modify(update, TableType, TableAlias, DefRec, Trigger, ORow, NRow, User, TrOpts).

merge(TableAlias, Row) ->
    merge(TableAlias, Row, meta_field_value(user), []).

merge(TableAlias, Row, TrOpts) when is_list(TrOpts) ->
    merge(TableAlias, Row, meta_field_value(user), TrOpts);
merge(TableAlias, Row, User) ->
    merge(TableAlias, Row, User, []).

merge({ddSysConf,TableAlias}, _Row, _User, _TrOpts) ->
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead",TableAlias});    
merge({_Node,Schema,TableAlias}, Row, User, TrOpts) ->
    merge({Schema,TableAlias}, Row, User, TrOpts);
merge({_Schema,TableAlias}, Row, User, TrOpts) ->
    merge(TableAlias, Row, User, TrOpts);                %% ToDo: may depend on schema
merge(TableAlias, Row, User, TrOpts) when is_atom(TableAlias), is_tuple(Row) ->
    {TableType, DefRec, Trigger} =  trigger_infos(TableAlias),
    modify(merge, TableType, TableAlias, DefRec, Trigger, {}, Row, User, TrOpts).

remove(TableAlias, Row) ->
    remove(TableAlias, Row, meta_field_value(user), []).

remove(TableAlias, Row, TrOpts) when is_list(TrOpts) ->
    remove(TableAlias, Row, meta_field_value(user), TrOpts);
remove(TableAlias, Row, User) ->
    remove(TableAlias, Row, User, []).

remove({ddSysConf,TableAlias}, _Row, _User, TrOpts) when is_list(TrOpts) ->
    ?UnimplementedException({"Cannot delete from ddSysConf schema, use DDerl GUI instead",TableAlias});
remove({_Node,Schema,TableAlias}, Row, User, TrOpts) ->
    remove({Schema,TableAlias}, Row, User, TrOpts); 
remove({_Schema,TableAlias}, Row, User, TrOpts) ->
    remove(TableAlias, Row, User, TrOpts);               %% ToDo: may depend on schema
remove(TableAlias, Row, User, TrOpts) when is_atom(TableAlias), is_tuple(Row), is_list(TrOpts)  ->
    {TableType, DefRec, Trigger} =  trigger_infos(TableAlias),
    modify(remove, TableType, TableAlias, DefRec, Trigger, Row, {}, User, TrOpts).

modify(Operation, TableType, TableAlias, DefRec, Trigger, ORow, NRow, User) ->
    modify(Operation, TableType, TableAlias, DefRec, Trigger, ORow, NRow, User,[]).

modify(Operation, TableType, TableAlias, DefRec, Trigger, ORow, NRow, User, TrOpts) when is_atom(TableAlias), is_tuple(ORow), is_tuple(NRow), is_list(TrOpts)  ->
    {Key,Row} = case {ORow,NRow} of  %% Old Key / New Row Value if in doubt
        {{},{}} ->  ?ClientError({"Bad modify arguments, old and new rows are empty",Operation});
        {_,{}} ->   {element(?KeyIdx,ORow),ORow};
        {{},_} ->   DefaultedRow = apply_defaults(DefRec, NRow),
                    {element(?KeyIdx,NRow),apply_validators(DefRec, DefaultedRow, TableAlias, User)};
        {_,_} ->    DefaultedRow = apply_defaults(DefRec, NRow),
                    {element(?KeyIdx,ORow),apply_validators(DefRec, DefaultedRow, TableAlias, User)}
    end,
    case ((TableAlias /= ddTable) and lists:member(?nav, tuple_to_list(Row))) of
        false ->
            PTN = physical_table_name(TableAlias, Key),  %% may refer to a partition (of the old key)
            Trans = fun() ->   
                case {Operation, TableType, read(PTN,Key)} of     %% TODO: Wrap in single transaction
                    {insert,bag,Bag} -> case lists:member(Row, Bag) of  
                                            true ->     ?ConcurrencyException({"Insert failed, object already exists", {PTN,Row}});
                                            false ->    write(PTN, Row),
                                                        Trigger({}, Row, PTN, User, TrOpts),
                                                        Row
                                        end;
                    {insert,_,[]} ->    write(PTN, Row),
                                        Trigger({},Row,TableAlias,User,TrOpts),
                                        Row;
                    {insert,_,[R]} ->   ?ConcurrencyException({"Insert failed, key already exists in", {PTN, R}});
                    {update,bag,_} ->   ?UnimplementedException({"Update is not supported on bag tables, use delete and insert", TableAlias});
                    {update,_,[]} ->    ?ConcurrencyException({"Update failed, key does not exist", {PTN, Key}});
                    {update,_,[ORow]}-> case element(?KeyIdx,NRow) of
                                            Key ->  write(PTN, Row);
                                            OK ->   %% key has changed
                                                    %% must evaluate new partition and delete old key
                                                    write(physical_table_name(TableAlias, element(?KeyIdx, Row)), Row),
                                                    delete(PTN,OK)
                                        end,
                                        Trigger(ORow, Row, TableAlias, User, TrOpts),
                                        Row;
                    {update,_,[R]}->    case record_match(R, ORow) of
                                            true -> 
                                                write(PTN, Row),
                                                Trigger(R, Row, TableAlias, User, TrOpts),
                                                Row;
                                            false ->   
                                                ?ConcurrencyException({"Data is modified by someone else", {PTN, R}})
                                        end;
                    {merge,bag,_} ->    ?UnimplementedException({"Merge is not supported on bag tables, use delete and insert", TableAlias});
                    {merge,_,[]} ->     write(PTN, Row),
                                        Trigger({}, Row, TableAlias, User, TrOpts),
                                        Row;
                    {merge,_,[R]} ->    write(PTN, Row),
                                        Trigger(R, Row, TableAlias, User, TrOpts),
                                        Row;
                    {remove,bag,[]} ->  ?ConcurrencyException({"Remove failed, object does not exist", {PTN, Row}});
                    {remove,_,[]} ->    ?ConcurrencyException({"Remove failed, key does not exist", {PTN, Key}});
                    {remove,bag,Bag} -> case lists:member(Row,Bag) of  
                                            false ->    ?ConcurrencyException({"Remove failed, object does not exist", {PTN, Row}});
                                            true ->     delete_object(PTN, Row),
                                                        Trigger(Row, {}, PTN, User, TrOpts),
                                                        Row
                                        end;
                    {remove,_,[ORow]}-> delete(TableAlias, Key),
                                        Trigger(ORow, {}, TableAlias, User, TrOpts),
                                        ORow;
                    {remove,_,[R]}->    case record_match(R, ORow) of
                                            true -> 
                                                delete(TableAlias, Key),
                                                Trigger(R, {}, TableAlias, User, TrOpts),
                                                R;
                                            false ->   
                                                ?ConcurrencyException({"Data is modified by someone else", {PTN, R}})
                                        end;
                    {Op,Type,R} ->      ?SystemException({"Unexpected result in row modify", {PTN, {Op, Type, R}}})
                end
            end,
            return_atomic(transaction(Trans));
        true ->     
            ?ClientError({"Not null constraint violation", {TableAlias, Row}})
    end.

record_match(Rec,Pattern) when is_tuple(Rec), is_tuple(Pattern), size(Rec)==size(Pattern) ->
    list_match(tuple_to_list(Rec),tuple_to_list(Pattern));
record_match(_,_) -> false. 

list_match([],[]) -> true;
list_match([A|List],[A|Pat]) -> list_match(List,Pat);
list_match([_|List],['_'|Pat]) -> list_match(List,Pat);
list_match(_,_) -> false.

delete({_Node,Schema,TableAlias}, Key) ->
    delete({Schema,TableAlias}, Key);
delete({_Schema,TableAlias}, Key) ->
    delete(TableAlias, Key);             %% ToDo: may depend on schema
delete(TableAlias, Key) ->
    imem_if_mnesia:delete(physical_table_name(TableAlias,Key), Key).

dirty_delete({_Node,Schema,TableAlias}, Key) ->
    dirty_delete({Schema,TableAlias}, Key);
dirty_delete({_Schema,TableAlias}, Key) ->
    dirty_delete(TableAlias, Key);             %% ToDo: may depend on schema
dirty_delete(TableAlias, Key) ->
    imem_if_mnesia:dirty_delete(physical_table_name(TableAlias,Key), Key).

delete_object({_Node,Schema,TableAlias}, Row) ->
    delete_object({Schema,TableAlias}, Row);
delete_object({_Schema,TableAlias}, Row) ->
    delete_object(TableAlias, Row);             %% ToDo: may depend on schema
delete_object(TableAlias, Row) ->
    imem_if_mnesia:delete_object(physical_table_name(TableAlias, element(?KeyIdx, Row)), Row).

subscribe({table, Tab, Mode}) ->
    PTN = physical_table_name(Tab),
    log_to_db(debug,?MODULE,subscribe,[{ec,{table, PTN, Mode}}],"subscribe to mnesia"),
    imem_if_mnesia:subscribe({table, PTN, Mode});
subscribe(EventCategory) ->
    log_to_db(debug,?MODULE,subscribe,[{ec,EventCategory}],"subscribe to mnesia"),
    imem_if_mnesia:subscribe(EventCategory).

unsubscribe({table, Tab, Mode}) ->
    PTN = physical_table_name(Tab),
    Result = imem_if_mnesia:unsubscribe({table, PTN, Mode}),
    log_to_db(debug,?MODULE,unsubscribe,[{ec,{table, PTN, Mode}}],"unsubscribe from mnesia"),
    Result;
unsubscribe(EventCategory) ->
    Result = imem_if_mnesia:unsubscribe(EventCategory),
    log_to_db(debug,?MODULE,unsubscribe,[{ec,EventCategory}],"unsubscribe from mnesia"),
    Result.

update_tables([[{Schema,_,_}|_]|_] = UpdatePlan, Lock) ->
    update_tables(Schema, UpdatePlan, Lock, []).

update_bound_counter(TableAlias, Field, Key, Incr, LimitMin, LimitMax) ->
    imem_if_mnesia:update_bound_counter(physical_table_name(TableAlias), Field, Key, Incr, LimitMin, LimitMax).

update_tables(ddSysConf, [], Lock, Acc) ->
    imem_if_sys_conf:update_tables(Acc, Lock);  
update_tables(_MySchema, [], Lock, Acc) ->
    imem_if_mnesia:update_tables(Acc, Lock);  
update_tables(MySchema, [UEntry|UPlan], Lock, Acc) ->
    % log_to_db(debug,?MODULE,update_tables,[{lock,Lock}],io_lib:format("~p",[UEntry])),
    update_tables(MySchema, UPlan, Lock, [update_table_name(MySchema, UEntry)|Acc]).

update_table_name(MySchema,[{MySchema,Tab,Type}, Item, Old, New, Trig, User, TrOpts]) ->
    case lists:member(?nav,tuple_to_list(New)) of
        false ->    [{physical_table_name(Tab), Type}, Item, Old, New, Trig, User, TrOpts];
        true ->     ?ClientError({"Not null constraint violation", {Item, {Tab, New}}})
    end.

admin_exec(Module, Function, _Params) ->
    ?ClientError({"Function cannot be called outside of security context", {Module, Function}}).

decode_json(_, {}) -> {};
decode_json([], Rec) -> Rec;
decode_json([Pos|Rest], Rec) ->
    Val = element(Pos, Rec),
    % ?LogDebug("decode_json Val ~p",[Val]),
    Decoded = try imem_json:decode(Val) catch _:_ -> ?nav end,
    % ?LogDebug("decode_json Decoded ~p",[Decoded]),
    decode_json(Rest, setelement(Pos, Rec, Decoded)).

update_index(_,_,_,_,_,#ddIdxPlan{def=[]}) -> 
    % ?LogDebug("update_index IdxPlan ~p",[#ddIdxPlan{def=[]}]),
    ok;   %% no index on this table
update_index(Old, New, Table, User, TrOpts, IdxPlan) ->
    % ?LogDebug("IdxPlan ~p",[IdxPlan]),
    update_index(Old, New, Table, index_table(Table), User, TrOpts, IdxPlan).

update_index(Old,New,Table,IndexTable,User,TrOpts,IdxPlan) -> 
    OldJ = decode_json(IdxPlan#ddIdxPlan.jpos,Old),
    NewJ = decode_json(IdxPlan#ddIdxPlan.jpos,New),
    update_index(Old,New,OldJ,NewJ,Table,IndexTable,User,TrOpts,IdxPlan#ddIdxPlan.def,[],[]). 

update_index(_Old,_New,_OldJ,_NewJ,_Table,IndexTable,_User,_TrOpts,[],Removes,Inserts) ->
    % ?LogDebug("update index table/Old/New ~p~n~p~n~p",[_Table,_Old,_New]),
    % ?LogDebug("update index table/rem/ins ~p~n~p~n~p",[IndexTable,Removes,Inserts]),
    imem_index:remove(IndexTable,Removes),
    imem_index:insert(IndexTable,Inserts);
update_index(Old,New,OldJ,NewJ,Table,IndexTable,User,TrOpts,[#ddIdxDef{id=ID,type=Type,pl=PL,vnf=Vnf,iff=Iff}|Defs],Removes0,Inserts0) ->
    Rem = lists:usort(index_items(Old,OldJ,Table,User,ID,Type,PL,Vnf,Iff,[])), 
    Ins = lists:usort(index_items(New,NewJ,Table,User,ID,Type,PL,Vnf,Iff,[])),
    %% ToDo: cancel Inserts against identical Removes
    update_index(Old,New,OldJ,NewJ,Table,IndexTable,User,TrOpts,Defs,Removes0++Rem,Inserts0++Ins).

index_items({},_,_,_,_,_,_,_,_,[]) -> [];
index_items(_,_,_,_,_,_,[],_,_,Changes) -> Changes;
index_items(Rec,RecJ,Table,User,ID,Type,[Pos|PL],Vnf,Iff,Changes0) when is_integer(Pos) ->
    Key = element(?KeyIdx,Rec),         %% index a field as a whole, no json path search
    KVPs = case element(Pos,Rec) of
        ?nav -> [];
        RV ->   case vnf_eval(Vnf,Key,RV) of         %% apply value normalising function
                    ?nav ->     [];
                    [?nav] ->   [];
                    [] ->       [];
                    [NVal] ->   [{Key,NVal}];
                    LVal ->     [{Key,V} || V <- LVal,V /= ?nav]
                end
    end,
    Ch = [{ID,Type,K,V} || {K,V} <- lists:filter(Iff,KVPs)], %% apply index filter function
    index_items(Rec,RecJ,Table,User,ID,Type,PL,Vnf,Iff,Changes0++Ch);
index_items(Rec,{},Table,User,ID,Type,[_|PL],Vnf,Iff,Changes) ->
    index_items(Rec,{},Table,User,ID,Type,PL,Vnf,Iff,Changes);
index_items(Rec,RecJ,Table,User,ID,Type,[{PT,FL}|PL],Vnf,Iff,Changes0) ->
    % ?LogDebug("index_items RecJ ~p",[RecJ]),
    % ?LogDebug("index_items ParseTree ~p",[PT]),
    % ?LogDebug("index_items FieldList ~p",[FL]),
    Key = element(?KeyIdx,RecJ),
    Binds = [{Name,element(Pos,RecJ)} || {Name,Pos} <- FL],
    KVPs = case lists:keyfind(?nav, 2, Binds) of
        false ->   
            Match = imem_json:eval(PT,Binds),
            case Match of
                MV when is_binary(MV);is_number(MV);is_atom(MV) ->    
                    [{Key,V} || V <- lists:flatten([vnf_eval(Vnf,Key,M) || M  <- [MV]]), V /= ?nav];
                ML when is_list(ML) ->      
                    [{Key,V} || V <- lists:flatten([vnf_eval(Vnf,Key,M) || M  <- ML]), V /= ?nav];
                {nomatch, {path_not_found, _}} ->          
                    [];
                {nomatch, {property_not_found, _, _}} ->   
                    [];
                {nomatch, {index_out_of_bound, _, _}} ->   
                    [];
                {error, {operation_not_supported, E1}} ->
                    ?ClientError({"Index error", {operation_not_supported, E1}});
                {error, {unimplimented, E2}} ->
                    ?UnimplementedException({"Index error", {unimplimented, E2}});
                {error, {unbound, E3}} ->
                    ?SystemException({"Index error", {unbound, E3}});
                {error, {malformed, E4}} ->
                    ?SystemException({"Index error", {malformed, E4}});
                _NotSupported -> [] %% For non supported datatypes, i.e. Maps/ports/pids
            end;
        _ ->
            []  %% one or more bind variables are not values. Don't try to evaluate the json path.
    end,
    %% ?LogDebug("index_items KVPs ~p",[KVPs]),
    Ch = [{ID,Type,K,V} || {K,V} <- lists:filter(Iff,KVPs)],
    index_items(Rec,RecJ,Table,User,ID,Type,PL,Vnf,Iff,Changes0++Ch).

vnf_eval(Vnf,Key,Val) ->
    case is_function(Vnf,1) of 
        true -> Vnf(Val);
        false -> Vnf(Key,Val)
    end.

transaction(Function) ->
    imem_if_mnesia:transaction(Function).

transaction(Function, Args) ->
    imem_if_mnesia:transaction(Function, Args).

transaction(Function, Args, Retries) ->
    imem_if_mnesia:transaction(Function, Args, Retries).

return_atomic_list(Result) ->
    imem_if_mnesia:return_atomic_list(Result). 

return_atomic_ok(Result) -> 
    imem_if_mnesia:return_atomic_ok(Result).

return_atomic(Result) -> 
    imem_if_mnesia:return_atomic(Result).

first(Table) ->             imem_if_mnesia:first(Table).

dirty_first(Table) ->       imem_if_mnesia:dirty_first(Table).

next(Table,Key) ->          imem_if_mnesia:next(Table,Key).

dirty_next(Table,Key) ->    imem_if_mnesia:dirty_next(Table,Key).

last(Table) ->              imem_if_mnesia:last(Table).

dirty_last(Table) ->        imem_if_mnesia:dirty_last(Table).

prev(Table,Key) ->          imem_if_mnesia:prev(Table,Key).

dirty_prev(Table,Key) ->    imem_if_mnesia:dirty_prev(Table,Key).

foldl(FoldFun, InputAcc, Table) ->  
    imem_if_mnesia:foldl(FoldFun, InputAcc, Table).

lock(LockItem, LockKind) -> 
    imem_if_mnesia:lock(LockItem, LockKind).

get_tables_count() ->
    {ok, MaxEtsNoTables} = application:get_env(imem, max_ets_tables),
    {MaxEtsNoTables, length(mnesia:system_info(tables))}.

-spec sql_jp_bind(Sql::string()) -> {NewSql::string(), BindParamsMeta::[{BindParam::binary(), BindType::atom(), JPPath::string()}]}.
sql_jp_bind(Sql) ->
    case re:run(Sql, ":[^ ,\)\n\r;]+", [global,{capture,all,list}]) of
        {match, Parameters} -> Parameters;
        Other ->
            Parameters = undefined,
            ?ClientError({"Bad format", Other})
    end,
    ParamsMap = [{lists:flatten(Param)
                  , ":"++re:replace(lists:flatten(Param), "[:_\\[\\]{}]+", ""
                                      , [global,{return,list}])}
                 || Param <- lists:usort(Parameters)],
    {lists:foldl(fun({M,R}, Sql0) ->
                         M1 =  re:replace(M, "([\\[\\]])", "\\\\&"
                                          , [global, {return, list}]),
                         re:replace(Sql0, M1, R, [global, {return, list}])
                 end, Sql, ParamsMap)
     , [list_to_tuple(
          [list_to_binary(R)
           | case re:run(M, "^:([a-zA-Z]+)_(.*)$"
                         ,[{capture,[1,2],list}]) of
                 {match,["a",    Jp]} -> [atom,      Jp];
                 {match,["b",    Jp]} -> [binary,    Jp];
                 {match,["rw",   Jp]} -> [raw,       Jp];
                 {match,["bb",   Jp]} -> [blob,      Jp];
                 {match,["rid",  Jp]} -> [rowid,     Jp];
                 {match,["bs",   Jp]} -> [binstr,    Jp];
                 {match,["cb",   Jp]} -> [clob,      Jp];
                 {match,["ncb",  Jp]} -> [nclob,     Jp];
                 {match,["vc",   Jp]} -> [varchar2,  Jp];
                 {match,["nvc",  Jp]} -> [nvarchar2, Jp];
                 {match,["c",    Jp]} -> [char,      Jp];
                 {match,["nc",   Jp]} -> [nchar,     Jp];
                 {match,["bl",   Jp]} -> [boolean,   Jp];
                 {match,["dt",   Jp]} -> [datetime,  Jp];
                 {match,["d",    Jp]} -> [decimal,   Jp];
                 {match,["f",    Jp]} -> [float,     Jp];
                 {match,["fn",   Jp]} -> ['fun',     Jp];
                 {match,["i",    Jp]} -> [integer,   Jp];
                 {match,["ip",   Jp]} -> [ipaddr,    Jp];
                 {match,["l",    Jp]} -> [list,      Jp];
                 {match,["n",    Jp]} -> [number,    Jp];
                 {match,["p",    Jp]} -> [pid,       Jp];
                 {match,["r",    Jp]} -> [ref,       Jp];
                 {match,["s",    Jp]} -> [string,    Jp];
                 {match,["t",    Jp]} -> [term,      Jp];
                 {match,["bt",   Jp]} -> [binterm,   Jp];
                 {match,["ts",   Jp]} -> [timestamp, Jp];
                 {match,["tp",   Jp]} -> [tuple,     Jp];
                 {match,["u",    Jp]} -> [userid,    Jp];
                 {match,[X,     _Jp]} -> ?ClientError({"Unknown type", X});
                 Other1               -> ?ClientError({"Bad format", Other1})
             end]) || {M,R} <- ParamsMap]
    }.

-spec sql_bind_jp_values(BindParamsMeta :: [{BindParam :: binary()
                                             , BindType :: atom()
                                             , JPPath :: string()}]
                        , JpPathBinds :: [{BindName :: binary()
                                           , BindObj :: any()}]) ->
    {Values :: list()} | no_return().
sql_bind_jp_values(BindParamsMeta, JpPathBinds) ->
    [imem_json:eval(Jp, JpPathBinds) || {_,_,Jp} <- BindParamsMeta].

%% ----- DATA MANIPULATIONS ---------------------------------------

-spec merge_diff(ddTable(), ddTable(), ddTable()) -> ok.
merge_diff(Left, Right, Merged) -> 
    merge_diff(Left, Right, Merged, []).

-spec merge_diff(ddTable(), ddTable(), ddTable(), ddOptions()) -> ok.
merge_diff(Left, Right, Merged, Opts) -> 
    merge_diff(Left, Right, Merged, Opts, meta_field_value(user)).

-spec merge_diff(ddTable(), ddTable(), ddTable(), ddOptions(), ddEntityId()) -> ok.
merge_diff(Left, Right, Merged, Opts, User) ->
    imem_merge:merge_diff(Left, Right, Merged, Opts, User).

-spec term_diff(atom(), term(), atom(), term(), ddOptions(), ddEntityId()) -> list(tuple()).
term_diff(LeftType, LeftData, RightType, RightData, Opts, User) ->
    imem_merge:term_diff(LeftType, LeftData, RightType, RightData, Opts, User).


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

sql_bind_jp_values_test_() ->
    {inparallel
     , [{P, fun() ->
                    {_, R} = ?MODULE:sql_jp_bind(S),
                    ?assertEqual(V, ?MODULE:sql_bind_jp_values(R, B))
            end}
        || {P,S,B,V} <-
           [
            {"common"
             , "select :a_obj:x,:b_obj:y from x where :a_obj:x = 1 "
               "and :b_obj:y = 0 and :rw_obj1:a > 0"
             , [{<<"obj">>, <<"{\"x\":1,\"y\":2}">>}
                , {<<"obj1">>, <<"{\"a\":3}">>}]
             , [<<"1">>,<<"2">>,<<"3">>]}
           ]
       ]
    }.

sql_jp_bind_test_() ->
    {inparallel
     , [{P, case R of
                {error, E} ->
                    ?_assertException(throw,{'ClientError',E}
                                      , ?MODULE:sql_jp_bind(S));
                R ->
                    fun() ->
                            {S1, R1} = ?MODULE:sql_jp_bind(S),
                            {S0, R0} = R,
                            ?assertEqual(S1, S0),
                            ?assertEqual(lists:usort(R1), lists:usort(R0))
                    end
            end}
        || {P,S,R} <-
           [
            {"common",
             "select :a_obj:x,:b_obj:x,:rw_obj:x,:bb_obj:x,:rid_obj:x,:bs_obj:x"
             ",:cb_obj:x,:ncb_obj:x,:vc_obj:x,:nvc_obj:x,:c_obj:x,:nc_obj:x"
             ",:bl_obj:x,:dt_obj:x,:d_obj:x,:f_obj:x,:fn_obj:x,:i_obj:x,"
             ":ip_obj:x,:l_obj:x,:n_obj:x,:p_obj:x,:r_obj:x,:s_obj:x,:t_obj:x,"
             ":bt_obj:x,:ts_obj:x,:tp_obj:x,:u_obj:x"
             " from x where "
             ":a_obj:x = 1 and :b_obj:x = 1 and :rw_obj:x = 1 and :bb_obj:x = 1 "
             "and :rid_obj:x = 1 and :bs_obj:x = 1 and :cb_obj:x = 1 "
             "and :ncb_obj:x = 1 and :vc_obj:x = 1 and :nvc_obj:x = 1 "
             "and :c_obj:x = 1 and :nc_obj:x = 1 and :bl_obj:x = 1 "
             "and :dt_obj:x  = 1 and :d_obj:x = 1 and :f_obj:x = 1 "
             "and :fn_obj:x  = 1 and :i_obj:x = 1 and :ip_obj:x = 1 "
             "and :l_obj:x = 1 and :n_obj:x = 1 and :p_obj:x = 1 "
             "and :r_obj:x = 1 and :s_obj:x = 1 and :t_obj:x = 1 "
             "and :bt_obj:x = 1 and :ts_obj:x = 1 and :tp_obj:x = 1 "
             "and :u_obj:x"
             , {"select :aobjx,:bobjx,:rwobjx,:bbobjx,:ridobjx,:bsobjx"
                ",:cbobjx,:ncbobjx,:vcobjx,:nvcobjx,:cobjx,:ncobjx"
                ",:blobjx,:dtobjx,:dobjx,:fobjx,:fnobjx,:iobjx,"
                ":ipobjx,:lobjx,:nobjx,:pobjx,:robjx,:sobjx,:tobjx,"
                ":btobjx,:tsobjx,:tpobjx,:uobjx"
                " from x where "
                ":aobjx = 1 and :bobjx = 1 and :rwobjx = 1 and :bbobjx = 1 "
                "and :ridobjx = 1 and :bsobjx = 1 and :cbobjx = 1 "
                "and :ncbobjx = 1 and :vcobjx = 1 and :nvcobjx = 1 "
                "and :cobjx = 1 and :ncobjx = 1 and :blobjx = 1 "
                "and :dtobjx  = 1 and :dobjx = 1 and :fobjx = 1 "
                "and :fnobjx  = 1 and :iobjx = 1 and :ipobjx = 1 "
                "and :lobjx = 1 and :nobjx = 1 and :pobjx = 1 "
                "and :robjx = 1 and :sobjx = 1 and :tobjx = 1 "
                "and :btobjx = 1 and :tsobjx = 1 and :tpobjx = 1 "
                "and :uobjx"
                , [{<<":aobjx">>,   atom,     "obj:x"}
                  ,{<<":bobjx">>,   binary,   "obj:x"}
                  ,{<<":rwobjx">>,  raw,      "obj:x"}
                  ,{<<":bbobjx">>,  blob,     "obj:x"}
                  ,{<<":ridobjx">>, rowid,    "obj:x"}
                  ,{<<":bsobjx">>,  binstr,   "obj:x"}
                  ,{<<":cbobjx">>,  clob,     "obj:x"}
                  ,{<<":ncbobjx">>, nclob,    "obj:x"}
                  ,{<<":vcobjx">>,  varchar2, "obj:x"}
                  ,{<<":nvcobjx">>, nvarchar2,"obj:x"}
                  ,{<<":cobjx">>,   char,     "obj:x"}
                  ,{<<":ncobjx">>,  nchar,    "obj:x"}
                  ,{<<":blobjx">>,  boolean,  "obj:x"}
                  ,{<<":dtobjx">>,  datetime, "obj:x"}
                  ,{<<":dobjx">>,   decimal,  "obj:x"}
                  ,{<<":fobjx">>,   float,    "obj:x"}
                  ,{<<":fnobjx">>,  'fun',    "obj:x"}
                  ,{<<":iobjx">>,   integer,  "obj:x"}
                  ,{<<":ipobjx">>,  ipaddr,   "obj:x"}
                  ,{<<":lobjx">>,   list,     "obj:x"}
                  ,{<<":nobjx">>,   number,   "obj:x"}
                  ,{<<":pobjx">>,   pid,      "obj:x"}
                  ,{<<":robjx">>,   ref,      "obj:x"}
                  ,{<<":sobjx">>,   string,   "obj:x"}
                  ,{<<":tobjx">>,   term,     "obj:x"}
                  ,{<<":btobjx">>,  binterm,  "obj:x"}
                  ,{<<":tsobjx">>,  timestamp,"obj:x"}
                  ,{<<":tpobjx">>,  tuple,    "obj:x"}
                  ,{<<":uobjx">>,   userid,   "obj:x"}]}}
           , {"exception", "select :y_b from s", {error, {"Unknown type", "y"}}}
           , {"exception", "select :yb from s", {error, {"Bad format", nomatch}}}
           ]
       ]
    }.

is_time_partitioned_alias_test_() ->
    [ {"T_1", ?_assertEqual(false, is_time_partitioned_alias("ddTest@"))}
    , {"T_2", ?_assertEqual(false, is_time_partitioned_alias(<<"ddTest@007">>))} 
    , {"T_3", ?_assertEqual(false, is_time_partitioned_alias(ddTest@007))} 
    , {"T_4", ?_assertEqual(false, is_time_partitioned_alias({imem_meta:schema(),ddTest@007}))} 
    , {"T_5", ?_assertEqual(false, is_time_partitioned_alias(<<"ddTest_1234567890@">>))}

    , {"P_1", ?_assertEqual(true, is_time_partitioned_alias("ddTest_1234@local"))} 
    , {"P_2", ?_assertEqual(true, is_time_partitioned_alias(<<"ddTest_1234@007">>))} 
    , {"P_3", ?_assertEqual(true, is_time_partitioned_alias(ddTest_1234@_))}
    , {"P_4", ?_assertEqual(true, is_time_partitioned_alias({imem_meta:schema(),ddTest_1234@local}))} 
    ].

is_node_sharded_alias_test_() -> 
    [ {"R_1", ?_assertEqual(false, is_node_sharded_alias(<<"ddTest@007">>))} 
    , {"R_2", ?_assertEqual(false, is_node_sharded_alias({imem_meta:schema(),ddTest@007}))} 
    , {"R_3", ?_assertEqual(false, is_node_sharded_alias(<<"ddTest_1234@007">>))} 
    , {"R_4", ?_assertEqual(false, is_node_sharded_alias(<<"ddTest_1234567890@007">>))} 

    , {"L_1", ?_assertEqual(false, is_node_sharded_alias("ddTest"))} 
    , {"L_2", ?_assertEqual(false, is_node_sharded_alias(<<"ddTest">>))} 
    , {"L_3", ?_assertEqual(false, is_node_sharded_alias(ddTest))}
    , {"L_4", ?_assertEqual(false, is_node_sharded_alias({imem_meta:schema(),ddTest}))} 
    , {"L_5", ?_assertEqual(false, is_node_sharded_alias("ddTest@_"))}
    , {"L_6", ?_assertEqual(false, is_node_sharded_alias(ddTest@local))} 

    , {"C_1", ?_assertEqual(true, is_node_sharded_alias("ddTest@"))} 
    , {"C_2", ?_assertEqual(true, is_node_sharded_alias(<<"ddTest@">>))} 
    , {"C_3", ?_assertEqual(true, is_node_sharded_alias(ddTest@))}
    , {"C_6", ?_assertEqual(true, is_node_sharded_alias({imem_meta:schema(),ddTest@}))}
    , {"C_6", ?_assertEqual(true, is_node_sharded_alias(<<"ddTest_1234567890@">>))} 

    , {"PC_1", ?_assertEqual(true, is_node_sharded_alias("ddTest_1234@"))}
    , {"PC_2", ?_assertEqual(true, is_node_sharded_alias(<<"ddTest_1234@">>))}
    , {"PC_3", ?_assertEqual(true, is_node_sharded_alias(ddTest_1234@))}
    , {"PC_4", ?_assertEqual(true, is_node_sharded_alias({imem_meta:schema(),ddTest_1234@}))}
    ].

is_local_alias_test_() ->
    [ {"R_1", ?_assertEqual(false, is_local_alias("ddTest@007"))}
    , {"R_2", ?_assertEqual(false, is_local_alias(<<"ddTest_1234@007">>))} 
    , {"R_3", ?_assertEqual(false, is_local_alias(ddTest_1234567890@007))} 
    , {"R_4", ?_assertEqual(false, is_local_alias({imem_meta:schema(),ddTest@007}))} 

    , {"L_1", ?_assertEqual(true, is_local_alias("ddTest"))} 
    , {"L_2", ?_assertEqual(true, is_local_alias(<<"ddTest@local">>))} 
    , {"L_3", ?_assertEqual(true, is_local_alias(ddTest@_))}
    , {"L_4", ?_assertEqual(true, is_local_alias({imem_meta:schema(),ddTest}))} 

    , {"C_1", ?_assertEqual(false, is_local_alias("ddTest@"))} 
    , {"C_2", ?_assertEqual(false, is_local_alias(<<"ddTest@">>))} 
    , {"C_3", ?_assertEqual(false, is_local_alias(ddTest@))}
    , {"C_4", ?_assertEqual(false, is_local_alias({imem_meta:schema(),ddTest@}))}
    , {"C_6", ?_assertEqual(false, is_local_alias(<<"ddTest_1234567890@">>))} 

    , {"PC_1", ?_assertEqual(false, is_local_alias("ddTest_1234@"))}
    , {"PC_2", ?_assertEqual(false, is_local_alias(<<"ddTest_1234@">>))}
    , {"PC_3", ?_assertEqual(false, is_local_alias(ddTest_1234@))}
    , {"PC_4", ?_assertEqual(false, is_local_alias({imem_meta:schema(),ddTest_1234@}))}

    , {"PR_1", ?_assertEqual(false, is_local_alias("ddTest_1234@007"))} 
    , {"PR_2", ?_assertEqual(false, is_local_alias(<<"ddTest_1234@007">>))} 
    , {"PR_3", ?_assertEqual(false, is_local_alias(ddTest_1234@007))}
    , {"PR_4", ?_assertEqual(false, is_local_alias({imem_meta:schema(),ddTest_1234@007}))} 
    ].

-endif.
