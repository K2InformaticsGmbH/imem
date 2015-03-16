%% -*- coding: utf-8 -*-
-module(imem_meta).

%% @doc == imem metadata and table management ==
%% Naming conventions for sharded/partitioned tables
%% Table creation / Index table creation
%% Triggers and validator funs
%% Virtual tables  


-include("imem.hrl").
-include("imem_meta.hrl").

%% HARD CODED CONFIGURATIONS

-define(DDNODE_TIMEOUT,3000).       % RPC timeout for ddNode evaluation

-define(META_TABLES,[?CACHE_TABLE,?LOG_TABLE,?MONITOR_TABLE,dual,ddNode,ddSchema,ddSize,ddAlias,ddTable]).
-define(META_FIELDS,[<<"rownum">>,<<"systimestamp">>,<<"user">>,<<"username">>,<<"sysdate">>,<<"schema">>,<<"node">>]). 
-define(META_OPTS,[purge_delay,trigger]). % table options only used in imem_meta and above

-define(CONFIG_TABLE_OPTS,  [{record_name,ddConfig}
                            ,{type,ordered_set}
                            ]).          

-define(LOG_TABLE_OPTS,     [{record_name,ddLog}
                            ,{type,ordered_set}
                            ,{purge_delay,430000}        %% 430000 = 5 Days - 2000 sec
                            ]).          

-define(ddTableTrigger,     <<"fun(__OR,__NR,__T,__U) -> imem_meta:dictionary_trigger(__OR,__NR,__T,__U) end.">> ).
-define(DD_ALIAS_OPTS,      [{trigger,?ddTableTrigger}]).          
-define(DD_TABLE_OPTS,      [{trigger,?ddTableTrigger}]).
-define(DD_CACHE_OPTS,      [{scope,local}
                            ,{local_content,true}
                            ,{record_name,ddCache}
                            ]).          
-define(DD_INDEX_OPTS,      [{record_name,ddIndex}
                            ,{type,ordered_set}         %% ,{purge_delay,430000}  %% inherit from parent table
                            ]).          

-define(INDEX_TABLE(__MasterTableName), __MasterTableName ++ "Idx").

-define(BAD_NAME_CHARACTERS,"!?#*:+-.\\<|>/").  %% invalid chars for tables and columns

% -define(RecIdx, 1).       %% Record name position in records
-define(FirstIdx, 2).       %% First field position in records (matching to hd(ColumnInfos))
-define(KeyIdx, 2).         %% Key position in records

-define(EmptyJPStr, <<>>).  %% placeholder for JSON path pointing to the whole document (record position)    
-define(EmptyJP, {}).       %% placeholder for compiled JSON path pointing to the whole document (record position)    

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
        , fail/1
        , get_tables_count/0
        , sql_jp_bind/1
        , sql_bind_jp_values/2
        ]).

-export([ schema/0
        , schema/1
        , system_id/0
        , data_nodes/0
        , host_fqdn/1
        , host_name/1
        , node_name/1
        , node_hash/1
        , all_aliases/0
        , all_tables/0
        , tables_starting_with/1
        , tables_ending_with/1
        , node_shard/0
        , qualified_table_name/1
        , qualified_new_table_name/1
        , physical_table_name/1
        , physical_table_name/2
        , physical_table_names/1
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
        , is_node_sharded_alias/1
        , is_local_node_sharded_table/1
        , time_of_partition_expiry/1
        , time_to_partition_expiry/1
        , table_type/1
        , table_columns/1
        , table_size/1
        , table_memory/1
        , table_record_name/1        
        , trigger_infos/1
        , dictionary_trigger/4
        , check_table/1
        , check_local_table_copy/1
        , check_table_meta/2
        , check_table_columns/2
        , meta_field_list/0        
        , meta_field/1
        , meta_field_info/1
        , meta_field_value/1
        , column_infos/1
        , from_column_infos/1
        , column_info_items/2
        ]).

-export([ add_attribute/2
        , update_opts/2
        , compile_fun/1
        , log_to_db/5
        , log_to_db/6
        , log_to_db/7
        , log_slow_process/6
        , failing_function/1
        , get_config_hlk/5
        , put_config_hlk/6
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
        , drop_trigger/1
        , drop_index/1
        , drop_index/2
        , purge_table/1
        , purge_table/2
        , truncate_table/1
        , truncate_table/2
        , snapshot_table/1  %% dump local table to snapshot directory
        , restore_table/1   %% replace local table by version in snapshot directory
        , read/1            %% read whole table, only use for small tables 
        , read/2            %% read by key
        , read_hlk/2        %% read using hierarchical list key
        , select/2          %% select without limit, only use for small result sets
        , select_virtual/2  %% select virtual table without limit, only use for small result sets
        , select/3          %% select with limit
        , select_sort/2
        , select_sort/3
        , modify/8          %% parameterized insert/update/merge/remove
        , insert/2          %% apply defaults, write row if key does not exist, apply trigger
        , insert/3          %% apply defaults, write row if key does not exist, apply trigger
        , update/2          %% apply defaults, write row if key exists, apply trigger (bags not supported)
        , update/3          %% apply defaults, write row if key exists, apply trigger (bags not supported)
        , merge/2           %% apply defaults, write row, apply trigger (bags not supported)
        , merge/3           %% apply defaults, write row, apply trigger (bags not supported)
        , remove/2          %% delete row if key exists (if bag row exists), apply trigger
        , remove/3          %% delete row if key exists (if bag row exists), apply trigger
        , write/2           %% write row for single key, no defaults applied, no trigger applied
        , write_log/1
        , dirty_read/2
        , dirty_write/2
        , delete/2          %% delete row by key
        , delete_object/2   %% delete single row in bag table 
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
        , update_tables/2
        , update_index/5            %% (Old,New,Tab,User,IdxDef)   
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
        , next/2
        , last/1
        , prev/2
        , foldl/3
        ]).

-export([ simple_or_local_node_sharded_tables/1]).


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

init_create_table(TableName,RecDef,Opts) ->
    init_create_table(TableName,RecDef,Opts,#ddTable{}#ddTable.owner).

init_create_table(TableName,RecDef,Opts,Owner) ->
    case (catch create_table(TableName, RecDef, Opts, Owner)) of
        {'ClientError',{"Table already exists", _}} = R ->   
            ?Info("creating ~p results in ~p", [TableName,"Table already exists"]),
            R;
        {'ClientError',{"Table does not exist",ddTable}} = R ->   
            ?Info("creating ~p results in ~p", [TableName,"Created without dictionary"]),
            R;
        {'ClientError',{"Table does not exist",_Table}} = R ->   
            ?Info("creating ~p results in \"Table ~p does not exist\"", [TableName,_Table]),
            R;
        {'ClientError',_Reason}=R ->   
            ?Info("creating ~p results in ~p", [TableName,_Reason]),
            R;
        R ->                   
            ?Info("creating ~p results in ~p", [TableName,R]),
            R
    end.

init_create_check_table(TableName,RecDef,Opts) ->
    init_create_check_table(TableName,RecDef,Opts,#ddTable{}#ddTable.owner).

init_create_check_table(TableName,RecDef,Opts,Owner) ->
    case (catch create_check_table(TableName, RecDef, Opts, Owner)) of
        ok ->   
            ?Info("table ~p created", [TableName]),
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

init_create_trigger(TableName,TriggerStr) ->
    case (catch create_trigger(TableName,TriggerStr)) of
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
        application:set_env(imem, node_shard, node_shard()),
        init_create_table(ddAlias, {record_info(fields, ddAlias),?ddAlias,#ddAlias{}}, ?DD_ALIAS_OPTS, system),         %% may not be able to register in ddTable
        init_create_table(?CACHE_TABLE, {record_info(fields, ddCache), ?ddCache, #ddCache{}}, ?DD_CACHE_OPTS, system),  %% may not be able to register in ddTable
        init_create_table(ddTable, {record_info(fields, ddTable),?ddTable,#ddTable{}}, ?DD_TABLE_OPTS, system),
        init_create_table(?CACHE_TABLE, {record_info(fields, ddCache), ?ddCache, #ddCache{}}, ?DD_CACHE_OPTS, system),  %% register in ddTable if not done yet
        init_create_table(ddAlias, {record_info(fields, ddAlias),?ddAlias,#ddAlias{}}, ?DD_ALIAS_OPTS, system),         %% register in ddTable if not done yet 
        catch check_table(ddTable),
        catch check_table_columns(ddTable, record_info(fields, ddTable)),
        catch check_table_meta(ddTable, {record_info(fields, ddTable), ?ddTable, #ddTable{}}),

        init_create_check_table(ddNode, {record_info(fields, ddNode),?ddNode,#ddNode{}}, [], system),    
        init_create_check_table(ddSchema, {record_info(fields, ddSchema),?ddSchema, #ddSchema{}}, [], system),    
        init_create_check_table(ddSize, {record_info(fields, ddSize),?ddSize, #ddSize{}}, [], system),    
        init_create_check_table(?CONFIG_TABLE, {record_info(fields, ddConfig),?ddConfig, #ddConfig{}}, ?CONFIG_TABLE_OPTS, system),
        init_create_check_table(?LOG_TABLE, {record_info(fields, ddLog), ?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system),    
        init_create_table(dual, {record_info(fields, dual),?dual, #dual{}}, [], system),
        write(dual,#dual{}),

        init_create_trigger(ddTable, ?ddTableTrigger),

        process_flag(trap_exit, true),
        {ok,#state{}}
    catch
        _Class:Reason -> {stop, {Reason,erlang:get_stacktrace()}} 
    end,
    Result.

create_partitioned_table_sync(TableAlias,TableName) when is_atom(TableAlias), is_atom(TableName) ->
    ImemMetaPid = erlang:whereis(?MODULE),
    case self() of
        ImemMetaPid ->
            {error,recursive_call};   %% cannot call myself
        _ ->
            gen_server:call(?MODULE, {create_partitioned_table, TableAlias, TableName},35000)
    end. 

create_partitioned_table(TableAlias, TableName) when is_atom(TableName) ->
    try 
        case imem_if:read(ddTable,{schema(), TableName}) of
            [#ddTable{}] ->
                % Table seems to exist, may need to load it
                case catch(check_table(TableName)) of
                    ok ->   ok;
                    {'ClientError',{"Table does not exist",TableName}} ->
                        create_nonexisting_partitioned_table(TableAlias,TableName);
                    Res ->
                        ?Info("Waiting for partitioned table ~p needed because of ~p", [TableName,Res]),
                        case mnesia:wait_for_tables([TableName], 30000) of
                            ok ->   ok;   
                            Error ->            
                                ?Error("Waiting for partitioned table failed with ~p", [Error]),
                                {error,Error}
                        end
                end;
            [] ->
                % Table does not exist in ddTable, must create it similar to existing
                create_nonexisting_partitioned_table(TableAlias,TableName)   
        end
    catch
        _:Reason1 ->
            ?Error("Create partitioned table failed with ~p", [Reason1]),
            {error,Reason1}
    end.

create_nonexisting_partitioned_table(TableAlias, TableName) ->
    % find out ColumnsInfos, Opts, Owner from ddAlias
    case imem_if:read(ddAlias,{schema(), TableAlias}) of
        [] ->
            ?Error("Table template not found in ddAlias~p", [TableAlias]),   
            {error, {"Table template not found in ddAlias", TableAlias}}; 
        [#ddAlias{columns=ColumnInfos,opts=Opts,owner=Owner}] ->
            try
                create_table(TableName, ColumnInfos, Opts, Owner)
            catch
                _:Reason2 -> {error, Reason2}
            end
    end.

handle_call({create_partitioned_table, TableAlias, TableName}, _From, State) ->
    {reply, create_partitioned_table(TableAlias,TableName), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%     {stop,{shutdown,Reason},State};
% handle_cast({stop, Reason}, State) ->
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

fail(Reason) ->
    throw(Reason).

dictionary_trigger(OldRec,NewRec,T,_User) when T==ddTable; T==ddAlias ->
    %% clears cached trigger information when ddTable is 
    %% modified in GUI or with insert/update/merge/remove functions
    case {OldRec,NewRec} of
        {{},{}} ->  %% truncate ddTable/ddAlias should never happen, allow for recovery operations
            ok;          
        {{},_}  ->  %% write new rec (maybe fixing something)
            {S,TN} = element(2,NewRec),
            imem_cache:clear({?MODULE, trigger, S, TN});
        {_,_}  ->  %% update old rec (maybe fixing something)
            {S,TO} = element(2,OldRec),
            %% ToDo: check for changed index definition
            %% ToDo: rebuild index table if index definition changed
            imem_if:write_table_property_in_transaction(TO,NewRec),
            imem_cache:clear({?MODULE, trigger, S, TO})
    end.

%% ------ META implementation -------------------------------------------------------


% is_system_table({_S,Table,_A}) -> is_system_table(Table);   % TODO: May depend on Schema
is_system_table({_,Table}) -> 
    is_system_table(Table);       % TODO: May depend on Schema
is_system_table(Table) when is_atom(Table) ->
    case lists:member(Table,?META_TABLES) of
        true ->     true;
        false ->    imem_if:is_system_table(Table)
    end;
is_system_table(Table) when is_binary(Table) ->
    try
        {S,T} = imem_sql_expr:binstr_to_qname2(Table), 
        is_system_table({?binary_to_existing_atom(S),?binary_to_existing_atom(T)})
    catch
        _:_ -> false
    end.

check_table(Table) when is_atom(Table) ->
    imem_if:table_size(physical_table_name(Table)),
    ok;
check_table({ddSysConf, _Table}) -> ok.

check_local_table_copy(Table) when is_atom(Table) ->
    imem_if:check_local_table_copy(physical_table_name(Table));
check_local_table_copy({ddSysConf, _Table}) -> true.

check_table_meta({ddSysConf, _}, _) -> ok;
check_table_meta(TableAlias, {Names, Types, DefaultRecord}) when is_atom(TableAlias) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(Names, Types, Defaults),
    case imem_if:read(ddTable,{schema(), physical_table_name(TableAlias)}) of
        [] ->   ?SystemException({"Missing table metadata",TableAlias}); 
        [#ddTable{columns=ColumnInfos}] ->
            CINames = column_info_items(ColumnInfos, name),
            CITypes = column_info_items(ColumnInfos, type),
            CIDefaults = column_info_items(ColumnInfos, default),
            if
                (CINames =:= Names) andalso (CITypes =:= Types) andalso (CIDefaults =:= Defaults) ->  
                    ok;
                true ->                 
                    ?SystemException({"Record does not match table metadata",TableAlias})
            end;
        Else -> 
            ?SystemException({"Column definition does not match table metadata",{TableAlias,Else}})    
    end;  
check_table_meta(TableAlias, ColumnNames) when is_atom(TableAlias) ->
    case imem_if:read(ddTable,{schema(), physical_table_name(TableAlias)}) of
        [] ->   ?SystemException({"Missing table metadata",TableAlias}); 
        [#ddTable{columns=ColumnInfo}] ->
            CINames = column_info_items(ColumnInfo, name),
            if
                CINames =:= ColumnNames ->  
                    ok;
                true ->                 
                    ?SystemException({"Record field names do not match table metadata",TableAlias})
            end          
    end.

check_table_columns({ddSysConf, _}, _) -> ok;
check_table_columns(TableAlias, {Names, Types, DefaultRecord}) when is_atom(TableAlias) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfo = column_infos(Names, Types, Defaults),
    TableColumns = table_columns(TableAlias),    
    MetaInfo = column_infos(TableAlias),    
    if
        Names /= TableColumns ->
            ?SystemException({"Column names do not match table structure",TableAlias});             
        ColumnInfo /= MetaInfo ->
            ?SystemException({"Column info does not match table metadata",TableAlias});
        true ->     ok
    end;
check_table_columns(TableAlias, [CI|_]=ColumnInfo) when is_atom(TableAlias), is_tuple(CI) ->
    ColumnNames = column_info_items(ColumnInfo, name),
    TableColumns = table_columns(TableAlias),
    MetaInfo = column_infos(TableAlias),    
    if
        ColumnNames /= TableColumns ->
            ?SystemException({"Column info does not match table structure",TableAlias}) ;
        ColumnInfo /= MetaInfo ->
            ?SystemException({"Column info does not match table metadata",TableAlias});
        true ->     ok                           
    end;
check_table_columns(TableAlias, ColumnNames) when is_atom(TableAlias) ->
    TableColumns = table_columns(TableAlias),
    if
        ColumnNames /= TableColumns ->
            ?SystemException({"Column info does not match table structure",TableAlias}) ;
        true ->     ok                           
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
meta_field_info(<<"node">>=N) ->
    #ddColumn{name=N, type='atom', len=30, prec=0};
meta_field_info(<<"user">>=N) ->
    #ddColumn{name=N, type='userid', len=20, prec=0};
meta_field_info(<<"username">>=N) ->
    #ddColumn{name=N, type='binstr', len=20, prec=0};
meta_field_info(<<"rownum">>=N) ->
    #ddColumn{name=N, type='integer', len=10, prec=0};
meta_field_info(Name) ->
    ?ClientError({"Unknown meta column",Name}). 

meta_field_value(<<"rownum">>) ->   1; 
meta_field_value(rownum) ->         1; 
meta_field_value(<<"username">>) -> <<"unknown">>; 
meta_field_value(username) ->       <<"unknown">>; 
meta_field_value(<<"user">>) ->     unknown; 
meta_field_value(user) ->           unknown; 
meta_field_value(Name) ->
    imem_if:meta_field_value(Name). 

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

column_infos(TableAlias) when is_atom(TableAlias) ->
    column_infos({schema(),TableAlias});    
column_infos({Schema,TableAlias}) when is_binary(Schema), is_binary(TableAlias) ->
    S= try 
        ?binary_to_existing_atom(Schema)
    catch 
        _:_ -> ?ClientError({"Schema does not exist",Schema})
    end,
    T = try 
        ?binary_to_existing_atom(TableAlias)
    catch 
        _:_ -> ?ClientError({"Table does not exist",TableAlias})
    end,        
    column_infos({S,T});
column_infos({Schema,TableAlias}) when is_atom(Schema), is_atom(TableAlias) ->
    case lists:member(TableAlias, ?DataTypes) of
        true -> 
            [#ddColumn{name=item, type=TableAlias, len=0, prec=0, default=undefined}
            ,#ddColumn{name=itemkey, type=binstr, len=0, prec=0, default=undefined}
            ];
        false ->
            case imem_if:read(ddTable,{Schema, physical_table_name(TableAlias)}) of
                [] ->                       ?ClientError({"Table does not exist",{Schema,TableAlias}}); 
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

create_table(TableAlias, Columns, Opts) ->
    create_table(TableAlias, Columns, Opts, #ddTable{}#ddTable.owner).

create_table(TableAlias, {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(ColumnNames, ColumnTypes, Defaults),
    create_physical_table(TableAlias,ColumnInfos,Opts,Owner);
create_table(TableAlias, [#ddColumn{}|_]=ColumnInfos, Opts, Owner) ->   
    Conv = fun(X) ->
        case X#ddColumn.name of
            A when is_atom(A) -> X; 
            B -> X#ddColumn{name=?binary_to_atom(B)} 
        end
    end,
    create_physical_table(qualified_new_table_name(TableAlias),lists:map(Conv,ColumnInfos),Opts,Owner);
create_table(TableAlias, ColumnNames, Opts, Owner) ->
    ColumnInfos = column_infos(ColumnNames),
    create_physical_table(qualified_new_table_name(TableAlias),ColumnInfos,Opts,Owner).

create_check_table(TableAlias, Columns, Opts) ->
    create_check_table(TableAlias, Columns, Opts, (#ddTable{})#ddTable.owner).

create_check_table(TableAlias, [#ddColumn{}|_]=ColumnInfos, Opts, Owner) ->
    Conv = fun(X) ->
        case X#ddColumn.name of
            A when is_atom(A) -> X; 
            B -> X#ddColumn{name=?binary_to_atom(B)} 
        end
    end,
    {ColumnNames, ColumnTypes, DefaultRecord} = from_column_infos(lists:map(Conv,ColumnInfos)),
    create_check_table(qualified_new_table_name(TableAlias), {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner);
create_check_table(TableAlias, {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(ColumnNames, ColumnTypes, Defaults),
    create_check_physical_table(TableAlias,ColumnInfos,Opts,Owner),
    check_table(TableAlias),
    check_table_columns(TableAlias, ColumnNames),
    check_table_meta(TableAlias, {ColumnNames, ColumnTypes, DefaultRecord}).

create_sys_conf(Path) ->
    imem_if_sys_conf:create_sys_conf(Path).    

create_check_physical_table(TableAlias,ColumnInfos,Opts,Owner) when is_atom(TableAlias) ->
    create_check_physical_table({schema(),TableAlias},ColumnInfos,Opts,Owner);    
create_check_physical_table({Schema,TableAlias},ColumnInfos,Opts0,Owner) ->
    MySchema = schema(),
    Opts1 = norm_opts(Opts0),
    case lists:member(Schema, [MySchema, ddSysConf]) of
        true ->
            PhysicalName=physical_table_name(TableAlias),
            case read(ddTable,{Schema,PhysicalName}) of 
                [] ->
                    create_physical_table({Schema,TableAlias},ColumnInfos,Opts1,Owner);
                [#ddTable{opts=Opts1,owner=Owner}] ->
                    catch create_physical_table({Schema,TableAlias},ColumnInfos,Opts1,Owner),
                    ok;
                [#ddTable{opts=Old,owner=Owner}] ->
                    OldOpts = lists:sort(lists:keydelete(purge_delay,1,Old)),
                    NewOpts = lists:sort(lists:keydelete(purge_delay,1,Opts1)),
                    case NewOpts of
                        OldOpts ->
                            catch create_physical_table({Schema,TableAlias},ColumnInfos,Opts1,Owner),
                            ok;
                        _ ->
                            catch create_physical_table({Schema,TableAlias},ColumnInfos,Opts1,Owner), 
                            ?SystemException({"Wrong table options",{TableAlias,Old}})
                    end;        
                [#ddTable{owner=Own}] ->
                    catch create_physical_table({Schema,TableAlias},ColumnInfos,Opts1,Owner),
                    ?SystemException({"Wrong table owner",{TableAlias,Own}})        
            end;
        _ ->        
            ?UnimplementedException({"Create/check table in foreign schema",{Schema,TableAlias}})
    end.

create_physical_table({Schema,TableAlias,_Alias},ColumnInfos,Opts,Owner) ->
    create_physical_table({Schema,TableAlias},ColumnInfos,Opts,Owner);
create_physical_table({Schema,TableAlias},ColumnInfos,Opts,Owner) ->
    MySchema = schema(),
    case Schema of
        MySchema ->  create_physical_table(TableAlias,ColumnInfos,Opts,Owner);
        ddSysConf -> create_table_sys_conf(TableAlias, ColumnInfos, norm_opts(Opts), Owner);
        _ ->    ?UnimplementedException({"Create table in foreign schema",{Schema,TableAlias}})
    end;
create_physical_table(TableAlias,ColInfos,Opts0,Owner) ->
    case is_valid_table_name(TableAlias) of
        true ->     ok;
        false ->    ?ClientError({"Invalid character(s) in table name",TableAlias})
    end,    
    case sqlparse:is_reserved(TableAlias) of
        false ->    ok;
        true ->     ?ClientError({"Reserved table name",TableAlias})
    end,
    Opts1 = norm_opts(Opts0),
    TypeMod = case lists:keyfind(type, 1, Opts1) of
        false ->                                        undefined;  % normal table
        {type,T} when T==set;T==ordered_set;T==bag ->   undefined;  % normal table
        {type,?MODULE} ->                                                 % module defined table
                ?ClientError({"Invalid module name for table type",{type,?MODULE}});
        {type,M} ->                                                 % module defined table
                case lists:member(M,erlang:loaded()) of
                    true -> case lists:member({create_table,4},M:module_info(exports)) of
                                true ->     M;
                                false ->    ?ClientError({"Bad module name for table type",{type,M}})
                            end;
                    false ->
                        ?ClientError({"Unknown module name for table type",{type,M}})
                end
    end,
    case {TypeMod,length(ColInfos)} of
        {undefined,0} ->    ?ClientError({"No columns given in create table",TableAlias});
        {undefined,1} ->    ?ClientError({"No value column given in create table, add dummy value column",TableAlias});
        _ ->                ok % no such check for module defined tables
    end,
    CharsCheck = [{is_valid_column_name(Name),Name} || Name <- column_info_items(ColInfos, name)],
    case lists:keyfind(false, 1, CharsCheck) of
        false ->    ok;
        {_,BadN} -> ?ClientError({"Invalid character(s) in column name",BadN})
    end,
    ReservedCheck = [{sqlparse:is_reserved(Name),Name} || Name <- column_info_items(ColInfos, name)],
    case lists:keyfind(true, 1, ReservedCheck) of
        false ->    ok;
        {_,BadC} -> ?ClientError({"Reserved column name",BadC})
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
        undefined ->    create_physical_standard_table(TableAlias,ColInfos,Opts1,Owner);
        _ ->            TypeMod:create_table(TableAlias,ColInfos,Opts1,Owner)
    end.

create_physical_standard_table(TableAlias,ColInfos,Opts0,Owner) ->
    MySchema = schema(),
    TableName = physical_table_name(TableAlias),
    case TableName of
        TA when TA==ddAlias;TA==?CACHE_TABLE ->  
            DDTableRow = #ddTable{qname={MySchema,TableName}, columns=ColInfos, opts=Opts0, owner=Owner},
            case (catch imem_if:read(ddTable, {MySchema,TableName})) of
                [] ->   
                    imem_if:write(ddTable, DDTableRow),
                    catch (imem_if:create_table(TableName, column_names(ColInfos), if_opts(Opts0) ++ [{user_properties, [DDTableRow]}]));    % ddTable meta data is missing
                _ ->    
                    imem_if:create_table(TableName, column_names(ColInfos), if_opts(Opts0) ++ [{user_properties, [DDTableRow]}])                                      % entry exists or ddTable does not exists yet
            end,
            imem_cache:clear({?MODULE, trigger, MySchema, TableName});
        TableAlias ->
            %% not a sharded table
            DDTableRow = #ddTable{qname={MySchema,TableName}, columns=ColInfos, opts=Opts0, owner=Owner},
            try
                imem_if:create_table(TableName, column_names(ColInfos), if_opts(Opts0) ++ [{user_properties, [DDTableRow]}]),
                imem_if:write(ddTable, DDTableRow)
            catch
                _:{'ClientError',{"Table already exists",TableName}} = Reason ->
                    case imem_if:read(ddTable, {MySchema,TableName}) of
                        [] ->   imem_if:write(ddTable, DDTableRow); % ddTable meta data was missing
                        _ ->    ok
                    end,
                    throw(Reason)
            end,
            imem_cache:clear({?MODULE, trigger, MySchema, TableName});
        _ ->
            %% Sharded table, check if ddAlias parameters match (if alias record already exists)
            [_,_,B,_,_,_] = parse_table_name(TableAlias), %% [Schema,".",Name,Period,"@",Node]
            Opts = case imem_if:read(ddAlias,{MySchema,TableAlias}) of
                [#ddAlias{columns=ColInfos, opts=Opts1, owner=Owner}] ->
                    Opts1;  %% ddAlias options override the given values for new partitions
                [#ddAlias{}] ->
                    ?ClientError({"Create table fails because columns or owner do not match with ddAlias",TableAlias});
                [] ->
                    case [ parse_table_name(TA) || #ddAlias{qname={S,TA}} <- imem_if:read(ddAlias),S==MySchema] of
                        [] ->           
                            ok;
                        ParsedTNs ->    
                            case length([ BB || [_,_,BB,_,_,_] <- ParsedTNs, BB==B]) of
                                0 ->    ok;
                                _ ->    ?ClientError({"Name conflict (different rolling period) in ddAlias",TableAlias})
                            end   
                    end,
                    case lists:keyfind(record_name, 1, Opts0) of
                        false ->    Opts0 ++ [{record_name,list_to_atom(B)}];
                        _ ->        Opts0
                    end
            end,
            DDAliasRow = #ddAlias{qname={MySchema,TableAlias}, columns=ColInfos, opts=Opts, owner=Owner},
            DDTableRow = #ddTable{qname={MySchema,TableName}, columns=ColInfos, opts=Opts, owner=Owner},
            try        
                imem_if:create_table(TableName, column_names(ColInfos), if_opts(Opts) ++ [{user_properties, [DDTableRow]}]),
                imem_if:write(ddTable, DDTableRow),
                imem_if:write(ddAlias, DDAliasRow)
            catch
                _:{'ClientError',{"Table already exists",TableName}} = Reason ->
                    case imem_if:read(ddTable, {MySchema,TableName}) of
                        [] ->   imem_if:write(ddTable, DDTableRow); % ddTable meta data was missing
                        _ ->    ok
                    end,
                    case imem_if:read(ddAlias, {MySchema,TableAlias}) of
                        [] ->   imem_if:write(ddAlias, DDAliasRow); % ddAlias meta data was missing
                        _ ->    ok
                    end,
                    throw(Reason)
            end,
            imem_cache:clear({?MODULE, trigger, MySchema, TableAlias}),
            imem_cache:clear({?MODULE, trigger, MySchema, TableName})
    end.

create_table_sys_conf(TableName, ColumnInfos, Opts, Owner) ->
    DDTableRow = #ddTable{qname={ddSysConf,TableName}, columns=ColumnInfos, opts=Opts, owner=Owner},
    return_atomic_ok(imem_if:write(ddTable, DDTableRow)).


get_trigger({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> get_trigger(Table);
        _ ->        ?UnimplementedException({"Get Trigger in foreign schema",{Schema,Table}})
    end;
get_trigger(Table) when is_atom(Table) ->
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=D] ->
            case lists:keysearch(trigger, 1, D#ddTable.opts) of
                false -> undefined;
                {value,{_,TFunStr}} ->  TFunStr
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end.

create_trigger({Schema,Table},TFun) ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_trigger(Table,TFun);
        _ ->        ?UnimplementedException({"Create Trigger in foreign schema",{Schema,Table}})
    end;
create_trigger(Table,TFunStr) when is_atom(Table) ->
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=D] -> 
            case lists:keysearch(trigger, 1, D#ddTable.opts) of
                false ->            create_or_replace_trigger(Table,TFunStr);
                {value,{_,TFunStr}} ->  ?ClientError({"Trigger already exists",{Table}});
                {value,{_,Trig}} ->     ?ClientError({"Trigger already exists",{Table,Trig}})
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end.

create_or_replace_trigger({Schema,Table},TFun) ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_or_replace_trigger(Table,TFun);
        _ ->        ?UnimplementedException({"Create Trigger in foreign schema",{Schema,Table}})
    end;
create_or_replace_trigger(Table,TFunStr) when is_atom(Table) ->
    imem_datatype:io_to_fun(TFunStr,4),
    case read(ddTable,{schema(), Table}) of
        [#ddTable{}=D] -> 
            Opts = lists:keydelete(trigger, 1, D#ddTable.opts) ++ [{trigger,TFunStr}],
            Trans = fun() ->
                write(ddTable, D#ddTable{opts=Opts}),                       
                imem_cache:clear({?MODULE, trigger, schema(), Table})
            end,
            return_atomic_ok(transaction(Trans));
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end;   
create_or_replace_trigger(Table,_) when is_atom(Table) ->
    ?ClientError({"Bad fun for create_or_replace_trigger, expecting arity 4", Table}).


create_index_table(IndexTable,ParentOpts,Owner) ->
    IndexOpts = case lists:keysearch(purge_delay, 1, ParentOpts) of
                false ->        ?DD_INDEX_OPTS;
                {value,PD} ->   ?DD_INDEX_OPTS ++ [{purge_delay,PD}]
    end,
    init_create_table(IndexTable, {record_info(fields, ddIndex), ?ddIndex, #ddIndex{}}, IndexOpts, Owner). 

create_index({Schema,Table},IndexDefinition) when is_list(IndexDefinition) ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_index(Table,IndexDefinition);
        _ ->        ?UnimplementedException({"Create Index in foreign schema",{Schema,Table}})
    end;
create_index(Table,IndexDefinition) when is_atom(Table),is_list(IndexDefinition) ->
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

create_or_replace_index({Schema,Table},IndexDefinition) when is_list(IndexDefinition) ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_or_replace_index(Table,IndexDefinition);
        _ ->        ?UnimplementedException({"Create Index in foreign schema",{Schema,Table}})
    end;
create_or_replace_index(Table,IndexDefinition) when is_atom(Table),is_list(IndexDefinition) ->
    Schema = schema(),
    case read(ddTable,{Schema, Table}) of
        [#ddTable{}=D] -> 
            Opts = lists:keydelete(index, 1, D#ddTable.opts) ++ [{index,IndexDefinition}],
            IndexTable = index_table(Table),
            case (catch check_table(IndexTable)) of
                ok ->   
                    Trans = fun() ->
                        lock({table, Table}, write),
                        write(ddTable, D#ddTable{opts=Opts}),                       
                        imem_cache:clear({?MODULE, trigger, Schema, Table}),
                        imem_if:truncate_table(IndexTable),
                        fill_index(Table,IndexDefinition)
                    end,
                    return_atomic_ok(transaction(Trans));
                _ ->
                    create_index_table(IndexTable,D#ddTable.opts,D#ddTable.owner),
                    Trans = fun() ->
                        lock({table, Table}, write),
                        write(ddTable, D#ddTable{opts=Opts}),                       
                        imem_cache:clear({?MODULE, trigger, Schema, Table}),
                        fill_index(Table,IndexDefinition)
                    end,
                    return_atomic_ok(transaction(Trans))
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end.

fill_index(Table,IndexDefinition) when is_atom(Table),is_list(IndexDefinition) ->
    Schema = schema(),
    case imem_if:read(ddTable,{Schema, Table}) of
        [] ->
            ?ClientError({"Table does not exist",{Schema, Table}}); 
        [#ddTable{columns=ColumnInfos}] ->
            IdxPlan = compiled_index_plan(IndexDefinition,ColumnInfos),
            FoldFun = fun(Row,_) -> imem_meta:update_index({},Row,Table,system,IdxPlan),ok end,
            foldl(FoldFun, ok, Table)
    end.

drop_index({Schema,Table}, Index) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_index(Table, Index);
        _ ->        ?UnimplementedException({"Drop Index in foreign schema",{Schema,Table}})
    end;
drop_index(Table, Index) when is_atom(Table) ->
    case read(ddTable,{schema(), Table}) of
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
                            ?ClientError({"Index does not exist for",Table,Index});
                        _ ->
                            if RestIndices =:= [] ->
                                   drop_index(Table);
                               true ->
                                   create_or_replace_index(Table, RestIndices)
                            end
                    end;
                false ->
                    drop_index(Table)
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end.

drop_index({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_index(Table);
        _ ->        ?UnimplementedException({"Drop Index in foreign schema",{Schema,Table}})
    end;
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


drop_trigger({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_trigger(Table);
        _ ->        ?UnimplementedException({"Drop Trigger in foreign schema",{Schema,Table}})
    end;
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

is_valid_table_name(Table) when is_atom(Table) ->
    is_valid_table_name(atom_to_list(Table));
is_valid_table_name(Table) when is_list(Table) ->
    [H|_] = Table,
    L = lists:last(Table),
    if
        H == $" andalso L == $" -> true;
        true -> (length(Table) == length(Table -- ?BAD_NAME_CHARACTERS))
    end.

is_valid_column_name(Column) ->
    is_valid_table_name(atom_to_list(Column)).

norm_opts(Opts) ->  % Normalize option datatypes (binstr -> atom) and do some checks 
    [norm_opt(O) || O <- Opts].

norm_opt({trigger,V}) -> {trigger,V};   % trigger is the only binary option for now
norm_opt({K,V}) when is_binary(V) ->
    case (catch ?binary_to_existing_atom(V)) of
        A when is_atom(A) -> {K,A};
        _ ->   ?ClientError({"Unsupported table option",{K,V}})
    end;
norm_opt(Opt) -> Opt.

if_opts(Opts) ->    % Remove imem_meta table options which are not recognized by imem_if
    if_opts(Opts,?META_OPTS).

if_opts([],_) -> [];
if_opts(Opts,[]) -> Opts;
if_opts(Opts,[MO|Others]) ->
    if_opts(lists:keydelete(MO, 1, Opts),Others).

truncate_table(TableAlias) ->
    truncate_table(TableAlias,meta_field_value(user)).

truncate_table({Schema,TableAlias,_Alias},User) ->
    truncate_table({Schema,TableAlias},User);    
truncate_table({Schema,TableAlias},User) ->
    MySchema = schema(),
    case Schema of
        MySchema -> truncate_table(TableAlias, User);
        _ ->        ?UnimplementedException({"Truncate table in foreign schema",{Schema,TableAlias}})
    end;
truncate_table(TableAlias,User) when is_atom(TableAlias) ->
    %% log_to_db(debug,?MODULE,truncate_table,[{table,TableAlias}],"truncate table"),
    truncate_partitioned_tables(lists:sort(simple_or_local_node_sharded_tables(TableAlias)),User);
truncate_table(TableAlias, User) ->
    truncate_table(qualified_table_name(TableAlias),User).

truncate_partitioned_tables([],_) -> ok;
truncate_partitioned_tables([TableName|TableNames], User) ->
    {_, _, Trigger} =  trigger_infos(TableName),
    IndexTable = index_table(TableName),
    Trans = case imem_if:is_readable_table(IndexTable) of
        true -> 
            fun() ->
                Trigger({},{},TableName,User),
                imem_if:truncate_table(IndexTable),
                imem_if:truncate_table(TableName)
            end;
        false ->
            fun() ->
                Trigger({},{},TableName,User),
                imem_if:truncate_table(TableName)
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
    log_to_db(debug,?MODULE,snapshot_table,[{table,Alias}],"snapshot table"),
    case lists:sort(simple_or_local_node_sharded_tables(Alias)) of
        [] ->   ?ClientError({"Table does not exist",Alias});
        PTNs -> case lists:usort([check_table(T) || T <- PTNs]) of
                    [ok] -> snapshot_partitioned_tables(PTNs);
                    _ ->    ?ClientError({"Table does not exist",Alias})
                end
    end;
snapshot_table(TableName) ->
    snapshot_table(qualified_table_name(TableName)).

snapshot_partitioned_tables([]) -> ok;
snapshot_partitioned_tables([TableName|TableNames]) ->
    imem_snap:take(TableName),
    snapshot_partitioned_tables(TableNames).

restore_table({_Schema,Table,_Alias}) ->
    restore_table({_Schema,Table});    
restore_table({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> restore_table(Table);
        _ ->        ?UnimplementedException({"Restore table in foreign schema",{Schema,Table}})
    end;
restore_table(Alias) when is_atom(Alias) ->
    log_to_db(debug,?MODULE,restore_table,[{table,Alias}],"restore table"),
    case lists:sort(simple_or_local_node_sharded_tables(Alias)) of
        [] ->   ?ClientError({"Table does not exist",Alias});
        PTNs -> case imem_snap:restore(bkp,PTNs,destroy,false) of
                    L when is_list(L) ->    ok;
                    E ->                    ?SystemException({"Restore table failed with",E})
                end
    end;    
restore_table(TableName) ->
    restore_table(qualified_table_name(TableName)).

drop_table({Schema,TableAlias}) when is_atom(Schema), is_atom(TableAlias) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_table(TableAlias);
        _ ->        ?UnimplementedException({"Drop table in foreign schema",{Schema,TableAlias}})
    end;
drop_table(TableAlias) when is_atom(TableAlias) ->
    case is_system_table(TableAlias) of
        true -> ?ClientError({"Cannot drop system table",TableAlias});
        false-> drop_tables_and_infos(TableAlias,lists:sort(simple_or_local_node_sharded_tables(TableAlias)))
    end;
drop_table(TableAlias) when is_binary(TableAlias) ->
    drop_table(qualified_table_name(TableAlias)).

drop_system_table(TableAlias) when is_atom(TableAlias) ->
    case is_system_table(TableAlias) of
        false -> ?ClientError({"Not a system table",TableAlias});
        true ->  drop_tables_and_infos(TableAlias,lists:sort(simple_or_local_node_sharded_tables(TableAlias)))
    end.

drop_tables_and_infos(TableName,[TableName]) ->
    drop_table_and_info(TableName);
drop_tables_and_infos(TableAlias, TableList) -> 
     imem_if:delete(ddAlias, {schema(),TableAlias}),
     drop_partitions_and_infos(TableList).

drop_partitions_and_infos([]) -> ok;
drop_partitions_and_infos([TableName|TableNames]) ->
    drop_table_and_info(TableName),
    drop_partitions_and_infos(TableNames).

drop_table_and_info(TableName) ->
    try
        imem_if:drop_table(TableName),
        case TableName of
            ddTable ->  ok;
            ddAlias ->  ok;
            _ ->        imem_if:delete(ddTable, {schema(),TableName})
        end
    catch
        throw:{'ClientError',{"Table does not exist",Table}} ->
            catch imem_if:delete(ddTable, {schema(),TableName}),
            throw({'ClientError',{"Table does not exist",Table}})
    end.       

purge_table(TableAlias) ->
    purge_table(TableAlias, []).

purge_table({Schema,TableAlias,_Alias}, Opts) -> 
    purge_table({Schema,TableAlias}, Opts);
purge_table({Schema,TableAlias}, Opts) ->
    MySchema = schema(),
    case Schema of
        MySchema -> purge_table(TableAlias, Opts);
        _ ->        ?UnimplementedException({"Purge table in foreign schema",{Schema,TableAlias}})
    end;
purge_table(TableAlias, Opts) ->
    case is_time_partitioned_alias(TableAlias) of
        false ->    
            ?UnimplementedException({"Purge not supported on this table type",TableAlias});
        true ->
            purge_time_partitioned_table(TableAlias, Opts)
    end.

purge_time_partitioned_table(TableAlias, Opts) ->
    case lists:sort(simple_or_local_node_sharded_tables(TableAlias)) of
        [] ->
            ?ClientError({"Table to be purged does not exist",TableAlias});
        [TableName|_] ->
            KeepTime = case proplists:get_value(purge_delay, Opts) of
                undefined ->    erlang:now();
                Seconds ->      {Mega,Secs,Micro} = erlang:now(),
                                {Mega,Secs-Seconds,Micro}
            end,
            KeepName = partitioned_table_name(TableAlias,KeepTime),
            if  
                TableName >= KeepName ->
                    0;      %% no memory could be freed       
                true ->
                    FreedMemory = table_memory(TableName),
                    ?Info("Purge time partition ~p~n",[TableName]),
                    drop_table_and_info(TableName),
                    FreedMemory
            end
    end.

simple_or_local_node_sharded_tables(TableAlias) ->    
    case is_node_sharded_alias(TableAlias) of
        true ->
            case is_time_partitioned_alias(TableAlias) of
                true ->
                    Tail = lists:reverse("@" ++ node_shard()),
                    Pred = fun(TN) -> lists:prefix(Tail, lists:reverse(atom_to_list(TN))) end,
                    lists:filter(Pred,physical_table_names(TableAlias));
                false ->
                    [physical_table_name(TableAlias)]
            end;        
        false ->
            physical_table_names(TableAlias)
    end.

is_node_sharded_alias(TableAlias) when is_atom(TableAlias) -> 
    is_node_sharded_alias(atom_to_list(TableAlias));
is_node_sharded_alias(TableAlias) when is_list(TableAlias) -> 
    (lists:last(TableAlias) == $@).

is_time_partitioned_alias(TableAlias) when is_atom(TableAlias) ->
    is_time_partitioned_alias(atom_to_list(TableAlias));
is_time_partitioned_alias(TableAlias) when is_list(TableAlias) ->
    TableAliasRev = lists:reverse(TableAlias),
    case TableAliasRev of
        [$_,$@|_] ->    is_reverse_timed_name(tl(TableAliasRev));
        [$@|_] ->       is_reverse_timed_name(TableAliasRev);
         _ ->           false
    end.

is_reverse_timed_name(TableAliasRev) ->
    case string:tokens(TableAliasRev, "_") of
        [[$@|RN]|_] -> 
            try 
                _ = list_to_integer(RN),
                true    % timestamp partitioned
            catch
                _:_ -> false
            end;
         _ ->      
            false       % unsharded or node sharded alias only
    end.

is_local_node_sharded_table(Name) when is_atom(Name) -> 
    is_local_node_sharded_table(atom_to_list(Name));
is_local_node_sharded_table(Name) when is_list(Name) -> 
    lists:suffix([$@|node_shard()],Name).

is_local_time_partitioned_table(Name) when is_atom(Name) ->
    is_local_time_partitioned_table(atom_to_list(Name));
is_local_time_partitioned_table(Name) when is_list(Name) ->
    case is_local_node_sharded_table(Name) of
        false ->    false;
        true ->     is_time_partitioned_table(Name)
    end.

is_time_partitioned_table(Name) when is_atom(Name) ->
    is_time_partitioned_table(atom_to_list(Name));
is_time_partitioned_table(Name) when is_list(Name) ->
    case parse_table_name(Name) of
        [_,_,_,"",_,_] ->   false;
        _ ->                true
    end.

-spec parse_table_name(atom()|list()|binary()) -> list(list()).
    %% TableName -> [Schema,".",Name,Period,"@",Node] all strings , all optional ("") except Name
parse_table_name(TableName) when is_atom(TableName) -> 
    parse_table_name(atom_to_list(TableName));
parse_table_name(TableName) when is_list(TableName) ->
    case string:tokens(TableName, ".") of
        [R2] ->         ["",""|parse_simple_name(R2)];
        [Schema|R1] ->  [Schema,"."|parse_simple_name(string:join(R1,"."))]
    end;
parse_table_name(TableName) when is_binary(TableName) ->
    parse_table_name(binary_to_list(TableName)).

parse_simple_name(TableName) when is_list(TableName) ->
    %% TableName -> [Name,Period,"@",Node] all strings , all optional except Name        
    case string:tokens(TableName, "@") of
        [TableName] ->    
            [TableName,"","",""];
        [Name] ->    
            case string:tokens(Name, "_") of  
                [Name] ->  
                    [Name,"","@",""];
                BL ->     
                    case catch list_to_integer(lists:last(BL)) of
                        I when is_integer(I) ->
                            [string:join(lists:sublist(BL,length(BL)-1),"."),lists:last(BL),"@",""];
                        _ ->
                            [Name,"","@",""]
                    end
            end;
        [Name,Node] ->
            case string:tokens(Name, "_") of  
                [Name] ->  
                    [Name,"","@",Node];
                BL ->     
                    case catch list_to_integer(lists:last(BL)) of
                        I when is_integer(I) ->
                            [string:join(lists:sublist(BL,length(BL)-1),"."),lists:last(BL),"@",Node];
                        _ ->
                            [Name,"","@",Node]
                    end
            end;
        _ ->
            [TableName,"","",""]
    end.


-spec index_table_name(atom()|binary()|list()) -> binary().
index_table_name(Table) when is_atom(Table) ->
    index_table_name(atom_to_list(Table));
index_table_name(Table) when is_binary(Table) ->
    index_table_name(binary_to_list(Table));
index_table_name(Table) when is_list(Table) ->  list_to_binary(?INDEX_TABLE(Table)).

-spec index_table(atom()|binary()|list()) -> atom().
index_table(Table) -> binary_to_atom(index_table_name(Table),utf8).

time_to_partition_expiry(Table) when is_atom(Table) ->
    time_to_partition_expiry(atom_to_list(Table));
time_to_partition_expiry(Table) when is_list(Table) ->
    case parse_table_name(Table) of
        [_Schema,_Dot,_BaseName,"",_Aterate,_Shard] ->
            ?ClientError({"Not a time partitioned table",Table});     
        [_Schema,_Dot,_BaseName,Number,_Aterate,_Shard] ->
            {Mega,Secs,_} = erlang:now(),
            list_to_integer(Number) - Mega * 1000000 - Secs
    end.

time_of_partition_expiry(Table) when is_atom(Table) ->
    time_of_partition_expiry(atom_to_list(Table));
time_of_partition_expiry(Table) when is_list(Table) ->
    case parse_table_name(Table) of
        [_Schema,_Dot,_BaseName,"",_Aterate,_Shard] ->
            ?ClientError({"Not a time partitioned table",Table});     
        [_Schema,_Dot,_BaseName,N,_Aterate,_Shard] ->
            Number = list_to_integer(N),
            {Number div 1000000, Number rem 1000000, 0}
    end.

% physical_table_name({_S,N,_A}) -> physical_table_name(N);
physical_table_name({_S,N}) -> physical_table_name(N);
physical_table_name(dba_tables) -> ddTable;
physical_table_name(all_tables) -> ddTable;
physical_table_name(all_aliases) -> ddAlias;
physical_table_name(user_tables) -> ddTable;
physical_table_name(TableAlias) when is_atom(TableAlias) ->
    case lists:member(TableAlias,?DataTypes) of
        true ->     TableAlias;
        false ->    physical_table_name(atom_to_list(TableAlias))
    end;
physical_table_name(TableAlias) when is_list(TableAlias) ->
    case lists:reverse(TableAlias) of
        [$@|_] ->       partitioned_table_name(TableAlias,erlang:now());    % node sharded, maybe time sharded
        [$_,$@|_] ->    partitioned_table_name(TableAlias,erlang:now());    % time sharded only
        _ ->            list_to_atom(TableAlias)
    end.

% physical_table_name({_S,N,_A},Key) -> physical_table_name(N,Key);
physical_table_name({_S,N},Key) -> physical_table_name(N,Key);
physical_table_name(dba_tables,_) -> ddTable;
physical_table_name(all_tables,_) -> ddTable;
physical_table_name(all_aliases,_) -> ddAlias;
physical_table_name(user_tables,_) -> ddTable;
physical_table_name(TableAlias,Key) when is_atom(TableAlias) ->
    case lists:member(TableAlias,?DataTypes) of
        true ->     TableAlias;
        false ->    physical_table_name(atom_to_list(TableAlias),Key)
    end;
physical_table_name(TableAlias,Key) when is_list(TableAlias) ->
    case lists:reverse(TableAlias) of
        [$@|_] ->       partitioned_table_name(TableAlias,Key);    % node sharded, maybe time sharded
        [$_,$@|_] ->    partitioned_table_name(TableAlias,Key);    % time sharded only
        _ ->            list_to_atom(TableAlias)
    end.

physical_table_names({_S,N,_A}) -> physical_table_names(N);
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
physical_table_names(TableAlias) when is_list(TableAlias) ->
    TableAliasRev = lists:reverse(TableAlias),
    case TableAliasRev of
        [$@|_] ->       
            case string:tokens(TableAliasRev, "_") of
                [[$@|RN]|_] ->                          % possibly time sharded node sharded partitions 
                    try 
                        _ = list_to_integer(lists:reverse(RN)),
                        {BaseName,_} = lists:split(length(TableAlias)-length(RN)-1, TableAlias),
                        Pred = fun(TN) -> lists:member($@, atom_to_list(TN)) end,
                        lists:filter(Pred,tables_starting_with(BaseName))
                    catch
                        _:_ -> tables_starting_with(TableAlias) % node sharded table only
                    end;
                 _ ->               
                    tables_starting_with(TableAlias)            % node sharded table only
            end;
        [$_,$@|_] ->    
            case string:tokens(tl(TableAliasRev), "_") of
                [[$@|RN]|_] when length(RN) >= 10 ->      
                     [list_to_atom(TableAlias)];                % timestamp sharded cluster table
                [[$@|RN]|_] ->                                  % timestamp sharded cluster alias 
                    try 
                        _ = list_to_integer(lists:reverse(RN)),
                        {BaseName,_} = lists:split(length(TableAlias)-length(RN)-2, TableAlias),
                        Pred = fun(TN) -> lists:member($@, atom_to_list(TN)) end,
                        lists:filter(Pred,tables_starting_with(BaseName))
                    catch
                        _:_ -> [list_to_atom(TableAlias)]       % plain table name, not time sharded
                    end;
                 _ ->   
                    [list_to_atom(TableAlias)]                  % plain table name, not time sharded
            end;
        _ ->
            [list_to_atom(TableAlias)]
    end.

partitioned_table_name(TableAlias,Key) ->
    list_to_atom(partitioned_table_name_str(TableAlias,Key)).

partitioned_table_name_str(TableAlias,Key) when is_atom(TableAlias) ->
    partitioned_table_name_str(atom_to_list(TableAlias),Key);
partitioned_table_name_str(TableAlias,Key) when is_list(TableAlias) ->
    TableAliasRev = lists:reverse(TableAlias),
    case TableAliasRev of
        [$@|_] ->       
            [[$@|RN]|_] = string:tokens(TableAliasRev, "_"), 
            try 
                Period = list_to_integer(lists:reverse(RN)),
                {Mega,Sec,_} = Key,
                PartitionEnd=integer_to_list(Period*((1000000*Mega+Sec) div Period) + Period),
                Prefix = lists:duplicate(10-length(PartitionEnd),$0),
                {BaseName,_} = lists:split(length(TableAlias)-length(RN)-1, TableAlias),
                lists:flatten(BaseName ++ Prefix ++ PartitionEnd ++ "@" ++ node_shard())
                % timestamp partitiond and node sharded table
            catch
                _:_ -> lists:flatten(TableAlias ++ node_shard())
            end;
        [$_,$@|_] ->       
            [[$@|RN]|_] = string:tokens(tl(TableAliasRev), "_"),
            case length(RN) of
                10 ->   TableAlias;         % this is a partition name
                _ ->
                    try 
                        Period = list_to_integer(lists:reverse(RN)),
                        {Mega,Sec,_} = Key,
                        PartitionEnd=integer_to_list(Period*((1000000*Mega+Sec) div Period) + Period),
                        Prefix = lists:duplicate(10-length(PartitionEnd),$0),
                        {BaseName,_} = lists:split(length(TableAlias)-length(RN)-2, TableAlias),
                        lists:flatten(BaseName ++ Prefix ++ PartitionEnd ++ "@_" )
                        % timestamp partitioned cluster table
                    catch
                        _:_ -> TableAlias   % this is a table name
                    end
            end;
        _ ->
            TableAlias    
    end.

qualified_table_name({undefined,Table}) when is_atom(Table) ->              {schema(),Table};
qualified_table_name(Table) when is_atom(Table) ->                          {schema(),Table};
qualified_table_name({Schema,Table}) when is_atom(Schema),is_atom(Table) -> {Schema,Table};
qualified_table_name({undefined,T}) when is_binary(T) ->
    try
        {schema(),?binary_to_existing_atom(T)}
    catch
        _:_ -> ?ClientError({"Unknown Table name",T})
    end;
qualified_table_name({S,T}) when is_binary(S),is_binary(T) ->
    try
        {?binary_to_existing_atom(S),?binary_to_existing_atom(T)}
    catch
        _:_ -> ?ClientError({"Unknown Schema or Table name",{S,T}})
    end;
qualified_table_name(Table) when is_binary(Table) ->                        
    qualified_table_name(imem_sql_expr:binstr_to_qname2(Table)).

qualified_new_table_name({undefined,Table}) when is_atom(Table) ->              {schema(),Table};
qualified_new_table_name({undefined,Table}) when is_binary(Table) ->            {schema(),?binary_to_atom(Table)};
qualified_new_table_name({Schema,Table}) when is_atom(Schema),is_atom(Table) -> {Schema,Table};
qualified_new_table_name({S,T}) when is_binary(S),is_binary(T) ->               {?binary_to_atom(S),?binary_to_atom(T)};
qualified_new_table_name(Table) when is_atom(Table) ->                          {schema(),Table};
qualified_new_table_name(Table) when is_binary(Table) ->
    qualified_new_table_name(imem_sql_expr:binstr_to_qname2(Table)).

tables_starting_with(Prefix) when is_atom(Prefix) ->
    tables_starting_with(atom_to_list(Prefix));
tables_starting_with(Prefix) when is_list(Prefix) ->
    atoms_starting_with(Prefix,all_tables()).

atoms_starting_with(Prefix,Atoms) ->
    atoms_starting_with(Prefix,Atoms,[]). 

atoms_starting_with(_,[],Acc) -> lists:sort(Acc);
atoms_starting_with(Prefix,[A|Atoms],Acc) ->
    case lists:prefix(Prefix,atom_to_list(A)) of
        true ->     atoms_starting_with(Prefix,Atoms,[A|Acc]);
        false ->    atoms_starting_with(Prefix,Atoms,Acc)
    end.

tables_ending_with(Suffix) when is_atom(Suffix) ->
    tables_ending_with(atom_to_list(Suffix));
tables_ending_with(Suffix) when is_list(Suffix) ->
    atoms_ending_with(Suffix,all_tables()).

atoms_ending_with(Suffix,Atoms) ->
    atoms_ending_with(Suffix,Atoms,[]).

atoms_ending_with(_,[],Acc) -> lists:sort(Acc);
atoms_ending_with(Suffix,[A|Atoms],Acc) ->
    case lists:suffix(Suffix,atom_to_list(A)) of
        true ->     atoms_ending_with(Suffix,Atoms,[A|Acc]);
        false ->    atoms_ending_with(Suffix,Atoms,Acc)
    end.


%% one to one from imme_if -------------- HELPER FUNCTIONS ------


compile_fun(Binary) when is_binary(Binary) ->
    compile_fun(binary_to_list(Binary)); 
compile_fun(String) when is_list(String) ->
    try  
        Code = case [lists:last(string:strip(String))] of
            "." -> String;
            _ -> String ++ "."
        end,
        {ok,ErlTokens,_}=erl_scan:string(Code),    
        {ok,ErlAbsForm}=erl_parse:parse_exprs(ErlTokens),    
        {value,Fun,_}=erl_eval:exprs(ErlAbsForm,[]),    
        Fun
    catch
        _:Reason ->
            ?Error("Compiling script function ~p results in ~p",[String,Reason]), 
            undefined
    end.

schema() ->
    imem_if:schema().

schema(Node) ->
    imem_if:schema(Node).

system_id() ->
    imem_if:system_id().

add_attribute(A, Opts) -> 
    imem_if:add_attribute(A, Opts).

update_opts(T, Opts) ->
    imem_if:update_opts(T, Opts).

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
    LogRec = #ddLog{logTime=erlang:now(),logLevel=Level,pid=self()
                    ,module=Module,function=Function,line=Line,node=node()
                    ,fields=Fields,message=Message,stacktrace=StackTrace
                    },
    dirty_write(?LOG_TABLE, LogRec).


log_slow_process(Module,Function,STT,LimitWarning,LimitError,Fields) ->
    DurationMs = imem_datatype:msec_diff(STT),
    if 
        DurationMs < LimitWarning ->    ok;
        DurationMs < LimitError ->      log_to_db(warning,Module,Function,Fields,"slow_process",[]);
        true ->                         log_to_db(error,Module,Function,Fields,"slow_process",[])
    end.

%% imem_if but security context added --- META INFORMATION ------

data_nodes() ->
    imem_if:data_nodes().

all_tables() ->
    imem_if:all_tables().

all_aliases() ->
    MySchema = schema(),
    [A || #ddAlias{qname={S,A}} <- imem_if:read(ddAlias),S==MySchema].

is_readable_table({_Schema,Table}) ->
    is_readable_table(Table);   %% ToDo: may depend on schema
is_readable_table(Table) ->
    imem_if:is_readable_table(Table).

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
                _:_ ->  ?Debug("bad config parameter ~p~n", [node_shard_fun]),
                        "nohost"
            end;
        {ok,host_name} ->                host_name(node());    
        {ok,host_fqdn} ->                host_fqdn(node());    
        {ok,node_name} ->                node_name(node());    
        {ok,node_hash} ->                node_hash(node());    
        {ok,NA} when is_atom(NA) ->      atom_to_list(NA);
        _Else ->    ?Debug("bad config parameter ~p ~p~n", [node_shard, _Else]),
                    node_hash(node())
    end.

node_shard_value({ok,FunStr},Node) ->
    % ?Debug("node_shard calculated for ~p~n", [FunStr]),
    Code = case [lists:last(string:strip(FunStr))] of
        "." -> FunStr;
        _ -> FunStr ++ "."
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

node_name(Node) when is_atom(Node) -> 
    NodeStr = atom_to_list(Node),
    [Name,_] = string:tokens(NodeStr, "@"),
    Name.

node_hash(Node) when is_atom(Node) ->
    io_lib:format("~6.6.0w",[erlang:phash2(Node, 1000000)]).

-spec trigger_infos(atom()|{atom(),atom()}) -> {TableType :: atom(), DefaultRecord :: tuple(), TriggerFun :: function()}.
trigger_infos(Table) when is_atom(Table) ->
    trigger_infos({schema(),Table});
trigger_infos({Schema,Table}) when is_atom(Schema),is_atom(Table) ->
    Key = {?MODULE,trigger,Schema,Table},
    case imem_cache:read(Key) of 
        [] ->
            case imem_if:read(ddTable,{Schema, Table}) of
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
                            fun(_Old,_New,_Tab,_User) -> ok end;
                        {_,false} ->                %% only index maintenance trigger needed
                            fun(Old,New,Tab,User) -> imem_meta:update_index(Old,New,Tab,User,IdxPlan) end;
                        {_,{_,TFun}} ->             %% custom trigger and index maintenance needed
                            TriggerWithIndexing = trigger_with_indexing(TFun,<<"imem_meta:update_index">>,<<"IdxPlan">>),
                            imem_datatype:io_to_fun(TriggerWithIndexing,undefined,[{'IdxPlan',IdxPlan}])
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
    IdxFun;                                                 %% return the arity 1 function for vnf
compile_or_generate(IdxFun) when is_function(IdxFun,2) ->
    IdxFun;                                                 %% return the arity 2 function for iff
compile_or_generate(IdxFun) when is_function(IdxFun,0) ->
    IdxFun();                                               %% arity 0 function is a function generator
compile_or_generate(Bad) ->
    ?ClientError({"Bad index fun",Bad}).

json_pos_list(PathList) -> 
    json_pos_list(PathList,[]).

json_pos_list([],Acc) -> Acc;
json_pos_list([{_ParseTreeTuple,FM}|Rest],Acc) -> json_pos_list(Rest,Acc ++ [Pos || {_Name,Pos} <- FM]);
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
    {ok, FieldList} = jpparse:roots(ParseTreeTuple),
    Pred = fun({Name,_Pos}) -> lists:member(Name,FieldList) end,
    case lists:filter(Pred, FieldMap) of
        [] ->
            {ok, JsonPath} = jpparse:string(ParseTreeTuple),
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

table_type({ddSysConf,Table}) ->
    imem_if_sys_conf:table_type(Table);
table_type({_Schema,Table}) ->
    table_type(Table);          %% ToDo: may depend on schema
table_type(Table) when is_atom(Table) ->
    imem_if:table_type(physical_table_name(Table)).

table_record_name({ddSysConf,Table}) ->
    imem_if_sys_conf:table_record_name(Table);   %% ToDo: may depend on schema
table_record_name({_Schema,Table}) ->
    table_record_name(Table);   %% ToDo: may depend on schema
table_record_name(ddNode)  -> ddNode;
table_record_name(ddSchema)  -> ddSchema;
table_record_name(ddSize)  -> ddSize;
table_record_name(Table) when is_atom(Table) ->
    imem_if:table_record_name(physical_table_name(Table)).

table_columns({ddSysConf,Table}) ->
    imem_if_sys_conf:table_columns(Table);
table_columns({_Schema,Table}) ->
    table_columns(Table);       %% ToDo: may depend on schema
table_columns(Table) ->
    imem_if:table_columns(physical_table_name(Table)).

table_size({ddSysConf,_Table}) ->
    %% imem_if_sys_conf:table_size(Table);
    0;                                                  %% ToDo: implement there
table_size({_Schema,Table}) ->  table_size(Table);      %% ToDo: may depend on schema
table_size(ddNode) ->           length(read(ddNode));
table_size(ddSchema) ->         length(read(ddSchema));
table_size(ddSize) ->           1;
table_size(Table) ->
    %% ToDo: sum should be returned for all local time partitions
    imem_if:table_size(physical_table_name(Table)).

table_memory({ddSysConf,_Table}) ->
    %% imem_if_sys_conf:table_memory(Table);
    0;                                                  %% ToDo: implement there                    
table_memory({_Schema,Table}) ->
    table_memory(Table);                                %% ToDo: may depend on schema
table_memory(Table) ->
    %% ToDo: sum should be returned for all local time partitions
    imem_if:table_memory(physical_table_name(Table)).

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
fetch_start(Pid, {_Schema,Table}, MatchSpec, BlockSize, Opts) ->
    fetch_start(Pid, Table, MatchSpec, BlockSize, Opts);          %% ToDo: may depend on schema
fetch_start(Pid, ddNode, MatchSpec, BlockSize, Opts) ->
    fetch_start_virtual(Pid, ddNode, MatchSpec, BlockSize, Opts);
fetch_start(Pid, ddSchema, MatchSpec, BlockSize, Opts) ->
    fetch_start_virtual(Pid, ddSchema, MatchSpec, BlockSize, Opts);
fetch_start(Pid, ddSize, MatchSpec, BlockSize, Opts) ->
    fetch_start_virtual(Pid, ddSize, MatchSpec, BlockSize, Opts);
fetch_start(Pid, Table, MatchSpec, BlockSize, Opts) ->
    imem_if:fetch_start(Pid, physical_table_name(Table), MatchSpec, BlockSize, Opts).

fetch_start_virtual(Pid, VTable, MatchSpec, _BlockSize, _Opts) ->
    % ?Debug("Virtual fetch start  : ~p ~p~n", [VTable,MatchSpec]),
    {Rows,true} = select(VTable, MatchSpec),
    % ?Debug("Virtual fetch result  : ~p~n", [Rows]),
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
read({_Schema,Table}) -> 
    read(Table);            %% ToDo: may depend on schema
read(ddNode) ->
    lists:flatten([read(ddNode,Node) || Node <- [node()|nodes()]]);
read(ddSchema) ->
    [{ddSchema,{Schema,Node},[]} || {Schema,Node} <- data_nodes()];
read(ddSize) ->
    [hd(read(ddSize,Name)) || Name <- all_tables()];
read(Table) ->
    imem_if:read(physical_table_name(Table)).

read({ddSysConf,Table}, _Key) -> 
    % imem_if_sys_conf:read(physical_table_name(Table),Key);
    ?UnimplementedException({"Cannot read from ddSysConf schema, use DDerl GUI instead",Table});
read({_Schema,Table}, Key) ->
    read(Table, Key);
read(ddNode,Node) when is_atom(Node) ->
    case rpc:call(Node,erlang,statistics,[wall_clock],?DDNODE_TIMEOUT) of
        {WC,WCDiff} when is_integer(WC), is_integer(WCDiff) ->
            case rpc:call(Node,erlang,now,[],?DDNODE_TIMEOUT) of
                {Meg,Sec,Mic} when is_integer(Meg),is_integer(Sec),is_integer(Mic) ->                        
                    [#ddNode{ name=Node
                             , wall_clock=WC
                             , time={Meg,Sec,Mic}
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
    imem_if:read(physical_table_name(Table), Key).

dirty_read({ddSysConf,Table}, Key) -> read({ddSysConf,Table}, Key);
dirty_read({_Schema,Table}, Key) ->   dirty_read(Table, Key);
dirty_read(ddNode,Node) ->  read(ddNode,Node); 
dirty_read(ddSchema,Key) -> read(ddSchema,Key);
dirty_read(ddSize,Table) -> read(ddSize,Table);
dirty_read(Table, Key) ->   imem_if:dirty_read(physical_table_name(Table), Key).


read_hlk({_Schema,Table}, HListKey) -> 
    read_hlk(Table, HListKey);
read_hlk(Table,HListKey) ->
    imem_if:read_hlk(Table,HListKey).

get_config_hlk({_Schema,Table}, Key, Owner, Context, Default) ->
    get_config_hlk(Table, Key, Owner, Context, Default);
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

put_config_hlk({_Schema,Table}, Key, Owner, Context, Value, Remark) ->
    put_config_hlk(Table, Key, Owner, Context, Value, Remark);
put_config_hlk(Table, Key, Owner, Context, Value, Remark) when is_atom(Table), is_list(Context), is_binary(Remark) ->
    write(Table,#ddConfig{hkl=[Key|Context], val=Value, remark=Remark, owner=Owner}).

select({ddSysConf,Table}, _MatchSpec) ->
    % imem_if_sys_conf:select(physical_table_name(Table), MatchSpec);
    ?UnimplementedException({"Cannot select from ddSysConf schema, use DDerl GUI instead",Table});
select({_Schema,Table}, MatchSpec) ->
    select(Table, MatchSpec);           %% ToDo: may depend on schema
select(ddNode, MatchSpec) ->
    select_virtual(ddNode, MatchSpec);
select(ddSchema, MatchSpec) ->
    select_virtual(ddSchema, MatchSpec);
select(ddSize, MatchSpec) ->
    select_virtual(ddSize, MatchSpec);
select(Table, MatchSpec) ->
    imem_if:select(physical_table_name(Table), MatchSpec).

select(Table, MatchSpec, 0) ->
    select(Table, MatchSpec);
select({ddSysConf,Table}, _MatchSpec, _Limit) ->
    % imem_if_sys_conf:select(physical_table_name(Table), MatchSpec, Limit);
    ?UnimplementedException({"Cannot select from ddSysConf schema, use DDerl GUI instead",Table});
select({_Schema,Table}, MatchSpec, Limit) ->
    select(Table, MatchSpec, Limit);        %% ToDo: may depend on schema
select(ddNode, MatchSpec, _Limit) ->
    select_virtual(ddNode, MatchSpec);
select(ddSchema, MatchSpec, _Limit) ->
    select_virtual(ddSchema, MatchSpec);
select(ddSize, MatchSpec, _Limit) ->
    select_virtual(ddSize, MatchSpec);
select(Table, MatchSpec, Limit) ->
    imem_if:select(physical_table_name(Table), MatchSpec, Limit).

select_virtual(_Table, [{_,[false],['$_']}]) ->
    {[],true};
select_virtual(Table, [{_,[true],['$_']}]) ->
    {read(Table),true};                 %% used in select * from virtual_table
select_virtual(Table, [{_,[],['$_']}]) ->
    {read(Table),true};                 %% used in select * from virtual_table
select_virtual(Table, [{MatchHead, [Guard], ['$_']}]=MatchSpec) ->
    Tag = element(2,MatchHead),
    % ?Debug("Virtual Select Tag / MatchSpec: ~p / ~p~n", [Tag,MatchSpec]),
    Candidates = case operand_match(Tag,Guard) of
        false ->                        read(Table);
        {'==',Tag,{element,N,Tup1}} ->  % ?Debug("Virtual Select Key : ~p~n", [element(N,Tup1)]),
                                        read(Table,element(N,Tup1));
        {'==',{element,N,Tup2},Tag} ->  % ?Debug("Virtual Select Key : ~p~n", [element(N,Tup2)]),
                                        read(Table,element(N,Tup2));
        {'==',Tag,Val1} ->              % ?Debug("Virtual Select Key : ~p~n", [Val1]),
                                        read(Table,Val1);
        {'==',Val2,Tag} ->              % ?Debug("Virtual Select Key : ~p~n", [Val2]),
                                        read(Table,Val2);
        _ ->                            read(Table)
    end,
    % ?Debug("Virtual Select Candidates  : ~p~n", [Candidates]),
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

write_log(Record) -> write(?LOG_TABLE, Record).

write({ddSysConf,TableAlias}, _Record) -> 
    % imem_if_sys_conf:write(TableAlias, Record);
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead",TableAlias});
write({_Schema,TableAlias}, Record) ->
    write(TableAlias, Record);           %% ToDo: may depend on schema 
write(TableAlias, Record) ->
    % log_to_db(debug,?MODULE,write,[{table,TableAlias},{rec,Record}],"write"), 
    PTN = physical_table_name(TableAlias,element(?KeyIdx,Record)),
    try
        imem_if:write(PTN, Record)
    catch
        throw:{'ClientError',{"Table does not exist",T}} ->
            % ToDo: instruct imem_meta gen_server to create the table
            case is_time_partitioned_alias(TableAlias) of
                true ->
                    case create_partitioned_table_sync(TableAlias,PTN) of
                        ok ->   
                            imem_if:write(PTN, Record);
                        {error,recursive_call} ->
                            ok; %% cannot create a new partition now, skip logging to database
                        E ->
                            ?ClientError({"Table partition cannot be created",{PTN,E}})
                    end;        
                false ->
                    ?ClientError({"Table does not exist",T})
            end;
        _Class:Reason ->
            ?Debug("Write error ~p:~p~n", [_Class,Reason]),
            throw(Reason)
    end. 

dirty_write({ddSysConf,TableAlias}, _Record) -> 
    % imem_if_sys_conf:dirty_write(TableAlias, Record);
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead",TableAlias});
dirty_write({_Schema,TableAlias}, Record) -> 
    dirty_write(TableAlias, Record);           %% ToDo: may depend on schema 
dirty_write(TableAlias, Record) -> 
    % log_to_db(debug,?MODULE,dirty_write,[{table,TableAlias},{rec,Record}],"dirty_write"), 
    PTN = physical_table_name(TableAlias,element(?KeyIdx,Record)),
    try
        imem_if:dirty_write(PTN, Record)
    catch
        throw:{'ClientError',{"Table does not exist",T}} ->
            case is_time_partitioned_alias(TableAlias) of
                true ->
                    case create_partitioned_table_sync(TableAlias,PTN) of
                        ok ->   
                            imem_if:dirty_write(PTN, Record);
                        {error,recursive_call} ->
                            ok; %% cannot create a new partition now, skip logging to database
                        E ->
                            ?ClientError({"Table partition cannot be created",{PTN,E}})
                    end;        
                false ->
                    ?ClientError({"Table does not exist",T})
            end;
        _Class:Reason ->
            ?Debug("Dirty write error ~p:~p~n", [_Class,Reason]),
            throw(Reason)
    end. 

insert(TableAlias, Row) ->
    insert(TableAlias,Row,meta_field_value(user)).

insert({ddSysConf,TableAlias}, _Row, _User) ->
    % imem_if_sys_conf:write(TableAlias, Row);     %% mapped to unconditional write
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead",TableAlias});
insert({_Schema,TableAlias}, Row, User) ->
    insert(TableAlias, Row, User);               %% ToDo: may depend on schema
insert(TableAlias, Row, User) when is_atom(TableAlias), is_tuple(Row) ->
    {TableType, DefRec, Trigger} =  trigger_infos(TableAlias),
    modify(insert, TableType, TableAlias, DefRec, Trigger, {}, Row, User).

update(TableAlias, Row) ->
    update(TableAlias, Row, meta_field_value(user)).

update({ddSysConf,TableAlias}, _Row, _User) ->
    % imem_if_sys_conf:write(TableAlias, Row);     %% mapped to unconditional write
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead",TableAlias});    
update({_Schema,TableAlias}, Row, User) ->
    update(TableAlias, Row, User);               %% ToDo: may depend on schema
update(TableAlias, {ORow,NRow}, User) when is_atom(TableAlias), is_tuple(ORow), is_tuple(NRow) ->
    {TableType, DefRec, Trigger} =  trigger_infos(TableAlias),
    modify(update, TableType, TableAlias, DefRec, Trigger, ORow, NRow, User).

merge(TableAlias, Row) ->
    merge(TableAlias, Row, meta_field_value(user)).

merge({ddSysConf,TableAlias}, _Row, _User) ->
    % imem_if_sys_conf:write(TableAlias, Row);     %% mapped to unconditional write
    ?UnimplementedException({"Cannot write to ddSysConf schema, use DDerl GUI instead",TableAlias});    
merge({_Schema,TableAlias}, Row, User) ->
    merge(TableAlias, Row, User);                %% ToDo: may depend on schema
merge(TableAlias, Row, User) when is_atom(TableAlias), is_tuple(Row) ->
    {TableType, DefRec, Trigger} =  trigger_infos(TableAlias),
    modify(merge, TableType, TableAlias, DefRec, Trigger, {}, Row, User).

remove(TableAlias, Row) ->
    remove(TableAlias, Row, meta_field_value(user)).

remove({ddSysConf,TableAlias}, _Row, _User) ->
    % imem_if_sys_conf:delete(TableAlias, Row);    %% mapped to unconditional delete
    ?UnimplementedException({"Cannot delete from ddSysConf schema, use DDerl GUI instead",TableAlias});
remove({_Schema,TableAlias}, Row, User) ->
    remove(TableAlias, Row, User);               %% ToDo: may depend on schema
remove(TableAlias, Row, User) when is_atom(TableAlias), is_tuple(Row) ->
    {TableType, DefRec, Trigger} =  trigger_infos(TableAlias),
    modify(remove, TableType, TableAlias, DefRec, Trigger, Row, {}, User).

modify(Operation, TableType, TableAlias, DefRec, Trigger, ORow, NRow, User) when is_atom(TableAlias), is_tuple(ORow), is_tuple(NRow) ->
    {Key,Row} = case {ORow,NRow} of  %% Old Key / New Row Value if in doubt
        {{},{}} ->  ?ClientError({"Bad modify arguments, old and new rows are empty",Operation});
        {_,{}} ->   {element(?KeyIdx,ORow),ORow};
        {{},_} ->   DefaultedRow = apply_defaults(DefRec, NRow),
                    {element(?KeyIdx,NRow),apply_validators(DefRec, DefaultedRow, TableAlias, User)};
        {_,_} ->    DefaultedRow = apply_defaults(DefRec, NRow),
                    {element(?KeyIdx,ORow),apply_validators(DefRec, DefaultedRow, TableAlias, User)}
    end,
    case ((TableAlias /= ddTable) and lists:member(?nav,tuple_to_list(Row))) of
        false ->
            PTN = physical_table_name(TableAlias,Key),  %% may refer to a partition (of the old key)
            Trans = fun() ->   
                case {Operation, TableType, read(PTN,Key)} of     %% TODO: Wrap in single transaction
                    {insert,bag,Bag} -> case lists:member(Row,Bag) of  
                                            true ->     ?ConcurrencyException({"Insert failed, object already exists", {PTN,Row}});
                                            false ->    write(PTN, Row),
                                                        Trigger({},Row,PTN,User),
                                                        Row
                                        end;
                    {insert,_,[]} ->    write(PTN, Row),
                                        Trigger({},Row,TableAlias,User),
                                        Row;
                    {insert,_,[R]} ->   ?ConcurrencyException({"Insert failed, key already exists in", {PTN,R}});
                    {update,bag,_} ->   ?UnimplementedException({"Update is not supported on bag tables, use delete and insert", TableAlias});
                    {update,_,[]} ->    ?ConcurrencyException({"Update failed, key does not exist", {PTN,Key}});
                    {update,_,[ORow]}-> case element(?KeyIdx,NRow) of
                                            Key ->  write(PTN, Row);
                                            OK ->   %% key has changed
                                                    %% must evaluate new partition and delete old key
                                                    write(physical_table_name(TableAlias,element(?KeyIdx,Row)), Row),
                                                    delete(PTN,OK)
                                        end,
                                        Trigger(ORow,Row,TableAlias,User),
                                        Row;
                    {update,_,[R]}->    case record_match(R,ORow) of
                                            true -> 
                                                write(PTN, Row),
                                                Trigger(R,Row,TableAlias,User),
                                                Row;
                                            false ->   
                                                ?ConcurrencyException({"Data is modified by someone else", {PTN,R}})
                                        end;
                    {merge,bag,_} ->    ?UnimplementedException({"Merge is not supported on bag tables, use delete and insert", TableAlias});
                    {merge,_,[]} ->     write(PTN, Row),
                                        Trigger({},Row,TableAlias,User),
                                        Row;
                    {merge,_,[R]} ->    write(PTN, Row),
                                        Trigger(R,Row,TableAlias,User),
                                        Row;
                    {remove,bag,[]} ->  ?ConcurrencyException({"Remove failed, object does not exist", {PTN,Row}});
                    {remove,_,[]} ->    ?ConcurrencyException({"Remove failed, key does not exist", {PTN,Key}});
                    {remove,bag,Bag} -> case lists:member(Row,Bag) of  
                                            false ->    ?ConcurrencyException({"Remove failed, object does not exist", {PTN,Row}});
                                            true ->     delete_object(PTN, Row),
                                                        Trigger(Row,{},PTN,User),
                                                        Row
                                        end;
                    {remove,_,[ORow]}-> delete(TableAlias, Key),
                                        Trigger(ORow,{},TableAlias,User),
                                        ORow;
                    {remove,_,[R]}->    case record_match(R,ORow) of
                                            true -> 
                                                delete(TableAlias, Key),
                                                Trigger(R,{},TableAlias,User),
                                                R;
                                            false ->   
                                                ?ConcurrencyException({"Data is modified by someone else", {PTN,R}})
                                        end;
                    {Op,Type,R} ->      ?SystemException({"Unexpected result in row modify", {PTN,{Op,Type,R}}})
                end
            end,
            return_atomic(transaction(Trans));
        true ->     
            ?ClientError({"Not null constraint violation", {TableAlias,Row}})
    end.

record_match(Rec,Pattern) when is_tuple(Rec), is_tuple(Pattern), size(Rec)==size(Pattern) ->
    list_match(tuple_to_list(Rec),tuple_to_list(Pattern));
record_match(_,_) -> false. 

list_match([],[]) -> true;
list_match([A|List],[A|Pat]) -> list_match(List,Pat);
list_match([_|List],['_'|Pat]) -> list_match(List,Pat);
list_match(_,_) -> false.

delete({_Schema,TableAlias}, Key) ->
    delete(TableAlias, Key);             %% ToDo: may depend on schema
delete(TableAlias, Key) ->
    imem_if:delete(physical_table_name(TableAlias,Key), Key).

delete_object({_Schema,TableAlias}, Row) ->
    delete_object(TableAlias, Row);             %% ToDo: may depend on schema
delete_object(TableAlias, Row) ->
    imem_if:delete_object(physical_table_name(TableAlias,element(?KeyIdx,Row)), Row).

subscribe({table, Tab, Mode}) ->
    PTN = physical_table_name(Tab),
    log_to_db(debug,?MODULE,subscribe,[{ec,{table, PTN, Mode}}],"subscribe to mnesia"),
    imem_if:subscribe({table, PTN, Mode});
subscribe(EventCategory) ->
    log_to_db(debug,?MODULE,subscribe,[{ec,EventCategory}],"subscribe to mnesia"),
    imem_if:subscribe(EventCategory).

unsubscribe({table, Tab, Mode}) ->
    PTN = physical_table_name(Tab),
    Result = imem_if:unsubscribe({table, PTN, Mode}),
    log_to_db(debug,?MODULE,unsubscribe,[{ec,{table, PTN, Mode}}],"unsubscribe from mnesia"),
    Result;
unsubscribe(EventCategory) ->
    Result = imem_if:unsubscribe(EventCategory),
    log_to_db(debug,?MODULE,unsubscribe,[{ec,EventCategory}],"unsubscribe from mnesia"),
    Result.

update_tables([[{Schema,_,_}|_]|_] = UpdatePlan, Lock) ->
    update_tables(Schema, UpdatePlan, Lock, []).

update_bound_counter(TableAlias, Field, Key, Incr, LimitMin, LimitMax) ->
    imem_if:update_bound_counter(physical_table_name(TableAlias), Field, Key, Incr, LimitMin, LimitMax).

update_tables(ddSysConf, [], Lock, Acc) ->
    imem_if_sys_conf:update_tables(Acc, Lock);  
update_tables(_MySchema, [], Lock, Acc) ->
    imem_if:update_tables(Acc, Lock);  
update_tables(MySchema, [UEntry|UPlan], Lock, Acc) ->
    % log_to_db(debug,?MODULE,update_tables,[{lock,Lock}],io_lib:format("~p",[UEntry])),
    update_tables(MySchema, UPlan, Lock, [update_table_name(MySchema, UEntry)|Acc]).

update_table_name(MySchema,[{MySchema,Tab,Type}, Item, Old, New, Trig, User]) ->
    case lists:member(?nav,tuple_to_list(New)) of
        false ->    [{physical_table_name(Tab),Type}, Item, Old, New, Trig, User];
        true ->     ?ClientError({"Not null constraint violation", {Item, {Tab,New}}})
    end.

admin_exec(Module, Function, _Params) ->
    ?ClientError({"Function cannot be called outside of security context",{Module, Function}}).

decode_json(_, {}) -> {};
decode_json([], Rec) -> Rec;
decode_json([Pos|Rest], Rec) ->
    Val = element(Pos,Rec),
    % ?LogDebug("decode_json Val ~p",[Val]),
    Decoded = try imem_json:decode(Val) catch _:_ -> ?nav end,
    % ?LogDebug("decode_json Decoded ~p",[Decoded]),
    decode_json(Rest, setelement(Pos,Rec,Decoded)).

update_index(_,_,_,_,#ddIdxPlan{def=[]}) -> 
    % ?LogDebug("update_index IdxPlan ~p",[#ddIdxPlan{def=[]}]),
    ok;   %% no index on this table
update_index(Old,New,Table,User,IdxPlan) ->
    % ?LogDebug("IdxPlan ~p",[IdxPlan]),
    update_index(Old,New,Table,index_table(Table),User,IdxPlan).

update_index(Old,New,Table,IndexTable,User,IdxPlan) -> 
    OldJ = decode_json(IdxPlan#ddIdxPlan.jpos,Old),
    NewJ = decode_json(IdxPlan#ddIdxPlan.jpos,New),
    update_index(Old,New,OldJ,NewJ,Table,IndexTable,User,IdxPlan#ddIdxPlan.def,[],[]). 

update_index(_Old,_New,_OldJ,_NewJ,_Table,IndexTable,_User,[],Removes,Inserts) ->
    % ?LogDebug("update index table/Old/New ~p~n~p~n~p",[_Table,_Old,_New]),
    % ?LogDebug("update index table/rem/ins ~p~n~p~n~p",[IndexTable,Removes,Inserts]),
    imem_index:remove(IndexTable,Removes),
    imem_index:insert(IndexTable,Inserts);
update_index(Old,New,OldJ,NewJ,Table,IndexTable,User,[#ddIdxDef{id=ID,type=Type,pl=PL,vnf=Vnf,iff=Iff}|Defs],Removes0,Inserts0) ->
    Rem = lists:usort(index_items(Old,OldJ,Table,User,ID,Type,PL,Vnf,Iff,[])), 
    Ins = lists:usort(index_items(New,NewJ,Table,User,ID,Type,PL,Vnf,Iff,[])),
    %% ToDo: cancel Inserts against identical Removes
    update_index(Old,New,OldJ,NewJ,Table,IndexTable,User,Defs,Removes0++Rem,Inserts0++Ins).

index_items({},_,_,_,_,_,_,_,_,[]) -> [];
index_items(_,_,_,_,_,_,[],_,_,Changes) -> Changes;
index_items(Rec,RecJ,Table,User,ID,Type,[Pos|PL],Vnf,Iff,Changes0) when is_integer(Pos) ->
    Key = element(?KeyIdx,Rec),         %% index a field as a whole, no json path search
    KVPs = case element(Pos,Rec) of
        ?nav -> [];
        RV ->   case Vnf(RV) of         %% apply value normalising function
                    ?nav ->     [];
                    [?nav] ->   [];
                    [] ->       [];
                    [NVal] ->   [{Key,NVal}];
                    LVal ->     [{Key,V} || V <- LVal,V /= ?nav]
                end
    end,
    Ch = [{ID,Type,K,V} || {K,V} <- lists:filter(Iff,KVPs)], %% apply index filter function
    index_items(Rec,RecJ,Table,User,ID,Type,PL,Vnf,Iff,Changes0 ++ Ch);
index_items(Rec,{},Table,User,ID,Type,[_|PL],Vnf,Iff,Changes) ->
    index_items(Rec,{},Table,User,ID,Type,PL,Vnf,Iff,Changes);
index_items(Rec,RecJ,Table,User,ID,Type,[{PT,FL}|PL],Vnf,Iff,Changes0) ->
    %?LogDebug("index_items RecJ ~p",[RecJ]),
    %?LogDebug("index_items ParseTree ~p",[PT]),
    %?LogDebug("index_items FieldList ~p",[FL]),
    Key = element(?KeyIdx,RecJ),
    Binds = [{Name,element(Pos,RecJ)} || {Name,Pos} <- FL],
    KVPs = case lists:keyfind(?nav, 2, Binds) of
        false ->   
            Match = imem_json:eval(PT,Binds),
            case Match of
                MV when is_binary(MV);is_number(MV) ->    
                    % ?LogDebug("Value from json ~p",[MV]),
                    % ?LogDebug("Vnf evaluates to ~p",[Vnf(MV)]),
                    % ?LogDebug("lists:flatten evaluates to ~p",[lists:flatten([Vnf(M1) || M1  <- [MV]])]),
                    [{Key,V} || V <- lists:flatten([Vnf(M) || M  <- [MV]]), V /= ?nav];
                ML when is_list(ML) ->      
                    [{Key,V} || V <- lists:flatten([Vnf(M) || M  <- ML]), V /= ?nav];
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
                    ?SystemException({"Index error", {malformed, E4}})
            end;
        _ ->
            []  %% one or more bind variables are not values. Don't try to evaluate the json path.
    end,
    %% ?LogDebug("index_items KVPs ~p",[KVPs]),
    Ch = [{ID,Type,K,V} || {K,V} <- lists:filter(Iff,KVPs)],
    index_items(Rec,RecJ,Table,User,ID,Type,PL,Vnf,Iff,Changes0++Ch).

transaction(Function) ->
    imem_if:transaction(Function).

transaction(Function, Args) ->
    imem_if:transaction(Function, Args).

transaction(Function, Args, Retries) ->
    imem_if:transaction(Function, Args, Retries).

return_atomic_list(Result) ->
    imem_if:return_atomic_list(Result). 

return_atomic_ok(Result) -> 
    imem_if:return_atomic_ok(Result).

return_atomic(Result) -> 
    imem_if:return_atomic(Result).

first(Table) ->     imem_if:first(Table).

next(Table,Key) ->  imem_if:next(Table,Key).

last(Table) ->      imem_if:last(Table).

prev(Table,Key) ->  imem_if:prev(Table,Key).

foldl(FoldFun, InputAcc, Table) ->  
    imem_if:foldl(FoldFun, InputAcc, Table).

lock(LockItem, LockKind) -> 
    imem_if:lock(LockItem, LockKind).

get_tables_count() ->
    {ok, MaxEtsNoTables} = application:get_env(max_ets_tables),
    {MaxEtsNoTables, length(mnesia:system_info(tables))}.

-spec sql_jp_bind(Sql :: string()) ->
    {NewSql :: string()
     , BindParamsMeta :: [{BindParam :: binary(), BindType :: atom()
                           , JPPath :: string()}]}
    | no_return().
sql_jp_bind(Sql) ->
    case re:run(Sql, ":[^ ,\)\n\r;]+", [global,{capture,all,list}]) of
        {match, Parameters} -> Parameters;
        Other ->
            Parameters = undefined,
            ?ClientError({"Bad format", Other})
    end,
    ParamsMap = [{lists:flatten(Param)
                  , ":" ++ re:replace(lists:flatten(Param), "[:_\\[\\]{}]+", ""
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
    
%% ----- DATA TYPES ---------------------------------------------


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-define(TPTEST0, tpTest_1000@).
-define(TPTEST1, tpTest1_999999999@_).
-define(TPTEST2, tpTest_100@).

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

setup() ->
    ?imem_test_setup.

teardown(_) ->
    catch drop_table(meta_table_3),
    catch drop_table(meta_table_2),
    catch drop_table(meta_table_1),
    catch drop_table(?TPTEST0),
    catch drop_table(?TPTEST1),
    catch drop_table(?TPTEST2),
    catch drop_table(test_config),
    catch drop_table(fakelog_1@),
    ?imem_test_teardown.

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun meta_operations/1
        ]}}.    

meta_operations(_) ->
    try 
        ClEr = 'ClientError',
        SyEx = 'SystemException', 
        UiEx = 'UnimplementedException', 

        ?LogDebug("---TEST---~p:test_mnesia~n", [?MODULE]),

        ?LogDebug("schema ~p~n", [imem_meta:schema()]),
        ?LogDebug("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        ?assertEqual(ok, check_table_columns(ddTable, record_info(fields, ddTable))),

        ?assertEqual(ok, create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system)),
        ?assertException(throw,{SyEx,{"Wrong table owner",{?LOG_TABLE,system}}} ,create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog},{type,ordered_set}], admin)),
        ?assertException(throw,{SyEx,{"Wrong table options",{?LOG_TABLE,_}}} ,create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog1},{type,ordered_set}], system)),
        ?assertEqual(ok, check_table(?LOG_TABLE)),

        ?assertEqual(ok, check_table(?CACHE_TABLE)),

        ?assertEqual(["Schema",".","BaseName","01234","@","Node"],parse_table_name("Schema.BaseName_01234@Node")),
        ?assertEqual(["Schema",".","BaseName","","",""],parse_table_name("Schema.BaseName")),
        ?assertEqual(["","","BaseName","01234","@","Node"],parse_table_name("BaseName_01234@Node")),
        ?assertEqual(["","","BaseName","","@","Node"],parse_table_name("BaseName@Node")),
        ?assertEqual(["","","BaseName","","@",""],parse_table_name("BaseName@")),
        ?assertEqual(["","","BaseName","","",""],parse_table_name("BaseName")),

        Now = erlang:now(),
        LogCount1 = table_size(?LOG_TABLE),
        ?LogDebug("ddLog@ count ~p~n", [LogCount1]),
        Fields=[{test_criterium_1,value1},{test_criterium_2,value2}],
        LogRec0 = #ddLog{logTime=Now,logLevel=info,pid=self()
                            ,module=?MODULE,function=meta_operations,node=node()
                            ,fields=Fields,message= <<"some log message">>},
        ?assertEqual(ok, write(?LOG_TABLE, LogRec0)),
        LogCount2 = table_size(?LOG_TABLE),
        ?LogDebug("ddLog@ count ~p~n", [LogCount2]),
        ?assert(LogCount2 > LogCount1),
        Log1=read(?LOG_TABLE,Now),
        ?LogDebug("ddLog@ content ~p~n", [Log1]),
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],"Message")),        
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],[])),        
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],[stupid_error_message,1])),        
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],{stupid_error_message,2})),        
        LogCount2a = table_size(?LOG_TABLE),
        ?assert(LogCount2a >= LogCount2+4),

        ?LogDebug("~p:test_database_operations~n", [?MODULE]),
        Types1 =    [ #ddColumn{name=a, type=string, len=10}     %% key
                    , #ddColumn{name=b1, type=binstr, len=20}    %% value 1
                    , #ddColumn{name=c1, type=string, len=30}    %% value 2
                    ],
        Types2 =    [ #ddColumn{name=a, type=integer, len=10}    %% key
                    , #ddColumn{name=b2, type=float, len=8, prec=3}   %% value
                    ],

        BadTypes0 = [ #ddColumn{name='a', type=integer, len=10}  
                    ],
        BadTypes1 = [ #ddColumn{name='a', type=integer, len=10}
                    , #ddColumn{name='a:b', type=integer, len=10}  
                    ],
        BadTypes2 = [ #ddColumn{name='a', type=integer, len=10}
                    , #ddColumn{name=current, type=integer, len=10}
                    ],
        BadTypes3 = [ #ddColumn{name='a', type=integer, len=10}
                    , #ddColumn{name=a, type=iinteger, len=10}
                    ],

        ?assertEqual(ok, create_table(meta_table_1, Types1, [])),
        ?assertEqual(ok, create_index(meta_table_1, [])),
        ?assertEqual(ok, check_table(meta_table_1Idx)),
        ?LogDebug("ddTable for meta_table_1~n~p~n", [read(ddTable,{schema(),meta_table_1})]),
        ?assertEqual(ok, drop_index(meta_table_1)),
        ?assertException(throw, {'ClientError',{"Table does not exist",meta_table_1Idx}}, check_table(meta_table_1Idx)),
        ?assertEqual(ok, create_index(meta_table_1, [])),
        ?assertException(throw, {'ClientError',{"Index already exists",{meta_table_1}}}, create_index(meta_table_1, [])),
        ?assertEqual([], read(meta_table_1Idx)),
        ?assertEqual(ok, write(meta_table_1Idx, #ddIndex{stu={1,2,3}})),
        ?assertEqual([#ddIndex{stu={1,2,3}}], read(meta_table_1Idx)),

        Idx1Def = #ddIdxDef{id=1,name= <<"string index on b1">>,type=ivk,pl=[<<"b1">>]},
        ?assertEqual(ok, create_or_replace_index(meta_table_1, [Idx1Def])),
        ?assertEqual([], read(meta_table_1Idx)),
        ?assertEqual([<<"table">>], imem_index:vnf_lcase_ascii_ne(<<"täble"/utf8>>)),
        ?assertEqual({meta_table_1,"meta",<<"täble"/utf8>>,"1"}, insert(meta_table_1, {meta_table_1,"meta",<<"täble"/utf8>>,"1"})),
        ?assertEqual([{meta_table_1,"meta",<<"täble"/utf8>>,"1"}], read(meta_table_1)),
        ?assertEqual([#ddIndex{stu={1,<<"table">>,"meta"}}], read(meta_table_1Idx)),
        ?assertEqual([<<"tuble">>], imem_index:vnf_lcase_ascii_ne(<<"tüble"/utf8>>)),
        ?assertException(throw, {'ConcurrencyException',{"Data is modified by someone else",_}}, update(meta_table_1, {{meta_table_1,"meta",<<"tible"/utf8>>,"1"}, {meta_table_1,"meta",<<"tüble"/utf8>>,"1"}})),
        ?assertEqual({meta_table_1,"meta",<<"tüble"/utf8>>,"1"}, update(meta_table_1, {{meta_table_1,"meta",<<"täble"/utf8>>,"1"}, {meta_table_1,"meta",<<"tüble"/utf8>>,"1"}})),
        ?assertEqual([{meta_table_1,"meta",<<"tüble"/utf8>>,"1"}], read(meta_table_1)),
        ?assertEqual([#ddIndex{stu={1,<<"tuble">>,"meta"}}], read(meta_table_1Idx)),
        ?assertEqual(ok, drop_index(meta_table_1)),
        ?assertEqual(ok, create_index(meta_table_1, [Idx1Def])),
        ?assertEqual([#ddIndex{stu={1,<<"tuble">>,"meta"}}], read(meta_table_1Idx)),
        ?assertException(throw, {'ConcurrencyException',{"Data is modified by someone else",_}}, remove(meta_table_1, {meta_table_1,"meta",<<"tible"/utf8>>,"1"})),
        ?assertEqual({meta_table_1,"meta",<<"tüble"/utf8>>,"1"}, remove(meta_table_1, {meta_table_1,"meta",<<"tüble"/utf8>>,"1"})),
        ?assertEqual([], read(meta_table_1)),
        ?assertEqual([], read(meta_table_1Idx)),

        Idx2Def = #ddIdxDef{id=2,name= <<"unique string index on b1">>,type=iv_k,pl=[<<"b1">>]},
        ?assertEqual(ok, create_or_replace_index(meta_table_1, [Idx2Def])),
        ?assertEqual({meta_table_1,"meta",<<"täble"/utf8>>,"1"}, insert(meta_table_1, {meta_table_1,"meta",<<"täble"/utf8>>,"1"})),
        ?assertEqual(1, length(read(meta_table_1))),
        ?assertEqual(1, length(read(meta_table_1Idx))),
        ?assertEqual({meta_table_1,"meta1",<<"tüble"/utf8>>,"2"}, insert(meta_table_1, {meta_table_1,"meta1",<<"tüble"/utf8>>,"2"})),
        ?assertEqual(2, length(read(meta_table_1))),
        ?assertEqual(2, length(read(meta_table_1Idx))),
        ?assertException(throw,{'ClientError',{"Unique index violation",{meta_table_1Idx,2,<<"table">>,"meta"}}}, insert(meta_table_1, {meta_table_1,"meta2",<<"table"/utf8>>,"2"})),
        ?assertEqual(2, length(read(meta_table_1))),
        ?assertEqual(2, length(read(meta_table_1Idx))),

        Idx3Def = #ddIdxDef{id=3,name= <<"json index on b1:b">>,type=ivk,pl=[<<"b1:b">>,<<"b1:c:a">>]},
        ?assertEqual(ok, create_or_replace_index(meta_table_1, [Idx3Def])),
        ?assertEqual(2, length(read(meta_table_1))),
        ?assertEqual(0, length(read(meta_table_1Idx))),
        JSON1 = <<
            "{"
                "\"a\":\"Value-a\","
                "\"b\":\"Value-b\","
                "\"c\":{"
                    "\"a\":\"Value-ca\","
                    "\"b\":\"Value-cb\""
                "}"
            "}"
                >>,
        % {
        %     "a": "Value-a",
        %     "b": "Value-b",
        %     "c": {
        %         "a": "Value-ca",
        %         "b": "Value-cb"
        %     }
        % }
        PROP1 = [{<<"a">>,<<"Value-a">>}
                ,{<<"b">>,<<"Value-b">>}
                ,{<<"c">>, [
                            {<<"a">>,<<"Value-ca">>}
                           ,{<<"b">>,<<"Value-cb">>}
                           ]
                 }
                ],
        ?assertEqual(PROP1,imem_json:decode(JSON1)),        
        ?assertEqual({meta_table_1,"json1",JSON1,"3"}, insert(meta_table_1, {meta_table_1,"json1",JSON1,"3"})),
        ?assertEqual([#ddIndex{stu={3, <<"value-b">>,"json1"}}
                     ,#ddIndex{stu={3, <<"value-ca">>,"json1"}}
                     ], read(meta_table_1Idx)),

        % Drop individual indices
        ?assertEqual(ok, drop_index(meta_table_1)),
        ?assertEqual(ok, create_index(meta_table_1, [Idx1Def, Idx2Def, Idx3Def])),
        ?assertEqual(ok, drop_index(meta_table_1, <<"json index on b1:b">>)),
        ?assertException(throw, {'ClientError', {"Index does not exist for"
                                                 , meta_table_1
                                                 , <<"non existent index">>}}
                         , drop_index(meta_table_1, <<"non existent index">>)),
        ?assertException(throw, {'ClientError', {"Index does not exist for"
                                                 , meta_table_1
                                                 , 7}}
                         , drop_index(meta_table_1, 7)),
        ?assertEqual(ok, drop_index(meta_table_1, 2)),
        ?assertEqual(ok, drop_index(meta_table_1, 1)),
        ?assertEqual({'ClientError',{"Table does not exist",meta_table_1Idx}}
                         , drop_index(meta_table_1)),

        ?LogDebug("meta_table_1 ~n~p",[read(meta_table_1)]),

        Idx4Def = #ddIdxDef{id=4,name= <<"integer index on b1">>,type=ivk,pl=[<<"c1">>],vnf = <<"fun imem_index:vnf_integer/1">>},
        ?assertEqual(ok, create_or_replace_index(meta_table_1, [Idx4Def])),
        ?assertEqual(3, length(read(meta_table_1Idx))),
        insert(meta_table_1, {meta_table_1,"11",<<"11">>,"11"}),
        ?assertEqual(4, length(read(meta_table_1Idx))),
        insert(meta_table_1, {meta_table_1,"12",<<"12">>,"c112"}),
        IdxExpect4 = [{ddIndex,{4,1,"meta"},0}
                     ,{ddIndex,{4,2,"meta1"},0}
                     ,{ddIndex,{4,3,"json1"},0}
                     ,{ddIndex,{4,11,"11"},0}
                     ],
        ?assertEqual(IdxExpect4, read(meta_table_1Idx)),

        Vnf5 = <<"fun(__X) -> case imem_index:vnf_integer(__X) of ['$not_a_value'] -> ['$not_a_value']; [__V] -> [2*__V] end end">>,
        Idx5Def = #ddIdxDef{id=5,name= <<"integer times 2 on b1">>,type=ivk,pl=[<<"c1">>],vnf = Vnf5},
        ?assertEqual(ok, create_or_replace_index(meta_table_1, [Idx5Def])),
        ?LogDebug("meta_table_1Idx ~n~p",[read(meta_table_1Idx)]),
        IdxExpect5 = [{ddIndex,{5,2,"meta"},0}
                     ,{ddIndex,{5,4,"meta1"},0}
                     ,{ddIndex,{5,6,"json1"},0}
                     ,{ddIndex,{5,22,"11"},0}
                     ],
        ?assertEqual(IdxExpect5, read(meta_table_1Idx)),

        ?assertEqual(ok, create_table(meta_table_2, Types2, [])),

        ?assertEqual(ok, create_table(meta_table_3, {[a,?nav],[datetime,term],{meta_table_3,?nav,undefined}}, [])),
        ?LogDebug("success ~p~n", [create_table_not_null]),
        Trig = <<"fun(O,N,T,U) -> imem_meta:log_to_db(debug,imem_meta,trigger,[{table,T},{old,O},{new,N},{user,U}],\"trigger\") end.">>,
        ?assertEqual(ok, create_or_replace_trigger(meta_table_3, Trig)),
        ?assertEqual(Trig, get_trigger(meta_table_3)),

        ?assertException(throw, {ClEr,{"No columns given in create table",bad_table_0}}, create_table('bad_table_0', [], [])),
        ?assertException(throw, {ClEr,{"No value column given in create table, add dummy value column",bad_table_0}}, create_table('bad_table_0', BadTypes0, [])),

        ?assertException(throw, {ClEr,{"Invalid character(s) in table name", 'bad_?table_1'}}, create_table('bad_?table_1', BadTypes1, [])),
        ?assertException(throw, {ClEr,{"Reserved table name", select}}, create_table(select, BadTypes2, [])),

        ?assertException(throw, {ClEr,{"Invalid character(s) in column name", 'a:b'}}, create_table(bad_table_1, BadTypes1, [])),
        ?assertException(throw, {ClEr,{"Reserved column name", current}}, create_table(bad_table_1, BadTypes2, [])),
        ?assertException(throw, {ClEr,{"Invalid data type", iinteger}}, create_table(bad_table_1, BadTypes3, [])),

        LogCount3 = table_size(?LOG_TABLE),
        ?assertEqual({meta_table_3,{{2000,1,1},{12,45,55}},undefined}, insert(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,55}},?nav})),
        ?assertEqual(1, table_size(meta_table_3)),
        ?assertEqual(LogCount3+1, table_size(?LOG_TABLE)),  %% trigger inserted one line      
        ?assertException(throw, {ClEr,{"Not null constraint violation", {meta_table_3,_}}}, insert(meta_table_3, {meta_table_3,?nav,undefined})),
        ?assertEqual(LogCount3+2, table_size(?LOG_TABLE)),  %% error inserted one line
        ?LogDebug("success ~p~n", [not_null_constraint]),
        ?assertEqual({meta_table_3,{{2000,1,1},{12,45,55}},undefined}, update(meta_table_3, {{meta_table_3,{{2000,1,1},{12,45,55}},undefined},{meta_table_3,{{2000,01,01},{12,45,55}},?nav}})),
        ?assertEqual(1, table_size(meta_table_3)),
        ?assertEqual(LogCount3+3, table_size(?LOG_TABLE)),  %% trigger inserted one line 
        ?assertEqual({meta_table_3,{{2000,1,1},{12,45,56}},undefined}, merge(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,56}},?nav})),
        ?assertEqual(2, table_size(meta_table_3)),
        ?assertEqual(LogCount3+4, table_size(?LOG_TABLE)),  %% trigger inserted one line 
        ?assertEqual({meta_table_3,{{2000,1,1},{12,45,56}},undefined}, remove(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,56}},undefined})),
        ?assertEqual(1, table_size(meta_table_3)),
        ?assertEqual(LogCount3+5, table_size(?LOG_TABLE)),  %% trigger inserted one line 
        ?assertEqual(ok, drop_trigger(meta_table_3)),
        ?LogDebug("meta_table_3 before update~n~p",[read(meta_table_3)]),
        Trans3 = fun() ->
            %% key update
            update(meta_table_3, {{meta_table_3,{{2000,01,01},{12,45,55}},undefined},{meta_table_3,{{2000,01,01},{12,45,56}},"alternative"}}),
            insert(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,57}},?nav})         %% return last result only
        end,
        ?assertEqual({meta_table_3,{{2000,1,1},{12,45,57}},undefined}, return_atomic(transaction(Trans3))),
        ?LogDebug("meta_table_3 after update~n~p",[read(meta_table_3)]),
        ?assertEqual(2, table_size(meta_table_3)),
        ?assertEqual(LogCount3+5, table_size(?LOG_TABLE)),  %% no trigger, no more log  

        Keys4 = [
        {1,{meta_table_3,{{2000,1,1},{12,45,59}},undefined}}
        ],
        U = unknown,
        {TT4,_DefRec,TrigFun} = trigger_infos(meta_table_3),
        ?assertEqual(Keys4, update_tables([[{imem,meta_table_3,set}, 1, {}, {meta_table_3,{{2000,01,01},{12,45,59}},undefined},TrigFun,U]], optimistic)),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {1,{meta_table_3,_}}}}, update_tables([[{imem,meta_table_3,set}, 1, {}, {meta_table_3, ?nav, undefined},TrigFun,U]], optimistic)),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {1,{meta_table_3,_}}}}, update_tables([[{imem,meta_table_3,set}, 1, {}, {meta_table_3,{{2000,01,01},{12,45,59}}, ?nav},TrigFun,U]], optimistic)),

        ?assertEqual([meta_table_1,meta_table_1Idx,meta_table_2,meta_table_3],lists:sort(tables_starting_with("meta_table_"))),
        ?assertEqual([meta_table_1,meta_table_1Idx,meta_table_2,meta_table_3],lists:sort(tables_starting_with(meta_table_))),

        ?assertEqual(["Schema",".","Name_Period",""     ,"@","Node"], parse_table_name('Schema.Name_Period@Node')),
        ?assertEqual(["Schema",".","Name"       ,"12345","@","Node"], parse_table_name('Schema.Name_12345@Node')),
        ?assertEqual(["Schema",".","Name_Period",""     ,"@","_"], parse_table_name('Schema.Name_Period@_')),
        ?assertEqual(["Schema",".","Name"       ,"12345","@","_"], parse_table_name('Schema.Name_12345@_')),
        ?assertEqual(["Schema",".","Name_Period",""     ,"@",""], parse_table_name('Schema.Name_Period@')),
        ?assertEqual(["Schema",".","Name"       ,"12345","@",""], parse_table_name('Schema.Name_12345@')),
        ?assertEqual(["Schema",".","Name_Period",""     ,"" ,""], parse_table_name('Schema.Name_Period')),
        ?assertEqual(["Schema",".","Name_12345" ,""     ,"" ,""], parse_table_name('Schema.Name_12345')),

        ?assertEqual(true,is_time_partitioned_alias(?TPTEST1)),
        ?assertEqual(true,is_time_partitioned_alias(?TPTEST0)),
        ?assertEqual(false,is_time_partitioned_alias(tpTest@)),
        ?assertEqual(false,is_time_partitioned_alias(tpTest@_)),
        ?assertEqual(false,is_time_partitioned_alias(tpTest1234@_)),
        ?assertEqual(false,is_time_partitioned_alias(tpTest_10A0@)),
        ?assertEqual(false,is_time_partitioned_alias(tpTest)),
        ?assertEqual(true,is_node_sharded_alias(?TPTEST0)),
        ?assertEqual(false,is_node_sharded_alias(tpTest_1000)),
        ?assertEqual(false,is_node_sharded_alias(tpTest1000)),
        ?assertEqual(false,is_node_sharded_alias(?TPTEST1)),
        ?assertEqual(false,is_node_sharded_alias(?TPTEST1)),
        
        LogTable = physical_table_name(?LOG_TABLE),
        ?assert(lists:member(LogTable,physical_table_names(?LOG_TABLE))),

        ?assertEqual([],physical_table_names(?TPTEST0)),

        ?assertException(throw, {ClEr,{"Table to be purged does not exist",?TPTEST0}}, purge_table(?TPTEST0)),
        ?assertException(throw, {UiEx,{"Purge not supported on this table type",not_existing_table}}, purge_table(not_existing_table)),
        ?assert(purge_table(?LOG_TABLE) >= 0),
        ?assertException(throw, {UiEx,{"Purge not supported on this table type",ddTable}}, purge_table(ddTable)),

        TimePartTable0 = physical_table_name(?TPTEST0),
        ?LogDebug("TimePartTable ~p~n", [TimePartTable0]),
        ?assertEqual(TimePartTable0, physical_table_name(?TPTEST0,erlang:now())),
        ?assertEqual(ok, create_check_table(?TPTEST0, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog},{type,ordered_set}], system)),
        ?assertEqual(ok, check_table(TimePartTable0)),
        ?assertEqual(0, table_size(TimePartTable0)),
        ?assertEqual([TimePartTable0],physical_table_names(?TPTEST0)),
        ?assertEqual(ok, create_check_table(?TPTEST0, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog},{type,ordered_set}], system)),

        Alias0 = read(ddAlias),
        ?LogDebug("Alias0 ~p~n", [[ element(2,A) || A <- Alias0]]),
        ?assert(lists:member({schema(),?TPTEST0},[element(2,A) || A <- Alias0])),

% FIXME: Currently failing in travis
%        ?assertException(throw, {'ClientError',{"Name conflict (different rolling period) in ddAlias",?TPTEST2}}, create_check_table(?TPTEST2, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog},{type,ordered_set}], system)),

        ?assert(lists:member({schema(),?TPTEST0},[element(2,A) || A <- read(ddAlias)])),

        ?assertEqual(ok, write(?TPTEST0, LogRec0)),
        ?assertEqual(1, table_size(TimePartTable0)),
        ?assertEqual(0, purge_table(?TPTEST0)),
        {Megs,Secs,Mics} = erlang:now(),
        FutureSecs = Megs*1000000 + Secs + 2000,
        Future = {FutureSecs div 1000000,FutureSecs rem 1000000,Mics}, 
        LogRecF = LogRec0#ddLog{logTime=Future},
        ?assertEqual(ok, write(?TPTEST0, LogRecF)),
        ?LogDebug("physical_table_names ~p~n", [physical_table_names(?TPTEST0)]),
        ?assertEqual(0, purge_table(?TPTEST0,[{purge_delay,10000}])),
        ?assertEqual(0, purge_table(?TPTEST0)),
        PurgeResult = purge_table(?TPTEST0,[{purge_delay,-3000}]),
        ?LogDebug("PurgeResult ~p~n", [PurgeResult]),
        ?assert(PurgeResult>0),
        ?assertEqual(0, purge_table(?TPTEST0)),
        ?assertEqual(ok, drop_table(?TPTEST0)),
        ?assertEqual([],physical_table_names(?TPTEST0)),
        Alias0a = read(ddAlias),
        ?assertEqual(false,lists:member({schema(),?TPTEST0},[element(2,A) || A <- Alias0a])),
        ?LogDebug("success ~p~n", [?TPTEST0]),

        TimePartTable1 = physical_table_name(?TPTEST1),
        ?assertEqual(tpTest1_1999999998@_, TimePartTable1),
        ?assertEqual(TimePartTable1, physical_table_name(?TPTEST1,erlang:now())),
        ?assertEqual(ok, create_check_table(?TPTEST1, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog},{type,ordered_set}], system)),
        ?assertEqual(ok, check_table(TimePartTable1)),
        ?assertEqual([TimePartTable1],physical_table_names(?TPTEST1)),
        ?assertEqual(0, table_size(TimePartTable1)),
        ?assertEqual(ok, create_check_table(?TPTEST1, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog},{type,ordered_set}], system)),

        Alias1 = read(ddAlias),
        ?LogDebug("Alias1 ~p~n", [[ element(2,A) || A <- Alias1]]),
        ?assert(lists:member({schema(),?TPTEST1},[element(2,A) || A <- Alias1])),

        ?assertEqual(ok, write(?TPTEST1, LogRec0)),
        ?assertEqual(1, table_size(TimePartTable1)),
        ?assertEqual(0, purge_table(?TPTEST1)),
        LogRecP = LogRec0#ddLog{logTime={900,0,0}},  
        ?LogDebug("Big Partition Tables before back-insert~n~p~n", [physical_table_names(?TPTEST1)]),
        % Error3 = {error,{'ClientError',{"Table already exists",tpTest1_1999999998@_}}},     % cannot create past partitions
        % ?assertException(throw,{'ClientError',{"Table partition cannot be created",{tpTest1_999999999@_,Error3}}},write(?TPTEST1, LogRecP)),
        ?assertEqual(ok, write(?TPTEST1, LogRecP)),
        ?LogDebug("Big Partition Tables after back-insert~n~p~n", [physical_table_names(?TPTEST1)]),
        LogRecFF = LogRec0#ddLog{logTime={2900,0,0}},  
        ?assertEqual(ok, write(?TPTEST1, LogRecFF)),
        ?LogDebug("Big Partition Tables after forward-insert~n~p~n", [physical_table_names(?TPTEST1)]),
        ?assertEqual(3,length(physical_table_names(?TPTEST1))),     % another partition created
        ?assert(purge_table(?TPTEST1) > 0),
        ?assertEqual(ok, drop_table(?TPTEST1)),
        Alias1a = read(ddAlias),
        ?assertEqual(false,lists:member({schema(),?TPTEST1},[element(2,A) || A <- Alias1a])),
        ?assertEqual([],physical_table_names(?TPTEST1)),
        ?LogDebug("success ~p~n", [?TPTEST1]),


        DdNode0 = read(ddNode),
        ?LogDebug("ddNode0 ~p~n", [DdNode0]),
        DdNode1 = read(ddNode,node()),
        ?LogDebug("ddNode1 ~p~n", [DdNode1]),
        DdNode2 = select(ddNode,?MatchAllRecords),
        ?LogDebug("ddNode2 ~p~n", [DdNode2]),

        Schema0 = [{ddSchema,{schema(),node()},[]}],
        ?assertEqual(Schema0, read(ddSchema)),
        ?assertEqual({Schema0,true}, select(ddSchema,?MatchAllRecords)),

        ?assertEqual(ok, create_table(test_config, {record_info(fields, ddConfig),?ddConfig, #ddConfig{}}, ?CONFIG_TABLE_OPTS, system)),
        ?assertEqual(test_value,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context], test_value)),
        ?assertMatch([#ddConfig{hkl=[{?MODULE,test_param}],val=test_value}],read(test_config)), %% default created, owner set
        ?assertEqual(test_value,get_config_hlk(test_config, {?MODULE,test_param}, not_test_owner, [test_context], other_default)),
        ?assertMatch([#ddConfig{hkl=[{?MODULE,test_param}],val=test_value}],read(test_config)), %% default not overwritten, wrong owner
        ?assertEqual(test_value1,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context], test_value1)),
        ?assertMatch([#ddConfig{hkl=[{?MODULE,test_param}],val=test_value1}],read(test_config)), %% new default overwritten by owner
        ?assertEqual(ok, put_config_hlk(test_config, {?MODULE,test_param}, test_owner, [],test_value2,<<"Test Remark">>)),
        ?assertEqual(test_value2,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context], test_value3)),
        ?assertMatch([#ddConfig{hkl=[{?MODULE,test_param}],val=test_value2}],read(test_config)),
        ?assertEqual(ok, put_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context],context_value,<<"Test Remark">>)),
        ?assertEqual(context_value,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context], test_value)),
        ?assertEqual(context_value,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context,details], test_value)),
        ?assertEqual(test_value2,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [another_context,details], another_value)),
        ?LogDebug("success ~p~n", [get_config_hlk]),

        ?assertEqual( {error,{"Table template not found in ddAlias",dummy_table_name}}, create_partitioned_table_sync(dummy_table_name,dummy_table_name)),
        ?assertEqual([],physical_table_names(fakelog_1@)),
        ?assertEqual(ok, create_check_table(fakelog_1@, {record_info(fields, ddLog),?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system)),    
        ?assertEqual(1,length(physical_table_names(fakelog_1@))),
        LogRec3 = #ddLog{logTime=erlang:now(),logLevel=debug,pid=self()
                        ,module=?MODULE,function=test,node=node()
                        ,fields=[],message= <<>>,stacktrace=[]
                    },
        ?assertEqual(ok, dirty_write(fakelog_1@, LogRec3)),
        timer:sleep(1100),
        ?assertEqual(ok, dirty_write(fakelog_1@, LogRec3#ddLog{logTime=erlang:now()})),
        ?assert(length(physical_table_names(fakelog_1@)) >= 3),
        timer:sleep(1100),
        % ?assertEqual(ok, create_partitioned_table_sync(fakelog_1@,physical_table_name(fakelog_1@))),
        ?assert(length(physical_table_names(fakelog_1@)) >= 4),
        ?LogDebug("success ~p~n", [create_partitioned_table]),

        ?assertEqual(ok, drop_table(meta_table_3)),
        ?assertEqual(ok, drop_table(meta_table_2)),
        ?assertEqual(ok, drop_table(meta_table_1)),
        ?assertEqual(ok, drop_table(test_config)),
        ?assertEqual(ok,drop_table(fakelog_1@)),

        ?LogDebug("success ~p~n", [drop_tables])
    catch
        Class:Reason ->     
            timer:sleep(1000),
            ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
            throw ({Class, Reason})
    end,
    ok.
    
-endif.
