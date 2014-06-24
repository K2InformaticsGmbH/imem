-module(imem_meta).

-include("imem.hrl").
-include("imem_meta.hrl").

%% HARD CODED CONFIGURATIONS

-define(DDNODE_TIMEOUT,3000).       %% RPC timeout for ddNode evaluation

-define(META_TABLES,[?CACHE_TABLE,ddTable,ddNode,ddSchema,ddSize,dual,?LOG_TABLE,?MONITOR_TABLE]).
-define(META_FIELDS,[<<"rownum">>,<<"systimestamp">>,<<"user">>,<<"username">>,<<"sysdate">>,<<"schema">>,<<"node">>]). 
-define(META_OPTS,[purge_delay]). % table options only used in imem_meta and above

-define(CONFIG_TABLE_OPTS, [{record_name,ddConfig}
                           ,{type,ordered_set}
                           ]).          

-define(LOG_TABLE_OPTS,    [{record_name,ddLog}
                           ,{type,ordered_set}
                           ,{purge_delay,430000}        %% 430000 = 5 Days - 2000 sec
                           ]).          

-define(BAD_NAME_CHARACTERS,"!?#*:+-.\\<|>/").  %% invalid chars for tables and columns

% -define(RecIdx, 1).                                       %% Record name position in records
% -define(FirstIdx, 2).                                     %% First field position in records
-define(KeyIdx, 2).                                       %% Key position in records


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
        , fail/1
        ]).


-export([ drop_meta_tables/0
        ]).

-export([ schema/0
        , schema/1
        , system_id/0
        , data_nodes/0
        , host_fqdn/1
        , host_name/1
        , node_name/1
        , node_hash/1
        , all_tables/0
        , tables_starting_with/1
        , tables_ending_with/1
        , node_shard/0
        , qualified_table_name/1
        , qualified_new_table_name/1
        , physical_table_name/1
        , physical_table_name/2
        , physical_table_names/1
        , parse_table_name/1
        , is_system_table/1
        , is_readable_table/1
        , is_virtual_table/1
        , is_time_partitioned_alias/1
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
        , check_table/1
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
        , failing_function/1
        , get_config_hlk/5
        , put_config_hlk/6
        ]).

-export([ create_table/3
        , create_table/4
        , create_trigger/2
        , create_partitioned_table/1
        , create_partitioned_table_sync/1
        , create_check_table/3
        , create_check_table/4
        , create_sys_conf/1
        , drop_table/1
        , drop_trigger/1
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
        , modify/7          %% parameterized insert/update/merge/remove
        , insert/2          %% apply defaults, write row if key does not exist, apply trigger
        , insert/3          %% apply defaults, write row if key does not exist, apply trigger
        , update/2          %% apply defaults, write row if key exists, apply trigger (bags not supported)
        , update/3          %% apply defaults, write row if key exists, apply trigger (bags not supported)
        , merge/2           %% apply defaults, write row, apply trigger (bags not supported)
        , merge/3           %% apply defaults, write row, apply trigger (bags not supported)
        , remove/2          %% delete row if key exists (if bag row exists), apply trigger
        , write/2           %% write row for single key, no defaults applied, no trigger applied
        , write_log/1
        , dirty_write/2
        , delete/2          %% delete row by key
        , delete_object/2   %% delete single row in bag table 
        ]).

-export([ update_prepare/3          %% stateless creation of update plan from change list
        , update_cursor_prepare/2   %% take change list and generate update plan (stored in state)
        , update_cursor_execute/2   %% take update plan from state and execute it (fetch aborted first)
        , apply_defaults/2          %% apply arity/0 funs of default record to ?nav values of current record
        , apply_validators/3          %% apply any arity funs of default record to current record
        , apply_validators/4          %% apply any arity funs of default record to current record
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
        ]).

-export([ simple_or_local_node_sharded_tables/1]).


start_link(Params) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]).

init(_Args) ->
    ?Info("~p starting...~n", [?MODULE]),
    Result = try
        application:set_env(imem, node_shard, node_shard()),

        catch create_table(ddTable, {record_info(fields, ddTable),?ddTable, #ddTable{}}, [], system),
        catch check_table(ddTable),
        catch check_table_columns(ddTable, record_info(fields, ddTable)),
        catch check_table_meta(ddTable, {record_info(fields, ddTable), ?ddTable, #ddTable{}}),

        CDef = {record_info(fields, ddCache), ?ddCache, #ddCache{}},
        COpts = [{scope,local}, {local_content,true},{record_name,ddCache}],
        catch create_check_table(?CACHE_TABLE, CDef, COpts, system),

        catch create_check_table(ddNode, {record_info(fields, ddNode),?ddNode, #ddNode{}}, [], system),    
        catch create_check_table(ddSchema, {record_info(fields, ddSchema),?ddSchema, #ddSchema{}}, [], system),    
        catch create_check_table(ddSize, {record_info(fields, ddSize),?ddSize, #ddSize{}}, [], system),    
        catch create_check_table(?CONFIG_TABLE, {record_info(fields, ddConfig),?ddConfig, #ddConfig{}}, ?CONFIG_TABLE_OPTS, system),
        catch create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system),    
        case catch create_table(dual, {record_info(fields, dual),?dual, #dual{}}, [], system) of
            ok ->   catch write(dual,#dual{});
            _ ->    ok
        end,
        ?Info("~p started!~n", [?MODULE]),
        {ok,#state{}}
    catch
        _Class:Reason -> {stop, {Reason,erlang:get_stacktrace()}} 
    end,
    Result.

create_partitioned_table_sync(Name) when is_atom(Name) ->
    ImemMetaPid = erlang:whereis(?MODULE),
    case self() of
        ImemMetaPid ->
            {error,recursive_call};   %% cannot call myself
        _ ->
            gen_server:call(?MODULE, {create_partitioned_table, Name},35000)
    end. 

create_partitioned_table(Name) when is_atom(Name) ->
    try 
        case imem_if:read(ddTable,{schema(), Name}) of
            [#ddTable{}] ->
                % Table seems to exist, may need to load it
                case catch(check_table(Name)) of
                    ok ->   ok;
                    {'ClientError',{"Table does not exist",Name}} ->
                        create_nonexisting_partitioned_table(Name);
                    Res ->
                        ?Info("Waiting for partitioned table ~p needed because of ~p", [Name,Res]),
                        case mnesia:wait_for_tables([Name], 30000) of
                            ok ->   ok;   
                            Error ->            
                                ?Error("Waiting for partitioned table failed with ~p", [Error]),
                                {error,Error}
                        end
                end;
            [] ->
                % Table does not exist in ddTable, must create it similar to existing
                create_nonexisting_partitioned_table(Name)   
        end
    catch
        _:Reason1 ->
            ?Error("Create partitioned table failed with ~p", [Reason1]),
            {error,Reason1}
    end.

create_nonexisting_partitioned_table(Name) ->
    NS = node_shard(),
    case parse_table_name(Name) of
        [_,_,_,"",_,_] ->
            ?Error("Invalid table name ~p", [Name]),   
            {error, {"Invalid table name",Name}};
        ["","",BaseName,_,"@",NS] ->
            % choose template table name (name pattern with highest timestamp)
            Pred = fun(TN) -> is_local_time_partitioned_table(TN) end,
            case lists:filter(Pred,tables_starting_with(BaseName)) of
                [] ->   
                    ?Error("No table template found starting/ending with  ~p / ~p", [BaseName,NS]), 
                    {error, {"No table template found starting/ending with", {BaseName,NS}}}; 
                Cand -> 
                    Template = lists:last(lists:sort(Cand)),
                    % find out ColumnsInfos, Opts, Owner from template table definition
                    case imem_if:read(ddTable,{schema(), Template}) of
                        [] ->
                            ?Error("Table template not found ~p", [Template]),   
                            {error, {"Table template not found", Template}}; 
                        [#ddTable{columns=ColumnInfos,opts=Opts,owner=Owner}] ->
                            try
                                create_table(Name, ColumnInfos, Opts, Owner)
                            catch
                                _:Reason2 -> {error, Reason2}
                            end
                    end
            end
    end.

handle_call({create_partitioned_table, Name}, _From, State) ->
    {reply, create_partitioned_table(Name), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%     {stop,{shutdown,Reason},State};
% handle_cast({stop, Reason}, State) ->
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

fail(Reason) ->
    throw(Reason).

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


check_table_meta({ddSysConf, _}, _) -> ok;
check_table_meta(Table, {Names, Types, DefaultRecord}) when is_atom(Table) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(Names, Types, Defaults),
    case imem_if:read(ddTable,{schema(), physical_table_name(Table)}) of
        [] ->   ?SystemException({"Missing table metadata",Table}); 
        [#ddTable{columns=ColumnInfos}] ->
            CINames = column_info_items(ColumnInfos, name),
            CITypes = column_info_items(ColumnInfos, type),
            CIDefaults = column_info_items(ColumnInfos, default),
            if
                (CINames =:= Names) andalso (CITypes =:= Types) andalso (CIDefaults =:= Defaults) ->  
                    ok;
                true ->                 
                    ?SystemException({"Record does not match table metadata",Table})
            end;
        Else -> 
            ?SystemException({"Column definition does not match table metadata",{Table,Else}})    
    end;  
check_table_meta(Table, ColumnNames) when is_atom(Table) ->
    case imem_if:read(ddTable,{schema(), physical_table_name(Table)}) of
        [] ->   ?SystemException({"Missing table metadata",Table}); 
        [#ddTable{columns=ColumnInfo}] ->
            CINames = column_info_items(ColumnInfo, name),
            if
                CINames =:= ColumnNames ->  
                    ok;
                true ->                 
                    ?SystemException({"Record field names do not match table metadata",Table})
            end          
    end.

check_table_columns({ddSysConf, _}, _) -> ok;
check_table_columns(Table, {Names, Types, DefaultRecord}) when is_atom(Table) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfo = column_infos(Names, Types, Defaults),
    TableColumns = table_columns(Table),    
    MetaInfo = column_infos(Table),    
    if
        Names /= TableColumns ->
            ?SystemException({"Column names do not match table structure",Table});             
        ColumnInfo /= MetaInfo ->
            ?SystemException({"Column info does not match table metadata",Table});
        true ->     ok
    end;
check_table_columns(Table, [CI|_]=ColumnInfo) when is_atom(Table), is_tuple(CI) ->
    ColumnNames = column_info_items(ColumnInfo, name),
    TableColumns = table_columns(Table),
    MetaInfo = column_infos(Table),    
    if
        ColumnNames /= TableColumns ->
            ?SystemException({"Column info does not match table structure",Table}) ;
        ColumnInfo /= MetaInfo ->
            ?SystemException({"Column info does not match table metadata",Table});
        true ->     ok                           
    end;
check_table_columns(Table, ColumnNames) when is_atom(Table) ->
    TableColumns = table_columns(Table),
    if
        ColumnNames /= TableColumns ->
            ?SystemException({"Column info does not match table structure",Table}) ;
        true ->     ok                           
    end.

drop_meta_tables() ->
    drop_table(?MONITOR_TABLE),
    drop_table(?LOG_TABLE),
    drop_table(?CONFIG_TABLE),
    drop_table(ddTable).     

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

column_infos(Table) when is_atom(Table) ->
    column_infos({schema(),Table});    
column_infos({Schema,Table}) when is_binary(Schema), is_binary(Table) ->
    S= try 
        ?binary_to_existing_atom(Schema)
    catch 
        _:_ -> ?ClientError({"Schema does not exist",Schema})
    end,
    T = try 
        ?binary_to_existing_atom(Table)
    catch 
        _:_ -> ?ClientError({"Table does not exist",Table})
    end,        
    column_infos({S,T});
column_infos({Schema,Table}) when is_atom(Schema), is_atom(Table) ->
    case lists:member(Table, ?DataTypes) of
        true -> 
            [#ddColumn{name=item, type=Table, len=0, prec=0, default=undefined}];
        false ->
            case imem_if:read(ddTable,{Schema, physical_table_name(Table)}) of
                [] ->                       ?ClientError({"Table does not exist",{Schema,Table}}); 
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

create_table(Table, Columns, Opts) ->
    create_table(Table, Columns, Opts, #ddTable{}#ddTable.owner).

create_table(Table, {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(ColumnNames, ColumnTypes, Defaults),
    create_physical_table(Table,ColumnInfos,Opts,Owner);
create_table(Table, [#ddColumn{}|_]=ColumnInfos, Opts, Owner) ->
    Conv = fun(X) ->
        case X#ddColumn.name of
            A when is_atom(A) -> X; 
            B -> X#ddColumn{name=?binary_to_atom(B)} 
        end
    end,
    create_physical_table(qualified_new_table_name(Table),lists:map(Conv,ColumnInfos),Opts,Owner);
create_table(Table, ColumnNames, Opts, Owner) ->
    ColumnInfos = column_infos(ColumnNames),
    create_physical_table(Table,ColumnInfos,Opts,Owner).

create_check_table(Table, Columns, Opts) ->
    create_check_table(Table, Columns, Opts, (#ddTable{})#ddTable.owner).

create_check_table(Table, [#ddColumn{}|_]=ColumnInfos, Opts, Owner) ->
    Conv = fun(X) ->
        case X#ddColumn.name of
            A when is_atom(A) -> X; 
            B -> X#ddColumn{name=?binary_to_atom(B)} 
        end
    end,
    {ColumnNames, ColumnTypes, DefaultRecord} = from_column_infos(lists:map(Conv,ColumnInfos)),
    create_check_table(qualified_new_table_name(Table), {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner);
create_check_table(Table, {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(ColumnNames, ColumnTypes, Defaults),
    create_check_physical_table(Table,ColumnInfos,Opts,Owner),
    check_table(Table),
    check_table_columns(Table, ColumnNames),
    check_table_meta(Table, {ColumnNames, ColumnTypes, DefaultRecord}).

create_sys_conf(Path) ->
    imem_if_sys_conf:create_sys_conf(Path).    

create_check_physical_table(Table,ColumnInfos,Opts,Owner) when is_atom(Table) ->
    create_check_physical_table({schema(),Table},ColumnInfos,Opts,Owner);    
create_check_physical_table({Schema,Table},ColumnInfos,Opts,Owner) ->
    MySchema = schema(),
    case lists:member(Schema, [MySchema, ddSysConf]) of
        true ->
            PhysicalName=physical_table_name(Table),
            case read(ddTable,{Schema,PhysicalName}) of 
                [] ->
                    create_physical_table({Schema,Table},ColumnInfos,Opts,Owner);
                [#ddTable{opts=Opts,owner=Owner}] ->
                    catch create_physical_table({Schema,Table},ColumnInfos,Opts,Owner),
                    ok;
                [#ddTable{opts=Old,owner=Owner}] ->
                    OldOpts = lists:sort(lists:keydelete(purge_delay,1,Old)),
                    NewOpts = lists:sort(lists:keydelete(purge_delay,1,Opts)),
                    case NewOpts of
                        OldOpts ->
                            catch create_physical_table({Schema,Table},ColumnInfos,Opts,Owner),
                            ok;
                        _ -> 
                            ?SystemException({"Wrong table options",{Table,Old}})
                    end;        
                [#ddTable{owner=Own}] ->
                    ?SystemException({"Wrong table owner",{Table,Own}})        
            end;
        _ ->        
            ?UnimplementedException({"Create/check table in foreign schema",{Schema,Table}})
    end.

create_physical_table({Schema,Table,_Alias},ColumnInfos,Opts,Owner) ->
    create_physical_table({Schema,Table},ColumnInfos,Opts,Owner);
create_physical_table({Schema,Table},ColumnInfos,Opts,Owner) ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_physical_table(Table,ColumnInfos,Opts,Owner);
        ddSysConf -> create_table_sys_conf(Table, ColumnInfos, Opts, Owner);
        _ ->    ?UnimplementedException({"Create table in foreign schema",{Schema,Table}})
    end;
create_physical_table(Table,ColInfos,Opts,Owner) ->
    case is_valid_table_name(Table) of
        true ->     ok;
        false ->    ?ClientError({"Invalid character(s) in table name",Table})
    end,    
    case sqlparse:is_reserved(Table) of
        false ->    ok;
        true ->     ?ClientError({"Reserved table name",Table})
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
    PhysicalName=physical_table_name(Table),
    DDTableRow = #ddTable{qname={schema(),PhysicalName}, columns=ColInfos, opts=Opts, owner=Owner},
    try
        % Trans = fun() ->
        %    ?LogDebug("Create Table ~p",[PhysicalName]),
            imem_if:create_table(PhysicalName, column_names(ColInfos), if_opts(Opts) ++ [{user_properties, [DDTableRow]}]),
        %    ?LogDebug("Insert ~p",[DDTableRow]),
            imem_if:write(ddTable, DDTableRow)
        %end,
        % transaction(Trans)
    catch
        % ddTable Meta data is attempted to be inserted only if missing and
        _:{'ClientError',{"Table already exists",PhysicalName}} = Reason ->
            case imem_if:read(ddTable, {schema(),PhysicalName}) of
                [] -> imem_if:write(ddTable, DDTableRow);
                _ -> ok
            end,
            throw(Reason)
    end.

create_table_sys_conf(PhysicalName, ColumnInfos, Opts, Owner) ->
    DDTableRow = #ddTable{qname={ddSysConf,PhysicalName}, columns=ColumnInfos, opts=Opts, owner=Owner},
    return_atomic_ok(imem_if:write(ddTable, DDTableRow)).

create_trigger({Schema,Table},TFun) ->
    MySchema = schema(),
    case Schema of
        MySchema -> create_trigger(Table,TFun);
        _ ->        ?UnimplementedException({"Create Trigger in foreign schema",{Schema,Table}})
    end;
create_trigger(Table,TFunStr) when is_atom(Table) ->
    % ?LogDebug("Create trigger ~p~n~p",[Table,TFunStr]),
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
create_trigger(Table,_) when is_atom(Table) ->
    ?ClientError({"Bad fun for create_trigger, expecting arity 4", Table}).

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

if_opts(Opts) ->
    % Remove imem_meta table options which are not recognized by imem_if
    if_opts(Opts,?META_OPTS).

if_opts([],_) -> [];
if_opts(Opts,[]) -> Opts;
if_opts(Opts,[MO|Others]) ->
    if_opts(lists:keydelete(MO, 1, Opts),Others).

truncate_table(Table) ->
    truncate_table(Table,meta_field_value(user)).

truncate_table({_Schema,Table,_Alias},User) ->
    truncate_table({_Schema,Table},User);    
truncate_table({Schema,Table},User) ->
    MySchema = schema(),
    case Schema of
        MySchema -> truncate_table(Table, User);
        _ ->        ?UnimplementedException({"Truncate table in foreign schema",{Schema,Table}})
    end;
truncate_table(Alias,User) when is_atom(Alias) ->
    %% log_to_db(debug,?MODULE,truncate_table,[{table,Alias}],"truncate table"),
    truncate_partitioned_tables(lists:sort(simple_or_local_node_sharded_tables(Alias)),User);
truncate_table(TableName, User) ->
    truncate_table(qualified_table_name(TableName),User).

truncate_partitioned_tables([],_) -> ok;
truncate_partitioned_tables([PhName|PhNames], User) ->
    {_, _, Trigger} =  trigger_infos(PhName),
    Trans = fun() ->
        Trigger({},{},PhName,User),
        imem_if:truncate_table(PhName)
    end,
    return_atomic_ok(transaction(Trans)),
    truncate_partitioned_tables(PhNames,User).

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
snapshot_partitioned_tables([PhName|PhNames]) ->
    imem_snap:take(PhName),
    snapshot_partitioned_tables(PhNames).

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

drop_table({Schema,Table}) when is_atom(Schema), is_atom(Table) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_table(Table);
        _ ->        ?UnimplementedException({"Drop table in foreign schema",{Schema,Table}})
    end;
drop_table(ddTable) -> 
    imem_if:drop_table(ddTable);
drop_table(?LOG_TABLE) ->
    drop_table_and_info(physical_table_name(?LOG_TABLE));
drop_table(Alias) when is_atom(Alias) ->
    %% log_to_db(debug,?MODULE,drop_table,[{table,Alias}],"drop table"),
    drop_partitioned_tables_and_infos(lists:sort(simple_or_local_node_sharded_tables(Alias)));
drop_table(Table) when is_binary(Table) ->
    drop_table(qualified_table_name(Table)).

drop_partitioned_tables_and_infos([]) -> ok;
drop_partitioned_tables_and_infos([PhName|PhNames]) ->
    drop_table_and_info(PhName),
    drop_partitioned_tables_and_infos(PhNames).

drop_table_and_info(PhysicalName) ->
    try
        imem_if:drop_table(PhysicalName),
        imem_if:delete(ddTable, {schema(),PhysicalName})
    catch
        throw:{'ClientError',{"Table does not exist",Table}} ->
            catch imem_if:delete(ddTable, {schema(),PhysicalName}),
            throw({'ClientError',{"Table does not exist",Table}})
    end.       

purge_table(Alias) ->
    purge_table(Alias, []).

purge_table({Schema,Alias,_Alias}, Opts) -> 
    purge_table({Schema,Alias}, Opts);
purge_table({Schema,Alias}, Opts) ->
    MySchema = schema(),
    case Schema of
        MySchema -> purge_table(Alias, Opts);
        _ ->        ?UnimplementedException({"Purge table in foreign schema",{Schema,Alias}})
    end;
purge_table(Alias, Opts) ->
    case is_time_partitioned_alias(Alias) of
        false ->    
            ?UnimplementedException({"Purge not supported on this table type",Alias});
        true ->
            purge_time_partitioned_table(Alias, Opts)
    end.

purge_time_partitioned_table(Alias, Opts) ->
    case lists:sort(simple_or_local_node_sharded_tables(Alias)) of
        [] ->
            ?ClientError({"Table to be purged does not exist",Alias});
        [PhName|Rest] ->
            KeepTime = case proplists:get_value(purge_delay, Opts) of
                undefined ->    erlang:now();
                Seconds ->      {Mega,Secs,Micro} = erlang:now(),
                                {Mega,Secs-Seconds,Micro}
            end,
            KeepName = partitioned_table_name(Alias,KeepTime),
            if  
                PhName >= KeepName ->
                    0; %% no memory could be freed       
                true ->
                    ?Debug("Purge PhName KeepName ~p ~p~n",[PhName,KeepName]),
                    case Rest of
                        [] ->   DummyName = partitioned_table_name(Alias,erlang:now()),
                                % ?Debug("Purge DummyName ~p~n",[DummyName]),
                                create_partitioned_table(DummyName);
                        _ ->    ok
                    end,
                    FreedMemory = table_memory(PhName),
                    % Fields = [{table,PhName},{table_size,table_size(PhName)},{table_memory,FreedMemory}],   
                    ?Info("Purge time partition ~p~n",[PhName]),
                    drop_table_and_info(PhName),
                    FreedMemory
            end
    end.

simple_or_local_node_sharded_tables(Alias) ->    
    case is_node_sharded_alias(Alias) of
        true ->
            case is_time_partitioned_alias(Alias) of
                true ->
                    Tail = lists:reverse("@" ++ node_shard()),
                    Pred = fun(TN) -> lists:prefix(Tail, lists:reverse(atom_to_list(TN))) end,
                    lists:filter(Pred,physical_table_names(Alias));
                false ->
                    [physical_table_name(Alias)]
            end;        
        false ->
            [physical_table_name(Alias)]
    end.

is_node_sharded_alias(Alias) when is_atom(Alias) -> 
    is_node_sharded_alias(atom_to_list(Alias));
is_node_sharded_alias(Alias) when is_list(Alias) -> (lists:last(Alias) == $@).

is_time_partitioned_alias(Alias) when is_atom(Alias) ->
    is_time_partitioned_alias(atom_to_list(Alias));
is_time_partitioned_alias(Alias) when is_list(Alias) ->
    case is_node_sharded_alias(Alias) of
        false -> 
            false;
        true ->
            case string:tokens(lists:reverse(Alias), "_") of
                [[$@|RN]|_] -> 
                    try 
                        _ = list_to_integer(lists:reverse(RN)),
                        true    % timestamp partitioned and node sharded alias
                    catch
                        _:_ -> false
                    end;
                 _ ->      
                    false       % node sharded alias only
            end
    end.

is_local_node_sharded_table(Name) when is_atom(Name) -> 
    is_local_node_sharded_table(atom_to_list(Name));
is_local_node_sharded_table(Name) when is_list(Name) -> 
    lists:suffix([$@|node_shard()],Name).

is_local_time_partitioned_table(Name) when is_atom(Name) ->
    is_local_time_partitioned_table(atom_to_list(Name));
is_local_time_partitioned_table(Name) when is_list(Name) ->
    case is_local_node_sharded_table(Name) of
        false -> 
            false;
        true ->
            is_time_partitioned_alias(lists:sublist(Name, length(Name)-length(node_shard())))
    end.

parse_table_name(Table) when is_atom(Table) -> 
    parse_table_name(atom_to_list(Table));
parse_table_name(Table) when is_list(Table) -> 
    case string:tokens(Table, ".") of
        [R2] ->         ["",""|parse_simple_name(R2)];
        [Schema|R1] ->  [Schema,"."|parse_simple_name(string:join(R1,"."))]
    end.

parse_simple_name(Table) when is_list(Table) ->         
    case string:tokens(Table, "@") of
        [BaseName] ->    
            [BaseName,"","",""];
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
            [Table,"","",""]
    end.

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
physical_table_name(user_tables) -> ddTable;
physical_table_name(Name) when is_atom(Name) ->
    case lists:member(Name,?DataTypes) of
        true ->     Name;
        false ->    physical_table_name(atom_to_list(Name))
    end;
physical_table_name(Name) when is_list(Name) ->
    case lists:last(Name) of
        $@ ->   partitioned_table_name(Name,erlang:now());
        _ ->    list_to_atom(Name)
    end.

% physical_table_name({_S,N,_A},Key) -> physical_table_name(N,Key);
physical_table_name({_S,N},Key) -> physical_table_name(N,Key);
physical_table_name(dba_tables,_) -> ddTable;
physical_table_name(all_tables,_) -> ddTable;
physical_table_name(user_tables,_) -> ddTable;
physical_table_name(Name,Key) when is_atom(Name) ->
    case lists:member(Name,?DataTypes) of
        true ->     Name;
        false ->    physical_table_name(atom_to_list(Name),Key)
    end;
physical_table_name(Name,Key) when is_list(Name) ->
    case lists:last(Name) of
        $@ ->
            partitioned_table_name(Name,Key);
        _ ->    
            list_to_atom(Name)
    end.

physical_table_names({_S,N,_A}) -> physical_table_names(N);
physical_table_names({_S,N}) -> physical_table_names(N);
physical_table_names(dba_tables) -> [ddTable];
physical_table_names(all_tables) -> [ddTable];
physical_table_names(user_tables) -> [ddTable];
physical_table_names(Name) when is_atom(Name) ->
    case lists:member(Name,?DataTypes) of
        true ->     [Name];
        false ->    physical_table_names(atom_to_list(Name))
    end;
physical_table_names(Name) when is_list(Name) ->
    case lists:last(Name) of
        $@ ->   
            case string:tokens(lists:reverse(Name), "_") of
                [[$@|RN]|_] ->
                    % timestamp sharded node sharded tables 
                    try 
                        _ = list_to_integer(lists:reverse(RN)),
                        {BaseName,_} = lists:split(length(Name)-length(RN)-1, Name),
                        Pred = fun(TN) -> lists:member($@, atom_to_list(TN)) end,
                        lists:filter(Pred,tables_starting_with(BaseName))
                    catch
                        _:_ -> tables_starting_with(Name)
                    end;
                 _ ->   
                    % node sharded tables only   
                    tables_starting_with(Name)
            end;
        _ ->    
            [list_to_atom(Name)]
    end.

partitioned_table_name(Name,Key) when is_atom(Name) ->
    partitioned_table_name(atom_to_list(Name),Key);
partitioned_table_name(Name,Key) when is_list(Name) ->
    case string:tokens(lists:reverse(Name), "_") of
        [[$@|RN]|_] ->
            % timestamp sharded node sharded table 
            try 
                Period = list_to_integer(lists:reverse(RN)),
                {Mega,Sec,_} = Key,
                PartitionEnd=integer_to_list(Period*((1000000*Mega+Sec) div Period) + Period),
                {BaseName,_} = lists:split(length(Name)-length(RN)-1, Name),
                list_to_atom(lists:flatten(BaseName ++ PartitionEnd ++ "@" ++ node_shard()))
            catch
                _:_ -> list_to_atom(lists:flatten(Name ++ node_shard()))
            end;
         _ ->
            % node sharded table only   
            list_to_atom(lists:flatten(Name ++ node_shard()))
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


%% imem_if but security context added --- META INFORMATION ------

data_nodes() ->
    imem_if:data_nodes().

all_tables() ->
    imem_if:all_tables().

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
                    Trigger = case lists:keyfind(trigger,1,Opts) of
                        false ->    fun(_Old,_New,_Tab,_User) -> ok end;
                        {_,TFun} -> imem_datatype:io_to_fun(TFun,undefined)
                    end,
                    Result = {TableType, DefRec, Trigger},
                    imem_cache:write(Key,Result),
                    ?LogDebug("trigger_infos ~p",[Result]),
                    Result
            end;
        [{TT, DR, TR}] ->
            {TT, DR, TR}
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

table_size({ddSysConf,Table}) ->
    imem_if_sys_conf:table_size(Table);
table_size({_Schema,Table}) ->  table_size(Table);          %% ToDo: may depend on schema
table_size(ddNode) ->           length(read(ddNode));
table_size(ddSchema) ->         length(read(ddSchema));
table_size(ddSize) ->           1;
table_size(Table) ->
    %% ToDo: sum should be returned for all local time partitions
    imem_if:table_size(physical_table_name(Table)).

table_memory({ddSysConf,Table}) ->
    imem_if_sys_conf:table_memory(Table);
table_memory({_Schema,Table}) ->
    table_memory(Table);          %% ToDo: may depend on schema
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

read({ddSysConf,Table}) -> 
    imem_if_sys_conf:read(physical_table_name(Table));
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

read({ddSysConf,Table}, Key) -> 
    imem_if_sys_conf:read(physical_table_name(Table),Key);
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
    case is_local_time_partitioned_table(PTN) of  % 
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

select({ddSysConf,Table}, MatchSpec) ->
    imem_if_sys_conf:select(physical_table_name(Table), MatchSpec);
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
select({ddSysConf,Table}, MatchSpec, Limit) ->
    imem_if_sys_conf:select(physical_table_name(Table), MatchSpec, Limit);
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

write({ddSysConf,Table}, Record) -> 
    imem_if_sys_conf:write(Table, Record);
write({_Schema,Table}, Record) ->
    write(Table, Record);           %% ToDo: may depend on schema 
write(Table, Record) ->
    % log_to_db(debug,?MODULE,write,[{table,Table},{rec,Record}],"write"), 
    PTN = physical_table_name(Table,element(?KeyIdx,Record)),
    try
        imem_if:write(PTN, Record)
    catch
        throw:{'ClientError',{"Table does not exist",T}} ->
            % ToDo: instruct imem_meta gen_server to create the table
            case is_time_partitioned_alias(Table) of
                true ->
                    case create_partitioned_table_sync(PTN) of
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

dirty_write({ddSysConf,Table}, Record) -> 
    imem_if_sys_conf:dirty_write(Table, Record);
dirty_write({_Schema,Table}, Record) -> 
    dirty_write(Table, Record);           %% ToDo: may depend on schema 
dirty_write(Table, Record) -> 
    % log_to_db(debug,?MODULE,dirty_write,[{table,Table},{rec,Record}],"dirty_write"), 
    PTN = physical_table_name(Table,element(?KeyIdx,Record)),
    try
        imem_if:dirty_write(PTN, Record)
    catch
        throw:{'ClientError',{"Table does not exist",T}} ->
            case is_time_partitioned_alias(Table) of
                true ->
                    case create_partitioned_table_sync(PTN) of
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

insert(Table, Row) ->
    insert(Table,Row,meta_field_value(user)).

insert({ddSysConf,Table}, Row, _User) ->
    imem_if_sys_conf:write(Table, Row);     %% mapped to unconditional write
insert({_Schema,Table}, Row, User) ->
    insert(Table, Row, User);               %% ToDo: may depend on schema
insert(Table, Row, User) when is_atom(Table), is_tuple(Row) ->
    {TableType, DefRec, Trigger} =  trigger_infos(Table),
    modify(insert, TableType, Table, DefRec, Trigger, Row, User).

update(Table, Row) ->
    update(Table, Row, meta_field_value(user)).

update({ddSysConf,Table}, Row, _User) ->
    imem_if_sys_conf:write(Table, Row);     %% mapped to unconditional write
update({_Schema,Table}, Row, User) ->
    update(Table, Row, User);               %% ToDo: may depend on schema
update(Table, Row, User) when is_atom(Table), is_tuple(Row) ->
    {TableType, DefRec, Trigger} =  trigger_infos(Table),
    modify(update, TableType, Table, DefRec, Trigger, Row, User).

merge(Table, Row) ->
    merge(Table, Row, meta_field_value(user)).

merge({ddSysConf,Table}, Row, _User) ->
    imem_if_sys_conf:write(Table, Row);     %% mapped to unconditional write
merge({_Schema,Table}, Row, User) ->
    merge(Table, Row, User);                %% ToDo: may depend on schema
merge(Table, Row, User) when is_atom(Table), is_tuple(Row) ->
    {TableType, DefRec, Trigger} =  trigger_infos(Table),
    modify(merge, TableType, Table, DefRec, Trigger, Row, User).

remove(Table, Row) ->
    remove(Table, Row, meta_field_value(user)).

remove({ddSysConf,Table}, Row, _User) ->
    imem_if_sys_conf:delete(Table, Row);    %% mapped to unconditional delete
remove({_Schema,Table}, Row, User) ->
    remove(Table, Row, User);               %% ToDo: may depend on schema
remove(Table, Row, User) when is_atom(Table), is_tuple(Row) ->
    {TableType, DefRec, Trigger} =  trigger_infos(Table),
    modify(remove, TableType, Table, DefRec, Trigger, Row, User).

modify(Operation, TableType, Table, DefRec, Trigger, Row0, User) when is_atom(Table), is_tuple(Row0) ->
    Row1=apply_defaults(DefRec, Row0),
    Row2=apply_validators(DefRec, Row1, Table, User),
    Key = element(?KeyIdx,Row2),
    case ((Table /= ddTable) and lists:member(?nav,tuple_to_list(Row2))) of
        false ->
            PTN = physical_table_name(Table,Key),
            Trans = fun() ->   
                case {Operation, TableType, read(PTN,Key)} of     %% TODO: Wrap in single transaction
                    {insert,bag,Bag} -> case lists:member(Row2,Bag) of  
                                            true ->     ?ConcurrencyException({"Insert failed, object already exists", {PTN,Row2}});
                                            false ->    write(PTN, Row2),
                                                        Trigger({},Row2,PTN,User),
                                                        Row2
                                        end;
                    {insert,_,[]} ->    write(PTN, Row2),
                                        Trigger({},Row2,Table,User),
                                        Row2;
                    {insert,_,[R]} ->   ?ConcurrencyException({"Insert failed, key already exists in", {PTN,R}});
                    {update,bag,_} ->   ?UnimplementedException({"Update is not supported on bag tables, use delete and insert", Table});
                    {update,_,[]} ->    ?ConcurrencyException({"Update failed, key does not exist", {PTN,Key}});
                    {update,_,[R]} ->   write(PTN, Row2),
                                        Trigger(R,Row2,Table,User),
                                        Row2;
                    {merge,bag,_} ->    ?UnimplementedException({"Merge is not supported on bag tables, use delete and insert", Table});
                    {merge,_,[]} ->     write(PTN, Row2),
                                        Trigger({},Row2,Table,User),
                                        Row2;
                    {merge,_,[R]} ->    write(PTN, Row2),
                                        Trigger(R,Row2,Table,User),
                                        Row2;
                    {remove,bag,[]} ->  ?ConcurrencyException({"Remove failed, object does not exist", {PTN,Row2}});
                    {remove,_,[]} ->    ?ConcurrencyException({"Remove failed, key does not exist", {PTN,Key}});
                    {remove,bag,Bag} -> case lists:member(Row2,Bag) of  
                                            false ->    ?ConcurrencyException({"Remove failed, object does not exist", {PTN,Row2}});
                                            true ->     delete_object(PTN, Row2),
                                                        Trigger(Row2,{},PTN,User),
                                                        Row2
                                        end;
                    {remove,_,[R]} ->   delete(Table, Key),
                                        Trigger(R,{},Table,User),
                                        R
                end
            end,
            return_atomic(transaction(Trans));
        true ->     
            ?ClientError({"Not null constraint violation", {Table,Row2}})
    end.


delete({_Schema,Table}, Key) ->
    delete(Table, Key);             %% ToDo: may depend on schema
delete(Table, Key) ->
    imem_if:delete(physical_table_name(Table,Key), Key).

delete_object({_Schema,Table}, Row) ->
    delete_object(Table, Row);             %% ToDo: may depend on schema
delete_object(Table, Row) ->
    imem_if:delete_object(physical_table_name(Table,element(?KeyIdx,Row)), Row).

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

update_bound_counter(Table, Field, Key, Incr, LimitMin, LimitMax) ->
    imem_if:update_bound_counter(physical_table_name(Table), Field, Key, Incr, LimitMin, LimitMax).

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

%% ----- DATA TYPES ---------------------------------------------


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ?imem_test_setup().

teardown(_) ->
    catch drop_table(meta_table_3),
    catch drop_table(meta_table_2),
    catch drop_table(meta_table_1),
    catch drop_table(tpTest_1000@),
    catch drop_table(test_config),
    catch drop_table(fakelog_1@),
    ?imem_test_teardown().

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

        ?Info("---TEST---~p:test_mnesia~n", [?MODULE]),

        ?Info("schema ~p~n", [imem_meta:schema()]),
        ?Info("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        ?assertEqual(ok, check_table_columns(ddTable, record_info(fields, ddTable))),

        ?assertEqual(ok, create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system)),
        ?assertException(throw,{SyEx,{"Wrong table owner",{?LOG_TABLE,system}}} ,create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog},{type,ordered_set}], admin)),
        ?assertException(throw,{SyEx,{"Wrong table options",{?LOG_TABLE,_}}} ,create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog1},{type,ordered_set}], system)),
        ?assertEqual(ok, check_table(?LOG_TABLE)),

        ?assertEqual(ok, check_table(?CACHE_TABLE)),

        Now = erlang:now(),
        LogCount1 = table_size(?LOG_TABLE),
        ?Info("ddLog@ count ~p~n", [LogCount1]),
        Fields=[{test_criterium_1,value1},{test_criterium_2,value2}],
        LogRec1 = #ddLog{logTime=Now,logLevel=info,pid=self()
                            ,module=?MODULE,function=meta_operations,node=node()
                            ,fields=Fields,message= <<"some log message 1">>},
        ?assertEqual(ok, write(?LOG_TABLE, LogRec1)),
        LogCount2 = table_size(?LOG_TABLE),
        ?Info("ddLog@ count ~p~n", [LogCount2]),
        ?assert(LogCount2 > LogCount1),
        Log1=read(?LOG_TABLE,Now),
        ?Info("ddLog@ content ~p~n", [Log1]),
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],"Message")),        
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],[])),        
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],[stupid_error_message,1])),        
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],{stupid_error_message,2})),        
        LogCount2a = table_size(?LOG_TABLE),
        ?assert(LogCount2a >= LogCount2+4),

        ?Info("~p:test_database_operations~n", [?MODULE]),
        Types1 =    [ #ddColumn{name=a, type=string, len=10}     %% key
                    , #ddColumn{name=b1, type=string, len=20}    %% value 1
                    , #ddColumn{name=c1, type=string, len=30}    %% value 2
                    ],
        Types2 =    [ #ddColumn{name=a, type=integer, len=10}    %% key
                    , #ddColumn{name=b2, type=float, len=8, prec=3}   %% value
                    ],

        BadTypes1 = [ #ddColumn{name='a:b', type=integer, len=10}  
                    ],
        BadTypes2 = [ #ddColumn{name=current, type=integer, len=10}
                    ],
        BadTypes3 = [ #ddColumn{name=a, type=iinteger, len=10}
                    ],

        ?assertEqual(ok, create_table(meta_table_1, Types1, [])),
        ?assertEqual(ok, create_table(meta_table_2, Types2, [])),

        ?assertEqual(ok, create_table(meta_table_3, {[a,?nav],[datetime,term],{meta_table_3,?nav,undefined}}, [])),
        ?Info("success ~p~n", [create_table_not_null]),
        Trig = <<"fun(O,N,T,U) -> imem_meta:log_to_db(debug,imem_meta,trigger,[{table,T},{old,O},{new,N},{user,U}],\"trigger\") end.">>,
        ?assertEqual(ok, create_trigger(meta_table_3, Trig)),

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
        ?Info("success ~p~n", [not_null_constraint]),
        ?assertEqual({meta_table_3,{{2000,1,1},{12,45,55}},undefined}, update(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,55}},?nav})),
        ?assertEqual(1, table_size(meta_table_3)),
        ?assertEqual(LogCount3+3, table_size(?LOG_TABLE)),  %% trigger inserted one line 
        ?assertEqual({meta_table_3,{{2000,1,1},{12,45,56}},undefined}, merge(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,56}},?nav})),
        ?assertEqual(2, table_size(meta_table_3)),
        ?assertEqual(LogCount3+4, table_size(?LOG_TABLE)),  %% trigger inserted one line 
        ?assertEqual({meta_table_3,{{2000,1,1},{12,45,56}},undefined}, remove(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,56}},?nav})),
        ?assertEqual(1, table_size(meta_table_3)),
        ?assertEqual(LogCount3+5, table_size(?LOG_TABLE)),  %% trigger inserted one line 
        ?assertEqual(ok, drop_trigger(meta_table_3)),
        Trans3 = fun() ->
            update(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,55}},?nav}),
            insert(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,57}},?nav})
        end,
        ?assertEqual({meta_table_3,{{2000,1,1},{12,45,57}},undefined}, return_atomic(transaction(Trans3))),
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
        
        LogTable = physical_table_name(?LOG_TABLE),
        ?assert(lists:member(LogTable,physical_table_names(?LOG_TABLE))),

        ?assertEqual([],physical_table_names(tpTest_1000@)),

        ?assertException(throw, {ClEr,{"Table to be purged does not exist",tpTest_1000@}}, purge_table(tpTest_1000@)),
        ?assertException(throw, {UiEx,{"Purge not supported on this table type",not_existing_table}}, purge_table(not_existing_table)),
        ?assert(purge_table(?LOG_TABLE) >= 0),
        ?assertException(throw, {UiEx,{"Purge not supported on this table type",ddTable}}, purge_table(ddTable)),

        TimePartTable0 = physical_table_name(tpTest_1000@),
        ?Info("TimePartTable ~p~n", [TimePartTable0]),
        ?assertEqual(TimePartTable0, physical_table_name(tpTest_1000@,erlang:now())),
        ?assertEqual(ok, create_check_table(tpTest_1000@, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog},{type,ordered_set}], system)),
        ?assertEqual(ok, check_table(TimePartTable0)),
        ?assertEqual(0, table_size(TimePartTable0)),
        ?assertEqual(ok, write(tpTest_1000@, LogRec1)),
        ?assertEqual(1, table_size(TimePartTable0)),
        ?assertEqual(0, purge_table(tpTest_1000@)),
        {Megs,Secs,Mics} = erlang:now(),
        FutureSecs = Megs*1000000 + Secs + 2000,
        Future = {FutureSecs div 1000000,FutureSecs rem 1000000,Mics}, 
        LogRec2 = #ddLog{logTime=Future,logLevel=info,pid=self()
                            ,module=?MODULE,function=meta_operations,node=node()
                            ,fields=Fields,message= <<"some log message 2">>},
        ?assertEqual(ok, write(tpTest_1000@, LogRec2)),
        ?Info("physical_table_names ~p~n", [physical_table_names(tpTest_1000@)]),
        ?assertEqual(0, purge_table(tpTest_1000@,[{purge_delay,10000}])),
        ?assertEqual(0, purge_table(tpTest_1000@)),
        PurgeResult = purge_table(tpTest_1000@,[{purge_delay,-3000}]),
        ?Info("PurgeResult ~p~n", [PurgeResult]),
        ?assert(PurgeResult>0),
        ?assertEqual(0, purge_table(tpTest_1000@)),
        ?assertEqual(ok, drop_table(tpTest_1000@)),
        ?assertEqual([],physical_table_names(tpTest_1000@)),
        ?Info("success ~p~n", [tpTest_1000@]),

        ?assertEqual([meta_table_1,meta_table_2,meta_table_3],lists:sort(tables_starting_with("meta_table_"))),
        ?assertEqual([meta_table_1,meta_table_2,meta_table_3],lists:sort(tables_starting_with(meta_table_))),

        DdNode0 = read(ddNode),
        ?Info("ddNode0 ~p~n", [DdNode0]),
        DdNode1 = read(ddNode,node()),
        ?Info("ddNode1 ~p~n", [DdNode1]),
        DdNode2 = select(ddNode,?MatchAllRecords),
        ?Info("ddNode2 ~p~n", [DdNode2]),

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
        ?Info("success ~p~n", [get_config_hlk]),

        ?assertEqual({error,{"Invalid table name",dummy_table_name}}, create_partitioned_table_sync(dummy_table_name)),
        ?assertEqual([],physical_table_names(fakelog_1@)),
        ?assertEqual(ok, create_check_table(fakelog_1@, {record_info(fields, ddLog),?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system)),    
        ?assertEqual(1,length(physical_table_names(fakelog_1@))),
        LogRec3 = #ddLog{logTime=erlang:now(),logLevel=debug,pid=self()
                        ,module=?MODULE,function=test,node=node()
                        ,fields=[],message= <<>>,stacktrace=[]
                    },
        ?assertEqual(ok, dirty_write(fakelog_1@, LogRec3)),
        timer:sleep(1000),
        ?assertEqual(ok, dirty_write(fakelog_1@, LogRec3#ddLog{logTime=erlang:now()})),
        ?assertEqual(2,length(physical_table_names(fakelog_1@))),
        timer:sleep(1100),
        ?assertEqual(ok, create_partitioned_table_sync(physical_table_name(fakelog_1@))),
        ?assertEqual(3,length(physical_table_names(fakelog_1@))),
        ?Info("success ~p~n", [create_partitioned_table]),

        ?assertEqual(ok, drop_table(meta_table_3)),
        ?assertEqual(ok, drop_table(meta_table_2)),
        ?assertEqual(ok, drop_table(meta_table_1)),
        ?assertEqual(ok, drop_table(test_config)),
        ?assertEqual(ok,drop_table(fakelog_1@)),

        ?Info("success ~p~n", [drop_tables])
    catch
        Class:Reason ->     
            timer:sleep(1000),
            ?Info("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
            throw ({Class, Reason})
    end,
    ok.
    
-endif.
