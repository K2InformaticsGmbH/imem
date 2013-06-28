-module(imem_meta).

-define(LOG_TABLE_OPTS,[{record_name,ddLog}
                       ,{type,ordered_set}
                       ,{purge_delay,432000}        %% 5 Days
                       ]).          

-define(MONITOR_TABLE_OPTS,[{record_name,ddMonitor}
                           ,{type,ordered_set}
                           ,{purge_delay,432000}    %% 5 Days
                           ]).  
-define(MONITOR_CYCLE_WAIT, 2000). 

-define(META_TABLES,[ddTable,ddNode,dual,?LOG_TABLE,?MONITOR_TABLE]).
-define(META_FIELDS,[user,username,schema,node,sysdate,systimestamp]). %% ,rownum
-define(META_OPTS,[purge_delay]). % table options only used in imem_meta and above
-define(PURGE_CYCLE_WAIT, 10000). % 10000
-define(PURGE_ITEM_WAIT, 10).
-define(BAD_NAME_CHARACTERS,"!?#*:+-.\\<|>/").

-include_lib("eunit/include/eunit.hrl").

-include("imem_meta.hrl").

-behavior(gen_server).

-record(state, {purgeList=[]}).

-export([ start_link/1
        ]).

% gen_server interface (monitoring calling processes)

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
        , physical_table_name/1
        , physical_table_name/2
        , physical_table_names/1
        , is_time_partitioned_alias/1
        , is_local_time_partitioned_table/1
        , is_node_sharded_alias/1
        , is_local_node_sharded_table/1
        , table_type/1
        , table_columns/1
        , table_size/1
        , table_memory/1
        , table_record_name/1        
        , check_table/1
        , check_table_meta/2
        , check_table_columns/2
        , system_table/1
        , meta_field_list/0        
        , meta_field/1
        , meta_field_info/1
        , meta_field_value/1
        , column_infos/1
        , column_info_items/2
        ]).

-export([ add_attribute/2
        , update_opts/2
        , log_to_db/5
        , failing_function/1
        , monitor/0
        ]).

-export([ create_table/3
        , create_table/4
        , create_partitioned_table/1
        , create_check_table/3
        , create_check_table/4
        , drop_table/1
        , purge_table/1
        , purge_table/2
        , truncate_table/1  %% truncate table
        , read/1            %% read whole table, only use for small tables 
        , read/2            %% read by key
        , select/2          %% select without limit, only use for small result sets
        , select/3          %% select with limit
        , select_sort/2
        , select_sort/3
        , insert/2    
        , write/2           %% write single key
        , dirty_write/2
        , delete/2          %% delete row by key
        , delete_object/2   %% delete single row in bag table 
        ]).

-export([ update_prepare/3          %% stateless creation of update plan from change list
        , update_cursor_prepare/2   %% take change list and generate update plan (stored in state)
        , update_cursor_execute/2   %% take update plan from state and execute it (fetch aborted first)
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


start_link(Params) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

init(_Args) ->
    ?Log("~p starting...~n", [?MODULE]),
    Result = try
        application:set_env(imem, node_shard, node_shard()),
        catch create_table(ddTable, {record_info(fields, ddTable),?ddTable, #ddTable{}}, [], system),
        check_table(ddTable),
        check_table_columns(ddTable, record_info(fields, ddTable)),
        check_table_meta(ddTable, {record_info(fields, ddTable), ?ddTable, #ddTable{}}),
        create_check_table(ddNode, {record_info(fields, ddNode),?ddNode, #ddNode{}}, [], system),    
        create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system),    
        create_check_table(?MONITOR_TABLE, {record_info(fields, ddMonitor),?ddMonitor, #ddMonitor{}}, ?MONITOR_TABLE_OPTS, system),    
        case catch create_table(dual, {record_info(fields, dual),?dual, #dual{}}, [], system) of
            ok ->   write(dual,#dual{});
            _ ->    ok
        end,
        check_table(dual),
        check_table_columns(dual, {record_info(fields, dual),?dual, #dual{}}),
        check_table_meta(dual, {record_info(fields, dual), ?dual, #dual{}}),
        erlang:send_after(?PURGE_CYCLE_WAIT, self(), purge_partitioned_tables),
        erlang:send_after(?MONITOR_CYCLE_WAIT, self(), monitor_loop),
        ?Log("~p started!~n", [?MODULE]),
        {ok,#state{}}
    catch
        Class:Reason -> ?Log("failed with ~p:~p~n", [Class,Reason]),
                        {stop, "Insufficient/invalid resources for start"}
    end,
    Result.

create_partitioned_table(Name) when is_atom(Name) ->
    gen_server:call(?MODULE, {create_partitioned_table, Name}). 

handle_call({create_partitioned_table, Name}, _From, State) ->
    case imem_if:read(ddTable,{schema(), Name}) of
        [#ddTable{}] ->
            % Table exists, may need to load it
            case catch(check_table(Name)) of
                ok ->       
                    {reply, ok, State};
                _ ->
                    % ToDo: try to load table and wait for it
                    case mnesia:wait_for_tables([Name], 10000) of
                        ok ->   
                            {reply, ok, State};
                        Error ->            
                            ?Log("Create partitioned table failed with ~p~n", [Error]),
                            {reply, Error, State}
                    end
            end;
        [] ->   
            % Table does not exist, must create it similar to existing
            NS = node_shard(),
            case string:tokens(atom_to_list(Name), "@") of
                [NameStr,NS] ->
                    % choose template table name (name pattern with highest timestamp)
                    {Prefix,_} = lists:split(length(NameStr)-10, NameStr),
                    Tail = "@" ++ NS,
                    LenNS = length(NameStr),
                    Pred = fun(TN) -> (lists:nthtail(LenNS, atom_to_list(TN)) == Tail) end,
                    Template = lists:last(lists:sort(lists:filter(Pred,tables_starting_with(Prefix)))),
                    % find out ColumnsInfos, Opts, Owner from template table definition
                    case imem_if:read(ddTable,{schema(), Template}) of
                        [] ->   
                            {reply, {error, {"Table template not found", Template}}, State}; 
                        [#ddTable{columns=ColumnInfos,opts=Opts,owner=Owner}] ->
                            try
                                create_table(Name, ColumnInfos, Opts, Owner),
                                {reply, ok, State}
                            catch
                                _:Reason -> 
                                    {reply, {error, Reason}, State}
                            end
                    end;                      
                _ -> 
                    {reply, {error, {"Invalid table name",Name}}, State}
            end
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%     {stop,{shutdown,Reason},State};
% handle_cast({stop, Reason}, State) ->
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(monitor_loop, State) ->
    % save one monitor record and trigger the nxt one
    % ?Log("monitor_loop start~n",[]), 
    monitor(),
    erlang:send_after(?MONITOR_CYCLE_WAIT, self(), monitor_loop),
    {noreply, State};
handle_info(purge_partitioned_tables, State=#state{purgeList=[]}) ->
    % restart purge cycle by collecting list of candidates
    % ?Log("Purge collect start~n",[]), 
    Pred = fun imem_meta:is_local_time_partitioned_table/1,
    case lists:sort(lists:filter(Pred,tables_ending_with("@" ++ node_shard()))) of
        [] ->   erlang:send_after(?PURGE_CYCLE_WAIT, self(), purge_partitioned_tables),
                {noreply, State};
        PL ->   handle_info(purge_partitioned_tables, State#state{purgeList=PL})   
    end;
handle_info(purge_partitioned_tables, State=#state{purgeList=[Tab|Rest]}) ->
    % process one purge candidate
    % ?Log("Purge try table ~p~n",[Tab]), 
    case imem_if:read(ddTable,{schema(), Tab}) of
        [] ->   
            ?Log("Table deleted before it could be purged ~p~n",[Tab]); 
        [#ddTable{opts=Opts}] ->
            case lists:keyfind(purge_delay, 1, Opts) of
                false ->
                    ok;             %% no purge delay in table create options, do not purge this file
                {purge_delay,PD} ->
                    Name = atom_to_list(Tab),
                    {BaseName,PartitionName} = lists:split(length(Name)-length(node_shard())-11, Name),
                    case Rest of
                        [] ->   
                            ok;                     %% no follower, do not purge this file
                        [Next|_] ->
                            NextName = atom_to_list(Next),
                            case lists:prefix(BaseName,NextName) of
                                false -> 
                                    ok;             %% no follower, do not purge this file
                                true ->
                                    {Mega,Sec,_} = erlang:now(),
                                    PurgeEnd=1000000*Mega+Sec-PD,
                                    PartitionEnd=list_to_integer(lists:sublist(PartitionName,10)),
                                    if
                                        (PartitionEnd >= PurgeEnd) ->
                                            ok;     %% too young, do not purge this file  
                                        true ->                     
                                            FreedMemory = table_memory(Tab),
                                            Fields = [{table,Tab},{table_size,table_size(Tab)},{table_memory,FreedMemory}],   
                                            ?Log("Purge time partition ~p~n",[Tab]),
                                            log_to_db(info,?MODULE,purge_time_partitioned_table,Fields,"purge table"),
                                            drop_table_and_info(Tab)
                                    end
                            end
                    end
            end
    end,  
    case Rest of
        [] ->   erlang:send_after(?PURGE_CYCLE_WAIT, self(), purge_partitioned_tables),
                {noreply, State#state{purgeList=[]}};
        Rest -> erlang:send_after(?PURGE_ITEM_WAIT, self(), purge_partitioned_tables),
                {noreply, State#state{purgeList=Rest}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


%% ------ META implementation -------------------------------------------------------

system_table({_S,Table,_A}) -> system_table(Table);
system_table({_,Table}) -> system_table(Table);
system_table(Table) when is_atom(Table) ->
    case lists:member(Table,?META_TABLES) of
        true ->     true;
        false ->    imem_if:system_table(Table)
    end.

check_table(Table) when is_atom(Table) ->
    imem_if:table_size(physical_table_name(Table)),
    ok.

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
    drop_table(ddTable).     

meta_field_list() -> ?META_FIELDS.

meta_field(Name) ->
    lists:member(Name,?META_FIELDS).

meta_field_info(sysdate) ->
    #ddColumn{name=sysdate, type='datetime', len=20, prec=0};
meta_field_info(systimestamp) ->
    #ddColumn{name=systimestamp, type='timestamp', len=20, prec=0};
meta_field_info(schema) ->
    #ddColumn{name=schema, type='atom', len=10, prec=0};
meta_field_info(node) ->
    #ddColumn{name=node, type='atom', len=30, prec=0};
meta_field_info(user) ->
    #ddColumn{name=user, type='userid', len=20, prec=0};
meta_field_info(username) ->
    #ddColumn{name=username, type='binstr', len=20, prec=0};
% meta_field_info(rownum) ->
%     #ddColumn{name=rownum, type='integer', len=10, prec=0};
meta_field_info(Name) ->
    ?ClientError({"Unknown meta column",Name}). 

meta_field_value(rownum) ->     1; 
meta_field_value(username) ->   <<"unknown">>; 
meta_field_value(user) ->       unknown; 
meta_field_value(Name) ->
    imem_if:meta_field_value(Name). 

column_info_items(Info, name) ->
    [C#ddColumn.name || C <- Info];
column_info_items(Info, type) ->
    [C#ddColumn.type || C <- Info];
column_info_items(Info, default) ->
    [C#ddColumn.default || C <- Info];
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
        ?ClientError({"Column defn params length missmatch", { {"Names", NamesLength}
                                                             , {"Types", TypesLength}
                                                             , {"Defaults", DefaultsLength}}});
    true -> ok
    end,
    [#ddColumn{name=list_to_atom(lists:flatten(io_lib:format("~p", [N]))), type=T, default=D} || {N,T,D} <- lists:zip3(Names, Types, Defaults)].

create_table(Table, Columns, Opts) ->
    create_table(Table, Columns, Opts, #ddTable{}#ddTable.owner).

create_table(Table, {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(ColumnNames, ColumnTypes, Defaults),
    create_physical_table(Table,ColumnInfos,Opts,Owner);
create_table(Table, [#ddColumn{}|_]=ColumnInfos, Opts, Owner) ->
    create_physical_table(Table,ColumnInfos,Opts,Owner);
create_table(Table, ColumnNames, Opts, Owner) ->
    ColumnInfos = column_infos(ColumnNames),
    create_physical_table(Table,ColumnInfos,Opts,Owner).

create_check_table(Table, Columns, Opts) ->
    create_check_table(Table, Columns, Opts, (#ddTable{})#ddTable.owner).

create_check_table(Table, {ColumnNames, ColumnTypes, DefaultRecord}, Opts, Owner) ->
    [_|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(ColumnNames, ColumnTypes, Defaults),
    create_check_physical_table(Table,ColumnInfos,Opts,Owner),
    check_table(Table),
    check_table_columns(Table, ColumnNames),
    check_table_meta(Table, {ColumnNames, ColumnTypes, DefaultRecord}).

create_check_physical_table(Table,ColumnInfos,Opts,Owner) when is_atom(Table) ->
    create_check_physical_table({schema(),Table},ColumnInfos,Opts,Owner);    
create_check_physical_table({Schema,Table},ColumnInfos,Opts,Owner) ->
    MySchema = schema(),
    case Schema of
        MySchema ->
            PhysicalName=physical_table_name(Table),
            case read(ddTable,{MySchema,PhysicalName}) of 
                [] ->
                    create_physical_table(Table,ColumnInfos,Opts,Owner);
                [#ddTable{opts=Opts,owner=Owner}] ->
                    catch create_physical_table(Table,ColumnInfos,Opts,Owner),
                    ok;
                [#ddTable{opts=Old,owner=Owner}] ->
                    OldOpts = lists:sort(lists:keydelete(purge_delay,1,Old)),
                    NewOpts = lists:sort(lists:keydelete(purge_delay,1,Opts)),
                    case NewOpts of
                        OldOpts ->
                            catch create_physical_table(Table,ColumnInfos,Opts,Owner),
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
        MySchema ->
            case Table of
                ddTable ->  
                    create_physical_table(Table,ColumnInfos,Opts,Owner);
                ?LOG_TABLE ->   
                    create_physical_table(Table,ColumnInfos,Opts,Owner);
                _ ->        
                    log_to_db(info,?MODULE,create_table,[{table,Table},{ops,Opts},{owner,Owner}],"create table"),  
                    create_physical_table(Table,ColumnInfos,Opts,Owner)
            end;
        _ ->        ?UnimplementedException({"Create table in foreign schema",{Schema,Table}})
    end;
create_physical_table(Table,ColumnInfos,Opts,Owner) ->
    case is_valid_table_name(Table) of
        true ->     ok;
        false ->    ?ClientError({"Invalid character(s) in table name",Table})
    end,    
    case sqlparse:is_reserved(Table) of
        false ->    ok;
        true ->     ?ClientError({"Reserved table name",Table})
    end,
    CharsCheck = [{is_valid_column_name(Name),Name} || Name <- column_info_items(ColumnInfos, name)],
    case lists:keyfind(false, 1, CharsCheck) of
        false ->    ok;
        {_,BadN} -> ?ClientError({"Invalid character(s) in column name",BadN})
    end,
    ReservedCheck = [{sqlparse:is_reserved(Name),Name} || Name <- column_info_items(ColumnInfos, name)],
    case lists:keyfind(true, 1, ReservedCheck) of
        false ->    ok;
        {_,BadC} -> ?ClientError({"Reserved column name",BadC})
    end,
    TypeCheck = [{imem_datatype:is_datatype(Type),Type} || Type <- column_info_items(ColumnInfos, type)],
    case lists:keyfind(false, 1, TypeCheck) of
        false ->    ok;
        {_,BadT} -> ?ClientError({"Invalid data type",BadT})
    end,
    PhysicalName=physical_table_name(Table),
    imem_if:create_table(PhysicalName, column_names(ColumnInfos), if_opts(Opts)),
    imem_if:write(ddTable, #ddTable{qname={schema(),PhysicalName}, columns=ColumnInfos, opts=Opts, owner=Owner}).

is_valid_table_name(Table) when is_atom(Table) ->
    is_valid_table_name(atom_to_list(Table));
is_valid_table_name(Table) when is_list(Table) ->
    (length(Table) == length(Table -- ?BAD_NAME_CHARACTERS)).   

is_valid_column_name(Column) ->
    is_valid_table_name(atom_to_list(Column)).

if_opts(Opts) ->
    % Remove imem_meta table options which are not recognized by imem_if
    if_opts(Opts,?META_OPTS).

if_opts([],_) -> [];
if_opts(Opts,[]) -> Opts;
if_opts(Opts,[MO|Others]) ->
    if_opts(lists:keydelete(MO, 1, Opts),Others).

drop_table({Schema,Table,_Alias}) -> 
    drop_table({Schema,Table});
drop_table({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema -> drop_table(Table);
        _ ->        ?UnimplementedException({"Drop table in foreign schema",{Schema,Table}})
    end;
drop_table(ddTable) -> 
    imem_if:drop_table(ddTable);
drop_table(?LOG_TABLE = Table) ->
    drop_table_and_info(physical_table_name(Table));
drop_table(Alias) ->
    log_to_db(info,?MODULE,drop_table,[{table,Alias}],"drop table"),
    drop_partitioned_tables_and_infos(lists:sort(simple_or_local_node_sharded_tables(Alias))).

drop_partitioned_tables_and_infos([]) -> ok;
drop_partitioned_tables_and_infos([PhName|PhNames]) ->
    drop_table_and_info(PhName),
    drop_partitioned_tables_and_infos(PhNames).

drop_table_and_info(PhysicalName) ->
    imem_if:drop_table(PhysicalName),
    imem_if:delete(ddTable, {schema(),PhysicalName}).

purge_table(Name) ->
    purge_table(Name, []).

purge_table({Schema,Table,_Alias}, Opts) -> 
    purge_table({Schema,Table}, Opts);
purge_table({Schema,Table}, Opts) ->
    MySchema = schema(),
    case Schema of
        MySchema -> purge_table(Table, Opts);
        _ ->        ?UnimplementedException({"Purge table in foreign schema",{Schema,Table}})
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
                    % ?Log("Purge PhName KeepName ~p ~p~n",[PhName,KeepName]),
                    case Rest of
                        [] ->   DummyName = partitioned_table_name(Alias,erlang:now()),
                                % ?Log("Purge DummyName ~p~n",[DummyName]),
                                create_partitioned_table(DummyName);
                        _ ->    ok
                    end,
                    FreedMemory = table_memory(PhName),
                    Fields = [{table,PhName},{table_size,table_size(PhName)},{table_memory,FreedMemory}],   
                    log_to_db(info,?MODULE,purge_time_partitioned_table,Fields,"purge table"),
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

physical_table_name({_S,N,_A}) -> physical_table_name(N);
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

physical_table_name({_S,N,_A},Key) -> physical_table_name(N,Key);
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
    {undefined,undefined};
failing_function([{imem_meta,throw_exception,_,_}|STrace]) -> 
    failing_function(STrace);
failing_function([{M,N,_,_}|STrace]) ->
    case lists:prefix("imem",atom_to_list(M)) of 
        true ->     {M,N};
        false ->    failing_function(STrace)
    end;
failing_function(Other) ->
    ?Log("unexpected stack trace ~p~n", [Other]),
    {undefined,undefined}.

log_to_db(Level,Module,Function,Fields,Message) when is_binary(Message) ->
    StackTrace = if
        (Level == error) -> erlang:get_stacktrace();
        true ->             []
    end,
    LogRec = #ddLog{logTime=erlang:now(),logLevel=Level,pid=self()
                        ,module=Module,function=Function,node=node()
                        ,fields=Fields,message=Message,stacktrace=StackTrace
                    },
    dirty_write(?LOG_TABLE, LogRec);
log_to_db(Level,Module,Function,Fields,Message) ->
    BinStr = try 
        list_to_binary(Message)
    catch
        _:_ ->  list_to_binary(lists:flatten(io_lib:format("~tp",[Message])))
    end,
    log_to_db(Level,Module,Function,Fields,BinStr).


%% imem_if but security context added --- META INFORMATION ------

data_nodes() ->
    imem_if:data_nodes().

all_tables() ->
    imem_if:all_tables().

node_shard() ->
    case application:get_env(imem, node_shard) of
        {ok,NS} when is_list(NS) ->      NS;
        {ok,NI} when is_integer(NI) ->   integer_to_list(NI);
        undefined ->                     node_hash(node());    
        {ok,node_shard_fun} ->  
            try 
                node_shard_value(application:get_env(imem, node_shard_fun),node())
            catch
                _:_ ->  ?Log("bad config parameter ~p~n", [node_shard_fun]),
                        "nohost"
            end;
        {ok,host_name} ->                host_name(node());    
        {ok,host_fqdn} ->                host_fqdn(node());    
        {ok,node_name} ->                node_name(node());    
        {ok,node_hash} ->                node_hash(node());    
        {ok,NA} when is_atom(NA) ->      atom_to_list(NA);
        Else ->     ?Log("bad config parameter ~p ~p~n", [node_shard, Else]),
                    node_hash(node())
    end.

node_shard_value({ok,FunStr},Node) ->
    % ?Log("node_shard calculated for ~p~n", [FunStr]),
    Code = case [lists:last(string:strip(FunStr))] of
        "." -> FunStr;
        _ -> FunStr ++ "."
    end,
    {ok,ErlTokens,_}=erl_scan:string(Code),    
    {ok,ErlAbsForm}=erl_parse:parse_exprs(ErlTokens),    
    {value,Value,_}=erl_eval:exprs(ErlAbsForm,[]),    
    Result = Value(Node),
    % ?Log("node_shard_value ~p~n", [Result]),
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


table_type({_Schema,Table}) ->
    table_type(Table);          %% ToDo: may depend on schema
table_type(Table) when is_atom(Table) ->
    imem_if:table_type(physical_table_name(Table)).

table_record_name({_Schema,Table}) ->
    table_record_name(Table);   %% ToDo: may depend on schema
table_record_name(ddNode)  -> ddNode;
table_record_name(Table) when is_atom(Table) ->
    imem_if:table_record_name(physical_table_name(Table)).

table_columns({_Schema,Table}) ->
    table_columns(Table);       %% ToDo: may depend on schema
table_columns(Table) ->
    imem_if:table_columns(physical_table_name(Table)).

table_size({_Schema,Table}) ->
    table_size(Table);          %% ToDo: may depend on schema
table_size(ddNode) ->
    length(read(ddNode));
table_size(Table) ->
    %% ToDo: sum should be returned for all local time partitions
    imem_if:table_size(physical_table_name(Table)).

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

fetch_start(Pid, {_Schema,Table}, MatchSpec, BlockSize, Opts) ->
    fetch_start(Pid, Table, MatchSpec, BlockSize, Opts);          %% ToDo: may depend on schema
fetch_start(Pid, ddNode, MatchSpec, _BlockSize, _Opts) ->
    {Rows,true} = select(ddNode, MatchSpec),
    spawn(
        fun() ->
            receive
                abort ->    ok;
                next ->     Pid ! {row, [?sot,?eot|Rows]}
            end
        end
    );
fetch_start(Pid, Table, MatchSpec, BlockSize, Opts) ->
    imem_if:fetch_start(Pid, physical_table_name(Table), MatchSpec, BlockSize, Opts).

close(Pid) ->
    imem_statement:close(none, Pid).

read({_Schema,Table}) -> 
    read(Table);            %% ToDo: may depend on schema
read(ddNode) -> 
    lists:flatten([read(ddNode,Node) || Node <- [node()|nodes()]]);
read(Table) -> 
    imem_if:read(physical_table_name(Table)).

read({_Schema,Table}, Key) -> 
    read(Table, Key);
read(ddNode,Node) when is_atom(Node) ->
    try  
        [#ddNode{ name=Node
                 , wall_clock=element(1,rpc:call(Node,erlang,statistics,[wall_clock]))
                 , time=rpc:call(Node,erlang,now,[])
                 , extra=[]     
                 }       
        ]
    catch
        Class:Reason ->
            ?Log("ddNode evaluation error ~p:~p~n", [Class,Reason]),
            []
    end;
read(ddNode,_) -> [];
read(Table, Key) -> 
    imem_if:read(physical_table_name(Table), Key).

select({_Schema,Table}, MatchSpec) ->
    select(Table, MatchSpec);           %% ToDo: may depend on schema
select(ddNode, ?MatchAllRecords) ->
    {read(ddNode),true};
select(ddNode, [{_,[],['$_']}]) ->
    {read(ddNode),true};                %% used in select * from ddNode
select(ddNode, [{_,[{'==',{element,N,Tuple},_}],['$_']}]) when is_tuple(Tuple) ->
    {read(ddNode,element(N,Tuple)),true};
select(ddNode, [{_,[{'==',_,{element,N,Tuple}}],['$_']}]) when is_tuple(Tuple) ->
    {read(ddNode,element(N,Tuple)),true};
select(ddNode, [{_,[{'==',K1,K2}],['$_']}]) when is_atom(K1), is_atom(K2) ->
    case atom_to_list(K1) of
        [$$|_] ->   {read(ddNode,K2),true};   % Key cannot match '$_'
        _ ->        {read(ddNode,K1),true}
    end;
select(ddNode, MatchSpec) ->
    ?UnimplementedException({"Unsupported match specification for virtual table",{MatchSpec,ddNode}});
select(Table, MatchSpec) ->
    imem_if:select(physical_table_name(Table), MatchSpec).

select(Table, MatchSpec, 0) ->
    select(Table, MatchSpec);
select({_Schema,Table}, MatchSpec, Limit) ->
    select(Table, MatchSpec, Limit);        %% ToDo: may depend on schema
select(ddNode, MatchSpec, _Limit) ->
    select(ddNode, MatchSpec);
select(Table, MatchSpec, Limit) ->
    imem_if:select(physical_table_name(Table), MatchSpec, Limit).

select_sort(Table, MatchSpec)->
    {L, true} = select(Table, MatchSpec),
    {lists:sort(L), true}.

select_sort(Table, MatchSpec, Limit) ->
    {Result, AllRead} = select(Table, MatchSpec, Limit),
    {lists:sort(Result), AllRead}.

write({_Schema,Table}, Record) -> 
    write(Table, Record);           %% ToDo: may depend on schema 
write(?LOG_TABLE, Record) ->
    imem_if:write(physical_table_name(?LOG_TABLE), Record);
write(Table, Record) ->
    % log_to_db(debug,?MODULE,write,[{table,Table},{rec,Record}],"write"), 
    PTN = physical_table_name(Table,element(2,Record)),
    try
        imem_if:write(PTN, Record)
    catch
        throw:{'ClientError',{"Table does not exist",T}} ->
            % ToDo: instruct imem_meta gen_server to create the table
            case is_time_partitioned_alias(Table) of
                true ->
                    create_partitioned_table(PTN);
                false ->
                    ?ClientError({"Table does not exist",T})
            end;
        Class:Reason ->
            ?Log("Write error ~p:~p~n", [Class,Reason]),
            throw(Reason)
    end. 

monitor() ->
    try  
        Now = erlang:now(),
        {{input,Input},{output,Output}} = erlang:statistics(io),
        Moni = #ddMonitor{ time=Now
                         , node = node()
                         , memory=erlang:memory(total)
                         , process_count=erlang:system_info(process_count)          
                         , port_count=erlang:system_info(port_count)
                         , run_queue=erlang:statistics(run_queue)
                         , wall_clock=element(1,erlang:statistics(wall_clock))
                         , reductions=element(1,erlang:statistics(reductions))
                         , input_io=Input
                         , output_io=Output
                         , extra=[]      
                         },
        imem_if:write(physical_table_name(?MONITOR_TABLE), Moni)        
    catch
        _:E ->     log_to_db(warning,?MODULE,monitor,[{error,E}],"cannot monitor")
    end.

dirty_write({_Schema,Table}, Record) -> 
    dirty_write(Table, Record);           %% ToDo: may depend on schema 
dirty_write(?LOG_TABLE, Record) -> 
    imem_if:dirty_write(physical_table_name(?LOG_TABLE), Record);
dirty_write(Table, Record) -> 
    % log_to_db(debug,?MODULE,dirty_write,[{table,Table},{rec,Record}],"dirty_write"), 
    PTN = physical_table_name(Table,element(2,Record)),
    try
        imem_if:dirty_write(PTN, Record)
    catch
        throw:{'ClientError',{"Table does not exist",T}} ->
            case is_time_partitioned_alias(Table) of
                true ->
                    create_partitioned_table(PTN);
                false ->
                    ?ClientError({"Table does not exist",T})
            end;
        Class:Reason ->
            ?Log("Dirty write error ~p:~p~n", [Class,Reason]),
            throw(Reason)
    end. 

insert({_Schema,Table}, Row) ->
    insert(Table, Row);             %% ToDo: may depend on schema
insert(ddTable, Row) ->
    write(ddTable, Row);
insert(Table, Row) when is_tuple(Row) ->
    case lists:member(?nav,tuple_to_list(Row)) of
        false ->    write(physical_table_name(Table), Row);
        true ->     ?ClientError({"Not null constraint violation", {Table,Row}})
    end.

delete({_Schema,Table}, Key) ->
    delete(Table, Key);             %% ToDo: may depend on schema
delete(Table, Key) ->
    imem_if:delete(physical_table_name(Table), Key).

delete_object({_Schema,Table}, Row) ->
    delete_object(Table, Row);             %% ToDo: may depend on schema
delete_object(Table, Row) ->
    imem_if:delete_object(physical_table_name(Table), Row).

truncate_table({_Schema,Table,_Alias}) ->
    truncate_table({_Schema,Table});    
truncate_table({_Schema,Table}) ->
    truncate_table(Table);                %% ToDo: may depend on schema
truncate_table(Alias) ->
    log_to_db(debug,?MODULE,truncate_table,[{table,Alias}],"truncate table"),
    truncate_partitioned_tables(lists:sort(simple_or_local_node_sharded_tables(Alias))).

truncate_partitioned_tables([]) -> ok;
truncate_partitioned_tables([PhName|PhNames]) ->
    imem_if:truncate_table(PhName),
    truncate_partitioned_tables(PhNames).

subscribe({table, Tab, Mode}) ->
    log_to_db(info,?MODULE,subscribe,[{ec,{table, physical_table_name(Tab), Mode}}],"subscribe to mnesia"),
    imem_if:subscribe({table, physical_table_name(Tab), Mode});
subscribe(EventCategory) ->
    log_to_db(info,?MODULE,subscribe,[{ec,EventCategory}],"subscribe to mnesia"),
    imem_if:subscribe(EventCategory).

unsubscribe({table, Tab, Mode}) ->
    log_to_db(info,?MODULE,unsubscribe,[{ec,{table, physical_table_name(Tab), Mode}}],"unsubscribe from mnesia"),
    imem_if:unsubscribe({table, physical_table_name(Tab), Mode});
unsubscribe(EventCategory) ->
    log_to_db(info,?MODULE,unsubscribe,[{ec,EventCategory}],"unsubscribe from mnesia"),
    imem_if:unsubscribe(EventCategory).

update_tables(UpdatePlan, Lock) ->
    update_tables(schema(), UpdatePlan, Lock, []).

update_bound_counter(Table, Field, Key, Incr, LimitMin, LimitMax) ->
    imem_if:update_bound_counter(physical_table_name(Table), Field, Key, Incr, LimitMin, LimitMax).

update_tables(_MySchema, [], Lock, Acc) ->
    imem_if:update_tables(Acc, Lock);  
update_tables(MySchema, [UEntry|UPlan], Lock, Acc) ->
    % log_to_db(debug,?MODULE,update_tables,[{lock,Lock}],io_lib:format("~p",[UEntry])),
    update_tables(MySchema, UPlan, Lock, [update_table_name(MySchema, UEntry)|Acc]).

update_table_name(MySchema,[{MySchema,Tab,Type}, Item, Old, New]) ->
    case lists:member(?nav,tuple_to_list(New)) of
        false ->    [{physical_table_name(Tab),Type}, Item, Old, New];
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

        ?Log("----TEST--~p:test_mnesia~n", [?MODULE]),

        ?Log("schema ~p~n", [imem_meta:schema()]),
        ?Log("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        ?assertEqual(ok, check_table_columns(ddTable, record_info(fields, ddTable))),

        ?assertEqual(ok, create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, ?LOG_TABLE_OPTS, system)),
        ?assertException(throw,{SyEx,{"Wrong table owner",{?LOG_TABLE,system}}} ,create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog},{type,ordered_set}], admin)),
        ?assertException(throw,{SyEx,{"Wrong table options",{?LOG_TABLE,_}}} ,create_check_table(?LOG_TABLE, {record_info(fields, ddLog),?ddLog, #ddLog{}}, [{record_name,ddLog1},{type,ordered_set}], system)),
        ?assertEqual(ok, check_table(?LOG_TABLE)),

        Now = erlang:now(),
        LogCount1 = table_size(?LOG_TABLE),
        ?Log("ddLog@ count ~p~n", [LogCount1]),
        Fields=[{test_criterium_1,value1},{test_criterium_2,value2}],
        LogRec1 = #ddLog{logTime=Now,logLevel=info,pid=self()
                            ,module=?MODULE,function=meta_operations,node=node()
                            ,fields=Fields,message= <<"some log message 1">>},
        ?assertEqual(ok, write(?LOG_TABLE, LogRec1)),
        LogCount2 = table_size(?LOG_TABLE),
        ?Log("ddLog@ count ~p~n", [LogCount2]),
        ?assertEqual(LogCount1+1,LogCount2),
        Log1=read(?LOG_TABLE,Now),
        ?Log("ddLog@ content ~p~n", [Log1]),
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],"Message")),        
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],[])),        
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],[stupid_error_message,1])),        
        ?assertEqual(ok, log_to_db(info,?MODULE,test,[{test_3,value3},{test_4,value4}],{stupid_error_message,2})),        
        LogCount2a = table_size(?LOG_TABLE),
        ?assertEqual(LogCount2+4,LogCount2a),

        ?Log("----TEST--~p:test_database_operations~n", [?MODULE]),
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
        ?Log("success ~p~n", [create_table_not_null]),

        ?assertException(throw, {ClEr,{"Invalid character(s) in table name", 'bad_?table_1'}}, create_table('bad_?table_1', BadTypes1, [])),
        ?assertException(throw, {ClEr,{"Reserved table name", select}}, create_table(select, BadTypes2, [])),

        ?assertException(throw, {ClEr,{"Invalid character(s) in column name", 'a:b'}}, create_table(bad_table_1, BadTypes1, [])),
        ?assertException(throw, {ClEr,{"Reserved column name", current}}, create_table(bad_table_1, BadTypes2, [])),
        ?assertException(throw, {ClEr,{"Invalid data type", iinteger}}, create_table(bad_table_1, BadTypes3, [])),

        ?assertEqual(ok, insert(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,55}},undefined})),
        ?assertEqual(1, table_size(meta_table_3)),
        LogCount3 = table_size(?LOG_TABLE),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {meta_table_3,_}}}, insert(meta_table_3, {meta_table_3,?nav,undefined})),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {meta_table_3,_}}}, insert(meta_table_3, {meta_table_3,{{2000,01,01},{12,45,56}},?nav})),
        LogCount4 = table_size(?LOG_TABLE),
        ?Log("success ~p~n", [not_null_constraint]),
        ?assertEqual(LogCount3+2, LogCount4),

        Keys4 = [
        {1,{meta_table_3,{{2000,1,1},{12,45,59}},undefined}}
        ],
        ?assertEqual(Keys4, update_tables([[{'Imem',meta_table_3,set}, 1, {}, {meta_table_3,{{2000,01,01},{12,45,59}},undefined}]], optimistic)),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {1,{meta_table_3,_}}}}, update_tables([[{'Imem',meta_table_3,set}, 1, {}, {meta_table_3, ?nav, undefined}]], optimistic)),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {1,{meta_table_3,_}}}}, update_tables([[{'Imem',meta_table_3,set}, 1, {}, {meta_table_3,{{2000,01,01},{12,45,59}}, ?nav}]], optimistic)),
        
        LogTable = physical_table_name(?LOG_TABLE),
        ?assert(lists:member(LogTable,physical_table_names(?LOG_TABLE))),

        ?assertEqual([],physical_table_names(tpTest_1000@)),

        ?assertException(throw, {ClEr,{"Table to be purged does not exist",tpTest_1000@}}, purge_table(tpTest_1000@)),
        ?assertException(throw, {UiEx,{"Purge not supported on this table type",not_existing_table}}, purge_table(not_existing_table)),
        ?assert(purge_table(?LOG_TABLE) >= 0),
        ?assertException(throw, {UiEx,{"Purge not supported on this table type",ddTable}}, purge_table(ddTable)),

        TimePartTable0 = physical_table_name(tpTest_1000@),
        ?Log("TimePartTable ~p~n", [TimePartTable0]),
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
        ?Log("physical_table_names ~p~n", [physical_table_names(tpTest_1000@)]),
        ?assertEqual(0, purge_table(tpTest_1000@,[{purge_delay,10000}])),
        ?assertEqual(0, purge_table(tpTest_1000@)),
        PurgeResult = purge_table(tpTest_1000@,[{purge_delay,-3000}]),
        ?Log("PurgeResult ~p~n", [PurgeResult]),
        ?assert(PurgeResult>0),
        ?assertEqual(0, purge_table(tpTest_1000@)),
        ?assertEqual(ok, drop_table(tpTest_1000@)),
        ?assertEqual([],physical_table_names(tpTest_1000@)),
        ?Log("success ~p~n", [tpTest_1000@]),

        ?assertEqual([meta_table_1,meta_table_2,meta_table_3],lists:sort(tables_starting_with("meta_table_"))),
        ?assertEqual([meta_table_1,meta_table_2,meta_table_3],lists:sort(tables_starting_with(meta_table_))),

        DdNode0 = read(ddNode),
        ?Log("ddNode0 ~p~n", [DdNode0]),
        DdNode1 = read(ddNode,node()),
        ?Log("ddNode1 ~p~n", [DdNode1]),
        DdNode2 = select(ddNode,?MatchAllRecords),
        ?Log("ddNode2 ~p~n", [DdNode2]),

        ?assertEqual(ok, monitor()),
        MonRecs = read(?MONITOR_TABLE),
        ?Log("MonRecs count ~p~n", [length(MonRecs)]),
        ?Log("MonRecs last ~p~n", [lists:last(MonRecs)]),
        % ?Log("MonRecs[1] ~p~n", [hd(MonRecs)]),
        % ?Log("MonRecs ~p~n", [MonRecs]),
        ?assert(length(MonRecs) > 0),

        ?assertEqual(ok, drop_table(meta_table_3)),
        ?assertEqual(ok, drop_table(meta_table_2)),
        ?assertEqual(ok, drop_table(meta_table_1)),
        ?Log("success ~p~n", [drop_tables])
    catch
        Class:Reason ->  ?Log("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
    
-endif.
