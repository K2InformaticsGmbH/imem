-module(imem_sec).

-define(SECO_TABLES,[ddTable,ddAccount,ddRole,ddSeCo@,ddPerm@,ddQuota@]).
-define(SECO_FIELDS,[<<"user">>,<<"username">>]).

-include("imem_seco.hrl").

-export([ schema/1
        , schema/2
        , system_id/1
        , data_nodes/1
        , all_tables/1
        , is_readable_table/2
        , tables_starting_with/2
        , node_shard/1
        , physical_table_name/2
        , physical_table_names/2
        , table_type/2
        , table_columns/2
        , table_size/2
        , table_record_name/2
        , check_table/2
        , check_table_meta/3
        , check_table_columns/3
        , is_system_table/2
        , meta_field_list/1                
        , meta_field/2
        , meta_field_info/2
        , meta_field_value/2
        , column_infos/2
        , column_info_items/3        
        , column_map/3
        , subscribe/2
        , unsubscribe/2
        ]).

-export([ update_opts/3
        , add_attribute/3
   %     , log_to_db/6
        , get_config_hlk/5  %% get single config value and put default if not found
        , put_config_hlk/6  %% put single config value with remark
        ]).

-export([ authenticate/4
        , login/1
        , change_credentials/3
        , logout/1
        , clone_seco/2
        ]).

-export([ create_table/4
        , create_check_table/4
        , create_sys_conf/2
		, drop_table/2
        , purge_table/2
        , purge_table/3
        , truncate_table/2
        , snapshot_table/2  %% dump local table to snapshot directory
        , restore_table/2   %% replace local table by version in snapshot directory
        , read/2
        , read/3
        , read_hlk/3        %% read hierarchical list key
        , select/3
        , select/4
        , insert/3
        , update/3
        , merge/3
        , remove/3
        , write/3
        , dirty_write/3    
        , delete/3
        , delete_object/3
        , admin_exec/4
        , dal_exec/4
        ]).


-export([ update_prepare/4          %% stateless creation of update plan from change list
        , update_cursor_prepare/3   %% stateful creation of update plan (stored in state)
        , update_cursor_execute/3   %% stateful execution of update plan (fetch aborted first)
        , apply_validators/4          %% apply any arity funs of default record to current record        
        , fetch_recs/4
        , fetch_recs_sort/4
        , fetch_recs_async/3        %% ToDo: implement proper return of RowFun(), match conditions and joins
        , fetch_recs_async/4        %% ToDo: implement proper return of RowFun(), match conditions and joins
        , filter_and_sort/4
        , filter_and_sort/5
        , fetch_close/2
        , exec/4
        , close/2
        ]).

-export([ fetch_start/6
        , update_tables/3           %% update (first) table and return updated keys 
        ]).

-export([ transaction/2
        , transaction/3
        , transaction/4
        , return_atomic_list/2
        , return_atomic_ok/2
        , return_atomic/2
        ]).

-export([ have_table_permission/3   %% includes table ownership and readonly
        , have_module_permission/3  
        , have_permission/2    
        ]).

% exported for test
-export([ select_rowfun_str/5
        ]).

%% one to one from dd_account ------------ AA FUNCTIONS _--------

authenticate(_SKey, SessionId, Name, Credentials) ->
    imem_seco:authenticate(SessionId, Name, Credentials).

login(SKey) ->
    imem_seco:login(SKey).

change_credentials(SKey, OldCred, NewCred) ->
    imem_seco:change_credentials(SKey, OldCred, NewCred).

logout(SKey) ->
    imem_seco:logout(SKey).

clone_seco(SKey, Pid) ->
    imem_seco:clone_seco(SKey, Pid).


%% from imem_meta --- HELPER FUNCTIONS do not export!! --------

if_is_system_table(_SKey,{_,Table}) -> 
    if_is_system_table(_SKey,Table);       % TODO: May depend on Schema
if_is_system_table(_SKey,Table) when is_atom(Table) ->
    case lists:member(Table,?SECO_TABLES) of
        true ->     true;
        false ->    imem_meta:is_system_table(Table)
    end;
if_is_system_table(_SKey,Table) when is_binary(Table) ->
    try
        {S,T} = imem_sql_expr:binstr_to_qname2(Table), 
        if_is_system_table(_SKey,{?binary_to_existing_atom(S),?binary_to_existing_atom(T)})
    catch
        _:_ -> false
    end.

if_meta_field_list(_SKey) ->
    imem_meta:meta_field_list().

if_meta_field(_SKey, Name) when is_atom(Name) ->
    if_meta_field(_SKey, ?atom_to_binary(Name));
if_meta_field(_SKey, Name) ->
    case lists:member(Name,?SECO_FIELDS) of
        true ->     true;
        false ->    imem_meta:meta_field(Name)
    end.

if_meta_field_info(_SKey, Name) ->              imem_meta:meta_field_info(Name).

if_meta_field_value(SKey, <<"user">>) ->        imem_seco:account_id(SKey);
if_meta_field_value(SKey, user) ->              imem_seco:account_id(SKey);
if_meta_field_value(SKey, <<"username">>) ->    imem_seco:account_name(SKey);
if_meta_field_value(SKey, username) ->          imem_seco:account_name(SKey);
if_meta_field_value(_SKey, Name) ->             imem_meta:meta_field_value(Name).

add_attribute(_SKey, A, Opts) ->                imem_meta:add_attribute(A, Opts).

update_opts(_SKey, T, Opts) ->                  imem_meta:update_opts(T, Opts).

%% imem_if but security context added --- META INFORMATION ------

schema(SKey) ->
    seco_authorized(SKey),
    imem_meta:schema().

schema(SKey, Node) ->
    seco_authorized(SKey),
    imem_meta:schema(Node).

% BIKRAM: exported for testing
select_rowfun_str(SKey, ColMap, DateFmt, NumFmt, StrFmt) ->
    io:format(user, "imem_sec:select_rowfun_str~n", []),
    seco_authorized(SKey),
    imem_datatype:select_rowfun_str(ColMap, DateFmt, NumFmt, StrFmt).

system_id(_Skey) ->
    imem_meta:system_id().

is_system_table(SKey, Table) ->
    seco_authorized(SKey),    
    if_is_system_table(SKey, Table).

meta_field_list(SKey) ->
    if_meta_field_list(SKey).

meta_field(SKey, Name) ->
    if_meta_field(SKey, Name).

meta_field_info(SKey, Name) ->
    if_meta_field_info(SKey, Name).

meta_field_value(SKey, Name) ->
    if_meta_field_value(SKey, Name).

column_map(_SKey, Tables, Columns) ->
    imem_sql:column_map(Tables, Columns).

column_infos(_SKey, Table) ->
    imem_meta:column_infos(Table).

column_info_items(_SKey, Info, Item) ->
    imem_meta:column_info_items(Info, Item).

data_nodes(SKey) ->
    seco_authorized(SKey),
    imem_meta:data_nodes().

all_tables(SKey) ->
    all_selectable_tables(SKey, imem_meta:all_tables(), []).

all_selectable_tables(_SKey, [], Acc) -> lists:sort(Acc);
all_selectable_tables(SKey, [Table|Rest], Acc0) -> 
    Acc1 = case have_table_permission(SKey, Table, select) of
        false ->    Acc0;
        true ->     [Table|Acc0]
    end,
    all_selectable_tables(SKey, Rest, Acc1).

is_readable_table(SKey,Table) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:is_readable_table(Table);
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

tables_starting_with(SKey,Prefix) when is_atom(Prefix) ->
    tables_starting_with(SKey,atom_to_list(Prefix));
tables_starting_with(SKey,Prefix) when is_list(Prefix) ->
    atoms_starting_with(Prefix,all_tables(SKey)).

atoms_starting_with(Prefix,Atoms) ->
    atoms_starting_with(Prefix,Atoms,[]). 

atoms_starting_with(_,[],Acc) -> lists:sort(Acc);
atoms_starting_with(Prefix,[A|Atoms],Acc) ->
    case lists:prefix(Prefix,atom_to_list(A)) of
        true ->     atoms_starting_with(Prefix,Atoms,[A|Acc]);
        false ->    atoms_starting_with(Prefix,Atoms,Acc)
    end.

node_shard(_SKey) ->
    imem_meta:node_shard().

table_type(SKey, Table) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:table_type(Table);
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

physical_table_name(SKey,Name) ->
    PhysicalName = imem_meta:physical_table_name(Name),
    case have_table_permission(SKey, PhysicalName, select) of
        true ->     PhysicalName;
        false ->    ?SecurityException({"Select unauthorized", {PhysicalName,SKey}})
    end.

physical_table_names(SKey,Name) ->
    PhysicalNames = imem_meta:physical_table_names(Name),
    Pred = fun(PN) -> have_table_permission(SKey, PN, select) end,
    lists:filter(Pred,PhysicalNames).

% log_to_db(SKey,Level,Module,Function,Fields,Message) ->
%     case have_table_permission(SKey, ?LOG_TABLE, insert) of
%         true ->     imem_meta:log_to_db(Level,Module,Function,Fields,Message);
%         false ->    ?SecurityException({"Insert into ddLog@ unauthorized", SKey})
%     end.    

table_columns(SKey, Table) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:table_columns(Table);
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

table_record_name(SKey, Table) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:table_record_name(Table);
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

table_size(SKey, Table) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:table_size(Table);
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

check_table(_SKey, Table) ->
    imem_meta:check_table(Table).

check_table_meta(_SKey, Table, ColumnNames) ->
    imem_meta:check_table_meta(Table, ColumnNames).

check_table_columns(_SKey, Table, ColumnInfo) ->
    imem_meta:check_table_columns(Table, ColumnInfo).

subscribe(SKey, {table, Table, Level}) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:subscribe({table, Table, Level});
        false ->    ?SecurityException({"Subscribe unauthorized", {Table,SKey}})
    end;
subscribe(SKey, EventCategory) ->
    ?SecurityException({"Unsupported event category", {EventCategory,SKey}}).

unsubscribe(_SKey, EventCategory) ->
    imem_meta:unsubscribe(EventCategory).

update_tables(_SKey, UpdatePlan, Lock) ->
    %% ToDo: Plan must be checked against permissions
    imem_meta:update_tables(UpdatePlan, Lock).

transaction(_SKey, Function) ->
    imem_meta:transaction(Function).

transaction(_SKey, Function, Args) ->
    imem_meta:transaction(Function, Args).

transaction(_SKey, Function, Args, Retries) ->
    imem_meta:transaction(Function, Args, Retries).

return_atomic_list(_SKey, Result) ->
    imem_meta:return_atomic_list(Result). 

return_atomic_ok(_SKey, Result) -> 
    imem_meta:return_atomic_ok(Result).

return_atomic(_SKey, Result) -> 
    imem_meta:return_atomic(Result).

%% imem_if but security context added --- DATA DEFINITIONimem_meta--


create_table(SKey, Table, RecordInfo, Opts) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    Owner = case if_is_system_table(SKey, Table) of
        true ->     
            system;
        false ->    
            case imem_seco:have_permission(SKey,[manage_user_tables, create_table]) of
                true ->     AccountId;
                false ->    false
            end
    end,
    case Owner of
        false ->
            ?SecurityException({"Create table unauthorized", {Table,SKey}});
        Owner ->
            imem_meta:create_table(Table, RecordInfo, Opts, Owner)
    end.

create_check_table(SKey, Table, RecordInfo, Opts) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    Owner = case if_is_system_table(SKey, Table) of
        true ->     
            system;
        false ->    
            case imem_seco:have_permission(SKey,[manage_user_tables, create_table]) of
                true ->     AccountId;
                false ->    false
            end
    end,
    case Owner of
        false ->
            ?SecurityException({"Create table unauthorized", {Table,SKey}});
        Owner ->        
            Conv = fun(X) ->
                case X#ddColumn.name of
                    A when is_atom(A) -> X; 
                    B -> X#ddColumn{name=?binary_to_atom(B)} 
                end
            end,
            imem_meta:create_check_table(imem_meta:qualified_new_table_name(Table), lists:map(Conv,RecordInfo), Opts, Owner)
    end.

create_sys_conf(SKey, Path) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    Owner = case if_is_system_table(SKey, Path) of
        true ->     
            system;
        false ->    
            case imem_seco:have_permission(SKey,[manage_user_tables, create_table]) of
                true ->     AccountId;
                false ->    false
            end
    end,
    case Owner of
        false ->
            ?SecurityException({"Create sys conf schema unauthorized", {Path,SKey}});
        Owner ->        
            imem_meta:create_sys_conf(Path)
    end.

drop_table(SKey, Table) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    case if_is_system_table(SKey, Table) of
        true  -> drop_system_table(SKey, Table, AccountId);
        false -> drop_user_table(SKey, Table, AccountId)
    end.

purge_table(SKey, Table) ->
    purge_table(SKey, Table, []).

purge_table(SKey, Table, Opts) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    case if_is_system_table(SKey, Table) of
        true  -> purge_system_table(SKey, Table, Opts, AccountId);
        false -> purge_user_table(SKey, Table, Opts, AccountId)
    end.

drop_user_table(SKey, Table, _AccountId) ->
    case imem_seco:have_permission(SKey, manage_user_tables) of
        true ->             
            imem_meta:drop_table(Table);
        false ->
            case have_table_permission(SKey, Table, drop) of
                true ->
                    imem_meta:drop_table(Table);
                false ->
                    case have_table_ownership(SKey, Table) of
                        true ->     imem_meta:drop_table(Table);
                        false ->    ?SecurityException({"Drop table unauthorized", {Table,SKey}})
                    end
            end
    end.

purge_user_table(SKey, Table, Opts, _AccountId) ->
    case imem_seco:have_permission(SKey, manage_user_tables) of
        true ->             
            imem_meta:purge_table(Table, Opts);
        false ->
            case have_table_permission(SKey, Table, drop) of
                true ->
                    imem_meta:purge_table(Table, Opts);
                false ->
                    case have_table_ownership(SKey, Table) of
                        true ->     imem_meta:purge_table(Table, Opts);
                        false ->    ?SecurityException({"Purge table unauthorized", {Table,SKey}})
                    end
            end
    end.

drop_system_table(SKey, Table, _AccountId) ->
    case imem_seco:have_permission(SKey, manage_system_tables) of
        true ->
            imem_meta:drop_table(Table);
        false ->
            ?SecurityException({"Drop system table unauthorized", {Table,SKey}})
    end. 

purge_system_table(SKey, Table, Opts, _AccountId) ->
    case imem_seco:have_permission(SKey, manage_system_tables) of
        true ->
            imem_meta:purge_table(Table, Opts);
        false ->
            ?SecurityException({"Purge system table unauthorized", {Table,SKey}})
    end. 

%% imem_if but security context added --- DATA ACCESS CRUD -----

apply_validators(SKey, DefRec, Rec, Table) ->
    case have_table_permission(SKey, Table, insert) of
        true ->     imem_meta:apply_validators(DefRec, Rec, Table, imem_seco:account_id(SKey));
        false ->    ?SecurityException({"Trigger unauthorized", {Table,SKey}})
    end.

insert(SKey, Table, Row) ->
    case have_table_permission(SKey, Table, insert) of
        true ->     imem_meta:insert(Table, Row, imem_seco:account_id(SKey)) ;
        false ->    ?SecurityException({"Insert unauthorized", {Table,SKey}})
    end.

update(SKey, Table, Row) ->
    case have_table_permission(SKey, Table, update) of
        true ->     imem_meta:update(Table, Row, imem_seco:account_id(SKey)) ;
        false ->    ?SecurityException({"Update unauthorized", {Table,SKey}})
    end.

merge(SKey, Table, Row) ->
    case have_table_permission(SKey, Table, update) of
        true ->     imem_meta:merge(Table, Row, imem_seco:account_id(SKey)) ;
        false ->    ?SecurityException({"Merge (insert/update) unauthorized", {Table,SKey}})
    end.

remove(SKey, Table, Row) ->
    case have_table_permission(SKey, Table, delete) of
        true ->     imem_meta:remove(Table, Row, imem_seco:account_id(SKey)) ;
        false ->    ?SecurityException({"Remove unauthorized (delete permission needed)", {Table,SKey}})
    end.

read(SKey, Table) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:read(Table);
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

read(SKey, Table, Key) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:read(Table, Key);
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

read_hlk(SKey, Table, HListKey) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:read_hlk(Table, HListKey);
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

get_config_hlk(SKey, Table, Key, Context, Default) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:get_config_hlk(Table, Key, Context, Default);
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

put_config_hlk(SKey, Table, Key, Context, Value, Remark) ->
    case have_table_permission(SKey, Table, update) of
        true ->     imem_meta:put_config_hlk(Table, Key, Context, Value, Remark);
        false ->    ?SecurityException({"Insert/update unauthorized", {Table,SKey}})
    end.    

exec(SKey, Statement, BlockSize, Opts) ->
    imem_sql:exec(SKey, Statement, BlockSize, Opts, true).   

fetch_recs(SKey, Pid, Sock, Timeout) ->
    imem_statement:fetch_recs(SKey, Pid, Sock, Timeout, true).

fetch_recs_sort(SKey, Pid, Sock, Timeout) ->
    imem_statement:fetch_recs_sort(SKey, Pid, Sock, Timeout, true).

fetch_recs_async(SKey, Pid, Sock) ->
    imem_statement:fetch_recs_async(SKey, Pid, Sock, true).

fetch_recs_async(SKey, Opts, Pid, Sock) ->
    imem_statement:fetch_recs_async(SKey, Pid, Sock, Opts, true).

filter_and_sort(SKey, Pid, FilterSpec, SortSpec) ->
    imem_statement:filter_and_sort(SKey, Pid, FilterSpec, SortSpec, true).

filter_and_sort(SKey, Pid, FilterSpec, SortSpec, Cols) ->
    imem_statement:filter_and_sort(SKey, Pid, FilterSpec, SortSpec, Cols, true).

close(SKey, Pid) ->
    imem_statement:close(SKey, Pid).

fetch_start(SKey, Pid, dba_tables, MatchSpec, BlockSize, Opts) ->
    case imem_seco:have_permission(SKey, [manage_system_tables]) of
        true ->     
            imem_meta:fetch_start(Pid, dba_tables, MatchSpec, BlockSize, Opts);
        false ->
            ?SecurityException({"Select unauthorized", {dba_tables,SKey}})
    end;
fetch_start(SKey, Pid, user_tables, MatchSpec, BlockSize, Opts) ->
    seco_authorized(SKey),
    imem_meta:fetch_start(Pid, dba_tables, MatchSpec, BlockSize, Opts);  %% ToDo: {select_filter_user(SKey, RList, []), true};
fetch_start(SKey, Pid, all_tables, MatchSpec, BlockSize, Opts) ->
    seco_authorized(SKey),
    imem_meta:fetch_start(Pid, all_tables, MatchSpec, BlockSize, Opts);  %% {select_filter_all(SKey, RList, []), true};
fetch_start(SKey, Pid, Table, MatchSpec, BlockSize, Opts) ->
    seco_authorized(SKey),
    case have_table_permission(SKey, Table, select) of
        true -> imem_meta:fetch_start(Pid, Table, MatchSpec, BlockSize, Opts);
        _ ->    ?SecurityException({"Select unauthorized", {Table,SKey}})  
    end.

fetch_close(SKey, Pid) ->
    imem_statement:fetch_close(SKey, Pid, false).

update_prepare(SKey, Tables, ColMap, ChangeList) ->
    imem_statement:update_prepare(true, SKey, Tables, ColMap, ChangeList).

update_cursor_prepare(SKey, Pid, ChangeList) ->
    imem_statement:update_cursor_prepare(SKey,  Pid, true, ChangeList).

update_cursor_execute(SKey, Pid, Lock) ->
    imem_statement:update_cursor_execute(SKey,  Pid, true, Lock).

write(SKey, Table, Row) ->
    case have_table_permission(SKey, Table, insert) of
        true ->     imem_meta:write(Table, Row);
        false ->    ?SecurityException({"Insert/update unauthorized", {Table,SKey}})
    end.

dirty_write(SKey, Table, Row) ->
    case have_table_permission(SKey, Table, insert) of
        true ->     imem_meta:dirty_write(Table, Row);
        false ->    ?SecurityException({"Insert/update unauthorized", {Table,SKey}})
    end.

delete(SKey, Table, Key) ->
    case have_table_permission(SKey, Table, delete) of
        true ->     imem_meta:delete(Table, Key);
        false ->    ?SecurityException({"Delete unauthorized", {Table,SKey}})
    end.

delete_object(SKey, Table, Row) ->
    case have_table_permission(SKey, Table, delete) of
        true ->     imem_meta:delete_object(Table, Row);
        false ->    ?SecurityException({"Delete unauthorized", {Table,SKey}})
    end.

truncate_table(SKey, Table) ->
    case have_table_permission(SKey, Table, delete) of
        true ->     imem_meta:truncate_table(Table, imem_seco:account_id(SKey));
        false ->    ?SecurityException({"Truncate unauthorized", {Table,SKey}})
    end.

snapshot_table(SKey, Table) ->
    case imem_seco:have_permission(SKey, [manage_system_tables]) of
        true ->     
            imem_meta:snapshot_table(Table);
        false ->
            case have_table_permission(SKey, Table, export) of
                true ->     imem_meta:snapshot_table(Table);
                false ->    ?SecurityException({"Snapshot table unauthorized", {Table,SKey}})
            end
    end.

restore_table(SKey, Table) ->
    case imem_seco:have_permission(SKey, [manage_system_tables]) of
        true ->     
            imem_meta:restore_table(Table);
        false ->
            case have_table_permission(SKey, Table, import) of
                true ->     imem_meta:restore_table(Table);
                false ->    ?SecurityException({"Restore table unauthorized", {Table,SKey}})
            end
    end.

select_filter_all(_SKey, [], Acc) ->    Acc;
select_filter_all(SKey, [#ddTable{qname=TableQN}=H|Tail], Acc0) ->
    Acc1 = case have_table_permission(SKey, TableQN, select) of
        true ->     [H|Acc0];
        false ->    Acc0
    end,  
    select_filter_all(SKey, Tail, Acc1);
select_filter_all(SKey, [TableQN|Tail], Acc0) ->
    Acc1 = case have_table_permission(SKey, TableQN, select) of
        true ->     [TableQN|Acc0];
        false ->    Acc0
    end,  
    select_filter_all(SKey, Tail, Acc1).

select_filter_user(_SKey, [], Acc) ->   Acc;
select_filter_user(SKey, [#ddTable{qname=TableQN}=H|Tail], Acc0) ->
    Acc1 = case have_table_ownership(SKey, TableQN) of
        true ->     [H|Acc0];
        false ->    Acc0
    end,  
    select_filter_user(SKey, Tail, Acc1);
select_filter_user(SKey, [TableQN|Tail], Acc0) ->
    Acc1 = case have_table_ownership(SKey, TableQN) of
        true ->     [TableQN|Acc0];
        false ->    Acc0
    end,  
    select_filter_user(SKey, Tail, Acc1).

select(SKey, dba_tables, MatchSpec) ->
    case imem_seco:have_permission(SKey, [manage_system_tables]) of
        true ->     
            imem_meta:select(ddTable, MatchSpec);
        false ->
            ?SecurityException({"Select unauthorized", {dba_tables,SKey}})
    end;
select(SKey, user_tables, MatchSpec) ->
    seco_authorized(SKey),
    {RList,true} = imem_meta:select(ddTable, MatchSpec),
    {select_filter_user(SKey, RList, []), true};
select(SKey, all_tables, MatchSpec) ->
    seco_authorized(SKey),
    {RList,true} = imem_meta:select(ddTable, MatchSpec),
    {select_filter_all(SKey, RList, []), true};
select(SKey, Table, MatchSpec) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:select(Table, MatchSpec) ;
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

select(SKey, Table, MatchSpec, 0) ->
    select(SKey, Table, MatchSpec);
select(SKey, dba_tables, MatchSpec, Limit) ->
    case imem_seco:have_permission(SKey, [manage_system_tables]) of
        true ->     
            imem_meta:select(ddTable, MatchSpec, Limit);
        false ->
            ?SecurityException({"Select unauthorized", {dba_tables,SKey}})
    end;
select(SKey, user_tables, MatchSpec, Limit) ->
    seco_authorized(SKey),
    {RList,true} = imem_meta:select(ddTable, MatchSpec, Limit),
    {select_filter_user(SKey, RList, []), true};
select(SKey, all_tables, MatchSpec, Limit) ->
    seco_authorized(SKey),
    {RList,true} = imem_meta:select(ddTable, MatchSpec, Limit),
    {select_filter_all(SKey, RList, []), true};
select(SKey, Table, MatchSpec, Limit) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:select(Table, MatchSpec, Limit) ;
        false ->    ?SecurityException({"Select unauthorized", {Table,SKey}})
    end.

admin_exec(SKey, imem_account, Function, Params) ->
    admin_apply(SKey, imem_account, Function, [SKey|Params], [manage_accounts, manage_system]);
admin_exec(SKey, imem_role, Function, Params) ->
    admin_apply(SKey, imem_role, Function, [SKey|Params], [manage_accounts, manage_system]);
admin_exec(SKey, Module, Function, Params) ->
    admin_apply(SKey, Module, Function, Params, [manage_system,{module,Module,execute}]).

admin_apply(SKey, Module, Function, Params, Permissions) ->
    case imem_seco:have_permission(SKey, Permissions) of
        true ->
            apply(Module,Function,Params);
        false ->
            ?SecurityException({"Admin execute unauthorized", {Module,Function,Params,SKey}})
    end.

dal_exec(SKey, Module, Function, Params) ->
    case re:run(atom_to_list(Module),"_dal_",[]) of
        nomatch ->  ?SecurityException({"dal_exec attempted on wrong module", {Module,Function,Params,SKey}});
        _ ->        dal_apply(SKey, Module, Function, Params, [manage_system,{module,Module,execute}])
    end.

dal_apply(SKey, Module, Function, Params, Permissions) ->
    case imem_seco:have_permission(SKey, Permissions) of
        true ->
            apply(Module,Function,[imem_seco:account_id(SKey)|Params]);
        false ->
            ?SecurityException({"Dal execute unauthorized", {Module,Function,Params,SKey}})
    end.


%% ------- security extension for sql and tables (exported) ---------------------------------

have_permission(SKey, Permission) ->
    case get_permission_cache(SKey, Permission) of
        true ->         true;
        false ->        false;
        no_exists ->    
            Result = imem_seco:have_permission(SKey, Permission),
            set_permission_cache(SKey, Permission, Result),
            Result
    end.      

have_table_permission(SKey, Table, Operation) ->
    Permission = {table,Table,Operation},
    case get_permission_cache(SKey, Permission) of
        true ->         true;
        false ->        false;
        no_exists ->
            Result = case Operation of
                select ->
                    case lists:member(Table, ?DataTypes) of
                        false -> 
                            have_table_permission(SKey, Table, Operation, if_is_system_table(SKey, Table));
                        true -> true
                    end;
                _ ->    
                    have_table_permission(SKey, Table, Operation, if_is_system_table(SKey, Table))
            end,
            set_permission_cache(SKey, Permission, Result),
            Result
    end.      

have_module_permission(SKey, Module, Operation) ->
    Permission = {module,Module,Operation},
    case get_permission_cache(SKey, Permission) of
        true ->         true;
        false ->        false;
        no_exists ->
            Result = imem_seco:have_permission(SKey, {module,Module,Operation}),
            set_permission_cache(SKey, Permission, Result),
            Result
    end.      

%% ------- local private security extension for sql and tables (do not export!!) ------------

seco_authorized(SKey) -> 
    case imem_meta:read(ddSeCo@, SKey) of
        [#ddSeCo{pid=Pid, state=authorized} = SeCo] when Pid == self() -> 
            SeCo;
        [#ddSeCo{pid=Pid}] ->      
            ?SecurityViolation({"Not logged in", {SKey,Pid}});
        [] ->               
            ?SecurityException({"Not logged in", SKey})
    end.   

% have_table_ownership(SKey, {Schema,Table,_Alias}) ->
%     have_table_ownership(SKey, {Schema,Table});
have_table_ownership(SKey, {Schema,Table}) when is_atom(Schema), is_atom(Table) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    Owner = case imem_meta:read(ddTable, {Schema,Table}) of
        [#ddTable{owner=O}] ->  O;
        _ ->                    no_one
    end,
    (Owner =:= AccountId);
have_table_ownership(SKey, Table) when is_binary(Table) ->
    have_table_ownership(SKey, imem_meta:qualified_table_name(Table)).

have_table_permission(SKey, {Schema,Table,_Alias}, Operation, Type) ->
    have_table_permission(SKey, {Schema,Table}, Operation, Type);
have_table_permission(_SKey, {_,dual}, select, _) ->  true;
have_table_permission(_SKey, {_,dual}, _, _) ->  false;
have_table_permission(SKey, {_,Table}, Operation, true) ->
    imem_seco:have_permission(SKey, [manage_system_tables, {table,Table,Operation}]);
have_table_permission(SKey, {Schema,Table}, select, false) ->
    case imem_seco:have_permission(SKey, [manage_user_tables, {table,Table,select}]) of
        true ->     true;
        false ->    have_table_ownership(SKey,{Schema,Table}) 
    end;
have_table_permission(SKey, {Schema,Table}, Operation, false) ->
    case imem_meta:read(ddTable, {Schema,Table}) of
        [#ddTable{qname={Schema,Table}, readonly=true}] -> 
            imem_seco:have_permission(SKey, manage_user_tables);
        [#ddTable{qname={Schema,Table}, readonly=false}] ->
            case have_table_ownership(SKey,{Schema,Table}) of
                true ->     true;
                false ->    imem_seco:have_permission(SKey, [manage_user_tables, {table,Table,Operation}])
            end;
        _ ->    false
    end;
have_table_permission(SKey, Table, Operation, Type) ->
    have_table_permission(SKey, imem_meta:qualified_table_name(Table), Operation, Type).

set_permission_cache(SKey, Permission, true) ->
    imem_meta:write(ddPerm@,#ddPerm{pkey={SKey,Permission}, skey=SKey, pid=self(), value=true});
set_permission_cache(SKey, Permission, false) ->
    imem_meta:write(ddPerm@,#ddPerm{pkey={SKey,Permission}, skey=SKey, pid=self(), value=false}).

get_permission_cache(SKey, Permission) ->
    case imem_meta:read(ddPerm@,{SKey,Permission}) of
        [#ddPerm{value=Value}] -> Value;
        [] -> no_exists 
    end.


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) -> 
    SKey=?imem_test_admin_login(),
    catch imem_account:delete(SKey, <<"test_user_123">>),
    catch imem_role:delete(SKey, table_creator),
    catch imem_role:delete(SKey, test_role),
    catch imem_seco:logout(SKey),
    catch imem_meta:drop_table(user_table_123),
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun test/1
        ]}}.    

    
test(_) ->
    try
        ClEr = 'ClientError',
        SeEx = 'SecurityException',
        CoEx = 'ConcurrencyException',
        % SeVi = 'SecurityViolation',
        % SyEx = 'SystemException',          %% cannot easily test that

        ?Info("---TEST---~p:test_mnesia~n", [?MODULE]),

        ?Info("schema ~p~n", [imem_meta:schema()]),
        ?Info("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        ?Info("~p:test_admin_login~n", [?MODULE]),

        SeCoAdmin=?imem_test_admin_login(),
        ?Info("success ~p~n", [admin_login]),
        ?assert(1 =< table_size(SeCoAdmin, ddSeCo@)),
        ?Info("success ~p~n", [seco_table_size]), 
        AllTablesAdmin = all_tables(SeCoAdmin),
        ?assertEqual(true, lists:member(ddAccount,AllTablesAdmin)),
        ?assertEqual(true, lists:member(imem_meta:physical_table_name(ddPerm@),AllTablesAdmin)),
        ?assertEqual(true, lists:member(imem_meta:physical_table_name(ddQuota@),AllTablesAdmin)),
        ?assertEqual(true, lists:member(ddRole,AllTablesAdmin)),
        ?assertEqual(true, lists:member(imem_meta:physical_table_name(ddSeCo@),AllTablesAdmin)),
        ?assertEqual(true, lists:member(ddTable,AllTablesAdmin)),
        ?Info("success ~p~n", [all_tables_admin]), 

        ?Info("~p:test_admin_exec~n", [?MODULE]),

        ?Info("accounts ~p~n", [table_size(SeCoAdmin, ddAccount)]),
        ?assertEqual(ok, admin_exec(SeCoAdmin, imem_account, create, [user, <<"test_user_123">>, <<"Test user 123">>, "PasswordMd5"])),
        ?Info("success ~p~n", [account_create_user]),
        UserId = admin_exec(SeCoAdmin, imem_account, get_id_by_name, [<<"test_user_123">>]),
        ?assert(is_integer(UserId)),
        ?Info("success (~p) ~p~n", [UserId, create_test_admin_permissions]), 
        ?assertEqual(ok, admin_exec(SeCoAdmin, imem_role, grant_permission, [<<"test_user_123">>, create_table])),
        ?Info("success ~p~n", [create_test_admin_permissions]), 
     
        ?Info("~p:test_user_login~n", [?MODULE]),

        SeCoUser0=authenticate(none, userSessionId, <<"test_user_123">>, {pwdmd5,"PasswordMd5"}),
        ?assertEqual(true, is_integer(SeCoUser0)),
        ?Info("success ~p~n", [user_authentication]), 
        ?assertException(throw,{SeEx,{?PasswordChangeNeeded, _}}, login(SeCoUser0)),
        ?Info("success ~p~n", [password_expired]), 
        SeCoUser=authenticate(none, someSessionId, <<"test_user_123">>, {pwdmd5,"PasswordMd5"}), 
        ?assertEqual(true, is_integer(SeCoUser)),
        ?Info("success ~p~n", [user_authentication]), 
        ?assertEqual(SeCoUser, change_credentials(SeCoUser, {pwdmd5,"PasswordMd5"}, {pwdmd5,"NewPasswordMd5"})),
        ?Info("success ~p~n", [password_changed]), 
        Type123a = {[a,b,c],[term,term,term],{user_table_123,undefined,undefined,undefined}},
        Type123b = {[a,b,a],[term,term,term],{user_table_123,undefined,undefined,undefined}},
        Type123c = {[a,b,x],[term,term,term],{user_table_123,undefined,undefined,undefined}},
        ?assertEqual(ok, create_table(SeCoUser, user_table_123, Type123a, [])),
        ?Info("success ~p~n", [create_user_table]),
        ?assertException(throw, {ClEr,{"Table already exists",user_table_123}}, create_table(SeCoUser, user_table_123, Type123b, [])),
        ?assertException(throw, {ClEr,{"Table already exists",user_table_123}}, create_table(SeCoUser, user_table_123, Type123c, [])),
        ?Info("success ~p~n", [create_user_table]),
        ?assertEqual(0, table_size(SeCoUser, user_table_123)),
        ?Info("success ~p~n", [own_table_size]),

        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, select)),
        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, insert)),
        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, delete)),
        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, update)),
        ?Info("success ~p~n", [permissions_own_table]), 

        ?assertEqual(ok, admin_exec(SeCoAdmin, imem_role, revoke_role, [<<"test_user_123">>, create_table])),
        ?Info("success ~p~n", [role_revoke_role]),
        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, select)),
        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, insert)),
        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, delete)),
        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, update)),
        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, drop)),
        ?assertEqual(true, have_table_permission(SeCoUser, user_table_123, alter)),
        ?Info("success ~p~n", [permissions_own_table]),

        ?assertException(throw, {SeEx,{"Select unauthorized", {dba_tables,SeCoUser}}}, select(SeCoUser, dba_tables, ?MatchAllKeys)),
        ?Info("success ~p~n", [dba_tables_unauthorized]),
        {DbaTables, true} = select(SeCoAdmin, dba_tables, ?MatchAllKeys),
        ?assertEqual(true, lists:member({imem,ddAccount}, DbaTables)),
        ?assertEqual(true, lists:member({imem,imem_meta:physical_table_name(ddPerm@)}, DbaTables)),
        ?assertEqual(true, lists:member({imem,imem_meta:physical_table_name(ddQuota@)}, DbaTables)),
        ?assertEqual(true, lists:member({imem,ddRole}, DbaTables)),
        ?assertEqual(true, lists:member({imem,imem_meta:physical_table_name(ddSeCo@)}, DbaTables)),
        ?assertEqual(true, lists:member({imem,ddTable}, DbaTables)),
        ?assertEqual(true, lists:member({imem,user_table_123}, DbaTables)),
        ?Info("success ~p~n", [dba_tables]),

        {AdminTables, true} = select(SeCoAdmin, user_tables, ?MatchAllKeys),
        ?assertEqual(false, lists:member({imem,ddAccount}, AdminTables)),
        ?assertEqual(false, lists:member({imem,imem_meta:physical_table_name(ddPerm@)}, AdminTables)),
        ?assertEqual(false, lists:member({imem,imem_meta:physical_table_name(ddQuota@)}, AdminTables)),
        ?assertEqual(false, lists:member({imem,ddRole}, AdminTables)),
        ?assertEqual(false, lists:member({imem,imem_meta:physical_table_name(ddSeCo@)}, AdminTables)),
        ?assertEqual(false, lists:member({imem,ddTable}, AdminTables)),
        ?assertEqual(false, lists:member({imem,user_table_123}, AdminTables)),
        ?Info("success ~p~n", [admin_tables]),

        {UserTables, true} = select(SeCoUser, user_tables, ?MatchAllKeys),
        ?assertEqual(false, lists:member({imem,ddAccount}, UserTables)),
        ?assertEqual(false, lists:member({imem,ddPerm@}, UserTables)),
        ?assertEqual(false, lists:member({imem,ddQuota@}, UserTables)),
        ?assertEqual(false, lists:member({imem,ddRole}, UserTables)),
        ?assertEqual(false, lists:member({imem,ddSeCo@}, UserTables)),
        ?assertEqual(false, lists:member({imem,ddTable}, UserTables)),
        ?assertEqual(true, lists:member({imem,user_table_123}, UserTables)),
        ?Info("success ~p~n", [user_tables]),

        % LogCount1 = table_size(SeCoAdmin,?LOG_TABLE),
        % ?assertEqual(ok, log_to_db(SeCoAdmin,info,?MODULE,test,[{test_1,value2},{test_3,value4}],"Message")),        
        % LogCount2 = table_size(SeCoAdmin,?LOG_TABLE),
        % ?assertEqual(LogCount1+1,LogCount2),
        % ?assertException(throw, {SeEx,{"Insert into ddLog@ unauthorized",SeCoUser}}, log_to_db(SeCoUser,info,?MODULE,test,[{test_5,value6},{test_7,value8}],"Message")),        
        % LogCount3 = table_size(SeCoAdmin,?LOG_TABLE),
        % ?assertEqual(LogCount2+1,LogCount3),


        LogTable = physical_table_name(SeCoAdmin,?LOG_TABLE),
        ?Info("success ~p ~p~n", [physical_table_name,LogTable]),
        ?assertException(throw, {SeEx,{"Select unauthorized",{_,SeCoUser}}}, physical_table_name(SeCoUser,?LOG_TABLE)),    
        LogTables = physical_table_names(SeCoAdmin,?LOG_TABLE),
        ?assert(lists:member(LogTable,LogTables)),        
        ?assertEqual(LogTables,physical_table_names(SeCoAdmin,atom_to_list(?LOG_TABLE))),
        ?assertEqual([],physical_table_names(SeCoUser,atom_to_list(?LOG_TABLE))),

        ?assertEqual(LogTables,tables_starting_with(SeCoAdmin,"ddLog_")),
        ?assertEqual([user_table_123],tables_starting_with(SeCoUser,"user_table_")),
        ?assertEqual([user_table_123],tables_starting_with(SeCoAdmin,user_table_)),
        ?assertEqual([ddTable],tables_starting_with(SeCoAdmin,ddTable)),
        ?assertEqual([],tables_starting_with(SeCoUser,ddTable)),
        ?assertEqual([],tables_starting_with(SeCoAdmin,"akkahadöl_")),

        ?assertEqual({user_table_123,"A","B","C"}, insert(SeCoUser, user_table_123, {user_table_123,"A","B","C"})),
        ?assertEqual(1, table_size(SeCoUser, user_table_123)),
        ?Info("success ~p~n", [insert_own_table]),
        ?assertEqual({user_table_123,"AA","BB","CC"}, merge(SeCoUser, user_table_123, {user_table_123,"AA","BB","CC"})),
        ?assertEqual(2, table_size(SeCoUser, user_table_123)),
        ?assertEqual({user_table_123,"AA","B0","CC"}, update(SeCoUser, user_table_123, {user_table_123,"AA","B0","CC"})),
        ?assertEqual(2, table_size(SeCoUser, user_table_123)),
        ?assertException(throw, {CoEx,{"Update failed, key does not exist",_ }}, update(SeCoUser, user_table_123, {user_table_123,"A0","B0","CC"})),    

        ?assertEqual(2, table_size(SeCoUser, user_table_123)),
        ?Info("success ~p~n", [insert_own_table]),
        ?assertEqual(ok, drop_table(SeCoUser, user_table_123)),
        ?Info("success ~p~n", [drop_own_table]),
        ?assertException(throw, {ClEr,{"Table does not exist",user_table_123}}, table_size(SeCoUser, user_table_123)),    
        ?Info("success ~p~n", [drop_own_table_no_exists]),

        ?assertEqual(ok, admin_exec(SeCoAdmin, imem_account, delete, [<<"test_user_123">>])),
        ?Info("success ~p~n", [account_create_user])

    catch
        Class:Reason ->  ?Info("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok.

-endif.
