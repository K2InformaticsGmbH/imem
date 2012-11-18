-module(imem_sec).

-define(SECO_TABLES,[ddTable,ddAccount,ddRole,ddSeCo,ddPerm,ddQuota]).
-define(SECO_FIELDS,[user]).

-include_lib("eunit/include/eunit.hrl").

-include("imem_seco.hrl").

-export([ schema/1
        , schema/2
        , data_nodes/1
        , all_tables/1
        , table_columns/2
        , table_size/2
        , check_table/2
        , check_table_record/3
        , check_table_columns/3
        , system_table/2
        , meta_field/2
        , meta_field_info/2
        , meta_field_value/2
        , column_info_items/3        
        , column_map/3
        , column_map_items/3        
        , subscribe/2
        , unsubscribe/2
        ]).

-export([ update_opts/3
        , add_attribute/3
        ]).

-export([ authenticate/4
        , login/1
        , change_credentials/3
        , logout/1
        ]).

-export([ create_table/4
		, drop_table/2
        , read/2
        , read/3
        , read_block/4                 
        , select/2
        , select/3
        , select/4
        , insert/3    
        , write/3
        , delete/3
        , truncate/2
		]).

-export([ have_table_permission/3   %% includes table ownership and readonly
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

%% one to one from imme_if -------------- HELPER FUNCTIONS ------

if_read(_SKey, Table, Key) -> 
    imem_meta:read(Table, Key).

if_system_table(_SKey, Table) ->
    case lists:member(Table,?SECO_TABLES) of
        true ->     true;
        false ->    imem_meta:system_table(Table)
    end.

if_meta_field(_SKey, Name) ->
    case lists:member(Name,?SECO_FIELDS) of
        true ->     true;
        false ->    imem_meta:meta_field(Name)
    end.

if_meta_field_info(_SKey, Name) ->
    imem_meta:meta_field_info(Name).

if_meta_field_value(SKey, user) ->
    #ddSeCo{accountId=AccountId} = SeCo = seco_authorized(SKey),
    [#ddAccount{name=Name}] = if_read(SeCo, ddAccount, AccountId),
    Name;
if_meta_field_value(_SKey, Name) ->
    imem_meta:meta_field_value(Name).

add_attribute(_SKey, A, Opts) -> 
    imem_meta:add_attribute(A, Opts).

update_opts(_SKey, T, Opts) ->
    imem_meta:update_opts(T, Opts).


%% imem_if but security context added --- META INFORMATION ------

schema(SKey) ->
    seco_authorized(SKey),
    imem_meta:schema().

schema(SKey, Node) ->
    seco_authorized(SKey),
    imem_meta:schema(Node).

system_table(SKey, Table) ->
    seco_authorized(SKey),    
    if_system_table(SKey, Table).

meta_field(SKey, Name) ->
    if_meta_field(SKey, Name).

meta_field_info(SKey, Name) ->
    if_meta_field_info(SKey, Name).

meta_field_value(SKey, Name) ->
    if_meta_field_value(SKey, Name).

column_map(_SKey, Tables, Columns) ->
    imem_meta:columns_map(Tables, Columns).

column_info_items(_SKey, Info, Item) ->
    imem_meta:column_map_items(Info, Item).

column_map_items(_SKey, Map, Item) ->
    imem_meta:column_map_items(Map, Item).

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

table_columns(SKey, Table) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:table_columns(Table);
        false ->    ?SecurityException({"Select unauthorized", SKey})
    end.

table_size(SKey, Table) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:table_size(Table);
        false ->    ?SecurityException({"Select unauthorized", SKey})
    end.

check_table(_SeKey, Table) ->
    imem_meta:check_table(Table).

check_table_record(_SeKey, Table, ColumnNames) ->
    imem_meta:check_table_record(Table, ColumnNames).

check_table_columns(_SeKey, Table, ColumnInfo) ->
    imem_meta:check_table_columns(Table, ColumnInfo).

subscribe(SKey, {table, Table, Level}) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:subscribe({table, Table, Level});
        false ->    ?SecurityException({"Subscribe unauthorized", SKey})
    end;
subscribe(_SKey, EventCategory) ->
    ?SecurityException({"Unsupported event category", EventCategory}).

unsubscribe(_Skey, EventCategory) ->
    imem_meta:unsubscribe(EventCategory).



%% imem_if but security context added --- DATA DEFINITIONimem_meta--

create_table(SKey, Table, RecordInfo, Opts) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    Owner = case if_system_table(SKey, Table) of
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
            ?SecurityException({"Create table unauthorized", SKey});
        Owner ->        
            imem_meta:create_table(Table, RecordInfo, Opts, Owner)
    end.

drop_table(SKey, Table) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    case if_system_table(SKey, Table) of
        true  -> drop_system_table(SKey, Table, AccountId);
        false -> drop_user_table(SKey, Table, AccountId)
    end.

drop_user_table(SKey, Table, AccountId) ->
    case have_table_permission(SKey, Table, drop) of
        true ->
            imem_meta:drop_table(Table);
        false ->
            case imem_meta:read(ddTable, Table) of
                [] ->
                    ?ClientError({"Drop table not found", SKey});
                [#ddTable{owner=AccountId}] -> 
                    imem_meta:drop_table(Table);
                _ ->     
                    ?SecurityException({"Drop table unauthorized", SKey})
            end
    end. 

drop_system_table(SKey, Table, _AccountId) ->
    case imem_seco:have_permission(SKey, manage_system_tables) of
        true ->
            imem_meta:drop_table(Table);
        false ->
            ?SecurityException({"Drop system table unauthorized", SKey})
    end. 

%% imem_if but security context added --- DATA ACCESS CRUD -----

insert(SKey, Table, Row) ->
    case have_table_permission(SKey, Table, insert) of
        true ->     imem_meta:insert(Table, Row) ;
        false ->    ?SecurityException({"Insert unauthorized", SKey})
    end.

read(SKey, Table) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:read(Table);
        false ->    ?SecurityException({"Select unauthorized", SKey})
    end.

read(SKey, Table, Key) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:read(Table, Key);
        false ->    ?SecurityException({"Select unauthorized", SKey})
    end.

read_block(SKey, Table, Key, BlockSize) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:read_block(Table, Key, BlockSize);
        false ->    ?SecurityException({"Select unauthorized", SKey})
    end.

write(SKey, Table, Row) ->
    case have_table_permission(SKey, Table, insert) of
        true ->     imem_meta:write(Table, Row);
        false ->    ?SecurityException({"Insert/update unauthorized", SKey})
    end.

delete(SKey, Table, Key) ->
    case have_table_permission(SKey, Table, delete) of
        true ->     imem_meta:delete(Table, Key);
        false ->    ?SecurityException({"Delete unauthorized", SKey})
    end.

truncate(SKey, Table) ->
    case have_table_permission(SKey, Table, delete) of
        true ->     imem_meta:truncate(Table);
        false ->    ?SecurityException({"Truncate unauthorized", SKey})
    end.

select(_SKey, Continuation) ->
        imem_meta:select(Continuation).

select(_SKey, all_tables, _MatchSpec) ->
    ?UnimplementedException({"Select metadata unimplemented"});
select(SKey, Table, MatchSpec) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:select(Table, MatchSpec) ;
        false ->    ?SecurityException({"Select unauthorized", SKey})
    end.

select(_SKey, all_tables, _MatchSpec, _Limit) ->
    ?UnimplementedException({"Select metadata unimplemented"});
select(SKey, Table, MatchSpec, Limit) ->
    case have_table_permission(SKey, Table, select) of
        true ->     imem_meta:select(Table, MatchSpec, Limit) ;
        false ->    ?SecurityException({"Select unauthorized", SKey})
    end.



%% ------- security extension for sql and tables (exported) ---------------------------------

have_table_permission(SKey, Table, Operation) ->
    case get_permission_cache(SKey, {Table,Operation}) of
        true ->         true;
        false ->        false;
        no_exists ->    
            Result = have_table_permission(SKey, Table, Operation, if_system_table(SKey, Table)),
            set_permission_cache(SKey, {Table,Operation}, Result),
            Result
    end.      

%% ------- local private security extension for sql and tables (do not export!!) ------------

seco_authorized(SKey) -> 
    case imem_meta:read(ddSeCo, SKey) of
        [#ddSeCo{pid=Pid, state=authorized} = SeCo] when Pid == self() -> 
            SeCo;
        [#ddSeCo{}] ->      
            ?SecurityViolation({"Not logged in", SKey});
        [] ->               
            ?SecurityException({"Not logged in", SKey})
    end.   

have_table_ownership(SKey, {Schema,Table}) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    Owner = case imem_meta:read(ddTable, {Schema,Table}) of
        [#ddTable{owner=O}] ->  O;
        _ ->                    no_one
    end,
    (Owner =:= AccountId);
have_table_ownership(SKey, Table) ->
    have_table_ownership(SKey, {imem_meta:schema(),Table}).

have_table_permission(SKey, Table, Operation, true) ->
    imem_seco:have_permission(SKey, [manage_system_tables, {Table,Operation}]);

have_table_permission(SKey, {Schema,Table}, select, false) ->
    case imem_seco:have_permission(SKey, [manage_user_tables, {Table,select}]) of
        true ->     true;
        false ->    have_table_ownership(SKey,{Schema,Table}) 
    end;
have_table_permission(SKey, Table, select, false) ->
    have_table_permission(SKey, {imem_meta:schema(),Table}, select, false) ;
have_table_permission(SKey, {Schema,Table}, Operation, false) ->
    case imem_meta:read(ddTable, {Schema,Table}) of
        [#ddTable{qname={Schema,Table}, readonly=true}] -> 
            imem_seco:have_permission(SKey, manage_user_tables);
        [#ddTable{qname={Schema,Table}, readonly=false}] ->
            case have_table_ownership(SKey,{Schema,Table}) of
                true ->     true;
                false ->    imem_seco:have_permission(SKey, [manage_user_tables, {Table,Operation}])
            end;
        _ ->    false
    end;
have_table_permission(SKey, Table, Operation, false) ->
    have_table_permission(SKey, {imem_meta:schema(),Table}, Operation, false).

set_permission_cache(SKey, Permission, true) ->
    imem_meta:write(ddPerm,#ddPerm{pkey={SKey,Permission}, skey=SKey, pid=self(), value=true});
set_permission_cache(SKey, Permission, false) ->
    imem_meta:write(ddPerm,#ddPerm{pkey={SKey,Permission}, skey=SKey, pid=self(), value=false}).

get_permission_cache(SKey, Permission) ->
    case imem_meta:read(ddPerm,{SKey,Permission}) of
        [#ddPerm{value=Value}] -> Value;
        [] -> no_exists 
    end.


