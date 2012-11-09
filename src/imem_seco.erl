-module(imem_seco).

-include_lib("eunit/include/eunit.hrl").

-include("dd_seco.hrl").

-export([ schema/1
        , schema/2
        , data_nodes/1
        , all_tables/1
        , table_columns/2
        , table_size/2
        , system_table/2
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
    dd_seco:authenticate(SessionId, Name, Credentials).

login(SKey) ->
    dd_seco:login(SKey).

change_credentials(SKey, OldCred, NewCred) ->
    dd_seco:change_credentials(SKey, OldCred, NewCred).

logout(SKey) ->
    dd_seco:logout(SKey).

%% one to one from imme_if -------------- HELPER FUNCTIONS ------

schema(_SKey) ->
    imem_meta:schema().

schema(_SKey, Node) ->
    imem_meta:schema(Node).

add_attribute(_SKey, A, Opts) -> 
    imem_meta:add_attribute(A, Opts).

update_opts(_SKey, T, Opts) ->
    imem_meta:update_opts(T, Opts).


%% imem_if but security context added --- META INFORMATION ------

system_table(_SKey, Table) ->
    dd_seco:system_table(_SKey, Table).    

data_nodes(_SKey) ->
    imem_meta:data_nodes().

all_tables(_SKey) ->
    imem_meta:all_tables().

table_columns(_SKey, Table) ->
    imem_meta:table_columns(Table).

table_size(_SKey, Table) ->
    imem_meta:table_size(Table).

%% imem_if but security context added --- DATA DEFINITION -------

create_table(SKey, Table, RecordInfo, Opts) ->
    case seco(SKey) of
        #ddSeCo{accountId=AccountId, state=authorized} -> 
            Owner = case dd_seco:system_table(SKey, Table) of
                true ->     
                    system;
                false ->    
                    case dd_seco:have_permission(SKey,[manage_user_tables, create_table]) of
                        true ->     AccountId;
                        false ->    false
                    end
            end,
            case Owner of
                false ->
                    ?SecurityException({"Create table unauthorized", SKey});
                Owner ->        
                    imem_meta:create_table(Table, RecordInfo, Opts, Owner)
            end;
        _ ->    
            ?SecurityException({"Create table not logged in", SKey})
    end.

drop_table(SKey, Table) ->
    SeCo = seco(SKey),
    case dd_seco:system_table(SKey, Table) of
        true  -> drop_system_table(SeCo, Table);
        false -> drop_user_table(SeCo, Table)
    end.

drop_user_table(#ddSeCo{skey=SKey,accountId=AccountId}, Table) ->
    case have_table_permission(SKey, Table, drop) of
        true ->
            imem_meta:drop_table(Table);
        false ->
            case imem_meta:read(ddTable, SKey) of
                [] ->
                    ?ClientError({"Drop table not found", SKey});
                [#ddTable{owner=AccountId}] -> 
                    imem_meta:drop_table(Table);
                _ ->     
                    ?SecurityException({"Drop table unauthorized", SKey})
            end
    end. 

drop_system_table(#ddSeCo{skey=SKey}=SeCo, Table) ->
    case dd_seco:have_permission(SeCo, manage_system_tables) of
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
            Result = have_table_permission(SKey, Table, Operation, system_table(SKey, Table)),
            set_permission_cache(SKey, {Table,Operation}, Result),
            Result
    end.      

%% ------- local private security extension for sql and tables (do not export!!) ------------

seco(SKey) -> 
    case imem_meta:read(ddSeCo, SKey) of
        [] ->               ?SecurityException({"Security context does not exist", SKey});
        [#ddSeCo{pid=Pid, state=authorized} = SeCo] when Pid == self() -> SeCo;
        [#ddSeCo{}] ->      ?SecurityViolation({"Security context does not match", SKey})
    end.   

table_metadata(_SKey,Table) ->
    case imem_meta:read(ddTable, Table) of
        [#ddTable{} = TM] ->    TM;
        _ ->                    ?ClientError({"Table does not exist", Table})
    end.    

have_table_ownership(SKey, Table) ->
    #ddTable{owner=Owner} = table_metadata(SKey, Table),
    #ddSeCo{accountId=AccountId} = seco(SKey),
    (Owner =:= AccountId).

have_table_permission(SKey, Table, Operation, true) ->
    dd_seco:have_permission(SKey, [manage_system_tables, {Table,Operation}]);

have_table_permission(SKey, Table, select, false) ->
    case dd_seco:have_permission(SKey, [manage_user_tables, {Table,select}]) of
        true ->     true;
        false ->    have_table_ownership(SKey,Table) 
    end;
have_table_permission(SKey, Table, Operation, false) ->
    case table_metadata(SKey,Table) of
        #ddTable{id=Table, readonly=true} -> 
            dd_seco:have_permission(SKey, manage_user_tables);
        #ddTable{id=Table, readonly=false} ->
            case have_table_ownership(SKey,Table) of
                true ->     true;
                false ->    dd_seco:have_permission(SKey, [manage_user_tables, {Table,Operation}])
            end
    end.

set_permission_cache(SKey, Permission, true) ->
    imem_meta:write(ddPerm,#ddPerm{pkey={SKey,Permission}, skey=SKey, pid=self(), value=true});
set_permission_cache(SKey, Permission, false) ->
    imem_meta:write(ddPerm,#ddPerm{pkey={SKey,Permission}, skey=SKey, pid=self(), value=false}).

get_permission_cache(SKey, Permission) ->
    case imem_meta:read(ddPerm,{SKey,Permission}) of
        [#ddPerm{value=Value}] -> Value;
        [] -> no_exists 
    end.


