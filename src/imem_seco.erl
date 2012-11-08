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


%% one to one from dd_account ------------ AA FUNCTIONS _--------

authenticate(_SeCo, SessionId, Name, Credentials) ->
    dd_seco:authenticate(SessionId, Name, Credentials).

login(SeCo) ->
    dd_seco:login(SeCo).

change_credentials(SeCo, OldCred, NewCred) ->
    dd_seco:change_credentials(SeCo, OldCred, NewCred).

logout(SeCo) ->
    dd_seco:logout(SeCo).

%% one to one from imme_if -------------- HELPER FUNCTIONS ------

schema(_SeCo) ->
    imem_meta:schema().

schema(_SeCo, Node) ->
    imem_meta:schema(Node).

add_attribute(_SeCo, A, Opts) -> 
    imem_meta:add_attribute(A, Opts).

update_opts(_SeCo, T, Opts) ->
    imem_meta:update_opts(T, Opts).


%% imem_if but security context added --- META INFORMATION ------

system_table(_SeCo, Table) ->
    dd_seco:system_table(_SeCo, Table).    

data_nodes(_SeCo) ->
    imem_meta:data_nodes().

all_tables(_SeCo) ->
    imem_meta:all_tables().

table_columns(_SeCo, TableName) ->
    imem_meta:table_columns(TableName).

table_size(_SeCo, TableName) ->
    imem_meta:table_size(TableName).

%% imem_if but security context added --- DATA DEFINITION -------

create_table(SeKey, Table, RecordInfo, Opts) ->
    case SeCo=dd_seco:seco(SeKey) of
        #ddSeCo{accountId=AccountId, state=authorized} -> 
            Owner = case dd_seco:system_table(SeKey, Table) of
                true ->     
                    system;
                false ->    
                    case dd_seco:has_permission(SeCo, AccountId, create_table) of
                        true ->     AccountId;
                        false ->    false;
                        Error1 ->    ?SystemException(Error1)
                    end
            end,
            case Owner of
                false ->
                    ?SecurityException({"Create table unauthorized", Table});
                Owner ->        
                    case imem_meta:create_table(Table, RecordInfo, Opts, Owner) of 
                        {atomic,ok} ->  {atomic,ok};
                        Error2 ->        ?SystemException(Error2)  
                    end
            end;
        _ ->    
            ?SecurityException({"Create table not logged in", SeKey})
    end.

drop_table(SeKey, Table) ->
    SeCo = dd_seco:seco(SeKey),
    case dd_seco:system_table(SeKey, Table) of
        true  -> drop_system_table(SeCo, Table);
        false -> drop_user_table(SeCo, Table)
    end.

drop_user_table(#ddSeCo{key=SeKey,accountId=AccountId}=SeCo, Table) ->
    case dd_seco:have_permission(SeCo, manage_user_tables) of
        true ->
            case imem_meta:drop_table(Table) of
                {atomic,ok} ->  {atomic,ok};
                Error ->        ?SystemException(Error)
            end;
        false ->
            case imem_meta:read(ddTable, SeKey) of
                [] ->
                    ?ClientError({"Drop table not found", SeKey});
                [#ddTable{owner=AccountId}] -> 
                    case imem_meta:drop_table(Table) of
                        {atomic,ok} ->  {atomic,ok};
                        Error ->        ?SystemException(Error)
                    end;
                _ ->     
                    ?SecurityException({"Drop table unauthorized", SeKey})
            end
    end. 

drop_system_table(#ddSeCo{key=SeKey}=SeCo, Table) ->
    case dd_seco:have_permission(SeCo, manage_system_tables) of
        true ->
            case imem_meta:drop_table(Table) of
                {atomic,ok} ->  {atomic,ok};
                Error ->        ?SystemException(Error)
            end;
        false ->
            ?SecurityException({"Drop system table unauthorized", SeKey})
    end. 

%% imem_if but security context added --- DATA ACCESS CRUD -----

insert(SeCo, TableName, Row) ->
    case dd_seco:have_permission(SeCo, {TableName,insert}) of
        true ->     imem_meta:insert(TableName, Row) ;
        false ->    ?SecurityException({"Insert unauthorized", SeCo})
    end.

read(SeCo, TableName) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_meta:read(TableName);
        false ->    ?SecurityException({"Select unauthorized", SeCo})
    end.

read(SeCo, TableName, Key) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_meta:read(TableName, Key);
        false ->    ?SecurityException({"Select unauthorized", SeCo})
    end.

read_block(SeCo, TableName, Key, BlockSize) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_meta:read_block(TableName, Key, BlockSize);
        false ->    ?SecurityException({"Select unauthorized", SeCo})
    end.

write(SeCo, TableName, Row) ->
    case dd_seco:have_permission(SeCo, {TableName,insert}) of
        true ->     imem_meta:write(TableName, Row);
        false ->    ?SecurityException({"Insert/update unauthorized", SeCo})
    end.

delete(SeCo, TableName, Key) ->
    case dd_seco:have_permission(SeCo, {TableName,delete}) of
        true ->     imem_meta:delete(TableName, Key);
        false ->    ?SecurityException({"Delete unauthorized", SeCo})
    end.

truncate(SeCo, TableName) ->
    case dd_seco:have_permission(SeCo, {TableName,delete}) of
        true ->     imem_meta:truncate(TableName);
        false ->    ?SecurityException({"Truncate unauthorized", SeCo})
    end.

select(_SeCo, Continuation) ->
        imem_meta:select(Continuation).

select(_SeCo, all_tables, _MatchSpec) ->
    ?UnimplementedException({"Select metadata unimplemented"});
select(SeCo, TableName, MatchSpec) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_meta:select(TableName, MatchSpec) ;
        false ->    ?SecurityException({"Select unauthorized", SeCo})
    end.

select(_SeCo, all_tables, _MatchSpec, _Limit) ->
    ?UnimplementedException({"Select metadata unimplemented"});
select(SeCo, TableName, MatchSpec, Limit) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_meta:select(TableName, MatchSpec, Limit) ;
        false ->    ?SecurityException({"Select unauthorized", SeCo})
    end.
