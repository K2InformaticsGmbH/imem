-module(imem_seco).

-include_lib("eunit/include/eunit.hrl").

-include("dd_meta.hrl").
-include("dd_seco.hrl").

-export([ schema/0
        , schema/1
        , add_attribute/2
        , update_opts/2
        , find_imem_nodes/2
        , all_tables/1
        , table_columns/2
        , table_size/2
        ]).

-export([ authenticate/3
        , login/1
        , change_credentials/3
        , logout/1
        ]).

-export([ create_table/4
		, drop_table/2
        , read_all/2
        , select/3
        , read/3                 
        , insert/3    
        , write/3
        , delete/3
		]).


if_create_table(SeCo, Table, RecordInfo, Opts, Owner) ->
    case imem_if:create_table(Table, RecordInfo, Opts) of
        {atomic,ok} -> 
            if_write(SeCo, ddTable, #ddTable{id=Table, recinfo=RecordInfo, opts=Opts, owner=Owner}),
            {atomic,ok};
        Error -> 
            Error
    end.

if_drop_table(_SeCoUser, Table) -> 
    imem_if:drop_table(Table).

if_read(_SeCoUser, Table, Key) -> 
    imem_if:read(Table, Key).

if_write(_SeCoUser, Table, Record) -> 
    imem_if:write(Table, Record).


%% one to one from dd_account ------------ AA FUNCTIONS _--------

authenticate(SessionId, Name, Credentials) ->
    dd_seco:authenticate(SessionId, Name, Credentials).

login(SeCo) ->
    dd_seco:login(SeCo).

change_credentials(SeCo, OldCred, NewCred) ->
    dd_seco:change_credentials(SeCo, OldCred, NewCred).

logout(SeCo) ->
    dd_seco:logout(SeCo).

%% one to one from imme_if -------------- HELPER FUNCTIONS ------

schema() ->
    imem_if:schema().

schema(Node) ->
    imem_if:schema(Node).

add_attribute(A, Opts) -> 
    imem_if:add_attribute(A, Opts).

update_opts(T, Opts) ->
    imem_if:update_opts(T, Opts).


%% imem_if but security context added --- META INFORMATION ------

find_imem_nodes(_SeCo, Schema) ->
    imem_if:find_imem_nodes(Schema).

all_tables(_SeCo) ->
    imem_if:all_tables().

table_columns(_SeCo, TableName) ->
    imem_if:table_columns(TableName).

table_size(_SeCo, TableName) ->
    imem_if:table_size(TableName).

%% imem_if but security context added --- DATA DEFINITION -------

create_table(SeKey, Table, RecordInfo, Opts) ->
    case SeCo=dd_seco:seco(SeKey) of
        #ddSeCo{accountId=AccountId, state=authorized} -> 
            Owner = case dd_seco:system_table(Table) of
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
                    case if_create_table(SeKey, Table, RecordInfo, Opts, Owner) of 
                        {atomic,ok} ->  {atomic,ok};
                        Error2 ->        ?SystemException(Error2)  
                    end
            end;
        _ ->    
            ?SecurityException({"Create table not logged in", SeKey})
    end.

drop_table(SeKey, Table) ->
    SeCo = dd_seco:seco(SeKey),
    case dd_seco:system_table(Table) of
        true  -> drop_system_table(SeCo, Table);
        false -> drop_user_table(SeCo, Table)
    end.

drop_user_table(#ddSeCo{key=SeKey,accountId=AccountId}=SeCo, Table) ->
    case dd_seco:have_permission(SeCo, manage_user_tables) of
        true ->
            case dd_seco:drop_table(SeKey, Table) of
                {atomic,ok} ->  {atomic,ok};
                Error ->        ?SystemException(Error)
            end;
        false ->
            case if_read(SeKey, ddTable, Table)  of
                [] ->
                    ?ClientError({"Drop table not found", SeKey});
                [#ddTable{owner=AccountId}] -> 
                    case if_drop_table(SeKey, Table) of
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
            case if_drop_table(SeKey, Table) of
                {atomic,ok} ->  {atomic,ok};
                Error ->        ?SystemException(Error)
            end;
        false ->
            ?SecurityException({"Drop system table unauthorized", SeKey})
    end. 

%% imem_if but security context added --- DATA ACCESS CRUD -----

insert(SeCo, TableName, Row) ->
    case dd_seco:have_permission(SeCo, {TableName,insert}) of
        true ->     imem_if:insert(TableName, Row) ;
        false ->    {error, {"Insert unauthorized", SeCo}};
        Error ->    Error
    end.

read(SeCo, TableName, Key) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_if:read(TableName, Key) ;
        false ->    {error, {"Select unauthorized", SeCo}};
        Error ->    Error
    end.

write(SeCo, TableName, Row) ->
    case dd_seco:have_permission(SeCo, {TableName,insert}) of
        true ->     imem_if:write(TableName, Row) ;
        false ->    {error, {"Insert/update unauthorized", SeCo}};
        Error ->    Error
    end.

delete(SeCo, TableName, Key) ->
    case dd_seco:have_permission(SeCo, {TableName,delete}) of
        true ->     imem_if:delete(TableName, Key) ;
        false ->    {error, {"Delete unauthorized", SeCo}};
        Error ->    Error
    end.

read_all(SeCo, TableName) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_if:read_all(TableName) ;
        false ->    {error, {"Select unauthorized", SeCo}};
        Error ->    Error
    end.

select(SeCo, TableName, MatchSpec) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_if:select(TableName, MatchSpec) ;
        false ->    {error, {"Select unauthorized", SeCo}};
        Error ->    Error
    end.

