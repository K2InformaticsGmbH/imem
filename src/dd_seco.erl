-module(dd_seco).

-include("dd.hrl").

-export([ system_table/1
        , create_cluster_tables/1
        , create_local_tables/1
        , drop_system_tables/1
        , create_table/4
        , drop_table/2
        ]).

-export([ create/3
        , seco/1
        , register/2
        , update/2
        , delete/1
        ]).

-export([ has_role/3
        , has_permission/3
%%        , my_quota/2
        ]).

-export([ have_role/2
        , have_permission/2
        ]).

%% --Interface functions  (calling imem_if for now) ----------------------------------

if_create_table(SeCo, Id, RecordInfo, Opts, Owner) ->
    case imem_if:create_table(Id, RecordInfo, Opts) of
        {ok,atomic} -> 
            if_write(SeCo, Id, #ddTable{id=Id, recinfo=RecordInfo, opts=Opts, owner=Owner}),
            {ok,atomic};
        Error -> 
            Error
    end.

if_drop_table(_SeCoUser, Id) -> 
    imem_if:drop_table(Id).

if_write(_SeCoUser, #ddSeCo{}=SeCo) -> 
    imem_if:write(ddSeCo, SeCo).

if_write(_SeCoUser, Table, Record) -> 
    imem_if:write(Table, Record).

if_read(_SeCoUser, Key) -> 
    imem_if:read(ddSeCo, Key).

if_read(_SeCoUser, Table, Key) -> 
    imem_if:read(Table, Key).

if_seco(_SeCoUser, Key) -> 
    case if_read(_SeCoUser, Key) of
        [] ->       {error, {"Security context does not exist", Key}};
        [SeCo] ->   SeCo
    end.   

if_delete(_SeCoUser, Key) ->
    imem_if:delete(ddSeCo, Key).


if_read_role(_SeCoUser, RoleId) -> 
    imem_if:read(ddRole, RoleId).

if_get_role(SeCoUser, RoleId) -> 
    case if_read_role(SeCoUser, RoleId) of
        [] -> {error, {"Role does not exist", RoleId}};
        [Role] -> Role
    end.

if_has_role(_SeCo, _RootRoleId, _RootRoleId) ->
    true;
if_has_role(SeCo, RootRoleId, RoleId) ->
    case if_get_role(SeCo, RootRoleId) of
        {error, Error} ->               {error, Error};
        #ddRole{roles=[]} ->            false;
        #ddRole{roles=ChildRoles} ->    if_has_child_role(SeCo,  ChildRoles, RoleId)
    end.

if_has_child_role(_SeCo, [], _RoleId) -> false;
if_has_child_role(SeCo, [RootRoleId|OtherRoles], RoleId) ->
    case if_has_role(SeCo, RootRoleId, RoleId) of
        {error, Error} ->               {error, Error};
        true ->                         true;
        false ->                        if_has_child_role(SeCo, OtherRoles, RoleId)
    end.

list_member([], _Permissions) ->
    false;
list_member([PermissionId|Rest], Permissions) ->
    case lists:member(PermissionId, Permissions) of
        true -> true;
        false -> list_member(Rest, Permissions)
    end.

if_has_permission(_SeCo, _RootRoleId, []) ->
    false;
if_has_permission(SeCo, RootRoleId, PermissionList) when is_list(PermissionList)->
    %% search for first match in list of permissions
    case if_get_role(SeCo, RootRoleId) of
        {error, Error} ->                       
            {error, Error};
        #ddRole{permissions=[],roles=[]} ->     
            false;
        #ddRole{permissions=Permissions, roles=[]} -> 
            list_member(PermissionList, Permissions);
        #ddRole{permissions=Permissions, roles=ChildRoles} ->
            case list_member(PermissionList, Permissions) of
                true ->     true;
                false ->    if_has_child_permission(SeCo,  ChildRoles, PermissionList)
            end            
    end;
if_has_permission(SeCo, RootRoleId, PermissionId) ->
    %% search for single permission
    case if_get_role(SeCo, RootRoleId) of
        {error, Error} ->                       
            {error, Error};
        #ddRole{permissions=[],roles=[]} ->     
            false;
        #ddRole{permissions=Permissions, roles=[]} -> 
            lists:member(PermissionId, Permissions);
        #ddRole{permissions=Permissions, roles=ChildRoles} ->
            case lists:member(PermissionId, Permissions) of
                true ->     true;
                false ->    if_has_child_permission(SeCo,  ChildRoles, PermissionId)
            end            
    end.

if_has_child_permission(_SeCo, [], _Permission) -> false;
if_has_child_permission(SeCo, [RootRoleId|OtherRoles], Permission) ->
    case if_has_permission(SeCo, RootRoleId, Permission) of
        {error, Error} ->               {error, Error};
        true ->                         true;
        false ->                        if_has_child_permission(SeCo, OtherRoles, Permission)
    end.


%% --Implementation ------------------------------------------------------------------

system_table(Table) ->
    lists:member(Table,?SYSTEM_TABLES).

create_table(SeKey, Table, RecordInfo, Opts) ->
    case SeCo=seco(SeKey) of
        #ddSeCo{accountId=AccountId, state=authorized} -> 
            Owner = case system_table(Table) of
                true ->     
                    system;
                false ->    
                    case if_has_permission(SeCo, AccountId, create_table) of
                        true ->     AccountId;
                        false ->    false
                    end
            end,
            case Owner of
                false ->
                    {error, {"Create table unauthorized", Table}};
                Owner ->        
                    if_create_table(SeKey, Table, RecordInfo, Opts, Owner)
            end;
        Error ->    
            Error 
    end.

create_cluster_tables(SeKey) ->
    if_create_table(SeKey, ddTable, record_info(fields, ddTable),[], system),        %% may fail if exists
    if_create_table(SeKey, ddAccount, record_info(fields, ddAccount),[], system),    %% may fail if exists
    if_create_table(SeKey, ddRole, record_info(fields, ddRole),[], system).          %% may fail if exists

create_local_tables(SeKey) ->
    if_create_table(SeKey, ddSeCo, record_info(fields, ddSeCo),[], system).      %% ToDo: [local]

drop_system_tables(SeKey) ->
    SeCo=seco(SeKey),
    case have_permission(SeCo, manage_system_tables) of
        true ->
            if_drop_table(SeKey, ddSeCo),     
            if_drop_table(SeKey, ddRole),         
            if_drop_table(SeKey, ddAccount),   
            if_drop_table(SeKey, ddTable);
        false ->
            {error, {"Drop system tables unauthorized", SeKey}};
        Error ->
            Error
    end.

drop_table(SeKey, Table) ->
    SeCo = seco(SeKey),
    case system_table(Table) of
        true  -> drop_system_table(SeCo, Table);
        false -> drop_user_table(SeCo, Table)
    end.

drop_user_table(#ddSeCo{key=SeKey,accountId=AccountId}=SeCo, Table) ->
    case have_permission(SeCo, manage_user_tables) of
        true ->
            if_drop_table(SeKey, Table);
        false ->
            case if_read(SeKey, ddTable, Table)  of
                [] ->
                    {error, {"Drop table not found", SeKey}};
                [#ddTable{owner=AccountId}] -> 
                    if_drop_table(SeKey, Table);
                _ ->     
                    {error, {"Drop table unauthorized", SeKey}}
            end;
        Error ->
            Error
    end. 

drop_system_table(#ddSeCo{key=SeKey}=SeCo, Table) ->
    case have_permission(SeCo, manage_system_tables) of
        true ->
            if_drop_table(SeKey, Table);
        false ->
            {error, {"Drop system table unauthorized", SeKey}};
        Error ->
            Error
    end. 

create(SessionId, Name, _Credentials) -> 
    SeCo = #ddSeCo{pid=self(), sessionId=SessionId, name=Name, authTime=erlang:now()},
    SeKey = erlang:phash2(SeCo), 
    SeCo#ddSeCo{key=SeKey, state=unauthorized}.

register(#ddSeCo{key=SeKey}=SeCo, AccountId) -> 
    case if_write(SeKey, SeCo#ddSeCo{accountId=AccountId}) of
        ok ->       %% ToDo: register SeKey for pid with monitor
                    SeKey;    %% hash is returned back to caller
        Error ->    Error    
    end.

seco(SeKey) -> 
    SeCo = if_seco(#ddSeCo{key=SeKey,pid=self()}, SeKey),
    case SeCo of
        #ddSeCo{pid=Pid} when Pid == self() -> SeCo;
        #ddSeCo{} -> {error, {"Security context does not match", SeKey}};
        Error ->    Error 
    end.

update(#ddSeCo{key=SeKey,pid=Pid}=SeCo, SeCoNew) when Pid == self() -> 
    case if_read(SeKey, SeKey) of
        [] -> {error, {"Security context does not exist", SeKey}};
        [SeCo] -> if_write(SeKey, SeCoNew);
        [_] -> {error, {"Security context is modified by someone else", SeKey}}
    end;
update(#ddSeCo{key=SeKey}, _) -> 
    {error, {"Invalid security context", SeKey}}.

delete(#ddSeCo{key=SeKey,pid=Pid}) when Pid == self() -> 
    if_delete(SeKey, SeKey);
delete(#ddSeCo{key=SeKey}) -> 
    {error, {"Delete security context unauthorized", SeKey}};
delete(SeKey) ->
    case SeCo=seco(SeKey) of
        #ddSeCo{} -> delete(SeCo);
        Error -> Error
    end.

has_role(#ddSeCo{key=SeKey}=SeCo, RootRoleId, RoleId) ->
    case have_permission(SeCo, manage_accounts) of
        true ->     if_has_role(SeKey, RootRoleId, RoleId); 
        _ ->        {error, {"Has role unauthorized",SeKey}}
    end;
has_role(SeKey, RootRoleId, RoleId) ->
    case have_permission(SeKey, manage_accounts) of
        true ->     if_has_role(SeKey, RootRoleId, RoleId); 
        _ ->        {error, {"Has role unauthorized",SeKey}}
    end.

has_permission(#ddSeCo{key=SeKey}=SeCo, RootRoleId, Permission) ->
    case have_permission(SeCo, manage_accounts) of
        true ->     if_has_permission(SeKey, RootRoleId, Permission); 
        _ ->        {error, {"Has permission unauthorized",SeKey}}
    end;
has_permission(SeKey, RootRoleId, Permission) ->
    case have_permission(SeKey, manage_accounts) of
        true ->     if_has_permission(SeKey, RootRoleId, Permission); 
        _ ->        {error, {"Has permission unauthorized",SeKey}}
    end.

have_role(#ddSeCo{key=SeKey}=SeCo, RoleId) ->
    case SeCo of
        #ddSeCo{pid=Pid, accountId=AccountId, state=authorized} when Pid == self() -> 
            if_has_role(SeKey, AccountId, RoleId);
        #ddSeCo{} -> 
            {error, {"Invalid security context", SeKey}};
        Error ->    
            Error 
    end;
have_role(SeKey, RoleId) ->
    case SeCo=seco(SeKey) of
        #ddSeCo{} -> have_role(SeCo, RoleId);
        Error -> Error
    end.

have_permission(#ddSeCo{key=SeKey}=SeCo, Permission) ->
    case SeCo of
        #ddSeCo{pid=Pid, accountId=AccountId, state=authorized} when Pid == self() -> 
            if_has_permission(SeKey, AccountId, Permission);
        #ddSeCo{} -> 
            {error, {"Invalid security context", SeKey}};
        Error ->    
            Error 
    end;
have_permission(SeKey, Permission) ->
    case SeCo=seco(SeKey) of
        #ddSeCo{} -> have_permission(SeCo, Permission);
        Error -> Error
    end.