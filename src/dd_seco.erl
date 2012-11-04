-module(dd_seco).

-include("dd.hrl").

-export([ system_table/1
        , create_system_tables/1
        , drop_system_tables/1
        , create_table/3
        , create_table/4
        , drop_table/2
        ]).

-export([ create/3
        , get/1
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

if_create_table(SeCo, Id, Opts, Owner) ->
    case imem_if:create_table(Id, Opts) of
        {ok,atomic} -> 
            if_write(SeCo, Id, #ddTable{id=Id, recinfo=[], opts=Opts, owner=Owner}),
            {ok,atomic};
        Error -> 
            Error
    end.

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

% if_read(_SeCoUser, Table, Key) -> 
%    imem_if:read(Table, Key).

if_get(_SeCoUser, Key) -> 
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

system_table(Id) ->
    lists:member(Id,?SYSTEM_TABLES).

create_table(SeCo, Id, Opts) ->
    Result = if_get(#ddSeCo{key=SeCo,phid=self()}, SeCo),
    case Result of
        #ddSeCo{phid=Pid, accountId=AccountId, state=authorized} when Pid == self() -> 
            Owner = case system_table(Id) of
                true ->     
                    system;
                false ->    
                    case have_permission(SeCo, create_table) of
                        true ->     AccountId;
                        false ->    false
                    end
            end,
            case Owner of
                false ->
                    {error, {"Create table unauthorized",Id}};
                Owner ->        
                    if_create_table(SeCo, Id, Opts, Owner) 
            end;
        #ddSeCo{} -> 
            {error, {"Invalid security context", SeCo}};
        Error ->    
            Error 
    end.

create_table(SeCo, Id, RecordInfo, Opts) ->
    Result = if_get(#ddSeCo{key=SeCo,phid=self()}, SeCo),
    case Result of
        #ddSeCo{phid=Pid, accountId=AccountId, state=authorized} when Pid == self() -> 
            Owner = case system_table(Id) of
                true ->     
                    system;
                false ->    
                    case have_permission(SeCo, create_table) of
                        true ->     AccountId;
                        false ->    false
                    end
            end,
            case Owner of
                false ->
                    {error, {"Create table unauthorized",Id}};
                Owner ->        
                    if_create_table(SeCo, Id, RecordInfo, Opts, Owner)
            end;
        #ddSeCo{} -> 
            {error, {"Invalid security context", SeCo}};
        Error ->    
            Error 
    end.

create_system_tables(SeCo) ->
    if_create_table(SeCo, ddTable, record_info(fields, ddTable),[], system),        %% may fail if exists
    if_create_table(SeCo, ddAccount, record_info(fields, ddAccount),[], system),    %% may fail if exists
    if_create_table(SeCo, ddRole, record_info(fields, ddRole),[], system),          %% may fail if exists
    if_create_table(SeCo, ddSeCo, record_info(fields, ddSeCo),[], system).      %% ToDo: [local]

drop_system_tables(SeCo) ->
    case have_permission(SeCo, manage_system_tables) of
        true ->
            if_drop_table(SeCo, ddSeCo),     
            if_drop_table(SeCo, ddRole),         
            if_drop_table(SeCo, ddAccount),   
            if_drop_table(SeCo, ddTable);
        false ->
            {error, {"Drop system tables unauthorized", SeCo}}
    end.

drop_table(SeCo, Id) -> 
    case have_permission(SeCo, manage_accounts) of
        true ->     if_drop_table(SeCo, Id);
        false ->    {error, {"Drop table unauthorized", SeCo}}
    end.

create(SessionId, Name, _Credentials) -> 
    SeCo = #ddSeCo{phid=self(), sessionId=SessionId, name=Name, authTime=erlang:now()},
    Key = erlang:phash2(SeCo), 
    SeCo#ddSeCo{key=Key, state=unauthorized}.

register(#ddSeCo{key=Key}=SeCo, AccountId) -> 
    case if_write(SeCo, SeCo#ddSeCo{accountId=AccountId}) of
        ok ->       Key;    %% hash is returned back to caller
        Error ->    Error    
    end.

get(Key) -> 
    Result = if_get(#ddSeCo{key=Key,phid=self()}, Key),
    case Result of
        #ddSeCo{phid=Pid} when Pid == self() -> Result;
        #ddSeCo{} -> {error, {"Security context does not match", Key}};
        Error ->    Error 
    end.

update(#ddSeCo{key=Key}=SeCo, SeCoNew) -> 
    case if_read(SeCo, Key) of
        [] -> {error, {"Security context does not exist", Key}};
        [#ddSeCo{phid=Pid}] when Pid == self() -> if_write(SeCo, SeCoNew);
        [#ddSeCo{}] -> {error, {"Invalid security context", Key}};
        [_] -> {error, {"Security context is modified by someone else", Key}}
    end.

delete(SeCo) -> 
    Result = if_get(#ddSeCo{key=SeCo,phid=self()}, SeCo),
    case Result of
        #ddSeCo{phid=Pid} when Pid == self() -> 
            if_delete(#ddSeCo{key=SeCo,phid=self()}, SeCo);
        #ddSeCo{} -> 
            {error, {"Delete security context unauthorized", SeCo}};
        Error ->    
            Error 
    end.

has_role(SeCo, RootRoleId, RoleId) ->
    case have_permission(SeCo, manage_accounts) of
        true ->     if_has_role(SeCo, RootRoleId, RoleId); 
        false ->    {error, {"Has role unauthorized",SeCo}}
    end.

has_permission(SeCo, RootRoleId, Permission) ->
    case have_permission(SeCo, manage_accounts) of
        true ->     if_has_permission(SeCo, RootRoleId, Permission); 
        false ->    {error, {"Has permission unauthorized",SeCo}}
    end.

have_role(SeCo, RoleId) ->
    Result = if_get(#ddSeCo{key=SeCo,phid=self()}, SeCo),
    case Result of
        #ddSeCo{phid=Pid, accountId=AccountId, state=authorized} when Pid == self() -> 
            if_has_role(SeCo, AccountId, RoleId);
        #ddSeCo{} -> 
            {error, {"Invalid security context", SeCo}};
        Error ->    
            Error 
    end.

have_permission(SeCo, Permission) ->
    Result = if_get(#ddSeCo{key=SeCo,phid=self()}, SeCo),
    case Result of
        #ddSeCo{phid=Pid, accountId=AccountId, state=authorized} when Pid == self() -> 
            if_has_permission(SeCo, AccountId, Permission);
        #ddSeCo{} -> 
            {error, {"Invalid security context", SeCo}};
        Error ->    
            Error 
    end.
