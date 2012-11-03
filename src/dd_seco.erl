-module(dd_seco).

-include("dd.hrl").

-export([ create_tables/1
        , drop_tables/1
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

if_create_tables(_SeCoUser) ->
    imem_if:create_table(ddSeCo, record_info(fields, ddSeCo),[]).  %% ToDo: local_

if_drop_tables(_SeCoUser) -> 
    imem_if:drop_table(ddSeCo).

if_write(_SeCoUser, #ddSeCo{}=SeCo) -> 
    imem_if:write(ddSeCo, SeCo).

if_read(_SeCoUser, Key) -> 
    imem_if:read(ddSeCo, Key).

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

if_has_permission(SeCo, RootRoleId, PermissionId) ->
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

if_has_child_permission(_SeCo, [], _PermissionId) -> false;
if_has_child_permission(SeCo, [RootRoleId|OtherRoles], PermissionId) ->
    case if_has_permission(SeCo, RootRoleId, PermissionId) of
        {error, Error} ->               {error, Error};
        true ->                         true;
        false ->                        if_has_child_permission(SeCo, OtherRoles, PermissionId)
    end.


%% --Implementation ------------------------------------------------------------------

create_tables(SeCoUser) ->
    if_create_tables(SeCoUser).

drop_tables(SeCoUser) -> 
    case have_permission(SeCoUser, manage_accounts) of
        true ->     if_drop_tables(SeCoUser);
        false ->    {error, {"Drop security context tables unauthorized", SeCoUser}}
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

has_permission(SeCo, RootRoleId, PermissionId) ->
    case have_permission(SeCo, manage_accounts) of
        true ->     if_has_permission(SeCo, RootRoleId, PermissionId); 
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

have_permission(SeCo, PermissionId) ->
    Result = if_get(#ddSeCo{key=SeCo,phid=self()}, SeCo),
    case Result of
        #ddSeCo{phid=Pid, accountId=AccountId, state=authorized} when Pid == self() -> 
            if_has_permission(SeCo, AccountId, PermissionId);
        #ddSeCo{} -> 
            {error, {"Invalid security context", SeCo}};
        Error ->    
            Error 
    end.
