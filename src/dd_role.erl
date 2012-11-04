-module(dd_role).

-include("dd.hrl").

-export([ create/2
        , get/2
        , update/3
        , delete/2
        , exists/2
        , grant_role/3
        , revoke_role/3
        , grant_permission/3
        , revoke_permission/3
        , grant_quota/4
        , revoke_quota/3
        ]).


%% --Interface functions  (calling imem_if for now) ----------------------------------

if_write(_SeCo, #ddRole{}=Role) -> 
    imem_if:write(ddRole, Role).

if_read(_SeCo, RoleId) -> 
    imem_if:read(ddRole, RoleId).

if_get(SeCo, RoleId) -> 
    case if_read(SeCo, RoleId) of
        [] -> {error, {"Role does not exist", RoleId}};
        [Role] -> Role
    end.

% if_select(_SeCo, MatchSpec) ->
%     imem_if:select_rows(ddRole, MatchSpec). 

if_delete(_SeCo, RoleId) ->
    imem_if:delete(ddRole, RoleId).


%% --Implementation ------------------------------------------------------------------

create(SeCo, #ddRole{id=RoleId}=Role) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, RoleId) of
                        [] ->   %% ToDo: Check if roles contained in Role are all defined $$$$$$$$$$$$$$
                                if_write(SeCo, Role);
                        [_] -> {error, {"Role already exists",RoleId}}
                    end;
        false ->    {error, {"Create role unauthorized",SeCo}}
    end;        
create(SeCo, RoleId) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, RoleId) of
                        [] ->   if_write(SeCo, #ddRole{id=RoleId});
                        [_] -> {error, {"Role already exists",RoleId}}
                    end;
        false ->    {error, {"Create role unauthorized",SeCo}}
    end.

get(SeCo, RoleId) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     if_get(SeCo, RoleId);
        false ->    {error, {"Get role unauthorized",SeCo}}
    end.            

update(SeCo, #ddRole{id=RoleId}=Role, RoleNew) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, RoleId) of
                        [] -> {error, {"Role does not exist", RoleId}};
                        [Role] -> if_write(SeCo, RoleNew);
                        [_] -> {error, {"Role is modified by someone else", RoleId}}
                    end;
        false ->    {error, {"Update role unauthorized",SeCo}}
    end.

delete(SeCo, #ddRole{id=RoleId}=Role) ->
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, RoleId) of
                        [] -> {error, {"Role does not exist", RoleId}};
                        [Role] -> delete(SeCo, RoleId);
                        [_] -> {error, {"Role is modified by someone else", RoleId}}
                    end;
        false ->    {error, {"Delete role unauthorized",SeCo}}
    end;
delete(SeCo, RoleId) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     if_delete(SeCo, RoleId);
        false ->    {error, {"Delete role unauthorized",SeCo}}
    end.

exists(SeCo, #ddRole{id=RoleId}=Role) ->    %% exists unchanged
    case if_read(SeCo, RoleId) of
        [] -> false;
        [Role] -> true;
        [_] -> false
    end;
exists(SeCo, RoleId) ->                     %% exists, maybe in changed form
    case if_read(SeCo, RoleId) of
        [] -> false;
        [_] -> true
    end.

grant_role(SeCo, #ddRole{id=ToRoleId}=ToRole, RoleId) -> 
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ToRoleId) of
                        [] -> {error, {"Role does not exist", ToRoleId}};
                        [ToRole] -> grant_role(SeCo, ToRoleId, RoleId);
                        [_] -> {error, {"Role is modified by someone else", ToRoleId}}
                    end;
        false ->    {error, {"Grant role unauthorized",SeCo}}
    end;
grant_role(SeCo, ToRoleId, RoleId) ->
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case exists(SeCo, RoleId) of
                        false ->  
                            {error, {"Role does not exist", RoleId}};
                        true ->   
                            case get(SeCo, ToRoleId) of
                                #ddRole{roles=Roles}=ToRole ->   
                                    NewRoles = case lists:member(RoleId, Roles) of
                                        true ->     Roles;
                                        false ->    lists:append(Roles, [RoleId])       %% append because newer = seldom used
                                    end,
                                    update(SeCo,ToRole,ToRole#ddRole{roles=NewRoles});   
                                Error ->    Error    
                            end
                    end;
        false ->    {error, {"Grant role unauthorized",SeCo}}
    end.            

revoke_role(SeCo, #ddRole{id=FromRoleId}=FromRole, RoleId) -> 
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, FromRoleId) of
                        [] -> {error, {"Role does not exist", FromRoleId}};
                        [FromRole] -> revoke_role(SeCo, FromRoleId, RoleId);
                        [_] -> {error, {"Role is modified by someone else", FromRoleId}}
                    end;
        false ->    {error, {"Revoke role unauthorized",SeCo}}
    end;            
revoke_role(SeCo, FromRoleId, RoleId) -> 
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case get(SeCo, FromRoleId) of
                        #ddRole{roles=Roles}=FromRole ->   
                            update(SeCo,FromRole,FromRole#ddRole{roles=lists:delete(RoleId, Roles)});   
                        Error ->    Error    
                    end;
        false ->    {error, {"Revoke role unauthorized",SeCo}}
    end.            

grant_permission(SeCo, #ddRole{id=ToRoleId}=ToRole, PermissionId) -> 
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ToRoleId) of
                        [] -> {error, {"Role does not exist", ToRoleId}};
                        [ToRole] -> grant_permission(SeCo, ToRoleId, PermissionId);
                        [_] -> {error, {"Role is modified by someone else", ToRoleId}}
                    end;
        false ->    {error, {"Grant permission unauthorized",SeCo}}
    end;
grant_permission(SeCo, ToRoleId, PermissionId) ->
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case get(SeCo, ToRoleId) of
                        #ddRole{permissions=Permissions}=ToRole ->   
                            NewPermissions = case lists:member(PermissionId, Permissions) of
                                true ->     Permissions;
                                false ->    lists:append(Permissions, [PermissionId])   %% append because newer = seldom used
                            end,
                            update(SeCo,ToRole,ToRole#ddRole{permissions=NewPermissions});   
                        Error ->    Error    
                    end;
        false ->    {error, {"Grant permission unauthorized",SeCo}}
    end.

revoke_permission(SeCo, #ddRole{id=FromRoleId}=FromRole, PermissionId) -> 
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, FromRoleId) of
                        [] -> {error, {"Role does not exist", FromRoleId}};
                        [FromRole] -> revoke_permission(SeCo, FromRoleId, PermissionId);
                        [_] -> {error, {"Role is modified by someone else", FromRoleId}}
                    end;
        false ->    {error, {"Revoke permission unauthorized",SeCo}}
    end;        
revoke_permission(SeCo, FromRoleId, PermissionId) -> 
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case get(SeCo, FromRoleId) of
                        #ddRole{permissions=Permissions}=FromRole ->   
                            update(SeCo,FromRole,FromRole#ddRole{permissions=lists:delete(PermissionId, Permissions)});   
                        Error ->    Error    
                    end;
        false ->    {error, {"Revoke permission unauthorized",SeCo}}
    end.

grant_quota(SeCo, #ddRole{id=ToRoleId}=ToRole, QuotaId, Value) -> 
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ToRoleId) of
                        [] -> {error, {"Role does not exist", ToRoleId}};
                        [ToRole] -> grant_quota(SeCo, ToRoleId, QuotaId, Value);
                        [_] -> {error, {"Role is modified by someone else", ToRoleId}}
                    end;
        false ->    {error, {"Grant quota unauthorized",SeCo}}
    end;
grant_quota(SeCo, ToRoleId, QuotaId, Value) ->
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case get(SeCo, ToRoleId) of
                        #ddRole{quotas=Quotas}=ToRole ->   
                            OtherQuotas = lists:key_delete(QuotaId, 1, Quotas),
                            update(SeCo,ToRole,ToRole#ddRole{quotas=lists:append(OtherQuotas, [{QuotaId,Value}])});   
                        Error ->    Error    
                    end;
        false ->    {error, {"Grant quota unauthorized",SeCo}}
    end.

revoke_quota(SeCo, #ddRole{id=FromRoleId}=FromRole, QuotaId) -> 
   case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, FromRoleId) of
                        [] -> {error, {"Role does not exist", FromRoleId}};
                        [FromRole] -> revoke_quota(SeCo, FromRoleId, QuotaId);
                        [_] -> {error, {"Role is modified by someone else", FromRoleId}}
                    end;
        false ->    {error, {"Revoke quota unauthorized",SeCo}}
    end;        
revoke_quota(SeCo, FromRoleId, QuotaId) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case get(SeCo, FromRoleId) of
                        #ddRole{quotas=Quotas}=FromRole ->   
                            update(SeCo,FromRole,FromRole#ddRole{quotas=lists:key_delete(QuotaId, 1, Quotas)});   
                        Error ->    Error    
                    end;
        false ->    {error, {"Revoke quota unauthorized",SeCo}}
    end.


