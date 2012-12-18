-module(imem_role).

-include("imem_seco.hrl").

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

if_write(_SeCo, Table, #ddRole{}=Role) -> 
    imem_meta:write(Table, Role).

if_read(_SeCo, Table, RoleId) -> 
    imem_meta:read(Table, RoleId).

if_delete(_SeCo, Table, RoleId) ->
    imem_meta:delete(Table, RoleId).

if_select(_SeKey, Table, MatchSpec) ->
    imem_meta:select(Table, MatchSpec). 

if_select_account_by_name(SKey, Name) -> 
    MatchHead = #ddAccount{name='$1', _='_'},
    Guard = {'==', '$1', Name},
    Result = '$_',
    if_select(SKey, ddAccount, [{MatchHead, [Guard], [Result]}]).

if_get_account_id_by_name(SKey, Name) ->
    case if_select_account_by_name(SKey, Name) of
        {[#ddAccount{id=AccountId}], true} -> AccountId;
        {[],true} ->                            ?ClientError({"Account does not exist", Name})
    end.

%% --Implementation ------------------------------------------------------------------

create(SeCo, #ddRole{id=RoleId}=Role) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, RoleId) of
                        [] ->   %% ToDo: Check if roles contained in Role are all defined $$$$$$$$$$$$$$
                                ok=if_write(SeCo, ddRole, Role);
                        [_] -> ?ClientError({"Role already exists",RoleId})
                    end;
        false ->    ?SecurityException({"Create role unauthorized",SeCo})
    end;        
create(SeCo, RoleId) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, RoleId) of
                        [] ->       ok = if_write(SeCo, ddRole, #ddRole{id=RoleId});
                        [_] ->      ?ClientError({"Role already exists",RoleId})
                    end;
        false ->    ?SecurityException({"Create role unauthorized",SeCo})
    end.

get(SeCo, RoleId) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, RoleId) of
                        [] ->       ?ClientError({"Role does not exist", RoleId});
                        [Role] ->   Role
                    end;
        false ->    ?SecurityException({"Get role unauthorized",SeCo})
    end.            

update(SeCo, #ddRole{id=RoleId}=Role, RoleNew) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, RoleId) of
                        [] ->       ?ClientError({"Role does not exist", RoleId});
                        [Role] ->   ok = if_write(SeCo, ddRole, RoleNew);
                        [_] ->      ?ConcurrencyException({"Role is modified by someone else", RoleId})
                    end;
        false ->    ?SecurityException({"Update role unauthorized",SeCo})
    end.

delete(SeCo, #ddRole{id=RoleId}=Role) ->
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, RoleId) of
                        [] ->       ?ClientError({"Role does not exist", RoleId});
                        [Role] ->   delete(SeCo, RoleId);
                        [_] ->      ?ConcurrencyException({"Role is modified by someone else", RoleId})
                    end;
        false ->    ?SecurityException({"Delete role unauthorized",SeCo})
    end;
delete(SeCo, RoleId) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     ok = if_delete(SeCo, ddRole, RoleId);
        false ->    ?SecurityException({"Delete role unauthorized",SeCo})
    end.

exists(SeCo, #ddRole{id=RoleId}=Role) ->    %% exists unchanged
    case if_read(SeCo, ddRole, RoleId) of
        [] -> false;
        [Role] -> true;
        [_] -> false
    end;
exists(SeCo, RoleId) ->                     %% exists, maybe in changed form
    case if_read(SeCo, ddRole, RoleId) of
        [] -> false;
        [_] -> true
    end.

grant_role(SeCo, AccountName, RoleId) when is_binary(AccountName) ->
    grant_role(SeCo, if_get_account_id_by_name(SeCo, AccountName), RoleId);    
grant_role(SeCo, #ddRole{id=ToRoleId}=ToRole, RoleId) -> 
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, ToRoleId) of
                        [] ->       ?ClientError({"Role does not exist", ToRoleId});
                        [ToRole] -> grant_role(SeCo, ToRoleId, RoleId);
                        [_] ->      ?ConcurrencyException({"Role is modified by someone else", ToRoleId})
                    end;
        false ->    ?SecurityException({"Grant role unauthorized",SeCo})
    end;
grant_role(SeCo, ToRoleId, RoleId) ->
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case exists(SeCo, RoleId) of
                        false ->  
                            ?ClientError({"Role does not exist", RoleId});
                        true ->   
                            #ddRole{roles=Roles} = ToRole = get(SeCo, ToRoleId),
                            NewRoles = case lists:member(RoleId, Roles) of
                                true ->     Roles;
                                false ->    lists:append(Roles, [RoleId])       %% append because newer = seldom used
                            end,
                            update(SeCo,ToRole,ToRole#ddRole{roles=NewRoles})   
                    end;
        false ->    ?SecurityException({"Grant role unauthorized",SeCo})
    end.            

revoke_role(SeCo, AccountName, RoleId) when is_binary(AccountName) ->
    revoke_role(SeCo, if_get_account_id_by_name(SeCo, AccountName), RoleId);    
revoke_role(SeCo, #ddRole{id=FromRoleId}=FromRole, RoleId) -> 
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, FromRoleId) of
                        [] ->           ?ClientError({"Role does not exist", FromRoleId});
                        [FromRole] ->   revoke_role(SeCo, FromRoleId, RoleId);
                        [_] ->          ?ConcurrencyException({"Role is modified by someone else", FromRoleId})
                    end;
        false ->    ?SecurityException({"Revoke role unauthorized",SeCo})
    end;            
revoke_role(SeCo, FromRoleId, RoleId) -> 
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     #ddRole{roles=Roles} = FromRole = get(SeCo, FromRoleId),
                    update(SeCo,FromRole,FromRole#ddRole{roles=lists:delete(RoleId, Roles)});   
        false ->    ?SecurityException({"Revoke role unauthorized",SeCo})
    end.            

grant_permission(SeCo, AccountName, PermissionId) when is_binary(AccountName) ->
    grant_permission(SeCo, if_get_account_id_by_name(SeCo, AccountName), PermissionId);    
grant_permission(SeCo, #ddRole{id=ToRoleId}=ToRole, PermissionId) -> 
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, ToRoleId) of
                        [] ->       ?ClientError({"Role does not exist", ToRoleId});
                        [ToRole] -> grant_permission(SeCo, ToRoleId, PermissionId);
                        [_] ->      ?ConcurrencyException({"Role is modified by someone else", ToRoleId})
                    end;
        false ->    ?SecurityException({"Grant permission unauthorized",SeCo})
    end;
grant_permission(SeCo, ToRoleId, PermissionId) ->
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     #ddRole{permissions=Permissions} = ToRole = get(SeCo, ToRoleId),
                    NewPermissions = case lists:member(PermissionId, Permissions) of
                        true ->     Permissions;
                        false ->    lists:append(Permissions, [PermissionId])   %% append because newer = seldom used
                    end,
                    update(SeCo,ToRole,ToRole#ddRole{permissions=NewPermissions});   
        false ->    ?SecurityException({"Grant permission unauthorized",SeCo})
    end.

revoke_permission(SeCo, AccountName, PermissionId) when is_binary(AccountName) ->
    revoke_permission(SeCo, if_get_account_id_by_name(SeCo, AccountName), PermissionId);    
revoke_permission(SeCo, #ddRole{id=FromRoleId}=FromRole, PermissionId) -> 
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, FromRoleId) of
                        [] ->           ?ClientError({"Role does not exist", FromRoleId});
                        [FromRole] ->   revoke_permission(SeCo, FromRoleId, PermissionId);
                        [_] ->          ?ConcurrencyException({"Role is modified by someone else", FromRoleId})
                    end;
        false ->    ?SecurityException({"Revoke permission unauthorized",SeCo})
    end;        
revoke_permission(SeCo, FromRoleId, PermissionId) -> 
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     #ddRole{permissions=Permissions} = FromRole = get(SeCo, FromRoleId),
                    update(SeCo,FromRole,FromRole#ddRole{permissions=lists:delete(PermissionId, Permissions)});   
        false ->    ?SecurityException({"Revoke permission unauthorized",SeCo})
    end.

grant_quota(SeCo, AccountName, QuotaId, Value) when is_binary(AccountName) ->
    grant_quota(SeCo, if_get_account_id_by_name(SeCo, AccountName), QuotaId, Value);    
grant_quota(SeCo, #ddRole{id=ToRoleId}=ToRole, QuotaId, Value) -> 
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, ToRoleId) of
                        [] ->       ?ClientError({"Role does not exist", ToRoleId});
                        [ToRole] -> grant_quota(SeCo, ToRoleId, QuotaId, Value);
                        [_] ->      ?ConcurrencyException({"Role is modified by someone else", ToRoleId})
                    end;
        false ->    ?SecurityException({"Grant quota unauthorized",SeCo})
    end;
grant_quota(SeCo, ToRoleId, QuotaId, Value) ->
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     #ddRole{quotas=Quotas} = ToRole = get(SeCo, ToRoleId),
                    OtherQuotas = lists:keydelete(QuotaId, 1, Quotas),
                    update(SeCo,ToRole,ToRole#ddRole{quotas=lists:append(OtherQuotas, [{QuotaId,Value}])});   
        false ->    ?SecurityException({"Grant quota unauthorized",SeCo})
    end.

revoke_quota(SeCo, AccountName, QuotaId) when is_binary(AccountName) ->
    revoke_quota(SeCo, if_get_account_id_by_name(SeCo, AccountName), QuotaId);    
revoke_quota(SeCo, #ddRole{id=FromRoleId}=FromRole, QuotaId) -> 
   case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddRole, FromRoleId) of
                        [] ->           ?ClientError({"Role does not exist", FromRoleId});
                        [FromRole] ->   revoke_quota(SeCo, FromRoleId, QuotaId);
                        [_] ->          ?ConcurrencyException({"Role is modified by someone else", FromRoleId})
                    end;
        false ->    ?SecurityException({"Revoke quota unauthorized",SeCo})
    end;        
revoke_quota(SeCo, FromRoleId, QuotaId) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     #ddRole{quotas=Quotas} = FromRole = get(SeCo, FromRoleId),
                    update(SeCo,FromRole,FromRole#ddRole{quotas=lists:keydelete(QuotaId, 1, Quotas)});   
        false ->    ?SecurityException({"Revoke quota unauthorized",SeCo})
    end.


