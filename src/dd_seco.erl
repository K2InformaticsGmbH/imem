-module(dd_seco).

-define(PASSWORD_VALIDITY,100).

-include("dd.hrl").

-export([ cleanup_pid/1
        ]).

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

-export([ authenticate/3
        , login/1
        , change_credentials/3
        , logout/1
        ]).

-export([ has_role/3
        , has_permission/3
%%        , my_quota/2
        ]).

-export([ have_role/2
        , have_permission/2
        ]).

%% --Interface functions  (duplicated in dd_account) ----------------------------------

if_select(_SeCo, Table, MatchSpec) ->
    imem_if:select(Table, MatchSpec). 

if_get_account(SeCo, AccountId) -> 
    case if_read(SeCo, ddAccount, AccountId) of
        [] -> {dd_error, {"Account does not exist", AccountId}};
        [Row] -> Row
    end.

if_read_seco_keys_by_pid(SeCo, Pid) -> 
    MatchHead = #ddSeCo{key='$1', pid='$2', _='_'},
    Guard = {'==', '$2', Pid},
    Result = '$1',
    if_select(SeCo, ddSeCo, [{MatchHead, [Guard], [Result]}]).

if_get_account_by_name(SeCo, Name) -> 
    MatchHead = #ddAccount{name='$1', _='_'},
    Guard = {'==', '$1', Name},
    Result = '$_',
    case if_select(SeCo, ddAccount, [{MatchHead, [Guard], [Result]}]) of
        [] ->           {dd_error, {"Account does not exist", Name}};
        [Account] ->    Account
    end.

%% --Interface functions  (calling imem_if for now) ----------------------------------

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

if_write(_SeCoUser, Table, Record) -> 
    imem_if:write(Table, Record).

if_read(_SeCoUser, Table, Key) -> 
    imem_if:read(Table, Key).

if_delete(_SeCo, Table, RowId) ->
    imem_if:delete(Table, RowId).

if_get_role(SeCoUser, RoleId) -> 
    case if_read(SeCoUser, ddRole, RoleId) of
        [] ->       {dd_error, {"Role does not exist", RoleId}};
        [Role] ->   Role;
        Error ->    Error
    end.

if_has_role(_SeCo, _RootRoleId, _RootRoleId) ->
    true;
if_has_role(SeCo, RootRoleId, RoleId) ->
    case if_get_role(SeCo, RootRoleId) of
        #ddRole{roles=[]} ->            false;
        #ddRole{roles=ChildRoles} ->    if_has_child_role(SeCo,  ChildRoles, RoleId);
        Error ->                        Error
    end.

if_has_child_role(_SeCo, [], _RoleId) -> false;
if_has_child_role(SeCo, [RootRoleId|OtherRoles], RoleId) ->
    case if_has_role(SeCo, RootRoleId, RoleId) of
        true ->                         true;
        false ->                        if_has_child_role(SeCo, OtherRoles, RoleId);
        Error ->                        Error        
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
        #ddRole{permissions=[],roles=[]} ->     
            false;
        #ddRole{permissions=Permissions, roles=[]} -> 
            list_member(PermissionList, Permissions);
        #ddRole{permissions=Permissions, roles=ChildRoles} ->
            case list_member(PermissionList, Permissions) of
                true ->     true;
                false ->    if_has_child_permission(SeCo,  ChildRoles, PermissionList)
            end;
        Error ->
            Error
    end;
if_has_permission(SeCo, RootRoleId, PermissionId) ->
    %% search for single permission
    case if_get_role(SeCo, RootRoleId) of
        #ddRole{permissions=[],roles=[]} ->     
            false;
        #ddRole{permissions=Permissions, roles=[]} -> 
            lists:member(PermissionId, Permissions);
        #ddRole{permissions=Permissions, roles=ChildRoles} ->
            case lists:member(PermissionId, Permissions) of
                true ->     true;
                false ->    if_has_child_permission(SeCo,  ChildRoles, PermissionId)
            end;
        Error ->
            Error
    end.

if_has_child_permission(_SeCo, [], _Permission) -> false;
if_has_child_permission(SeCo, [RootRoleId|OtherRoles], Permission) ->
    case if_has_permission(SeCo, RootRoleId, Permission) of
        true ->     true;
        false ->    if_has_child_permission(SeCo, OtherRoles, Permission);
        Error ->    Error
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

create_cluster_tables(SeKey) ->
    %% Does not throw errors, returns {atomic,ok} or {Error, Reason}
    if_create_table(SeKey, ddTable, record_info(fields, ddTable),[], system),        %% may fail if exists
    if_create_table(SeKey, ddAccount, record_info(fields, ddAccount),[], system),    %% may fail if exists
    if_create_table(SeKey, ddRole, record_info(fields, ddRole),[], system).          %% may fail if exists

create_local_tables(SeKey) ->
    %% Does not throw errors, returns {atomic,ok} or {Error, Reason}
    if_create_table(SeKey, ddSeCo, record_info(fields, ddSeCo),[local], system).     

drop_system_tables(SeKey) ->
    SeCo=seco(SeKey),
    case have_permission(SeCo, manage_system_tables) of
        true ->
            if_drop_table(SeKey, ddSeCo),     
            if_drop_table(SeKey, ddRole),         
            if_drop_table(SeKey, ddAccount),   
            case if_drop_table(SeKey, ddTable) of
                {atomic,ok} ->  {atomic,ok};
                Error ->        ?SystemException(Error)
            end;
        false ->
            ?SecurityException({"Drop system tables unauthorized", SeKey})
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
            case if_drop_table(SeKey, Table) of
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
    case have_permission(SeCo, manage_system_tables) of
        true ->
            case if_drop_table(SeKey, Table) of
                {atomic,ok} ->  {atomic,ok};
                Error ->        ?SystemException(Error)
            end;
        false ->
            ?SecurityException({"Drop system table unauthorized", SeKey})
    end. 

create(SessionId, Name, {AuthMethod,_}) -> 
    SeCo = #ddSeCo{pid=self(), sessionId=SessionId, name=Name, authMethod=AuthMethod, authTime=erlang:now()},
    SeKey = erlang:phash2(SeCo), 
    SeCo#ddSeCo{key=SeKey, state=unauthorized}.

register(#ddSeCo{key=SeKey, pid=Pid}=SeCo, AccountId) when Pid == self() -> 
    case if_write(SeKey, ddSeCo, SeCo#ddSeCo{accountId=AccountId}) of
        ok ->       case if_read_seco_keys_by_pid(#ddSeCo{pid=self(),name= <<"register">>},Pid) of
                        [] ->   imem_monitor:monitor(Pid);
                        _ ->    ok
                    end,
                    SeKey;    %% hash is returned back to caller
        Error ->    ?SystemException(Error)    
    end.

seco(SeKey) -> 
    case if_read(#ddSeCo{key=SeKey,pid=self()}, ddSeCo, SeKey) of
        [] ->               ?SecurityException({"Security context does not exist", SeKey});
        [#ddSeCo{pid=Pid} = SeCo] when Pid == self() -> SeCo;
        [#ddSeCo{}] ->      ?SecurityViolation({"Security context does not match", SeKey});
        Error ->            ?SystemException(Error)
    end.   

update(#ddSeCo{key=SeKey,pid=Pid}=SeCo, #ddSeCo{key=SeKey,pid=Pid}=SeCoNew) when Pid == self() -> 
    case if_read(SeKey, ddSeCo, SeKey) of
        [] ->       ?SecurityException({"Security context does not exist", SeKey});
        [SeCo] ->   case if_write(SeKey, ddSeCo, SeCoNew) of
                        ok ->       ok;
                        Error ->    ?SystemException(Error)
                    end;
        [_] ->      ?SecurityException({"Security context is modified by someone else", SeKey});
        Error ->    ?SystemException(Error)
    end;
update(#ddSeCo{key=SeKey}, _) -> 
    ?SecurityViolation({"Invalid security context", SeKey}).

delete(#ddSeCo{key=SeKey,pid=Pid}) when Pid == self() -> 
    case if_delete(SeKey, ddSeCo, SeKey) of
        ok ->       ok;
        Error ->    ?SystemException(Error)
    end;
delete(#ddSeCo{key=SeKey}) -> 
    ?SecurityViolation({"Delete security context unauthorized", SeKey});
delete(SeKey) ->
    delete(seco(SeKey)).

cleanup_pid(Pid) ->
    MonitorPid =  whereis(imem_monitor),
    case self() of
        MonitorPid ->    
            cleanup_context(if_read_seco_keys_by_pid(#ddSeCo{pid=self(),name= <<"cleanup_pid">>},Pid),[]);
        _ ->
            ?SecurityViolation({"Cleanup unauthorized",{self(),Pid}})
    end.

cleanup_context([],[]) ->
    ok;
cleanup_context([],ErrorAcc) ->
    {error,{"Security context cleanup failed for some keys",ErrorAcc}};
cleanup_context([{SeKey},Rest], ErrorAcc) ->
    NewAcc = case if_delete(none, ddSeCo, SeKey) of
        ok ->       ErrorAcc;
        _ ->        [SeKey|ErrorAcc]
    end,
    cleanup_context(Rest, NewAcc).

has_role(#ddSeCo{key=SeKey}=SeCo, RootRoleId, RoleId) ->
    case have_permission(SeCo, manage_accounts) of
        true ->     case if_has_role(SeKey, RootRoleId, RoleId) of
                        true ->     true;
                        false ->    false;
                        Error ->    ?SystemException(Error)
                    end; 
        false ->    ?SecurityException({"Has role unauthorized",SeKey})
    end;
has_role(SeKey, RootRoleId, RoleId) ->
    case have_permission(SeKey, manage_accounts) of
        true ->     case if_has_role(SeKey, RootRoleId, RoleId) of
                        true ->     true;
                        false ->    false;
                        Error ->    ?SystemException(Error)            
                    end; 
        false ->    ?SecurityException({"Has role unauthorized",SeKey})
    end.

has_permission(#ddSeCo{key=SeKey}=SeCo, RootRoleId, Permission) ->
    case have_permission(SeCo, manage_accounts) of
        true ->     case if_has_permission(SeKey, RootRoleId, Permission) of
                        true ->     true;
                        false ->    false;
                        Error ->    ?SystemException(Error)            
                    end; 
        false ->    ?SecurityException({"Has permission unauthorized",SeKey})
    end;
has_permission(SeKey, RootRoleId, Permission) ->
    case have_permission(SeKey, manage_accounts) of
        true ->     case if_has_permission(SeKey, RootRoleId, Permission) of
                        true ->     true;
                        false ->    false;
                        Error ->    ?SystemException(Error)                                    
                    end; 
        false ->    ?SecurityException({"Has permission unauthorized",SeKey})
    end.

have_role(#ddSeCo{key=SeKey}=SeCo, RoleId) ->
    case SeCo of
        #ddSeCo{pid=Pid, accountId=AccountId, state=authorized} when Pid == self() -> 
            case if_has_role(SeKey, AccountId, RoleId) of
                true ->     true;
                false ->    false;
                Error ->    ?SystemException(Error)                                                    
            end;
        #ddSeCo{} -> 
            ?SecurityViolation({"Invalid security context", SeKey});
        Error ->    
            ?SecurityViolation(Error)
    end;
have_role(SeKey, RoleId) ->
    have_role(seco(SeKey), RoleId).

have_permission(#ddSeCo{key=SeKey}=SeCo, Permission) ->
    case SeCo of
        #ddSeCo{pid=Pid, accountId=AccountId, state=authorized} when Pid == self() -> 
            case if_has_permission(SeKey, AccountId, Permission) of
                true ->     true;
                false ->    false;
                Error ->    ?SystemException(Error)                                                                    
            end;
        #ddSeCo{} -> 
            ?SecurityViolation({"Invalid security context", SeKey});
        Error ->    
            ?SecurityViolation(Error)
    end;
have_permission(SeKey, Permission) ->
    have_permission(seco(SeKey), Permission).

authenticate(SessionId, Name, Credentials) ->
    LocalTime = calendar:local_time(),
    SeCo = dd_seco:create(SessionId, Name, Credentials),
    Result = if_get_account_by_name(SeCo, Name),
    case Result of
        #ddAccount{locked='true'} ->
            ?SecurityException({"Account is locked. Contact a system administrator", Name});
        #ddAccount{lastFailureTime=LocalTime} ->
            %% lie a bit, don't show a fast attacker that this attempt might have worked
            if_write(SeCo, ddAccount, Result#ddAccount{lastFailureTime=calendar:local_time(), locked='true'}),
            ?SecurityException({"Invalid account credentials. Please retry", Name});
        #ddAccount{id=AccountId, credentials=CredList} -> 
            case lists:member(Credentials,CredList) of
                false ->    if_write(SeCo, ddAccount, Result#ddAccount{lastFailureTime=calendar:local_time()}),
                            ?SecurityException({"Invalid account credentials. Please retry", Name});
                true ->     ok=if_write(SeCo, ddAccount, Result#ddAccount{lastFailureTime=undefined}),
                            dd_seco:register(SeCo, AccountId)  % return (hash) value to client
            end;
        {dd_error,Error} -> 
            ?SecurityException(Error);
        Error ->        
            ?SystemException(Error)    
    end.

login(SeKey) ->
    #ddSeCo{accountId=AccountId, authMethod=AuthenticationMethod}=SeCo=dd_seco:seco(SeKey),
    LocalTime = calendar:local_time(),
    PwdExpireSecs = calendar:datetime_to_gregorian_seconds(LocalTime),
    PwdExpireDate = calendar:gregorian_seconds_to_datetime(PwdExpireSecs-24*3600*?PASSWORD_VALIDITY),
    case {Result=if_get_account(SeCo, AccountId), AuthenticationMethod} of
        {#ddAccount{lastPasswordChangeTime=undefined}, pwdmd5} -> 
            logout(SeKey),
            ?SecurityException({"Password expired. Please change it", AccountId});
        {#ddAccount{lastPasswordChangeTime=LastChange}, pwdmd5} when LastChange < PwdExpireDate -> 
            logout(SeKey),
            ?SecurityException({"Password expired. Please change it", AccountId});
        {#ddAccount{}, _} ->
            ok = dd_seco:update(SeCo,SeCo#ddSeCo{state=authorized}),
            if_write(SeCo, ddAccount, Result#ddAccount{lastLoginTime=calendar:local_time()}),
            SeKey;            
        {{dd_error,Error}, _} ->                    
            logout(SeKey),
            ?SecurityException(Error);
        {Error, _} ->                    
            logout(SeKey),
            ?SystemException(Error)
    end.

change_credentials(SeKey, {pwdmd5,_}=OldCred, {pwdmd5,_}=NewCred) ->
    #ddSeCo{accountId=AccountId} = SeCo = dd_seco:seco(SeKey),
    LocalTime = calendar:local_time(),
    #ddAccount{credentials=CredList} = Account = if_get_account(SeCo, AccountId),
    if_write(SeCo, ddAccount, Account#ddAccount{lastPasswordChangeTime=LocalTime, credentials=[NewCred|lists:delete(OldCred,CredList)]}),
    login(SeKey);
change_credentials(SeKey, {CredType,_}=OldCred, {CredType,_}=NewCred) ->
    #ddSeCo{accountId=AccountId} = SeCo = dd_seco:seco(SeKey),
    #ddAccount{credentials=CredList} = Account = if_get_account(SeCo, AccountId),
    if_write(SeCo, ddAccount, Account#ddAccount{credentials=[NewCred|lists:delete(OldCred,CredList)]}),
    login(SeKey).

logout(SeKey) ->
    dd_seco:delete(SeKey).
