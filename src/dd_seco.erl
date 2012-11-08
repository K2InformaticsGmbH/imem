-module(dd_seco).

-define(PASSWORD_VALIDITY,100).

-define(SECO_TABLES,[ddTable,ddAccount,ddRole,ddSeCo,ddPerm,ddQuota]).

-include("dd_seco.hrl").

-behavior(gen_server).

-record(state, {
        }).

-export([ start_link/0
        ]).

% gen_server interface (monitoring calling processes)
-export([ monitor/1
        , cleanup_pid/1
        ]).

% gen_server behavior callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        ]).

% security context library interface
-export([ system_table/2
        , drop_seco_tables/1
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

%       returns a ref() of the monitor
monitor(Pid) when is_pid(Pid) -> gen_server:call(?MODULE, {monitor, Pid}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    io:format(user, "~p starting...~n", [?MODULE]),
    try
        check_table(ddTable),
        if_create_table(none, ddAccount, record_info(fields, ddAccount),[], system),    %% may fail if exists
        check_table(ddAccount),
        if_create_table(none, ddRole, record_info(fields, ddRole),[], system),          %% may fail if exists
        check_table(ddRole),
        if_create_table(none, ddSeCo, record_info(fields, ddSeCo),[local, {local_content,true}], system),     
        check_table(ddSeCo),
        if_create_table(none, ddPerm, record_info(fields, ddPerm),[local, {local_content,true}], system),     
        check_table(ddPerm),
        if_create_table(none, ddQuota, record_info(fields, ddQuota),[local, {local_content,true}], system),     
        check_table(ddQuota),
        UserName= <<"admin">>,
        case if_read_account_by_name(none, UserName) of
            [] ->  
                    UserId = make_ref(),
                    UserCred={pwdmd5, erlang:md5(<<"change_on_install">>)},
                    User = #ddAccount{id=UserId, name=UserName, credentials=[UserCred]
                                        ,fullName= <<"DB Administrator">>, lastLoginTime=calendar:local_time()},
                    if_write(none, ddAccount, User),                    
                    if_write(none, ddRole, #ddRole{id=UserId,roles=[],permissions=[manage_accounts]});
            _ ->    ok       
        end,        
        io:format(user, "~p started!~n", [?MODULE])
    catch
        _:_ -> gen_server:cast(self(),{stop, "Insufficient resources for start"}) 
    end,
    {ok,#state{}}.

handle_call({monitor, Pid}, _From, State) ->
    io:format(user, "~p - started monitoring pid ~p~n", [?MODULE, Pid]),
    {reply, erlang:monitor(process, Pid), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({'DOWN', Ref, process, Pid, Reason}, State) ->
    io:format(user, "~p - died pid ~p ref ~p as ~p~n", [?MODULE, Pid, Ref, Reason]),
    cleanup_pid(Pid),
    {noreply, State};
handle_cast({stop, Reason}, State) ->
    {stop,{shutdown,Reason},State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


%% --Interface functions  (duplicated in dd_account) ----------------------------------

if_system_table(_SeCo, Table) ->
    imem_meta:system_table(Table).

if_select(_SeCo, Table, MatchSpec) ->
    imem_meta:select(Table, MatchSpec). 

if_read_seco_keys_by_pid(SeCo, Pid) -> 
    MatchHead = #ddSeCo{key='$1', pid='$2', _='_'},
    Guard = {'==', '$2', Pid},
    Result = '$1',
    if_select(SeCo, ddSeCo, [{MatchHead, [Guard], [Result]}]).

if_read_account_by_name(SeCo, Name) -> 
    MatchHead = #ddAccount{name='$1', _='_'},
    Guard = {'==', '$1', Name},
    Result = '$_',
    if_select(SeCo, ddAccount, [{MatchHead, [Guard], [Result]}]).

if_table_size(TableName) ->
    imem_meta:table_size(TableName).

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

check_table(Table) ->
    if_table_size(Table).

system_table(_SeCo, Table) ->
    case lists:member(Table,?SECO_TABLES) of
        true ->     true;
        false ->    if_system_table(_SeCo, Table)  
    end.

list_member([], _Permissions) ->
    false;
list_member([PermissionId|Rest], Permissions) ->
    case lists:member(PermissionId, Permissions) of
        true -> true;
        false -> list_member(Rest, Permissions)
    end.

drop_seco_tables(SeKey) ->
    SeCo=seco(SeKey),
    case have_permission(SeCo, manage_system_tables) of
        true ->
            if_drop_table(SeKey, ddSeCo),     
            if_drop_table(SeKey, ddRole),         
            if_drop_table(SeKey, ddAccount);   
        false ->
            ?SecurityException({"Drop system tables unauthorized", SeKey})
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
    case if_read_account_by_name(SeCo, Name) of
        [#ddAccount{locked='true'}] ->
            ?SecurityException({"Account is locked. Contact a system administrator", Name});
        [#ddAccount{lastFailureTime=LocalTime} = Account] ->
            %% lie a bit, don't show a fast attacker that this attempt might have worked
            if_write(SeCo, ddAccount, Account#ddAccount{lastFailureTime=calendar:local_time(), locked='true'}),
            ?SecurityException({"Invalid account credentials. Please retry", Name});
        [#ddAccount{id=AccountId, credentials=CredList} = Account] -> 
            case lists:member(Credentials,CredList) of
                false ->    if_write(SeCo, ddAccount, Account#ddAccount{lastFailureTime=calendar:local_time()}),
                            ?SecurityException({"Invalid account credentials. Please retry", Name});
                true ->     ok=if_write(SeCo, ddAccount, Account#ddAccount{lastFailureTime=undefined}),
                            dd_seco:register(SeCo, AccountId)  % return (hash) value to client
            end;
        [] -> 
            ?SecurityException({"Account does not exist", Name});
        Error ->        
            ?SystemException(Error)    
    end.

login(SeKey) ->
    #ddSeCo{accountId=AccountId, authMethod=AuthenticationMethod}=SeCo=dd_seco:seco(SeKey),
    LocalTime = calendar:local_time(),
    PwdExpireSecs = calendar:datetime_to_gregorian_seconds(LocalTime),
    PwdExpireDate = calendar:gregorian_seconds_to_datetime(PwdExpireSecs-24*3600*?PASSWORD_VALIDITY),
    case {if_read(SeCo, ddAccount, AccountId), AuthenticationMethod} of
        {[#ddAccount{lastPasswordChangeTime=undefined}], pwdmd5} -> 
            logout(SeKey),
            ?SecurityException({"Password expired. Please change it", AccountId});
        {[#ddAccount{lastPasswordChangeTime=LastChange}], pwdmd5} when LastChange < PwdExpireDate -> 
            logout(SeKey),
            ?SecurityException({"Password expired. Please change it", AccountId});
        {[#ddAccount{}=Account], _} ->
            ok = dd_seco:update(SeCo,SeCo#ddSeCo{state=authorized}),
            if_write(SeCo, ddAccount, Account#ddAccount{lastLoginTime=calendar:local_time()}),
            SeKey;            
        {[], _} ->                    
            logout(SeKey),
            ?SecurityException({"Account does not exist", AccountId})
    end.

change_credentials(SeKey, {pwdmd5,_}=OldCred, {pwdmd5,_}=NewCred) ->
    #ddSeCo{accountId=AccountId} = SeCo = dd_seco:seco(SeKey),
    LocalTime = calendar:local_time(),
    [#ddAccount{credentials=CredList} = Account] = if_read(SeCo, ddAccount, AccountId),
    if_write(SeCo, ddAccount, Account#ddAccount{lastPasswordChangeTime=LocalTime, credentials=[NewCred|lists:delete(OldCred,CredList)]}),
    login(SeKey);
change_credentials(SeKey, {CredType,_}=OldCred, {CredType,_}=NewCred) ->
    #ddSeCo{accountId=AccountId} = SeCo = dd_seco:seco(SeKey),
    [#ddAccount{credentials=CredList} = Account]= if_read(SeCo, ddAccount, AccountId),
    if_write(SeCo, ddAccount, Account#ddAccount{credentials=[NewCred|lists:delete(OldCred,CredList)]}),
    login(SeKey).

logout(SeKey) ->
    dd_seco:delete(SeKey).
