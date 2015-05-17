-module(imem_seco).

-include("imem_seco.hrl").

-define(GET_PASSWORD_LIFE_TIME(__AccountId),?GET_CONFIG(passwordLifeTime,[__AccountId],100)).
-define(SALT_BYTES,32).
-define(PWD_HASH,scrypt).                       %% target hash: pwdmd5,md4,md5,sha512,scrypt 
-define(PWD_HASH_LIST,[scrypt,sha512,pwdmd5]).  %% allowed hash types
-define(REQUIRE_PWDMD5,<<"fun(Factors,NetCtx) -> [pwdmd5] -- Factors end">>).
-define(FULL_ACCESS,<<"fun(NetCtx) -> true end">>).

-behavior(gen_server).

-record(state, {
        }).

-export([ start_link/1
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
-export([ drop_seco_tables/1
        , create_credentials/2      % parameters: (Name,Password) or (pwdmd5,Password)
        ]).

-export([ authenticate/3            % deprecated
        , auth_start/3
        , auth_add_cred/2
        , auth_abort/1
        , login/1
        , change_credentials/3
        , set_credentials/3
        , set_login_time/2
        , logout/1
        , clone_seco/2
        , account_id/1
        , account_name/1
        ]).

-export([ has_role/3
        , has_permission/3
%%        , my_quota/2
        ]).

-export([ have_role/2
        , have_permission/2
        ]).

%% SMS APIs
-export([sc_send_sms_token/8, sc_send_sms_token/5, sc_send_sms_token/1,
         sc_verify_sms_token/4, sc_verify_sms_token/2]).

%       returns a ref() of the monitor
monitor(Pid) when is_pid(Pid) -> gen_server:call(?MODULE, {monitor, Pid}).

start_link(Params) ->
    ?Info("~p starting...~n", [?MODULE]),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]) of
        {ok, _} = Success ->
            ?Info("~p started!~n", [?MODULE]),
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [?MODULE, Error]),
            Error
    end.

init(_Args) ->
    try %% try creating system tables, may fail if they exist, then check existence 
        if_check_table(none, ddTable),

        ADef = {record_info(fields, ddAccount),?ddAccount,#ddAccount{}},
        catch imem_meta:create_check_table(ddAccount, ADef, [], system),
        imem_meta:create_or_replace_index(ddAccount, name),

        ADDef = {record_info(fields, ddAccountDyn),?ddAccountDyn,#ddAccountDyn{}},
        catch imem_meta:create_check_table(ddAccountDyn, ADDef, [], system),

        RDef = {record_info(fields, ddRole), ?ddRole, #ddRole{}},
        catch imem_meta:create_check_table(ddRole, RDef, [], system),

        SDef = {record_info(fields, ddSeCo), ?ddSeCo, #ddSeCo{}},
        case (catch imem_meta:table_columns(ddSeCo@)) of
            L when L==element(1,SDef) ->    ok;     % field names in table match the new record
            L when is_list(L) ->            imem_meta:drop_table(ddSeCo@);
            _ ->                            ok      % table does not exist
        end,

        catch imem_meta:create_check_table(ddSeCo@, SDef
              , [{scope,local}, {local_content,true},{record_name,ddSeCo}], system),

        PDef = {record_info(fields, ddPerm),?ddPerm, #ddPerm{}},
        catch imem_meta:create_check_table(ddPerm@, PDef
              , [{scope,local}, {local_content,true},{record_name,ddPerm}], system),

        QDef = {record_info(fields, ddQuota), ?ddQuota, #ddQuota{}},
        catch imem_meta:create_check_table(ddQuota@, QDef
              , [{scope,local}, {local_content,true},{record_name,ddQuota}], system),

        case if_select_account_by_name(none, <<"system">>) of
            {[],true} ->  
                    {ok, Pwd} = application:get_env(imem, default_admin_pswd),
                    LocalTime = calendar:local_time(),
                    UserCred=create_credentials(pwdmd5, Pwd),
                    Account = #ddAccount{id=system, type=deamon, name= <<"system">>, credentials=[UserCred]
                                , fullName= <<"DB Administrator">>, lastPasswordChangeTime=LocalTime},
                    AccountDyn = #ddAccountDyn{id=system},
                    if_write(none, ddAccount, Account),                    
                    if_write(none, ddAccountDyn, AccountDyn),                    
                    if_write(none, ddRole, #ddRole{id=system,roles=[],permissions=[manage_system, manage_accounts, manage_system_tables, manage_user_tables]});
            _ ->    ok
        end,
        % imem_meta:fail({"Fail in imem_seco:init on purpose"}),        
        if_truncate_table(none,ddSeCo@),
        if_truncate_table(none,ddPerm@),
        if_truncate_table(none,ddQuota@),

        process_flag(trap_exit, true),
        ok = inets:start(), % for httpc use
        {ok,#state{}}    
    catch
        _Class:Reason -> {stop, {Reason,erlang:get_stacktrace()}} 
    end.

handle_call({monitor, Pid}, _From, State) ->
    %% ?Debug("~p - started monitoring pid ~p~n", [?MODULE, Pid]),
    {reply, erlang:monitor(process, Pid), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

% handle_cast({stop, Reason}, State) ->
%     {stop,{shutdown,Reason},State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, normal}, State) ->
    % ?Debug("~p - received exit for monitored pid ~p ref ~p reason ~p~n", [?MODULE, Pid, _Ref, _Reason]),
    cleanup_pid(Pid),
    {noreply, State};
handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    ?Debug("~p - received exit for monitored pid ~p ref ~p reason ~p~n", [?MODULE, Pid, _Ref, _Reason]),
    cleanup_pid(Pid),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.


terminate(Reason, _State) ->
    ok = inets:stop(),
    case Reason of
        normal -> ?Info("~p normal stop~n", [?MODULE]);
        shutdown -> ?Info("~p shutdown~n", [?MODULE]);
        {shutdown, _Term} -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
        _ -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason])
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


%% --Interface functions  (duplicated in dd_account) ----------------------------------

if_select(_SKey, Table, MatchSpec) ->
    imem_meta:select(Table, MatchSpec). 

if_select_seco_keys_by_pid(SKey, Pid) -> 
    MatchHead = #ddSeCo{skey='$1', pid='$2', _='_'},
    Guard = {'==', '$2', Pid},
    Result = '$1',
    if_select(SKey, ddSeCo@, [{MatchHead, [Guard], [Result]}]).

if_select_perm_keys_by_skey(_SKeyM, SKey) ->      %% M=Monitor / MasterContext 
    MatchHead = #ddPerm{pkey='$1', skey='$2', _='_'},
    Guard = {'==', '$2', SKey},
    Result = '$1',
    if_select(SKey, ddPerm@, [{MatchHead, [Guard], [Result]}]).

if_check_table(_SeKey, Table) ->
    imem_meta:check_table(Table).

%% -- See similar Implementation in imem_account, imem_seco, imem_role -------------- 

if_dirty_index_read(_SeKey, Table, SecKey, Index) -> 
    imem_meta:dirty_index_read(Table, SecKey, Index).

if_select_account_by_name(_SeKey, <<"system">>) -> 
    {if_read(_SeKey, ddAccount, system),true};
if_select_account_by_name(_SeKey, Name) -> 
    {if_dirty_index_read(_SeKey,ddAccount,Name, #ddAccount.name),true}.

%% --Interface functions  (calling imem_meta) ----------------------------------

if_drop_table(_SKey, Table) -> 
    imem_meta:drop_table(Table).

if_truncate_table(_SKey, Table) -> 
    imem_meta:truncate_table(Table).

if_write(_SKey, Table, Record) -> 
    imem_meta:write(Table, Record).

if_read(_SKey, Table, Key) -> 
    imem_meta:read(Table, Key).

if_delete(_SKey, Table, RowId) ->
    imem_meta:delete(Table, RowId).

if_missing_role(RoleId) when is_atom(RoleId) ->
    ?Warn("Role ~p does not exist", [RoleId]),
    false;
if_missing_role(_) -> false.

if_has_role(_SKey, _RootRoleId, _RootRoleId) ->
    true;
if_has_role(SKey, RootRoleId, RoleId) ->
    case if_read(SKey, ddRole, RootRoleId) of
        [#ddRole{roles=[]}] ->          false;
        [#ddRole{roles=ChildRoles}] ->  if_has_child_role(SKey,  ChildRoles, RoleId);
        [] ->                           if_missing_role(RootRoleId)
    end.

if_has_child_role(_SKey, [], _RoleId) -> false;
if_has_child_role(SKey, [RootRoleId|OtherRoles], RoleId) ->
    case if_has_role(SKey, RootRoleId, RoleId) of
        true ->                         true;
        false ->                        if_has_child_role(SKey, OtherRoles, RoleId)
    end.

if_has_permission(_SKey, _RootRoleId, []) ->
    false;
if_has_permission(SKey, RootRoleId, PermissionList) when is_list(PermissionList)->
    %% search for first match in list of permissions
    case if_read(SKey, ddRole, RootRoleId) of
        [#ddRole{permissions=[],roles=[]}] ->     
            false;
        [#ddRole{permissions=Permissions, roles=[]}] -> 
            list_member(PermissionList, Permissions);
        [#ddRole{permissions=Permissions, roles=ChildRoles}] ->
            case list_member(PermissionList, Permissions) of
                true ->     true;
                false ->    if_has_child_permission(SKey,  ChildRoles, PermissionList)
            end;
        [] ->
            if_missing_role(RootRoleId)
    end;
if_has_permission(SKey, RootRoleId, PermissionId) ->
    %% search for single permission
    case if_read(SKey, ddRole, RootRoleId) of
        [#ddRole{permissions=[],roles=[]}] ->     
            false;
        [#ddRole{permissions=Permissions, roles=[]}] -> 
            lists:member(PermissionId, Permissions);
        [#ddRole{permissions=Permissions, roles=ChildRoles}] ->
            case lists:member(PermissionId, Permissions) of
                true ->     true;
                false ->    if_has_child_permission(SKey,  ChildRoles, PermissionId)
            end;
        [] ->
            if_missing_role(RootRoleId)
    end.

if_has_child_permission(_SKey, [], _Permission) -> false;
if_has_child_permission(SKey, [RootRoleId|OtherRoles], Permission) ->
    case if_has_permission(SKey, RootRoleId, Permission) of
        true ->     true;
        false ->    if_has_child_permission(SKey, OtherRoles, Permission)
    end.


%% --Implementation (exported helper functions) ----------------------------------------

-spec create_credentials(binary()|pwdmd5, binary()) -> ddCredential().
create_credentials(Name, Password) when is_binary(Name),is_binary(Password) ->
    {pwdmd5, {Name,erlang:md5(Password)}};  % username/password credential for auth_start/3 and auth_add_credential/2
create_credentials(pwdmd5, Password) when is_list(Password) ->
    create_credentials(pwdmd5, list_to_binary(Password));     
create_credentials(pwdmd5, Password) when is_integer(Password) ->
    create_credentials(pwdmd5, list_to_binary(integer_to_list(Password)));    
create_credentials(pwdmd5, Password) when is_binary(Password) ->
    {pwdmd5, erlang:md5(Password)}.         % for use in authenticate/3 and in raw credentials in ddAccount

cleanup_pid(Pid) ->
    MonitorPid =  whereis(?MODULE),
    case self() of
        MonitorPid ->    
            {SKeys,true} = if_select_seco_keys_by_pid(none,Pid),
            seco_delete(none, SKeys);
        _ ->
            ?SecurityViolation({"Cleanup unauthorized",{self(),Pid}})
    end.

list_member([], _Permissions) ->
    false;
list_member([PermissionId|Rest], Permissions) ->
    case lists:member(PermissionId, Permissions) of
        true -> true;
        false -> list_member(Rest, Permissions)
    end.

drop_seco_tables(SKey) ->
    case have_permission(SKey, manage_system_tables) of
        true ->
            if_drop_table(SKey, ddSeCo@),     
            if_drop_table(SKey, ddRole),         
            if_drop_table(SKey, ddAccountDyn),
            if_drop_table(SKey, ddAccount);   
        false ->
            ?SecurityException({"Drop seco tables unauthorized", SKey})
    end.

seco_create(AppId,SessionId) -> 
    SessionCtx = #ddSessionCtx{appId=AppId, sessionId=SessionId},
    SeCo = #ddSeCo{pid=self(), sessionCtx=SessionCtx, authTime=erlang:now()},
    SKey = erlang:phash2(SeCo), 
    SeCo#ddSeCo{skey=SKey}.

seco_register(#ddSeCo{skey=SKey, pid=Pid}=SeCo, AuthState) when Pid == self() -> 
    if_write(SKey, ddSeCo@, SeCo#ddSeCo{authState=AuthState}),
    case if_select_seco_keys_by_pid(#ddSeCo{pid=self(),accountName= <<"register">>},Pid) of
        {[],true} ->    monitor(Pid);
        _ ->            ok
    end,
    SKey.    %% hash is returned back to caller

seco_unregister(#ddSeCo{skey=SKey, pid=Pid}) when Pid == self() -> 
    catch if_delete(SKey, ddSeCo@, SKey).


seco_existing(SKey) -> 
    case if_read(SKey, ddSeCo@, SKey) of
        [#ddSeCo{pid=Pid} = SeCo] when Pid == self() -> 
            SeCo;
        [] ->               
            ?SecurityException({"Not logged in", SKey})
    end.   

seco_authenticated(SKey) -> 
    case if_read(SKey, ddSeCo@, SKey) of
        [#ddSeCo{pid=Pid, authState=authenticated} = SeCo] when Pid == self() -> 
            SeCo;
        [#ddSeCo{pid=Pid, authState=authorized} = SeCo] when Pid == self() -> 
            SeCo;
        [#ddSeCo{}] ->      
            ?SecurityViolation({"Not logged in", SKey});    % Not authenticated
        [] ->               
            ?SecurityException({"Not logged in", SKey})
    end.   

seco_authorized(SKey) -> 
    case if_read(SKey, ddSeCo@, SKey) of
        [#ddSeCo{pid=Pid, authState=authorized} = SeCo] when Pid == self() -> 
            SeCo;
        [#ddSeCo{}] ->      
            ?SecurityViolation({"Not logged in", SKey});
        [] ->               
            ?SecurityException({"Not logged in", SKey})
    end.   

seco_update(#ddSeCo{skey=SKey,pid=Pid}=SeCo, #ddSeCo{skey=SKey,pid=Pid}=SeCoNew) when Pid == self() -> 
    case if_read(SKey, ddSeCo@, SKey) of
        [] ->       ?SecurityException({"Not logged in", SKey});
        [SeCo] ->   if_write(SKey, ddSeCo@, SeCoNew);
        [_] ->      ?SecurityException({"Security context is modified by someone else", SKey})
    end;
seco_update(#ddSeCo{skey=SKey}, _) -> 
    ?SecurityViolation({"Not logged in", SKey}).

seco_delete(_SKeyM, []) -> ok;
seco_delete(SKeyM, [SKey|SKeys]) ->
    seco_delete(SKeyM, SKey),
    seco_delete(SKeyM, SKeys);    
seco_delete(SKeyM, SKey) ->
    {Keys,true} = if_select_perm_keys_by_skey(SKeyM, SKey), 
    seco_perm_delete(SKeyM, Keys),
    try 
        if_delete(SKeyM, ddSeCo@, SKey)
    catch
        _Class:_Reason -> ?Debug("~p:seco_delete(~p) - exception ~p:~p~n", [?MODULE, SKey, _Class, _Reason])
    end.

seco_perm_delete(_SKeyM, []) -> ok;
seco_perm_delete(SKeyM, [PKey|PKeys]) ->
    try
        if_delete(SKeyM, ddPerm@, PKey)
    catch
        _Class:_Reason -> ?Debug("~p:seco_perm_delete(~p) - exception ~p:~p~n", [?MODULE, PKey, _Class, _Reason])
    end,
    seco_perm_delete(SKeyM, PKeys).

account_id(SKey) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    AccountId.

account_name(SKey) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    case if_read(SKey, ddAccount, AccountId) of
        [#ddAccount{name=Name}] ->  Name;
        [] ->                       ?ClientError({"Account does not exist", AccountId})
    end.

has_role(SKey, RootRoleId, RoleId) ->
    case have_permission(SKey, read_accounts) of
        true ->
            if_has_role(SKey, RootRoleId, RoleId);
        false ->     
            case have_permission(SKey, manage_accounts) of
                true ->     if_has_role(SKey, RootRoleId, RoleId); 
                false ->    ?SecurityException({"Has role unauthorized",SKey})
            end
    end.

has_permission(SKey, RootRoleId, Permission) ->
    case have_permission(SKey, read_accounts) of
        true ->     
            if_has_permission(SKey, RootRoleId, Permission); 
        false ->    
            case have_permission(SKey, manage_accounts) of
                true ->     if_has_permission(SKey, RootRoleId, Permission); 
                false ->    ?SecurityException({"Has permission unauthorized",SKey})
            end
    end.

have_role(SKey, RoleId) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    if_has_role(SKey, AccountId, RoleId).

have_permission(SKey, Permission) ->
    #ddSeCo{accountId=AccountId} = seco_authorized(SKey),
    if_has_permission(SKey, AccountId, Permission).


-spec authenticate(any(), binary(), ddCredential()) -> ddSeCoKey() | {ddSeCoKey(),atom()}. 
authenticate(SessionId, Name, {pwdmd5,Token}) ->            % old direct API for simple password authentication
    auth_start(imem, SessionId, {pwdmd5,{Name,Token}}).

-spec auth_start(atom(), any(), ddCredential()) -> ddSeCoKey() | {ddSeCoKey(),atom()}. 
auth_start(AppId, SessionId, Credential) ->                % access context / network parameters 
    auth_step(seco_create(AppId, SessionId), Credential).

-spec auth_add_cred(ddSeCoKey(), ddCredential()) -> ddSeCoKey() | {ddSeCoKey(),atom()}. 
auth_add_cred(SKey, Credential) ->
    auth_step(seco_existing(SKey), Credential).

-spec auth_abort(ddSeCoKey()) -> ok. 
auth_abort(SKey) ->
    seco_unregister(seco_existing(SKey)).

auth_step(#ddSeCo{sessionCtx=SessionCtx}=SeCo, {access,NetworkCtx}) ->
    AccessCheckFunStr = ?GET_CONFIG(accessCheckFun,[SessionCtx#ddSessionCtx.appId],?FULL_ACCESS),
    CacheKey = {?MODULE,accessCheckFun,AccessCheckFunStr},
    AccessCheckFun = case imem_cache:read(CacheKey) of 
        [] ->
            case imem_datatype:io_to_fun(AccessCheckFunStr) of
                ACF when is_function(ACF,1) ->
                    imem_cache:write(CacheKey,ACF),
                    ACF;
                _ ->
                    authenticate_fail(SeCo,{"Invalid accessCheckFun", AccessCheckFunStr}) 
            end;    
        [ACF] when is_function(ACF,1) -> ACF;
        Err -> authenticate_fail(SeCo,{"Invalid accessCheckFun", Err})
    end,
    case AccessCheckFun(SessionCtx#ddSessionCtx.networkCtx) of
        true -> ok;
        _ ->    authenticate_fail(SeCo,{"Network access denied",NetworkCtx})
    end,   
    AuthFactors = [access|SeCo#ddSeCo.authFactors],
    SessionCtx = (SeCo#ddSeCo.sessionCtx)#ddSessionCtx{networkCtx=NetworkCtx},
    auth_step_succeed(SeCo#ddSeCo{authFactors=AuthFactors, sessionCtx=SessionCtx});
auth_step(SeCo, {pwdmd5,{Name,Token}}) ->
    #ddSeCo{skey=SKey,accountId=AccId, authFactors=AFs} = SeCo,   % may not yet exist in ddSeco@
    case if_select_account_by_name(SKey, Name) of
        {[#ddAccount{locked='true'}],true} ->
            authenticate_fail(SeCo,"Account is locked. Contact a system administrator");
        {[#ddAccount{id=AccountId} = Account],true} when AccId==AccountId; AccId==undefined ->
            LocalTime = calendar:local_time(),
            case if_read(SKey, ddAccountDyn, AccountId) of
                [#ddAccountDyn{lastFailureTime=LocalTime}] ->
                    %% lie a bit, don't show a fast attacker that this attempt might have worked
                    if_write(SKey, ddAccount, Account#ddAccount{lastFailureTime=LocalTime,locked='true'}),
                    authenticate_fail(SeCo,"Invalid account credentials. Please retry");
                _ -> ok
            end,
            ok = check_re_hash(SeCo, Account, Token, Token, ?PWD_HASH_LIST),
            auth_step_succeed(SeCo#ddSeCo{accountName=Name, accountId=AccountId, authFactors=[pwdmd5|AFs]});
        {[],true} ->
            authenticate_fail(SeCo,"Invalid account credentials. Please retry")
    end;
auth_step(SeCo, Credential) ->
    authenticate_fail(SeCo,{"Invalid credential type",element(1,Credential)}).

authenticate_fail(SeCo,ErrorTerm) ->
    seco_unregister(SeCo),
    ?SecurityException(ErrorTerm).

auth_step_succeed(#ddSeCo{skey=SKey, accountId=AccountId, sessionCtx=SessionCtx, authFactors=AFs} = SeCo) ->
    AuthRequireFunStr = ?GET_CONFIG(authenticateRequireFun,[SessionCtx#ddSessionCtx.appId],?REQUIRE_PWDMD5),
    CacheKey = {?MODULE,authenticateRequireFun,AuthRequireFunStr},
    AuthRequireFun = case imem_cache:read(CacheKey) of 
        [] ->
            case imem_datatype:io_to_fun(AuthRequireFunStr) of
                CF when is_function(CF,2) ->
                    imem_cache:write(CacheKey,CF),
                    CF;
                _ ->
                    authenticate_fail(SeCo,{"Invalid authenticatonRequireFun", AuthRequireFunStr}) 
            end;    
        [AF] when is_function(AF,2) -> AF;
        Err -> authenticate_fail(SeCo,{"Invalid authenticatonRequireFun", Err})
    end,
    case AuthRequireFun(AFs,SessionCtx#ddSessionCtx.networkCtx) of
        [] ->   
            case if_read(SKey, ddAccountDyn, AccountId) of
                [] ->   
                    AD = #ddAccountDyn{id=AccountId},
                    if_write(SKey, ddAccountDyn, AD);   % create dynamic account record if missing
                [#ddAccountDyn{lastFailureTime=undefined}] ->
                    ok;
                [#ddAccountDyn{} = AD] ->
                    if_write(SKey, ddAccountDyn, AD#ddAccountDyn{lastFailureTime=undefined})
            end,
            seco_register(SeCo, authenticated);   % return SKey (hash) value to client
        OFs ->  
            {seco_register(SeCo, undefined), OFs} % return Skey and list of more authentcation factors to try
    end.       

check_re_hash(SeCo, _Account, _OldToken, _NewToken, []) ->
    authenticate_fail(SeCo,"Invalid account credentials. Please retry");
check_re_hash(SeCo, Account, OldToken, NewToken, [pwdmd5|Types]) ->
    case lists:member({pwdmd5,OldToken},Account#ddAccount.credentials) of
        true ->  
            re_hash(SeCo, {pwdmd5,OldToken}, OldToken, NewToken, Account);
        false ->
            check_re_hash(SeCo, Account, OldToken, NewToken, Types)
    end;
check_re_hash(SeCo, Account, OldToken, NewToken, [Type|Types]) ->
    case lists:keyfind(Type,1,Account#ddAccount.credentials) of
        {Type,{Salt,Hash}} ->
            case hash(Type,Salt,OldToken) of
                Hash ->
                    re_hash(SeCo, {Type,{Salt,Hash}}, OldToken, NewToken, Account);
                _ ->
                    authenticate_fail(SeCo,"Invalid account credentials. Please retry")
            end;
        false ->
            check_re_hash(SeCo, Account, OldToken, NewToken, Types)
    end.

find_re_hash(SeCo, Account, NewToken, []) ->
    re_hash(SeCo, undefined, undefined, NewToken, Account);
find_re_hash(SeCo, Account, NewToken, [Type|Types]) ->
    case lists:keyfind(Type,1,Account#ddAccount.credentials) of
        false ->
            find_re_hash(SeCo, Account, NewToken, Types);
        FoundCred ->
            re_hash(SeCo, FoundCred, <<>>, NewToken, Account)
    end.

re_hash( _ , {?PWD_HASH,_}, Token, Token, _) -> ok;   %% re_hash not needed, already using target hash
re_hash(SeCo, FoundCred, OldToken, NewToken, Account) ->
    Salt = crypto:rand_bytes(?SALT_BYTES),
    Hash = hash(?PWD_HASH, Salt, NewToken),
    NewCreds = [{?PWD_HASH,{Salt,Hash}} | lists:delete(FoundCred,Account#ddAccount.credentials)],
    NewAccount = case NewToken of
        OldToken -> Account#ddAccount{credentials=NewCreds};
        _ ->        Account#ddAccount{credentials=NewCreds,lastPasswordChangeTime=calendar:local_time()}
    end,
    ok=if_write(SeCo#ddSeCo.skey, ddAccount, NewAccount).


hash(scrypt,Salt,Token) when is_binary(Salt), is_binary(Token) ->
    %io:format(user,"scrypt hash start ~p ~p~n",[Salt,Token]),
    %Self = self(),
    %spawn(fun() -> 
    %    {T,Res}=timer:tc(fun()-> erlscrypt:scrypt(Token, Salt, 16384, 8, 1, 64) end),
    %    io:format(user,"scrypt hash result after ~p ~p~n",[T,Res]),
    %    Self! Res
    %end),
    %receive
    %    Res2 -> Res2
    %after 2000 ->
    %    throw(scrypt_timeout)
    %end;
    erlscrypt:scrypt(nif, Token, Salt, 16384, 8, 1, 64);
hash(Type,Salt,Token) when is_atom(Type), is_binary(Salt), is_binary(Token) ->
    crypto:hash(Type,<<Salt/binary,Token/binary>>).

login(SKey) ->
    #ddSeCo{accountId=AccountId, authFactors=AuthenticationFactors} = SeCo = seco_authenticated(SKey),
    LocalTime = calendar:local_time(),
    PwdExpireSecs = calendar:datetime_to_gregorian_seconds(LocalTime),
    PwdExpireDate = case ?GET_PASSWORD_LIFE_TIME(AccountId) of
        infinity -> 0;      % sorts in after any date tuple
        PVal ->     calendar:gregorian_seconds_to_datetime(PwdExpireSecs-24*3600*PVal)
    end,
    case {if_read(SKey, ddAccount, AccountId), lists:member(pwdmd5,AuthenticationFactors)} of
        {[#ddAccount{lastPasswordChangeTime=undefined}], true} -> 
            logout(SKey),
            ?SecurityException({?PasswordChangeNeeded, AccountId});
        {[#ddAccount{lastPasswordChangeTime=LastChange}], true} when LastChange < PwdExpireDate -> 
            logout(SKey),
            ?SecurityException({?PasswordChangeNeeded, AccountId});
        {[_], _} ->
            [AccountDyn] = if_read(SKey,ddAccountDyn,AccountId),
            ok = seco_update(SeCo, SeCo#ddSeCo{authState=authorized}),
            if_write(SKey, ddAccountDyn, AccountDyn#ddAccountDyn{lastLoginTime=LocalTime}),
            SKey;            
        {[], _} ->                    
            logout(SKey),
?Debug("~n----------------------~n~p~n----------------------~n", [erlang:get_stacktrace()]),
            ?SecurityException({"Invalid account credentials. Please retry", AccountId})
    end.

change_credentials(SKey, {pwdmd5,Token}, {pwdmd5,Token}) ->
    #ddSeCo{accountId=AccountId} = seco_authenticated(SKey),
    ?SecurityException({"The same password cannot be re-used. Please retry", AccountId});
change_credentials(SKey, {pwdmd5,OldToken}, {pwdmd5,NewToken}) ->
    #ddSeCo{accountId=AccountId} = SeCo = seco_authenticated(SKey),
    [Account] = if_read(SKey, ddAccount, AccountId),
    ok = check_re_hash(SeCo, Account, OldToken, NewToken, ?PWD_HASH_LIST),
    login(SKey).

set_credentials(SKey, Name, {pwdmd5,NewToken}) ->
    SeCo = seco_authorized(SKey),
    case have_permission(SKey, manage_accounts) of
        true ->     Account = imem_account:get_by_name(SKey, Name),
                    find_re_hash(SeCo, Account, NewToken, ?PWD_HASH_LIST); 
        false ->    ?SecurityException({"Set credentials unauthorized",SKey})
    end.

set_login_time(SKey, AccountId) ->
    case have_permission(SKey, manage_accounts) of
        true ->
            AccountDyn = case if_read(SKey,ddAccountDyn,AccountId) of
                             [AccountDynRec] ->  AccountDynRec;
                             [] -> #ddAccountDyn{id = AccountId}
                         end,
            if_write(SKey, ddAccountDyn, AccountDyn#ddAccountDyn{lastLoginTime=erlang:now()});
        false ->    ?SecurityException({"Set login time unauthorized",SKey})
    end.

logout(SKey) ->
    seco_delete(SKey, SKey).

clone_seco(SKeyParent, Pid) ->
    SeCoParent = seco_authorized(SKeyParent),
    SeCo = SeCoParent#ddSeCo{skey=undefined, pid=Pid},
    SKey = erlang:phash2(SeCo), 
    if_write(SKeyParent, ddSeCo@, SeCo#ddSeCo{skey=SKey}),
    monitor(Pid),
    SKey.

% @doc
sc_send_sms_token(To) ->
    sc_send_sms_token(
      To,
      ?GET_CONFIG(smsTokenValidationText,[],
                  <<"Imem verification code: %TOKEN% \r\n"
                    "This token will expire in 60 seconds.">>),
      ?GET_CONFIG(smsTokenValidationTokenType,[],<<"SHORT_NUMERIC">>),
      ?GET_CONFIG(smsTokenValidationExpireTime,[],60),
      ?GET_CONFIG(smsTokenValidationTokenLength,[],8)).
sc_send_sms_token(To, Text, TokenType, ExpireTime, TokenLength) ->
    sc_send_sms_token(
      ?GET_CONFIG(smsTokenValidationServiceUrl,[],
                  "https://api.swisscom.com/v1/tokenvalidation"),
      ?GET_CONFIG(smsTokenValidationClientId,[],"YOUR_CLIENT_ID_GOES_HERE"),
      To, Text, TokenType, ExpireTime, TokenLength,
      ?GET_CONFIG(smsTokenValidationTraceId,[],"IMEM")).
sc_send_sms_token(Url, ClientId, To, Text, TokenType, ExpireTime,
                  TokenLength, TraceId)
  when is_integer(ExpireTime), is_integer(TokenLength) ->
    case lists:member(
           TokenType, [<<"SHORT_NUMERIC">>, <<"SHORT_ALPHANUMERIC">>,
                       <<"SHORT_SMALL_AND_CAPITAL">>, <<"LONG_CRYPTIC">>]) of
        true -> ok;
        _ ->
            ?ClientError(
               io_lib:format(
                 "Invalid token type ~p, MUST be one of <<\"SHORT_NUMERIC\">>,"
                 "<<\"SHORT_ALPHANUMERIC\">>, <<\"SHORT_SMALL_AND_CAPITAL\">>"
                 " or <<\"LONG_CRYPTIC\">>", [TokenType]))
    end,
    ReqMap = #{to => To, text => Text, tokenType => TokenType,
               expireTime => integer_to_binary(ExpireTime),
               tokenLength => TokenLength},
    Req = imem_json:encode(
            if TraceId /= <<>> -> maps:put(traceId, TraceId, ReqMap);
               true -> ReqMap end),
    ?Info("Sending ~p", [Req]),
    case httpc:request(
           post, {Url,
                  [{"client_id",ClientId},
                   {"Accept","application/json; charset=utf-8"}],
                  "application/json; charset=utf-8",
                  Req},
           [{ssl,[{verify,0}]}],
           [{full_result, false}]) of
        {ok,{200,[]}} -> ok;
        {ok,{400,Body}} ->
            ?ClientError(lists:flatten(io_lib:format("HTTP 400: ~s", [Body])));
        {ok,{401,_}} -> ?ClientError("HTTP 401: Unauthorized");
        {ok,{403,_}} -> ?ClientError("HTTP 403: Client IP not whitelisted");
        {ok,{404,_}} -> ?ClientError("HTTP 404: Wrong URL or the given customer is"
                                     " not found");
        {ok,{500,Body}} ->
            ?SystemException(lists:flatten(io_lib:format("HTTP 500: ~s", [Body])));
        {error, Error} ->
            ?SystemException(Error)
    end.

sc_verify_sms_token(To, Token) ->
    sc_verify_sms_token(
      ?GET_CONFIG(smsTokenValidationServiceUrl,[],
                  "https://api.swisscom.com/v1/tokenvalidation"),
      ?GET_CONFIG(smsTokenValidationClientId,[],"YOUR_CLIENT_ID_GOES_HERE"),
      To, Token).
sc_verify_sms_token(Url, ClientId, To, Token) ->
    case httpc:request(
           get, {string:join([Url,To,Token],"/"),
                  [{"client_id",ClientId},
                   {"Accept","application/json; charset=utf-8"}]},
           [{ssl,[{verify,0}]},{url_encode,false}],
           [{full_result, false}]) of
        {ok,{200,[]}} -> ok;
        {ok,{400,Body}} ->
            ?ClientError(lists:flatten(io_lib:format("HTTP 400: ~s", [Body])));
        {ok,{401,_}} -> ?ClientError("HTTP 401: Unauthorized");
        {ok,{403,_}} -> ?ClientError("HTTP 403: Client IP not whitelisted");
        {ok,{404,_}} -> ?ClientError("HTTP 404: Wrong URL or the given customer is"
                                     " not found");
        {ok,{500,Body}} ->
            ?SystemException(lists:flatten(io_lib:format("HTTP 500: ~s", [Body])));
        {error, Error} ->
            ?SystemException(Error)
    end.

%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ?imem_test_setup.

teardown(_) ->
    SKey = ?imem_test_admin_login(),
    catch imem_account:delete(SKey, <<"test">>),
    catch imem_account:delete(SKey, <<"test_admin">>),
    catch imem_role:delete(SKey, table_creator),
    catch imem_role:delete(SKey, test_role),
    catch imem_seco:logout(SKey),
    catch imem_meta:drop_table(user_table_123),
    ?imem_test_teardown.

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun test/1
        ]}}.    

    
test(_) ->
    try
        % ClEr = 'ClientError',
        % CoEx = 'ConcurrencyException',
        % SeEx = 'SecurityException',
        % SeVi = 'SecurityViolation',
        % SyEx = 'SystemException',          %% cannot easily test that

        ?LogDebug("---TEST---~p~n", [?MODULE]),

        ?LogDebug("schema ~p~n", [imem_meta:schema()]),
        ?LogDebug("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        ?LogDebug("~p:test_database~n", [?MODULE]),

        Seco0 = imem_meta:table_size(ddSeCo@),
        Perm0 = imem_meta:table_size(ddPerm@),
        ?assert(0 =< imem_meta:table_size(ddSeCo@)),
        ?assert(0 =< imem_meta:table_size(ddPerm@)),
        ?LogDebug("success ~p~n", [minimum_table_sizes]),

        ?LogDebug("~p:test_admin_login~n", [?MODULE]),

        SeCoAdmin0=?imem_test_admin_login(),
        ?LogDebug("success ~p~n", [test_admin_login]),

        Seco1 = imem_meta:table_size(ddSeCo@),
        Perm1 = imem_meta:table_size(ddPerm@),
        ?assertEqual(Seco0+1,Seco1),
        ?assertEqual(Perm0,Perm1),        
        ?LogDebug("success ~p~n", [status1]),
        Seco2 = imem_sec:table_size(SeCoAdmin0, ddSeCo@),
        Perm2 = imem_sec:table_size(SeCoAdmin0, ddPerm@),
        ?assertEqual(Seco0+1,Seco2),
        ?assertEqual(Perm0+2,Perm2),        
        ?LogDebug("success ~p~n", [status1]),

        imem_seco ! {'DOWN', simulated_reference, process, self(), simulated_exit},
        timer:sleep(2000),
        Seco3 = imem_meta:table_size(ddSeCo@),
        Perm3 = imem_meta:table_size(ddPerm@),
        ?assertEqual(Seco0,Seco3),
        ?assertEqual(Perm0,Perm3),        
        ?LogDebug("success ~p~n", [status2]),

        ?LogDebug("~p:test_imem_seco~n", [?MODULE])
    catch
        Class:Reason ->  ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.

-endif.
