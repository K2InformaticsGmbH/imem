-module(imem_seco).

-include("imem_seco.hrl").

-define(GET_PASSWORD_LIFE_TIME(__AccountId),?GET_IMEM_CONFIG(passwordLifeTime,[__AccountId],100)).
-define(SALT_BYTES,32).
-define(PWD_HASH,scrypt).                       %% target hash: pwdmd5,md4,md5,sha512,scrypt 
-define(PWD_HASH_LIST,[scrypt,sha512,pwdmd5]).  %% allowed hash types

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
        , create_credentials/1
        , create_credentials/2
        ]).

-export([ authenticate/3
        , login/1
        , change_credentials/3
        , set_credentials/3
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

        ADDef = {record_info(fields, ddAccountDyn),?ddAccountDyn,#ddAccountDyn{}},
        catch imem_meta:create_check_table(ddAccountDyn, ADDef, [], system),

        RDef = {record_info(fields, ddRole), ?ddRole, #ddRole{}},
        catch imem_meta:create_check_table(ddRole, RDef, [], system),

        SDef = {record_info(fields, ddSeCo), ?ddSeCo, #ddSeCo{}},
        catch imem_meta:create_check_table(ddSeCo@, SDef, [{scope,local}, {local_content,true},{record_name,ddSeCo}], system),

        PDef = {record_info(fields, ddPerm),?ddPerm, #ddPerm{}},
        catch imem_meta:create_check_table(ddPerm@, PDef, [{scope,local}, {local_content,true},{record_name,ddPerm}], system),

        QDef = {record_info(fields, ddQuota), ?ddQuota, #ddQuota{}},
        catch imem_meta:create_check_table(ddQuota@, QDef, [{scope,local}, {local_content,true},{record_name,ddQuota}], system),

        case if_select_account_by_name(none, <<"system">>) of
            {[],true} ->  
                    {ok, Pwd} = application:get_env(imem, default_admin_pswd),
                    LocalTime = calendar:local_time(),
                    UserCred=create_credentials(pwdmd5, Pwd),
                    Account = #ddAccount{id=system, name= <<"system">>, credentials=[UserCred]
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

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, _Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

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

if_select_account_by_name(SKey, Name) -> 
    MatchHead = #ddAccount{name='$1', _='_'},
    Guard = {'==', '$1', Name},
    Result = '$_',
    if_select(SKey, ddAccount, [{MatchHead, [Guard], [Result]}]).

if_check_table(_SeKey, Table) ->
    imem_meta:check_table(Table).

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

if_has_role(_SKey, _RootRoleId, _RootRoleId) ->
    true;
if_has_role(SKey, RootRoleId, RoleId) ->
    case if_read(SKey, ddRole, RootRoleId) of
        [#ddRole{roles=[]}] ->          false;
        [#ddRole{roles=ChildRoles}] ->  if_has_child_role(SKey,  ChildRoles, RoleId);
        [] ->                           %% ToDo: log missing role
                                        false
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
            %% ToDo: log missing role
            false
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
             %% ToDo: log missing role
            false
    end.

if_has_child_permission(_SKey, [], _Permission) -> false;
if_has_child_permission(SKey, [RootRoleId|OtherRoles], Permission) ->
    case if_has_permission(SKey, RootRoleId, Permission) of
        true ->     true;
        false ->    if_has_child_permission(SKey, OtherRoles, Permission)
    end.


%% --Implementation (exported helper functions) ----------------------------------------

create_credentials(Password) ->
    create_credentials(pwdmd5, Password).

create_credentials(Type, Password) when is_list(Password) ->
    create_credentials(Type, list_to_binary(Password));
create_credentials(Type, Password) when is_integer(Password) ->
    create_credentials(Type, list_to_binary(integer_to_list(Password)));
create_credentials(pwdmd5, Password) ->
    {pwdmd5, erlang:md5(Password)}.


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

seco_create(SessionId, Name, {AuthMethod,_}) -> 
    SeCo = #ddSeCo{pid=self(), sessionId=SessionId, name=Name, authMethod=AuthMethod, authTime=erlang:now()},
    SKey = erlang:phash2(SeCo), 
    SeCo#ddSeCo{skey=SKey, state=unauthorized}.

seco_register(#ddSeCo{skey=SKey, pid=Pid}=SeCo, AccountId) when Pid == self() -> 
    if_write(SKey, ddSeCo@, SeCo#ddSeCo{accountId=AccountId}),
    case if_select_seco_keys_by_pid(#ddSeCo{pid=self(),name= <<"register">>},Pid) of
        {[],true} ->    monitor(Pid);
        _ ->            ok
    end,
    SKey.    %% hash is returned back to caller

seco_authenticated(SKey) -> 
    case if_read(SKey, ddSeCo@, SKey) of
        [#ddSeCo{pid=Pid} = SeCo] when Pid == self() -> 
            SeCo;
        [#ddSeCo{}] ->      
            ?SecurityViolation({"Not logged in", SKey});
        [] ->               
            ?SecurityException({"Not logged in", SKey})
    end.   

seco_authorized(SKey) -> 
    case if_read(SKey, ddSeCo@, SKey) of
        [#ddSeCo{pid=Pid, state=authorized} = SeCo] when Pid == self() -> 
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

authenticate(SessionId, Name, {pwdmd5,Token}) ->
    LocalTime = calendar:local_time(),
    #ddSeCo{skey=SKey} = SeCo = seco_create(SessionId, Name, {pwdmd5,Token}),
    case if_select_account_by_name(SKey, Name) of
        {[#ddAccount{locked='true'}],true} ->
            authenticate_fail(SeCo, locked, Name);
        {[#ddAccount{id=AccountId} = Account],true} ->
            AccountDyn = case if_read(SKey, ddAccountDyn, AccountId) of
                [] ->   
                    AD = #ddAccountDyn{id=AccountId},
                    if_write(SKey, ddAccountDyn, AD),
                    AD;
                [#ddAccountDyn{lastFailureTime=LocalTime} = AD] ->
                    %% lie a bit, don't show a fast attacker that this attempt might have worked
                    if_write(SKey, ddAccount, Account#ddAccount{lastFailureTime=LocalTime,locked='true'}),
                    authenticate_fail(SeCo, AD, Name);
                [AD] -> AD
            end,
            ok = check_re_hash(SeCo, Account, AccountDyn, Name, Token, Token, ?PWD_HASH_LIST),
            authenticate_succeed(SeCo, AccountId, AccountDyn, Name);
        {[],true} ->
            authenticate_fail(SeCo, nomatch, Name)
    end.

authenticate_fail(_,locked,Name) ->
    ?SecurityException({"Account is locked. Contact a system administrator", Name});
authenticate_fail(_,nomatch,Name) ->
    ?SecurityException({"Invalid account credentials. Please retry", Name});
authenticate_fail(SeCo, AD, Name) ->
    SKey = SeCo#ddSeCo.skey,
    LocalTime = calendar:local_time(),
    if_write(SKey, ddAccountDyn, AD#ddAccountDyn{lastFailureTime=LocalTime}),
    authenticate_fail(SKey, nomatch, Name).

authenticate_succeed(SeCo, AccountId, #ddAccountDyn{lastFailureTime=undefined}, _Name) ->
    seco_register(SeCo, AccountId);   % return (hash) value to client
authenticate_succeed(SeCo, AccountId, AD, _Name) ->
    if_write(SeCo#ddSeCo.skey, ddAccountDyn, AD#ddAccountDyn{lastFailureTime=undefined}),
    seco_register(SeCo, AccountId).   % return (hash) value to client


check_re_hash(SeCo, _Account, AccountDyn, Name, _OldToken, _NewToken, []) ->
    authenticate_fail(SeCo, AccountDyn, Name);
check_re_hash(SeCo, Account, AccountDyn, Name, OldToken, NewToken, [pwdmd5|Types]) ->
    case lists:member({pwdmd5,OldToken},Account#ddAccount.credentials) of
        true ->  
            re_hash(SeCo, {pwdmd5,OldToken}, OldToken, NewToken, Account);
        false ->
            check_re_hash(SeCo, Account, AccountDyn, Name, OldToken, NewToken, Types)
    end;
check_re_hash(SeCo, Account, AccountDyn, Name, OldToken, NewToken, [Type|Types]) ->
    case lists:keyfind(Type,1,Account#ddAccount.credentials) of
        {Type,{Salt,Hash}} ->
            case hash(Type,Salt,OldToken) of
                Hash ->
                    re_hash(SeCo, {Type,{Salt,Hash}}, OldToken, NewToken, Account);
                _ ->
                    authenticate_fail(SeCo, AccountDyn, Name)
            end;
        false ->
            check_re_hash(SeCo, Account, AccountDyn, Name, OldToken, NewToken, Types)
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
%hash(scrypt,Salt,Token) when is_binary(Salt), is_binary(Token) ->
%    erlscrypt:scrypt(nif, Token, Salt, 16384, 8, 1, 64);
hash(Type,Salt,Token) when is_atom(Type), is_binary(Salt), is_binary(Token) ->
    crypto:hash(Type,<<Salt/binary,Token/binary>>).

login(SKey) ->
    #ddSeCo{accountId=AccountId, authMethod=AuthenticationMethod} = SeCo = seco_authenticated(SKey),
    LocalTime = calendar:local_time(),
    PwdExpireSecs = calendar:datetime_to_gregorian_seconds(LocalTime),
    PwdExpireDate = case ?GET_PASSWORD_LIFE_TIME(AccountId) of
        infinity -> 0;
        PVal ->     calendar:gregorian_seconds_to_datetime(PwdExpireSecs-24*3600*PVal)
    end,
    case {if_read(SKey, ddAccount, AccountId), AuthenticationMethod} of
        {[#ddAccount{lastPasswordChangeTime=undefined}], pwdmd5} -> 
            logout(SKey),
            ?SecurityException({?PasswordChangeNeeded, AccountId});
        {[#ddAccount{lastPasswordChangeTime=LastChange}], pwdmd5} when LastChange < PwdExpireDate -> 
            logout(SKey),
            ?SecurityException({?PasswordChangeNeeded, AccountId});
        {[_], _} ->
            [AccountDyn] = if_read(SKey,ddAccountDyn,AccountId),
            ok = seco_update(SeCo, SeCo#ddSeCo{state=authorized}),
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
    [#ddAccount{name=Name}=Account] = if_read(SKey, ddAccount, AccountId),
    [AccountDyn] = if_read(SKey,ddAccountDyn,AccountId),
    ok = check_re_hash(SeCo, Account, AccountDyn, Name, OldToken, NewToken, ?PWD_HASH_LIST),
    login(SKey).
% change_credentials(SKey, {CredType,_}=OldCred, {CredType,_}=NewCred) ->
%     #ddSeCo{accountId=AccountId} = seco_authenticated(SKey),
%     [#ddAccount{credentials=CredList} = Account]= if_read(SKey, ddAccount, AccountId),
%     if_write(SKey, ddAccount, Account#ddAccount{credentials=[NewCred|lists:delete(OldCred,CredList)]}),
%     login(SKey).

set_credentials(SKey, Name, {pwdmd5,NewToken}) ->
    SeCo = seco_authenticated(SKey),
    Account = imem_account:get_by_name(SKey, Name),
    find_re_hash(SeCo, Account, NewToken, ?PWD_HASH_LIST).

logout(SKey) ->
    seco_delete(SKey, SKey).


clone_seco(SKeyParent, Pid) ->
    SeCoParent = seco_authenticated(SKeyParent),
    SeCo = SeCoParent#ddSeCo{skey=undefined, pid=Pid},
    SKey = erlang:phash2(SeCo), 
    if_write(SKeyParent, ddSeCo@, SeCo#ddSeCo{skey=SKey}),
    monitor(Pid),
    SKey.

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
