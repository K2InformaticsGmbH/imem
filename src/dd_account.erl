-module(dd_account).

-define(PASSWORD_VALIDITY,100).

-include_lib("eunit/include/eunit.hrl").

-include("dd.hrl").

-export([ authenticate/3
        , login/1
        , change_credentials/3
        , logout/1
        ]).

-export([ create/2
        , get/2
        , get_by_name/2
        , update/3
        , delete/2
        , exists/2
        , lock/2
        , unlock/2
        ]).

%% --Interface functions  (calling imem_if for now, not exported) -------------------

if_schema() ->
    imem_if:schema().

if_write(_SeCo, #ddAccount{}=Account) -> 
    imem_if:write(ddAccount, Account).

if_read(_SeCo, AccountId) -> 
    imem_if:read(ddAccount, AccountId).

if_get(SeCo, AccountId) -> 
    case if_read(SeCo, AccountId) of
        [] -> {error, {"Account does not exist", AccountId}};
        [Account] -> Account
    end.

if_select(_SeCo, MatchSpec) ->
    imem_if:select(ddAccount, MatchSpec). 

if_get_by_name(SeCo, Name) -> 
    MatchHead = #ddAccount{name='$1', _='_'},
    Guard = {'==', '$1', Name},
    Result = '$_',
    case if_select(SeCo, [{MatchHead, [Guard], [Result]}]) of
        [] ->           {error, {"Account does not exist", Name}};
        [Account] ->    Account
    end.

if_delete(_SeCo, AccountId) ->
    imem_if:delete(ddAccount, AccountId).

% for test setup only, not exported
if_write(#ddAccount{}=Account) -> 
    imem_if:write(ddAccount, Account);
if_write(Role) -> 
    imem_if:write(ddRole, Role).


%% --Implementation ------------------------------------------------------------------


create(SeCo, #ddAccount{id=AccountId, name=Name}=Account) when is_binary(Name) ->
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_get(SeCo, AccountId) of
                        {error, {"Account does not exist", AccountId}} ->   
                            case if_get_by_name(SeCo, Name) of
                                {error, {"Account does not exist", Name}} ->   
                                    ok = if_write(SeCo, Account),
                                    case dd_role:create(SeCo,AccountId) of
                                        ok  ->      ok;
                                        Error ->    %% simple transaction rollback
                                                    delete(SeCo, Account),
                                                    Error
                                    end;
                                #ddAccount{} ->     {error, {"Account name already exists for",Name}};
                                Error ->            Error
                            end;
                        #ddAccount{} ->  
                            {error, {"Account already exists",AccountId}}
                    end;
        false ->    {error, {"Create account unauthorized",SeCo}}
    end.

get(SeCo, AccountId) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     if_get(SeCo, AccountId);
        false ->    {error, {"Get account unauthorized",SeCo}}
    end.

get_by_name(SeCo, Name) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     if_get_by_name(SeCo, Name);
        false ->    {error, {"Get account unauthorized",SeCo}}
    end.

update(SeCo, #ddAccount{id=AccountId}=Account, AccountNew) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, AccountId) of
                        [] -> {error, {"Account does not exist", AccountId}};
                        [Account] -> if_write(SeCo, AccountNew);
                        [_] -> {error, {"Account is modified by someone else", AccountId}}
                    end;
        false ->    {error, {"Update account unauthorized",SeCo}}
    end.    

delete(SeCo, #ddAccount{id=AccountId}=Account) ->
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, AccountId) of
                        [] -> {error, {"Account does not exist", AccountId}};
                        [Account] -> delete(SeCo, AccountId);
                        [_] -> {error, {"Account is modified by someone else", AccountId}}
                    end;
        false ->    {error, {"Delete account unauthorized",SeCo}}
    end;        
delete(SeCo, AccountId) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     ok=if_delete(SeCo, AccountId),
                    ok=dd_role:delete(SeCo, AccountId);
        false ->    {error, {"Delete account unauthorized",SeCo}}
    end.        

lock(SeCo, #ddAccount{}=Account) -> 
    update(SeCo, Account, Account#ddAccount{locked=true});
lock(SeCo, AccountId) -> 
    Account = get(SeCo, AccountId),
    update(SeCo,  Account, Account#ddAccount{locked=true}).

unlock(SeCo, #ddAccount{}=Account) -> 
    update(SeCo, Account, Account#ddAccount{locked=false,lastFailureTime=undefined});
unlock(SeCo, AccountId) -> 
    Account = get(SeCo, AccountId),
    update(SeCo, Account, Account#ddAccount{locked=false,lastFailureTime=undefined}).

exists(SeCo, #ddAccount{id=AccountId}=Account) ->   %% exists unchanged
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, AccountId) of
                        [] -> false;
                        [Account] -> true;
                        [_] -> false
                    end;
        false ->    {error, {"Exists account unauthorized",SeCo}}
    end;                    
exists(SeCo, AccountId) ->                          %% exists, maybe in changed form
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, AccountId) of
                        [] -> false;
                        [_] -> true
                    end;
        false ->    {error, {"Exists account unauthorized",SeCo}}
    end.            


authenticate(SessionId, Name, Credentials) ->
    LocalTime = calendar:local_time(),
    SeCo = dd_seco:create(SessionId, Name, Credentials),
    Result = if_get_by_name(SeCo, Name),
    case Result of
        #ddAccount{locked='true'} ->
            {error,{"Account is locked. Contact a system administrator", Name}};
        #ddAccount{lastFailureTime=LocalTime} ->
            %% lie a bit, don't show a fast attacker that this attempt might have worked
            if_write(SeCo, Result#ddAccount{lastFailureTime=calendar:local_time(), locked='true'}),
            {error,{"Invalid account credentials. Please retry", Name}};
        #ddAccount{id=AccountId, credentials=CredList} -> 
            case lists:member(Credentials,CredList) of
                false ->    if_write(SeCo, Result#ddAccount{lastFailureTime=calendar:local_time()}),
                            {error,{"Invalid account credentials. Please retry", Name}};
                true ->     if_write(SeCo, Result#ddAccount{lastFailureTime=undefined}),
                            dd_seco:register(SeCo, AccountId)  % return (hash) value to client
            end;
        Error -> 
            Error
    end.

login(SeKey) ->
    case dd_seco:seco(SeKey) of
        #ddSeCo{accountId=AccountId} = SeCo ->
            LocalTime = calendar:local_time(),
            AuthenticationMethod = pwdmd5,      %% ToDo: get it from security context
            PwdExpireSecs = calendar:datetime_to_gregorian_seconds(LocalTime),
            PwdExpireDate = calendar:gregorian_seconds_to_datetime(PwdExpireSecs-24*3600*?PASSWORD_VALIDITY),
            case {Result=if_get(SeCo, AccountId), AuthenticationMethod} of
                {#ddAccount{lastPasswordChangeTime=undefined}, pwdmd5} -> 
                    logout(SeKey),
                    {error,{"Password expired. Please change it", AccountId}};
                {#ddAccount{lastPasswordChangeTime=LastChange}, pwdmd5} when LastChange < PwdExpireDate -> 
                    logout(SeKey),
                    {error,{"Password expired. Please change it", AccountId}};
                {#ddAccount{}, _} ->
                    ok = dd_seco:update(SeCo,SeCo#ddSeCo{state=authorized}),
                    if_write(SeCo, Result#ddAccount{lastLoginTime=calendar:local_time()}),
                    SeKey;            
                {Error, _} ->                    
                    logout(SeKey),
                    Error
            end;
        Error  ->   Error
    end.

change_credentials(SeKey, {pwdmd5,_}=OldCred, {pwdmd5,_}=NewCred) ->
    case dd_seco:seco(SeKey) of
        #ddSeCo{accountId=AccountId} = SeCo ->
            LocalTime = calendar:local_time(),
            #ddAccount{credentials=CredList} = Account = if_get(SeCo, AccountId),
            if_write(SeCo, Account#ddAccount{lastPasswordChangeTime=LocalTime, credentials=[NewCred|lists:delete(OldCred,CredList)]}),
            login(SeKey);
        Error -> Error
    end;
change_credentials(SeKey, {CredType,_}=OldCred, {CredType,_}=NewCred) ->
    case dd_seco:seco(SeKey) of
        #ddSeCo{accountId=AccountId} = SeCo ->
            #ddAccount{credentials=CredList} = Account = if_get(SeCo, AccountId),
            if_write(SeCo, Account#ddAccount{credentials=[NewCred|lists:delete(OldCred,CredList)]}),
            login(SeKey);
        Error -> Error
    end.

logout(SeKey) ->
    dd_seco:delete(SeKey).


%% ----- TESTS ------------------------------------------------

setup() -> 
    application:start(imem).

teardown(_) -> 
    application:stop(imem).

account_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun test/1
            %%, fun test_create_account/1
        ]}}.    

    
test(_) ->
    io:format(user, "----TEST--~p:test_mnesia~n", [?MODULE]),

    ?assertEqual("Mnesia", if_schema()),
    io:format(user, "success ~p~n", [schema]),

    io:format(user, "----TEST--~p:test_create_seco_tables~n", [?MODULE]),

    ?assertEqual({atomic,ok}, dd_seco:create_cluster_tables(none)),
    io:format(user, "success ~p~n", [create_cluster_tables]),
    ?assertMatch({aborted,{already_exists,_}}, dd_seco:create_cluster_tables(none)),
    io:format(user, "success ~p~n", [create_account_table_already_exists]),
    ?assertEqual({atomic,ok}, dd_seco:create_local_tables(none)),
    io:format(user, "success ~p~n", [create_cluster_tables]),
    ?assertMatch({aborted,{already_exists,_}}, dd_seco:create_local_tables(none)),
    io:format(user, "success ~p~n", [create_account_table_already_exists]),

    UserId = make_ref(),
    UserName= <<"test_admin">>,
    UserCred={pwdmd5, erlang:md5(<<"t1e2s3t4_5a6d7m8i9n">>)},
    UserCredNew={pwdmd5, erlang:md5(<<"test_5a6d7m8i9n">>)},
    User = #ddAccount{id=UserId,name=UserName,credentials=[UserCred],fullName= <<"TestAdmin">>},

    ?assertEqual(ok, if_write(User)),
    io:format(user, "success ~p~n", [create_test_admin]), 
    ?assertEqual(ok, if_write(#ddRole{id=UserId,roles=[],permissions=[manage_accounts]})),
    io:format(user, "success ~p~n", [create_test_admin_role]),
    ?assertEqual([User], if_read(none, UserId)),
    io:format(user, "success ~p~n", [if_read]),
    ?assertEqual(User, if_get(none, UserId)),
    io:format(user, "success ~p~n", [if_get]),
    ?assertEqual(User, if_get_by_name(none, UserName)),
    io:format(user, "success ~p~n", [if_get_by_name]),
 
    io:format(user, "----TEST--~p:test_authentification~n", [?MODULE]),

    SeCo0=authenticate(someSessionId, UserName, UserCred),
    ?assertEqual(is_integer(SeCo0), true),
    io:format(user, "success ~p~n", [test_admin_authentification]), 
    ?assertEqual({error,{"Password expired. Please change it", UserId}}, login(SeCo0)),
    io:format(user, "success ~p~n", [new_password]),
    SeCo1=authenticate(someSessionId, UserName, UserCred), 
    ?assertEqual(is_integer(SeCo1), true),
    io:format(user, "success ~p~n", [test_admin_authentification]), 
    ?assertEqual(SeCo1, change_credentials(SeCo1, UserCred, UserCredNew)),
    io:format(user, "success ~p~n", [password_changed]), 
    ?assertEqual(true, dd_seco:have_permission(SeCo1, manage_accounts)), 
    ?assertEqual(false, dd_seco:have_permission(SeCo1, manage_bananas)), 
    ?assertEqual(true, dd_seco:have_permission(SeCo1, [manage_accounts])), 
    ?assertEqual(false, dd_seco:have_permission(SeCo1, [manage_bananas])), 
    ?assertEqual(true, dd_seco:have_permission(SeCo1, [manage_accounts,some_unknown_permission])), 
    ?assertEqual(false, dd_seco:have_permission(SeCo1, [manage_bananas,some_unknown_permission])), 
    ?assertEqual(true, dd_seco:have_permission(SeCo1, [some_unknown_permission,manage_accounts])), 
    ?assertEqual(false, dd_seco:have_permission(SeCo1, [some_unknown_permission,manage_bananas])), 
    io:format(user, "success ~p~n", [have_permission]),
    ?assertEqual(ok, logout(SeCo1)),
    io:format(user, "success ~p~n", [logout]), 
    SeCo2=authenticate(someSessionId, UserName, UserCredNew),
    ?assertEqual(is_integer(SeCo2), true),
    io:format(user, "success ~p~n", [test_admin_reauthentification]),
    ?assertEqual({error,{"Invalid security context",SeCo2}}, dd_seco:have_permission(SeCo2, manage_bananas)), 
    io:format(user, "success ~p~n", [have_permission_rejected]),
    ?assertEqual(SeCo2, login(SeCo2)),
    io:format(user, "success ~p~n", [login]),
    ?assertEqual(true, dd_seco:have_permission(SeCo2, manage_accounts)), 
    ?assertEqual(false, dd_seco:have_permission(SeCo2, manage_bananas)), 
    io:format(user, "success ~p~n", [have_permission]),
    ?assertEqual({error,{"Security context does not exist",SeCo1}}, dd_seco:have_permission(SeCo1, manage_accounts)), 
    io:format(user, "success ~p~n", [have_permission_rejected]),

    io:format(user, "----TEST--~p:test_manage_accounts~n", [?MODULE]),

    AccountId = make_ref(),
    AccountCred={pwdmd5, erlang:md5(<<"TestPwd">>)},
    AccountCredNew={pwdmd5, erlang:md5(<<"TestPwd1">>)},
    AccountName= <<"test">>,
    Account = #ddAccount{id=AccountId,name=AccountName,credentials=[AccountCred],fullName= <<"FullName">>},
    AccountId0 = make_ref(),
    Account0 = #ddAccount{id=AccountId0,name=AccountName,credentials=[AccountCred],fullName= <<"AnotherName">>},
    Account1 = Account#ddAccount{credentials=[AccountCredNew],fullName= <<"NewFullName">>,locked='true'},
    Account2 = Account#ddAccount{credentials=[AccountCredNew],fullName= <<"OldFullName">>},

    SeCo = SeCo2, %% belonging to user <<"test_admin">>

    ?assertEqual(ok, create(SeCo, Account)),
    io:format(user, "success ~p~n", [account_create]),
    ?assertEqual({error, {"Account already exists",AccountId}}, create(SeCo, Account)),
    io:format(user, "success ~p~n", [account_create_already_exists]), 
    ?assertEqual({error, {"Account name already exists for",<<"test">>}}, create(SeCo, Account0)),
    io:format(user, "success ~p~n", [account_create_name_already_exists]), 
    ?assertEqual(Account, get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [account_get]), 
    ?assertEqual(#ddRole{id=AccountId}, dd_role:get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [role_get]), 
    ?assertEqual(ok, delete(SeCo, AccountId)),
    io:format(user, "success ~p~n", [account_delete]), 
    ?assertEqual(ok, delete(SeCo, AccountId)),
    io:format(user, "success ~p~n", [account_delete_even_no_exists]), 
    ?assertEqual({error, {"Account does not exist", AccountId}}, delete(SeCo, Account)),
    io:format(user, "success ~p~n", [account_delete_no_exists]), 
    ?assertEqual(false, exists(SeCo, AccountId)),
    io:format(user, "success ~p~n", [account_no_exists]), 
    ?assertEqual({error, {"Account does not exist", AccountId}}, get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [account_get_no_exists]), 
    ?assertEqual({error, {"Role does not exist", AccountId}}, dd_role:get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [role_get_no_exists]), 
    ?assertEqual(ok, create(SeCo, Account)),
    io:format(user, "success ~p~n", [account_create]), 
    ?assertEqual({error, {"Account is modified by someone else", AccountId}}, delete(SeCo, Account1)),
    io:format(user, "success ~p~n", [account_delete_wrong_version]), 
    ?assertEqual(ok, delete(SeCo, Account)),
    io:format(user, "success ~p~n", [account_delete_with_check]), 
    ?assertEqual(ok, create(SeCo, Account)),
    io:format(user, "success ~p~n", [account_create]), 
    ?assertEqual(true, exists(SeCo, AccountId)),
    io:format(user, "success ~p~n", [account_exists]), 
    ?assertEqual(Account, get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [account_get]), 
    ?assertEqual(#ddRole{id=AccountId}, dd_role:get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [role_get]), 
    ?assertEqual(ok, update(SeCo, Account, Account1)),
    io:format(user, "success ~p~n", [update_account]), 
    ?assertEqual(Account1, get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [account_get_modified]), 
    ?assertEqual({error, {"Account is modified by someone else",AccountId}}, update(SeCo, Account, Account2)),
    io:format(user, "success ~p~n", [update_account_reject]), 
    ?assertEqual(Account1, get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [account_get_unchanged]), 
    ?assertEqual(false, dd_seco:has_permission(SeCo, AccountId, manage_accounts)), 
    ?assertEqual(false, dd_seco:has_permission(SeCo, AccountId, manage_bananas)), 
    io:format(user, "success ~p~n", [has_permission]),

    ?assertEqual({error,{"Account is locked. Contact a system administrator",<<"test">>}},authenticate(someSessionId, AccountName, AccountCredNew)), 
    io:format(user, "success ~p~n", [is_locked]),
    ?assertEqual(ok, unlock(SeCo, AccountId)),
    io:format(user, "success ~p~n", [unlock]),
    SeCo3=authenticate(someSessionId, AccountName, AccountCredNew),
    ?assertEqual(is_integer(SeCo3), true),
    io:format(user, "success ~p~n", [test_authentification]),
    ?assertEqual({error,{"Password expired. Please change it", AccountId}}, login(SeCo3)),
    io:format(user, "success ~p~n", [new_password]),
    SeCo4=authenticate(someSessionId, AccountName, AccountCredNew), 
    ?assertEqual(is_integer(SeCo4), true),
    io:format(user, "success ~p~n", [test_authentification]), 
    ?assertEqual(SeCo4, change_credentials(SeCo4, AccountCredNew, AccountCred)),
    io:format(user, "success ~p~n", [password_changed]), 
    ?assertEqual(true, dd_seco:have_role(SeCo4, AccountId)), 
    ?assertEqual(false, dd_seco:have_role(SeCo4, some_unknown_role)), 
    ?assertEqual(false, dd_seco:have_permission(SeCo4, manage_accounts)), 
    ?assertEqual(false, dd_seco:have_permission(SeCo4, manage_bananas)), 
    io:format(user, "success ~p~n", [have_permission]),

    io:format(user, "----TEST--~p:test_manage_account_rejectss~n", [?MODULE]),

    ?assertEqual({error, {"Drop system table unauthorized",SeCo4}}, dd_seco:drop_table(SeCo4,ddTable)),
    io:format(user, "success ~p~n", [drop_table_table_rejected]), 
    ?assertEqual({error, {"Drop system table unauthorized",SeCo4}}, dd_seco:drop_table(SeCo4,ddAccount)),
    io:format(user, "success ~p~n", [drop_account_table_rejected]), 
    ?assertEqual({error, {"Drop system table unauthorized",SeCo4}}, dd_seco:drop_table(SeCo4,ddRole)),
    io:format(user, "success ~p~n", [drop_role_table_rejected]), 
    ?assertEqual({error, {"Drop system table unauthorized",SeCo4}}, dd_seco:drop_table(SeCo4,ddSeCo)),
    io:format(user, "success ~p~n", [drop_seco_table_rejected]), 
    ?assertEqual({error, {"Create account unauthorized",SeCo4}}, create(SeCo4, Account)),
    ?assertEqual({error, {"Create account unauthorized",SeCo4}}, create(SeCo4, Account0)),
    ?assertEqual({error, {"Get account unauthorized",SeCo4}}, get(SeCo4, AccountId)),
    ?assertEqual({error, {"Delete account unauthorized",SeCo4}}, delete(SeCo4, AccountId)),
    ?assertEqual({error, {"Delete account unauthorized",SeCo4}}, delete(SeCo4, Account)),
    ?assertEqual({error, {"Exists account unauthorized",SeCo4}}, exists(SeCo4, AccountId)),
    ?assertEqual({error, {"Get role unauthorized",SeCo4}}, dd_role:get(SeCo4, AccountId)),
    ?assertEqual({error, {"Delete account unauthorized",SeCo4}}, delete(SeCo4, Account1)),
    ?assertEqual({error, {"Update account unauthorized",SeCo4}}, update(SeCo4, Account, Account1)),
    ?assertEqual({error, {"Update account unauthorized",SeCo4}}, update(SeCo4, Account, Account2)),
    io:format(user, "success ~p~n", [manage_accounts_rejected]),


    io:format(user, "----TEST--~p:test_manage_account_roles~n", [?MODULE]),

    ?assertEqual(true, dd_seco:has_role(SeCo, AccountId, AccountId)),
    io:format(user, "success ~p~n", [role_has_own_role]), 
    ?assertEqual(false, dd_seco:has_role(SeCo, AccountId, some_unknown_role)),
    io:format(user, "success ~p~n", [role_has_some_unknown_role]), 
    ?assertEqual({error, {"Role does not exist", some_unknown_role}}, dd_role:grant_role(SeCo, AccountId, some_unknown_role)),
    io:format(user, "success ~p~n", [role_grant_reject]), 
    ?assertEqual({error, {"Role does not exist", some_unknown_role}}, dd_role:grant_role(SeCo, some_unknown_role, AccountId)),
    io:format(user, "success ~p~n", [role_grant_reject]), 
    ?assertEqual(ok, dd_role:create(SeCo, admin)),
    io:format(user, "success ~p~n", [role_create_empty_role]), 
    ?assertEqual({error, {"Role already exists",admin}}, dd_role:create(SeCo, admin)),
    io:format(user, "success ~p~n", [role_create_existing_role]), 
    ?assertEqual(false, dd_seco:has_role(SeCo, AccountId, admin)),
    io:format(user, "success ~p~n", [role_has_not_admin_role]), 
    ?assertEqual(ok, dd_role:grant_role(SeCo, AccountId, admin)),
    io:format(user, "success ~p~n", [role_grant_admin_role]), 
    ?assertEqual(true, dd_seco:has_role(SeCo, AccountId, admin)),
    io:format(user, "success ~p~n", [role_has_admin_role]), 
    ?assertEqual(ok, dd_role:grant_role(SeCo, AccountId, admin)),
    io:format(user, "success ~p~n", [role_re_grant_admin_role]), 
    ?assertEqual(#ddRole{id=AccountId,roles=[admin]}, dd_role:get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [role_get]), 
    ?assertEqual(ok, dd_role:revoke_role(SeCo, AccountId, admin)),
    io:format(user, "success ~p~n", [role_revoke_admin_role]), 
    ?assertEqual(#ddRole{id=AccountId,roles=[]}, dd_role:get(SeCo, AccountId)),
    io:format(user, "success ~p~n", [role_get]),
    ?assertEqual(ok, dd_role:grant_role(SeCo, AccountId, admin)),
    io:format(user, "success ~p~n", [role_grant_admin_role]),      
    ?assertEqual(ok, dd_role:create(SeCo, #ddRole{id=test_role,roles=[],permissions=[perform_tests]})),
    io:format(user, "success ~p~n", [role_create_test_role]), 
    ?assertEqual(true, dd_seco:has_permission(SeCo, test_role, perform_tests)),
    io:format(user, "success ~p~n", [role_has_test_permission]), 
    ?assertEqual(false, dd_seco:has_permission(SeCo, test_role, stupid_permission)),
    io:format(user, "success ~p~n", [role_has_stupid_permission]), 
    ?assertEqual(false, dd_seco:has_role(SeCo, AccountId, test_role)),
    io:format(user, "success ~p~n", [role_has_test_role]), 
    ?assertEqual(false, dd_seco:has_permission(SeCo, AccountId, perform_tests)),
    io:format(user, "success ~p~n", [role_has_test_permission]), 
    ?assertEqual(ok, dd_role:grant_role(SeCo, admin, test_role)),
    io:format(user, "success ~p~n", [role_grant_test_role]), 
    ?assertEqual(true, dd_seco:has_role(SeCo, AccountId, test_role)),
    io:format(user, "success ~p~n", [role_has_test_role]), 
    ?assertEqual(true, dd_seco:has_permission(SeCo, AccountId, perform_tests)),
    ?assertEqual(true, dd_seco:has_permission(SeCo, AccountId, [perform_tests])),
    ?assertEqual(true, dd_seco:has_permission(SeCo, AccountId, [crap1,perform_tests,{crap2,read}])),
    io:format(user, "success ~p~n", [role_has_test_permission]), 

    io:format(user, "----TEST--~p:test_manage_account_role rejects~n", [?MODULE]),

    ?assertEqual({error, {"Create role unauthorized",SeCo4}}, dd_role:create(SeCo4, #ddRole{id=test_role,roles=[],permissions=[perform_tests]})),
    ?assertEqual({error, {"Create role unauthorized",SeCo4}}, dd_role:create(SeCo4, admin)),
    ?assertEqual({error, {"Get role unauthorized",SeCo4}}, dd_role:get(SeCo4, AccountId)),
    ?assertEqual({error, {"Grant role unauthorized",SeCo4}}, dd_role:grant_role(SeCo4, AccountId, admin)),
    ?assertEqual({error, {"Grant role unauthorized",SeCo4}}, dd_role:grant_role(SeCo4, AccountId, some_unknown_role)),
    ?assertEqual({error, {"Grant role unauthorized",SeCo4}}, dd_role:grant_role(SeCo4, admin, test_role)),
    ?assertEqual({error, {"Has role unauthorized",SeCo4}}, dd_seco:has_role(SeCo4, AccountId, AccountId)),
    ?assertEqual({error, {"Has role unauthorized",SeCo4}}, dd_seco:has_role(SeCo4, AccountId, admin)),
    ?assertEqual({error, {"Revoke role unauthorized",SeCo4}}, dd_role:revoke_role(SeCo4, AccountId, admin)),
    io:format(user, "success ~p~n", [manage_account_roles_rejects]), 

    io:format(user, "----TEST--~p:test_manage_account_permissions~n", [?MODULE]),

    ?assertEqual(ok, dd_role:grant_permission(SeCo, test_role, delete_tests)),
    io:format(user, "success ~p~n", [role_grant_test_role_delete_tests]), 
    ?assertEqual(ok, dd_role:grant_permission(SeCo, test_role, fake_tests)),
    io:format(user, "success ~p~n", [role_grant_test_role_fake_tests]), 
    ?assertEqual(true, dd_seco:has_permission(SeCo, AccountId, delete_tests)),
    io:format(user, "success ~p~n", [role_has_delete_tests_permission]), 
    ?assertEqual(true, dd_seco:has_permission(SeCo, AccountId, fake_tests)),
    io:format(user, "success ~p~n", [role_has_fake_tests_permission]), 
    ?assertEqual(true, dd_seco:has_permission(SeCo, admin, delete_tests)),
    io:format(user, "success ~p~n", [role_has_delete_tests_permission]), 
    ?assertEqual(true, dd_seco:has_permission(SeCo, admin, fake_tests)),
    io:format(user, "success ~p~n", [role_has_fake_tests_permission]), 
    ?assertEqual(true, dd_seco:has_permission(SeCo, test_role, delete_tests)),
    io:format(user, "success ~p~n", [role_has_delete_tests_permission]), 
    ?assertEqual(true, dd_seco:has_permission(SeCo, test_role, fake_tests)),
    io:format(user, "success ~p~n", [role_has_fake_tests_permission]), 
    ?assertEqual(ok, dd_role:revoke_permission(SeCo, test_role, delete_tests)),
    io:format(user, "success ~p~n", [role_revoke_test_role_delete_tests]), 
    ?assertEqual(false, dd_seco:has_permission(SeCo, AccountId, delete_tests)),
    io:format(user, "success ~p~n", [role_has_delete_tests_permission]), 
    ?assertEqual(false, dd_seco:has_permission(SeCo, admin, delete_tests)),
    io:format(user, "success ~p~n", [role_has_delete_tests_permission]), 
    ?assertEqual(false, dd_seco:has_permission(SeCo, test_role, delete_tests)),
    io:format(user, "success ~p~n", [role_has_delete_tests_permission]), 
    ?assertEqual(ok, dd_role:revoke_permission(SeCo, test_role, delete_tests)),
    io:format(user, "success ~p~n", [role_revoket_test_role_delete_tests]), 

    io:format(user, "----TEST--~p:test_manage_account_permission_rejects~n", [?MODULE]),

    ?assertEqual({error, {"Has permission unauthorized",SeCo4}}, dd_seco:has_permission(SeCo4, UserId, manage_accounts)), 
    ?assertEqual({error, {"Has permission unauthorized",SeCo4}}, dd_seco:has_permission(SeCo4, AccountId, perform_tests)),
    ?assertEqual({error, {"Grant permission unauthorized",SeCo4}}, dd_role:grant_permission(SeCo4, test_role, delete_tests)),
    ?assertEqual({error, {"Revoke permission unauthorized",SeCo4}}, dd_role:revoke_permission(SeCo4, test_role, delete_tests)),
    io:format(user, "success ~p~n", [test_manage_account_permission_rejects]), 


    %% Cleanup only if we arrive at this point
    ?assertEqual({error,{"Drop system tables unauthorized",SeCo}}, dd_seco:drop_system_tables(SeCo)),
    io:format(user, "success ~p~n", [drop_system_tables_reject]), 
    ?assertEqual(ok, dd_role:grant_permission(SeCo, UserId, manage_system_tables)),
    io:format(user, "success ~p~n", [grant_manage_system_tables]), 
    ?assertEqual({atomic,ok}, dd_seco:drop_system_tables(SeCo)),
    io:format(user, "success ~p~n", [drop_cluster_tables]), 
    ok.

