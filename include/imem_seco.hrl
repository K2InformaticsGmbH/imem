-ifndef(IMEM_SECO_HRL).

-define(IMEM_SECO_HRL, true).

-include("imem.hrl").
-include("imem_meta.hrl").
-include("imem_exports.hrl").

-type ddIdentity() :: system | binary().

% AccountName / Login user name
-type ddCredential() :: % {pwdmd5, md5(password)} in ddAccount.credentials for storing hash separate from name
                      % {sha512,{Salt,Hash}}    in ddAccount.credentials
                      % {scrypt,{Salt,Hash}}    in ddAccount.credentials
                      % {access,NetworkCtx}                   input to auth_start / auth_add_cred
                      % {pwdmd5, {AccountName,md5(password)}  input to auth_start / auth_add_cred
                      % {smsott,Token}                        input to auth_start / auth_add_cred
                      {pwdmd5, binary()}
                      | {sha512, {binary(), binary()}}
                      | {scrypt, {binary(), binary()}}
                      | {access, map()}
                      | {pwdmd5, {binary(), binary()}}
                      | {smsott, binary()}
                      | {saml, binary()}.

% {saml, AccountName}                   input to auth_start / auth_add_cred
-type ddCredRequest() :: % {pwdmd5,#{}} | {smsott,#{accountName=>AccountName}} (any / fixed AccountName)
                       % {smsott,#{accountName=>AccountName,to=>To}}
                       % {access,#{}} (request for all network parameters, maybe more selective requests later)
                       {pwdmd5, map()} | {smsott, map()} | {access, map()} | {saml, map()}.

% {saml,#{}}   (request for saml authenticate handshake, parameters TBD)
-type ddPermission() :: atom() | tuple().

% e.g. manage_accounts, {table,ddSeCo,select}
-type ddQuota() :: {atom(), any()}.

% e.g. {max_memory, 1000000000}
-type ddSeCoKey() :: integer().

% security context key ('random' hash)
-type ddAuthState() :: undefined | authenticated | authorized.

-record(
    %% imem cluster account (shared by application)
    ddAccount,
    {
        id :: ddEntityId(),
        %% unique login id (mutable)
        name :: ddIdentity(),
        %% user | driver | deamon | application
        type = user :: atom(),
        credentials = [] :: [ddCredential()],
        %% simple binary name or profile as json binary
        fullName :: binary(),
        %% deprecated
        lastLoginTime :: ddDatetime(),
        %% last locking time for now
        lastFailureTime :: ddDatetime(),
        %% change time (undefined or too old  => must change it now and reconnect)
        lastPasswordChangeTime :: ddDatetime(),
        locked = false :: true | false
    }
).

-define(ddAccount, [userid, binstr, atom, list, binstr, datetime, datetime, datetime, boolean]).

-record(
    %% imem cluster account (dynamic state)
    ddAccountDyn,
    {
        id :: ddEntityId(),
        %% erlang time of last login success
        lastLoginTime :: ddDatetime(),
        %% erlang time of last login failure (for existing account name)
        lastFailureTime :: ddDatetime(),
        opts = [] :: list()
    }
).

-define(ddAccountDyn, [userid, datetime, datetime, list]).

-record(
    %% hierarchy of roles with permissions and access privileges to connections and commands
    ddRole,
    {
        %% lookup starts with ddAccount.id, other roles are atoms
        id :: ddEntityId(),
        %% granted roles
        roles = [] :: [atom()],
        %% granted permissions
        permissions = [] :: [ddPermission()],
        %% granted quotas
        quotas = [] :: [ddQuota()]
    }
).

-define(ddRole, [userid, list, list, list]).

-record(
    %% session context
    ddSessionCtx,
    {
        %% imem | dderl | ...
        appId :: atom(),
        %% phash2({dderl_session, self()})
        sessionId :: any(),
        %% network parameters in a map
        networkCtx = #{} :: map()
    }
).
-record(
    %% security context
    ddSeCo,
    {
        %% random hash value
        skey :: ddSeCoKey(),
        %% caller physical id
        pid :: pid(),
        %% {AppId, phash2({dderl_session, self()})}
        sessionCtx :: #ddSessionCtx{},
        %% account name
        accountName :: ddIdentity(),
        accountId :: ddEntityId(),
        %% checked auth factors see ddCredential()
        authFactors = [] :: [atom()],
        %% (re)authentication timestamp ?TIMESTAMP
        authTime :: ddTimestamp(),
        %% authentication state
        authState :: ddAuthState(),
        %% for proprietary extensions
        authOpts = [] :: list()
    }
).

-define(ddSeCo, [integer, pid, tuple, binstr, userid, list, timestamp, atom, list]).

-record(
    %% acquired permission cache bag table
    ddPerm,
    {
        %% permission key
        pkey :: {ddSeCoKey(), ddPermission()},
        %% security key
        skey :: ddSeCoKey(),
        pid :: pid(),
        value :: true | false
    }
).

-define(ddPerm, [tuple, integer, pid, boolean]).

-record(
    %% security context
    ddQuota,
    {
        %% quota key
        qkey :: {ddSeCoKey(), atom()},
        %% security key
        skey :: ddSeCoKey(),
        pid :: pid(),
        %% granted quota
        value :: any()
    }
).

-define(ddQuota, [tuple, integer, pid, term]).
-define(IMEM_SKEY_NAME, imem_skey).

% security key name in process context
-define(IMEM_SKEY_PUT(__SKey), erlang:put(?IMEM_SKEY_NAME, __SKey)).

% store security key in process dict
-define(IMEM_SKEY_GET, erlang:get(?IMEM_SKEY_NAME)).

% retrieve security key from process dict
-define(IMEM_SKEY_GET_FUN, fun () -> erlang:get(?IMEM_SKEY_NAME) end).

% retrieve security key from process dict
-define(SecurityException(Reason), ?THROW_EXCEPTION('SecurityException', Reason)).
-define(SecurityViolation(Reason), ?THROW_EXCEPTION('SecurityViolation', Reason)).

% -define(imem_system_login, fun() ->
%     __MH = #ddAccount{name='$1', _='_'},
%     {[__S],true} = imem_meta:select(ddAccount, [{__MH,[{'==','$1',<<"system">>}],['$_']}]),
%     imem_seco:login(imem_seco:authenticate(adminSessionId, __S#ddAccount.name, hd(__S#ddAccount.credentials)))
% end
% ).
-define(
    imem_test_admin_drop,
    fun
        () ->
            __MH = #ddAccount{name = '$1', _ = '_'},
            case imem_meta:select(ddAccount, [{__MH, [{'==', '$1', <<"_test_admin_">>}], ['$_']}]) of
                {[__Ta], true} ->
                    catch (imem_meta:delete(ddRole, __Ta#ddAccount.id)),
                    catch (imem_meta:delete(ddAccountDyn, __Ta#ddAccount.id)),
                    catch (imem_meta:delete(ddAccount, __Ta#ddAccount.id)),
                    ok;
                {[], true} -> ok
            end
    end
).
-define(
    imem_test_admin_login,
    fun
        () ->
            ?imem_test_admin_drop(),
            {pwdmd5, __Token} = imem_seco:create_credentials(pwdmd5, crypto:strong_rand_bytes(50)),
            __AcId = imem_account:make_id(),
            imem_meta:write(
                ddAccount,
                #ddAccount{
                    id = __AcId,
                    name = <<"_test_admin_">>,
                    fullName = <<"{\"MOBILE\":\"41794321750\"}">>,
                    credentials = [{pwdmd5, __Token}],
                    lastPasswordChangeTime = calendar:local_time()
                }
            ),
            imem_meta:write(
                ddAccountDyn,
                #ddAccountDyn{id = __AcId, lastLoginTime = calendar:local_time()}
            ),
            imem_meta:write(
                ddRole,
                #ddRole{
                    id = __AcId,
                    permissions =
                        [manage_system, manage_accounts, manage_system_tables, manage_user_tables]
                }
            ),
            % login with old style authentication
            % imem_seco:login(imem_seco:authenticate(testAdminSessionId, <<"_test_admin_">>, {pwdmd5,__Token}))
            % login with new style authentication
            {__SKey, []} =
                imem_seco:auth_start(
                    imem,
                    testAdminSessionId,
                    {pwdmd5, {<<"_test_admin_">>, __Token}}
                ),
            imem_seco:login(__SKey)
    end
).
-define(
    imem_test_admin_grant,
    fun
        (__Perm) ->
            [#ddAccount{id = __AcId}] =
                imem_meta:dirty_index_read(ddAccount, <<"_test_admin_">>, #ddAccount.name),
            [#ddRole{permissions = __Perms0} = __Role0] = imem_meta:read(ddRole, __AcId),
            imem_meta:write(ddRole, __Role0#ddRole{permissions = lists:usort([__Perm | __Perms0])})
    end
).
-define(
    imem_test_admin_revoke,
    fun
        (__Perm) ->
            [#ddAccount{id = __AcId}] =
                imem_meta:dirty_index_read(ddAccount, <<"_test_admin_">>, #ddAccount.name),
            [#ddRole{permissions = __Perms0} = __Role0] = imem_meta:read(ddRole, __AcId),
            imem_meta:write(ddRole, __Role0#ddRole{permissions = lists:delete(__Perm, __Perms0)})
    end
).
-define(imem_logout, fun (__X) -> imem_seco:logout(__X) end).
-endif.
