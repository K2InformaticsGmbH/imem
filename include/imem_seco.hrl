-ifndef(IMEM_SECO_HRL).
-define(IMEM_SECO_HRL, true).

-include("imem.hrl").
-include("imem_meta.hrl").
-include("imem_exports.hrl").

-type ddIdentity() :: system | binary().              % AccountName / Login user name
-type ddCredential() :: {pwdmd5,binary()}             % {pwdmd5, md5(password)} in ddAccount.credentials for storing hash separate from name
                      | {sha512,{binary(),binary()}}  % {sha512,{Salt,Hash}}    in ddAccount.credentials 
                      | {scrypt,{binary(),binary()}}  % {scrypt,{Salt,Hash}}    in ddAccount.credentials 
                      | {access,map()}                % {access,NetworkCtx}                   input to auth_start / auth_add_cred
                      | {pwdmd5,{binary(),binary()}}  % {pwdmd5, {AccountName,md5(password)}  input to auth_start / auth_add_cred
                      | {smsott,binary()}.            % {smsott,Token}                        input to auth_start / auth_add_cred 

-type ddCredRequest() :: {pwdmd5,map()}               % {pwdmd5,#{}} | {smsott,#{accountName=>AccountName}} (any / fixed AccountName) 
                       | {smsott,map()}               % {smsott,#{accountName=>AccountName,to=>To}}
                       | {access,map()}               % {access,#{}} (request for all network parameters, maybe more selective requests later)
                       | {saml,map()}.                % {saml,#{}}   (request for saml authenticate handshake, parameters TBD)

-type ddPermission() :: atom() | tuple().             % e.g. manage_accounts, {table,ddSeCo,select}
-type ddQuota() :: {atom(),any()}.                    % e.g. {max_memory, 1000000000}
-type ddSeCoKey() :: integer().                       % security context key ('random' hash)
-type ddAuthState() :: undefined | authenticated | authorized.

-record(ddAccount,  %% imem cluster account (shared by application)
                    { id                        ::ddEntityId() 
                    , name                      ::ddIdentity()        %% unique login id (mutable)
                    , type='user'               ::atom()              %% user | driver | deamon | application
                    , credentials=[]            ::[ddCredential()]  
                    , fullName                  ::binary()            %% simple binary name or profile as json binary                    
                    , lastLoginTime             ::ddDatetime()        %% deprecated
                    , lastFailureTime           ::ddDatetime()        %% last locking time for now
                    , lastPasswordChangeTime    ::ddDatetime()        %% change time (undefined or too old  => must change it now and reconnect)
                    , locked='false'            ::'true' | 'false'
                    }
       ).
-define(ddAccount, [userid,binstr,atom,list,binstr,datetime,datetime,datetime,boolean]).

-record(ddAccountDyn,  %% imem cluster account (dynamic state)
                    { id                        ::ddEntityId() 
                    , lastLoginTime             ::ddDatetime()        %% erlang time of last login success
                    , lastFailureTime           ::ddDatetime()        %% erlang time of last login failure (for existing account name)
                    , opts=[]                   ::list()
                    }
       ).
-define(ddAccountDyn, [userid,datetime,datetime,list]).

-record(ddRole,     %% hierarchy of roles with permissions and access privileges to connections and commands  
                    { id                        ::ddEntityId()            %% lookup starts with ddAccount.id, other roles are atoms
                    , roles=[]                  ::[atom()]                %% granted roles
                    , permissions=[]            ::[ddPermission()]        %% granted permissions
                    , quotas=[]                 ::[ddQuota()]             %% granted quotas
                    }
       ). 
-define(ddRole, [userid,list,list,list]).

-record(ddSessionCtx,    %% session context              
                    { appId                     :: atom()             %% imem | dderl | ...
                    , sessionId                 :: any()              %% phash2({dderl_session, self()})
                    , networkCtx=#{}            :: map()              %% network parameters in a map
                    }     
       ). 

-record(ddSeCo,     %% security context              
                    { skey                      :: ddSeCoKey()        %% random hash value
                    , pid                       :: pid()              %% caller physical id
                    , sessionCtx                :: #ddSessionCtx{}    %% {AppId, phash2({dderl_session, self()})}
                    , accountName               :: ddIdentity()       %% account name
                    , accountId                 :: ddEntityId()
                    , authFactors=[]            :: [atom()]           %% checked auth factors see ddCredential() 
                    , authTime                  :: ddTimestamp()      %% (re)authentication timestamp erlang:now()
                    , authState                 :: ddAuthState()      %% authentication state
                    , authOpts = []             :: list()             %% for proprietary extensions
                    }     
       ). 
-define(ddSeCo, [integer,pid,tuple,binstr,userid,list,timestamp,atom,list]).

-record(ddPerm,     %% acquired permission cache bag table             
                    { pkey                      :: {ddSeCoKey(), ddPermission()}  %% permission key
                    , skey                      :: ddSeCoKey()                    %% security key
                    , pid                       :: pid()                  
                    , value                     :: 'true' | 'false'
                    }
       ). 
-define(ddPerm, [tuple,integer,pid,boolean]).

-record(ddQuota,    %% security context              
                    { qkey                      :: {ddSeCoKey(),atom()}           %% quota key
                    , skey                      :: ddSeCoKey()                    %% security key
                    , pid                       :: pid()                  
                    , value                     :: any()                          %% granted quota
                    }
       ). 
-define(ddQuota, [tuple,integer,pid,term]).

-define(IMEM_SKEY_NAME,imem_skey).                                    % security key name in process context
-define(IMEM_SKEY_PUT(__SKey),erlang:put(?IMEM_SKEY_NAME,__SKey)).    % store security key in process dict
-define(IMEM_SKEY_GET,erlang:get(?IMEM_SKEY_NAME)).                   % retrieve security key from process dict

-define(SecurityException(Reason), ?THROW_EXCEPTION('SecurityException',Reason)).
-define(SecurityViolation(Reason), ?THROW_EXCEPTION('SecurityViolation',Reason)).

% -define(imem_system_login, fun() -> 
%     __MH = #ddAccount{name='$1', _='_'},
%     {[__S],true} = imem_meta:select(ddAccount, [{__MH,[{'==','$1',<<"system">>}],['$_']}]),
%     imem_seco:login(imem_seco:authenticate(adminSessionId, __S#ddAccount.name, hd(__S#ddAccount.credentials)))
% end 
% ).

-define(imem_test_admin_drop, fun() -> 
    __MH = #ddAccount{name='$1', _='_'},
    case imem_meta:select(ddAccount, [{__MH,[{'==','$1',<<"_test_admin_">>}],['$_']}]) of 
        {[__Ta],true} ->    catch (imem_meta:delete(ddRole, __Ta#ddAccount.id)),
                            catch (imem_meta:delete(ddAccountDyn, __Ta#ddAccount.id)),
                            catch (imem_meta:delete(ddAccount, __Ta#ddAccount.id)),
                            ok;
        {[],true} ->        ok
    end
end 
).

-define(imem_test_admin_login, fun() -> 
    ?imem_test_admin_drop(),    
    {pwdmd5,__Token} = imem_seco:create_credentials(pwdmd5,crypto:rand_bytes(50)),
    __AcId = imem_account:make_id(),
    imem_meta:write( ddAccount
                   , #ddAccount{ id=__AcId
                               , name= <<"_test_admin_">>
                               , fullName= <<"{\"MOBILE\":\"41794321750\"}">>
                               , credentials=[{pwdmd5,__Token}]
                               , lastPasswordChangeTime= calendar:local_time()
                               }
                   ),
    imem_meta:write( ddAccountDyn
                   , #ddAccountDyn{id=__AcId,lastLoginTime= calendar:local_time()}
                   ),
    imem_meta:write( ddRole
                   , #ddRole{ id=__AcId
                            , permissions=[ manage_system
                                          , manage_accounts
                                          , manage_system_tables
                                          , manage_user_tables
                                          ]
                            }
                   ),
    % login with old style authentication
    % imem_seco:login(imem_seco:authenticate(testAdminSessionId, <<"_test_admin_">>, {pwdmd5,__Token}))
    
    % login with new style authentication
    {__SKey,[]} = imem_seco:auth_start(imem, testAdminSessionId, {pwdmd5,{<<"_test_admin_">>,__Token}}),
    imem_seco:login(__SKey)
end 
).

-define(imem_test_admin_grant(__Perm),
  begin
    [#ddAccount{id=__AcId}] = imem_meta:dirty_index_read(ddAccount, <<"_test_admin_">>, #ddAccount.name),
    [#ddRole{permissions=__Perms0} = __Role0] = imem_meta:read(ddRole,__AcId),
    imem_meta:write(ddRole, __Role0#ddRole{permissions=lists:usort([__Perm|__Perms0])})
  end 
).

-define(imem_test_admin_revoke(__Perm),
  begin
    [#ddAccount{id=__AcId}] = imem_meta:dirty_index_read(ddAccount, <<"_test_admin_">>, #ddAccount.name),
    [#ddRole{permissions=__Perms0} = __Role0] = imem_meta:read(ddRole,__AcId),
    imem_meta:write(ddRole, __Role0#ddRole{permissions=lists:delete(__Perm,__Perms0)})
  end 
).

-define(imem_logout, fun(__X) -> imem_seco:logout(__X) end).

-endif.
