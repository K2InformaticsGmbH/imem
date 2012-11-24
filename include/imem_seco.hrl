
-include("imem_meta.hrl").

-type ddIdentity() :: binary().                 %% Account name
-type ddCredential() :: {pwdmd5, binary()}.     %% {pwdmd5, md5(password)} for now
-type ddPermission() :: atom() | tuple().       %% e.g. manage_accounts, {table,ddSeCo,select}
-type ddQuota() :: {atom(),any()}.              %% e.g. {max_memory, 1000000000}
-type ddSeCoKey() :: integer().                 %% security context key ('random' hash)

-record(ddAccount,                          %% imem cluster account (shared by application)
                  { id                      ::ddEntityId() 
                  , name                    ::ddIdentity()        %% unique login id (mutable)
                  , type='user'             ::atom()              %% user | driver | deamon | application 
                  , credentials             ::[ddCredential()]  
                  , fullName                ::binary()                    
                  , lastLoginTime           ::ddDatetime()        %% erlang time of last login success
                  , lastFailureTime         ::ddDatetime()        %% erlang time of last login failure (for existing account name)
                  , lastPasswordChangeTime  ::ddDatetime()        %% change time (undefined or too old  => must change it now and reconnect)
                  , locked='false'          ::'true' | 'false'
                  }
       ).
-define(ddAccount, [any,binary,atom,list,binary,ddDatetime,ddDatetime,ddDatetime,boolean]).

-record(ddRole,                             %% hierarchy of roles with permissions and access privileges to connections and commands  
                  { id                      ::ddEntityId()            %% lookup starts with ddAccount.id, other roles are atoms
                  , roles=[]                ::[atom()]                %% granted roles
                  , permissions=[]          ::[ddPermission()]        %% granted permissions
                  , quotas=[]               ::[ddQuota()]             %% granted quotas
                  }
       ). 
-define(ddRole, [any,list,list,list]).

-record(ddSeCo,                             %% security context              
                  { skey                    :: ddSeCoKey()        %% random hash value
                  , pid                     :: any()              %% caller physical id
                  , sessionId               :: integer()          %% erlang:phash2({dderl_session, self()})
                  , name                    :: ddIdentity()       %% account name
                  , accountId               :: ddEntityId()
                  , authMethod              :: atom()             %% see ddCredential() 
                  , authTime                :: ddTimestamp()      %% (re)authentication timestamp erlang:now()
                  , state                   :: any()              %% authentication state
                  }     
       ). 
-define(ddSeCo, [integer,pid,integer,binary,ref,atom,ddTimestamp,any]).

-record(ddPerm,                             %% acquired permission cache bag table             
                  { pkey                    :: {ddSeCoKey(), ddPermission()}  %% permission key
                  , skey                    :: ddSeCoKey()                    %% security key
                  , pid                     :: pid()                  
                  , value                   :: 'true' | 'false'
                  }
       ). 
-define(ddPerm, [tuple,integer,pid,boolean]).

-record(ddQuota,                            %% security context              
                  { qkey                    :: {ddSeCoKey(),atom()}           %% quota key
                  , skey                    :: ddSeCoKey()                    %% security key
                  , pid                     :: pid()                  
                  , value                   :: any()                          %% granted quota
                  }
       ). 
-define(ddQuota, [tuple,integer,pid,any]).

-define(SecurityException(Reason), throw({'SecurityException',Reason})).
-define(SecurityViolation(Reason), exit({'SecurityViolation',Reason})).

-define(PasswordChangeNeeded, "Password expired. Please change it").

-define(imem_test_admin_login, fun() -> MatchHead = #ddAccount{name='$1', _='_'},
      Guard = {'==', '$1', <<"admin">>},
      Result = '$_',
      {[#ddAccount{credentials=[AdminCred|_]}],true} = imem_meta:select(ddAccount, [{MatchHead, [Guard], [Result]}]),    
      SKey = imem_seco:authenticate(adminSessionId, <<"admin">>, AdminCred),
      imem_seco:login(SKey)
    end 
    ).

-define(imem_logout, fun(X) -> imem_seco:logout(X) end).
