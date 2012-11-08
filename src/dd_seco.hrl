
-include("dd_meta.hrl").

-type ddTimestamp() :: 'undefined' | {integer(), integer(), integer()}.
-type ddDatetime() :: 'undefined' | {{integer(), integer(), integer()},{integer(), integer(), integer()}}.
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

-record(ddRole,                             %% hierarchy of roles with permissions and access privileges to connections and commands  
                  { id                      ::ddEntityId()            %% lookup starts with ddAccount.id, other roles are atoms
                  , roles=[]                ::[atom()]                %% granted roles
                  , permissions=[]          ::[ddPermission()]        %% granted permissions
                  , quotas=[]               ::[ddQuota()]             %% granted quotas
                  }
       ). 

-record(ddSeCo,                             %% security context              
                  { key                     :: ddSeCoKey()        %% random hash value
                  , pid                     :: any()              %% caller physical id
                  , sessionId               :: integer()          %% erlang:phash2({dderl_session, self()})
                  , name                    :: ddIdentity()       %% account name
                  , accountId               :: ddEntityId()
                  , authMethod              :: atom()             %% see ddCredential() 
                  , authTime                :: ddTimestamp()      %% (re)authentication timestamp erlang:now()
                  , state                   :: any()              %% authentication state
                  }     
       ). 

-record(ddPerm,                             %% acquired permission cache bag table             
                  { key                     :: ddSeCoKey()
                  , permission              :: ddPermission()
                  }
       ). 

-record(ddQuota,                            %% security context              
                  { key                     ::ddSeCoKey()         %% random hash value
                  , quota                   ::ddQuota()           %% granted quota
                  }
       ). 

-define(SecurityException(Reason), throw({'SecurityException',Reason})).
-define(SecurityViolation(Reason), exit({'SecurityViolation',Reason})).
