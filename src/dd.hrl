
-type ddTimestamp() :: 'undefined' | {integer(), integer(), integer()}.
-type ddDatetime() :: 'undefined' | {{integer(), integer(), integer()},{integer(), integer(), integer()}}.
-type ddEntityId() :: reference() | atom().                    
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

-record(ddTable,                            %% table    
                  { id                      ::atom()
                  , recinfo                 ::list()
                  , opts = []               ::list()      
                  , owner                   ::ddEntityId()        %% binary account.name of creator / owner
                  , readonly='false'        ::'true' | 'false'
                  }
       ).

-record(ddAdapter,                          %% DDerl adapter (connect to databases)              
                  { id                      :: atom()             %% oci | imem | ets | os_text | dfs_text | hdfs_text
                  , fullName                :: string()           %% displayed in drop down box
                  }
       ). 

-record(ddInterface,                        %% DDerl client interface (connect to ui / applications)               
                  { id                      :: atom()             %% ddjson
                  , fullName                :: string()           %% displayed in drop down box
                  }
       ).

-record(dbConn,                             %% DB connection    
                  { id                      ::ddEntityId()       
                  , name                    ::binary()          %% connection name (mutable)
                  , owner                   ::ddEntityId()      %% account.id of creator / owner
                  , adapter                 ::atom()            %% oci | imem | ets | os_text | dfs_text | hdfs_text
                  , access                  ::any()             %% erlang term depending on adapter (e.g. ip+service or tns)
                  , schema                  ::any()             %% erlang term depending on adapter (e.g. name or uri or data root path)
                  }
       ).
      
-record(dbCmd,                              %% DB command     
                  { id                      ::ddEntityId()       
                  , name                    ::binary()          %% command template name (mutable)
                  , owner                   ::ddEntityId()      %% account.id of creator / owner
                  , adapters                ::[atom()]          %% can be used for this list of ddAdap
                  , conns                   ::[ddEntityId()]    %% can be used for this list of dbConn references
                  , command                 ::string()          %% erlang term depending on adapter (e.g. SQL text)
                  , opts                    ::any()             %% command options ()
                  }
       ).

-record(ddView,                             %% user representation of a db command including rendering parameters
                  { id                      ::ddEntityId()
                  , interface               ::atom()            %% interface plugin (ddjson for now)  
                  , owner                   ::ddEntityId()      %% account.id of creator / owner
                  , name                    ::binary()          %% should default to command name
                  , cmd                     ::ddEntityId()      %% db command id
                  , state                   ::any()             %% transparent viewstate (managed by client application)
                  }
       ).

-record(ddDash,                             %% user representation of a dashboard (collection of views)
                  { id                      ::ddEntityId()
                  , interface               ::atom()            %% interface plugin (ddjson for now)  
                  , owner                   ::ddEntityId()      %% account.id of creator / owner
                  , name                    ::binary()          %% should default to command name
                  , views                   ::[ddEntityId()]    %% ddView.ids
                  }
       ).

-define(SYSTEM_TABLES,[ddTable,ddAccount,ddRole,ddSeCo,ddPerm,ddQuota]).