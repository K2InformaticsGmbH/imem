
-include("imem_if.hrl").

-type ddEntityId() :: 	reference() | atom().
-type ddType() ::		atom(). 			%% any | list | tuple | integer | float | binary | string | json | xml                  


-record(ddColumn,                           %% column definition    
                  { name                    ::atom()
                  , type              		  ::ddType()
                  , length					        ::integer()
                  , precision				        ::integer()
                  , opts = []               ::list()
                  }
        ).

-record(ddColMap,                           %% column map entry
                  { tag                     ::any()
                  , schema                  ::atom()
                  , table                   ::atom()
                  , name                    ::atom()    
                  , tind = 0                ::integer()               
                  , cind = 0                ::integer()               
                  , type                    ::ddType()
                  , length                  ::integer()
                  , precision               ::integer()
                  }                  
       ).

-record(ddTable,                            %% table    
                  { qname                   ::{atom(),atom()}		%% {Schema,Table}
                  , columns                 ::list(#ddColumn{})
                  , opts = []               ::list()      
                  , owner                   ::ddEntityId()        	%% AccountId of creator / owner
                  , readonly='false'        ::'true' | 'false'
                  }
       ).

-define(UnimplementedException(Reason), throw({'UnimplementedException',Reason})).
-define(ConcurrencyException(Reason), throw({'ConcurrencyException',Reason})).
