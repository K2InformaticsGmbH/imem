
-include("imem_if.hrl").

-type ddEntityId() :: 	reference() | atom().
-type ddType() ::		atom(). 			%% any | list | tuple | integer | float | binary | string | json | xml                  
-type ddTimestamp() :: 'undefined' | {integer(), integer(), integer()}.
-type ddDatetime() :: 'undefined' | {{integer(), integer(), integer()},{integer(), integer(), integer()}}.

-record(statement,                                  %% Select statement 
                    { tables = []                   ::list()        %% first one is master table
                    , block_size = 100              ::integer()     %% get data in chunks of (approximately) this size
                    , limit = 0                     ::integer()     %% limit the total number or returned rows approximately
                    , stmt_str = ""                 ::string()      %% SQL statement (optional)
                    , stmt_parse = undefined        ::any()         %% SQL parse tree
                    , cols = []                     ::list()        %% column infos
                    , rowfun                        ::fun()         %% rendering fun for row
                    , matchspec = undefined     ::list()        %% how to find master records
                    , joinspec = []                 ::list()        %% how to find joined records
                    , cont = undefined            ::any()         %% traversal state continuation
                    , key = ?sot                    ::any()
                    }).

-record(ddColumn,                           %% column definition    
                  { name                    ::atom()
                  , type              		  ::ddType()
                  , length					        ::integer()
                  , precision				        ::integer()
                  , default                 ::any()
                  , opts = []               ::list()
                  }
        ).

-record(ddColMap,                           %% column map entry
                  { tag                     ::any()
                  , oname                   ::binary()    %% original name
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
-define(ddTable, [tuple,list,list,any,boolean]).

-record(dual,                               %% table    
                  { x                       ::integer()   
                  , y                       ::integer()
                  }
       ).
-define(dual, [integer,integer]).
