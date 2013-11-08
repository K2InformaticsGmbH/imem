
-define(ComparisonOperators, ['==','/=','>=','=<','<','>']).	%% as supported here in matchspecs 

-define(NoFilter,{undefined,[]}).   %% empty filter spec $$$ also used in erlimem_fsm.erl $$$
-define(NoMoreFilter,{_,[]}).       %% empty filter spec $$$ also used in erlimem_fsm.erl $$$

-define(EmptyWhere, {}).            %% empty where in the parse tree

-record(scanSpec,                                   %% scanner specification 
                    { sspec = []                	::list()            %% scan matchspec for raw scan  [{MatchHead, Guards, [Result]}]
                    , sbinds = []               	::list()            %% map for binding the scan Guards to meta values
                    , fguard = true               ::tuple()|true      %% condition tree for filtering scan results
                    , mbinds = []                 ::list()            %% map for binding the filter guard to meta values
                    , fbinds = []              	  ::list()      		  %% map for binding the filter guard to main fields
                    , limit = undefined           ::integer()         %% limit the total number or returned rows approximately
                    }).


-record(ddColMap,                           %% column map entry
                  { tag = ""                ::any()
                  , schema                  ::atom()
                  , table                   ::atom()
                  , name                    ::atom()
                  , alias                   ::binary()    
                  , tind = 0                ::integer()               
                  , cind = 0                ::integer()               
                  , type = term             ::atom()
                  , len = 0                 ::integer()
                  , prec = 0                ::integer()
                  , default                 ::any()
                  , readonly = false        ::true|false
                  , func = undefined        ::any()
                  , ptree = undefined       ::any()     
                  }                  
       ).

-record(statement,                                  %% Select statement 
                    { tables = []                   ::list()            %% first one is master table others lookup joins
                    , blockSize = 100               ::integer()         %% get data in chunks of (approximately) this size
                    , stmtStr = ""                  ::string()          %% SQL statement (optional)
                    , stmtParse = undefined         ::any()             %% SQL parse tree
                    , colMaps = []                  ::list(#ddColMap{}) %% column map
                    , fullMaps = []                 ::list(#ddColMap{}) %% full map
                    , metaFields = []               ::list(atom())      %% list of meta_field names needed by RowFun
                    , rowFun                        ::fun()             %% rendering fun for row {table recs} -> [ResultValues]
                    , sortFun                       ::fun()             %% rendering fun for sorting {table recs} -> SortColumn
                    , sortSpec = []                 ::list()
                    , mainSpec = undefined          ::list()            %% how to find master records
                    , joinSpecs = []                ::list()            %% how to find joined records list({MatchSpec,Binds})
                    }).

-record(stmtCol,                                    %% simplified column map for client
                  { tag                             ::any()
                  , alias                           ::binary()          %% column name or expression
                  , type = term                     ::atom()
                  , len                             ::integer()
                  , prec                            ::integer()
                  , readonly                        ::true|false
                  }                  
       ).

-record(stmtResult,                                 %% result record for exec function call
                  { rowCount = 0                    %% RowCount
                  , stmtRef = undefined             %% id needed for fetching
                  , stmtCols = undefined            ::list(#stmtCol{})  %% simplified column map of main statement
                  , rowFun  = undefined             ::fun()             %% rendering fun for row {key rec} -> [ResultValues]
                  , sortFun = undefined             ::fun()             %% rendering fun for sorting {key rec} -> SortColumn
                  , sortSpec = []                   ::list()
                  }
       ).

-define(TAIL_VALID_OPTS, [fetch_mode, tail_mode]).
