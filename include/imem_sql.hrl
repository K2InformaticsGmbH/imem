
-define(ComparisonOperators, ['==','/=','>=','=<','<','>']).	%% as supported here in matchspecs 

-record(scanSpec,                                   %% scanner specification 
                    { sspec = []                	::list()            %% scan matchspec for raw scan  [{MatchHead, Guards, [Result]}]
                    , sbinds = []               	::list()            %% map for binding the scan Guards to meta values
                    , fguard = true               ::tuple()|true      %% condition tree for filtering scan results
                    , mbinds = []                 ::list()            %% map for binding the filter guard to meta values
                    , fbinds = []              	::list()      		%% map for binding the filter guard to main fields
                    , limit = 10000               ::integer()         %% limit the total number or returned rows approximately
                    }).


-record(statement,                                  %% Select statement 
                    { tables = []                   ::list()            %% first one is master table
                    , blockSize = 100               ::integer()         %% get data in chunks of (approximately) this size
                    , stmtStr = ""                  ::string()          %% SQL statement (optional)
                    , stmtParse = undefined         ::any()             %% SQL parse tree
                    , colMaps = []                  ::list(#ddColMap{}) %% column map 
                    , metaFields = []               ::list(atom())      %% list of meta_field names needed by RowFun
                    , rowFun                        ::fun()             %% rendering fun for row {table recs} -> [field values]
                    , mainSpec = undefined          ::list()            %% how to find master records
                    , joinSpecs = []                ::list()            %% how to find joined records list({MatchSpec,Binds})
                    }).



