
-define(ComparisonOperators, ['==','/=','>=','=<','<','>']).	%% as supported here in matchspecs 

-record(scanSpec,                                   %% scanner specification 
                    { sspec = []                	::list()            %% scan matchspec for raw scan  [{MatchHead, Guards, [Result]}]
                    , sbinds = []               	::list()            %% map for binding the scan Guards to meta fields
                    , fguard = true               ::tuple()|true      %% condition tree for filtering scan results
                    , mbinds = []                 ::list()            %% map for binding the filter guard to meta fields
                    , fbinds = []              	::list()      		%% map for binding the filter guard to scan fields
                    }).


-record(statement,                                  %% Select statement 
                    { tables = []                   ::list()            %% first one is master table
                    , block_size = 100              ::integer()         %% get data in chunks of (approximately) this size
                    , limit = 100000                ::integer()         %% limit the total number or returned rows approximately
                    , stmt_str = ""                 ::string()          %% SQL statement (optional)
                    , stmt_parse = undefined        ::any()             %% SQL parse tree
                    , cols = []                     ::list(#ddColMap{}) %% column map 
                    , meta = []                     ::list(atom())      %% list of meta_field names needed by RowFun
                    , rowfun                        ::fun()             %% rendering fun for row {table recs} -> [field values]
                    , scanspec = undefined          ::list()            %% how to find master records
                    , joinspecs = []                ::list()            %% how to find joined records list({MatchSpec,Binds})
                    }).



