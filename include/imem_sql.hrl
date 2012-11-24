
-include("imem_seco.hrl").

-record(statement,                                  %% Select statement 
                    { tables = []                   ::list()        %% first one is master table
                    , block_size = 100              ::integer()     %% get data in chunks of (approximately) this size
                    , limit = 0                     ::integer()     %% limit the total number or returned rows approximately
                    , stmt_str = ""                 ::string()      %% SQL statement (optional)
                    , stmt_parse = undefined        ::any()         %% SQL parse tree
                    , cols = []                     ::list()        %% column infos
                    , rowfun                        ::fun()         %% rendering fun for row
                    , matchspec = undefined 		::list()        %% how to find master records
                    , joinspec = []                 ::list()        %% how to find joined records
                    , cont = undefined  	        ::any()         %% traversal state continuation
                    , key = ?sot                    ::any()
                    }).
