
-define(ComparisonOperators, ['==','/=','>=','=<','<','>']).	%% as supported here in matchspecs 

-define(NoFilter,{undefined,[]}).   %% empty filter spec $$$ also used in erlimem_fsm.erl $$$
-define(NoMoreFilter,{_,[]}).       %% empty filter spec $$$ also used in erlimem_fsm.erl $$$
-define(FilterTrue, fun(_) -> true end).
-define(FilterFalse, fun(_) -> false end).


-define(RecIdx, 1).                                       %% Record name position in records
-define(KeyIdx, 2).                                       %% Key position in records
-define(MetaIdx, 1).                                      %% Meta record (constants) is placed as first tuple in the SQL result
-define(MainIdx, 2).                                      %% The main record is placed as second tuple in the SQL result
-define(TableIdx(__N), 1+__N).                            %% Tables are numbered 0=Meta, 1=Main, 2=FirstJoinTable ...
-define(MetaMain(__Meta,__Main), {__Meta,__Main}).        %% contstruct initial result tuple with meta and main table
-define(Meta(__Rec), element(?MetaIdx,__Rec)).            %% pick meta tuple out of master tuple
-define(Main(__Rec), element(?MainIdx,__Rec)).            %% pick main tuple (main table) out of master tuple 
-define(Table(__N,__Rec), element(?TableIdx(__N),__Rec)). %% pick table N tuple out for master tuple

-define(BoundVal(__Bind,__X), 
          element(__Bind#bind.cind,element(__Bind#bind.tind,__X))
        ).

-define(EmptyWhere, {}).            %% empty where in the parse tree

-record(bind,                               %% bind record, column map entry
                  { tag = ""                ::any()
                  , schema                  ::atom()
                  , table                   ::atom()
                  , name                    ::atom()
                  , alias                   ::binary()    
                  , tind = 0                ::integer()               
                  , cind = 0                ::integer()               
                  , type                    ::atom()
                  , len = 0                 ::integer()
                  , prec = 0                ::integer()
                  , default                 ::any()
                  , readonly = false        ::true|false
                  , func                    ::any()
                  , ptree                   ::any()
                  , btree                   ::any()     
                  }                  
       ).

-record(scanSpec,                                   %% scanner specification 
                    { sspec = []                  ::list()              %% scan matchspec for raw scan  [{MatchHead, Guards, [Result]}]
                    , sbinds = []                 ::list(#bind{})       %% map for binding the scan Guards to meta values
                    , fguard = true               ::tuple()|true|false  %% condition tree for filtering scan results
                    , mbinds = []                 ::list(#bind{})       %% map for binding the filter guard to meta values
                    , fbinds = []                 ::list(#bind{})       %% map for binding the filter guard to main fields
                    , limit = undefined           ::integer()|undefined %% limit the total number or returned rows approximately
                    }).

-record(statement,                                  %% Select statement 
                    { tables = []                   ::list({atom(),atom(),atom()})  %% {Schema,Name,Alias} first one is master table others lookup joins
                    , blockSize = 100               ::integer()         %% get data in chunks of (approximately) this size
                    , stmtStr = ""                  ::string()          %% SQL statement (optional)
                    , stmtParse = undefined         ::any()             %% SQL parse tree
                    , colMaps = []                  ::list(#bind{}) %% column map
                    , fullMaps = []                 ::list(#bind{}) %% full map
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
