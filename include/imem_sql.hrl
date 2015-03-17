-ifndef(IMEM_SQL_HRL).
-define(IMEM_SQL_HRL, true).

-include("imem_meta.hrl").

-define(ComparisonOperators, ['==','/=','>=','=<','<','>']).	%% as supported here in matchspecs 

-define(NoFilter,{undefined,[]}).   %% empty filter spec $$$ also used in erlimem_fsm.erl $$$
-define(NoMoreFilter,{_,[]}).       %% empty filter spec $$$ also used in erlimem_fsm.erl $$$
-define(FilterTrue, fun(_) -> true end).
-define(FilterFalse, fun(_) -> false end).


-define(RecIdx, 1).                                       %% Record name position in records
-define(FirstIdx, 2).                                     %% First field position in records
-define(KeyIdx, 2).                                       %% Key position in records
-define(MetaIdx, 1).                                      %% Meta record (constants) is placed as first tuple in the SQL result
-define(MainIdx, 2).                                      %% The main record is placed as second tuple in the SQL result
-define(TableIdx(__N), 1+__N).                            %% Tables are numbered 0=Meta, 1=Main, 2=FirstJoinTable ...
-define(MetaMain(__Meta,__Main), {__Meta,__Main}).        %% contstruct initial result tuple with meta and main table
-define(Meta(__Rec), element(?MetaIdx,__Rec)).            %% pick meta tuple out of master tuple
-define(Main(__Rec), element(?MainIdx,__Rec)).            %% pick main tuple (main table) out of master tuple 
-define(Table(__N,__Rec), element(?TableIdx(__N),__Rec)). %% pick table N tuple out for master tuple

-define(RownumIdx,1).                                     %% Position of rownum value in Meta Record
-define(RownumBind, #bind{tind=1,cind=1,table= <<"_meta_">>,name= <<"rownum">>}).  %% Bind pattern for rownum variable

-define(BoundVal(__Bind,__X), 
          case __Bind#bind.cind of
            0 ->        element(__Bind#bind.tind,__X);
            _ ->        element(__Bind#bind.cind,element(__Bind#bind.tind,__X))
          end
        ).

-define(EmptyWhere, {}).                    %% empty where in the parse tree
-define(EmptyMR, {}).                       %% empty (unused) meta record

-record(bind,                               %% bind record, column map entry
                  { tind = 0                ::integer()           %% tind=0 means as constant / tind=1 means a meta value               
                  , cind = 0                ::integer()           %% cind=0 for tind=0, cind=1..n for tind=1, cind=2..m for tables
                  , schema                  ::undefined|binary()
                  , table                   ::undefined|binary()
                  , alias                   ::undefined|binary()  %% table alias in FullMap, column alias in ColMap   
                  , name                    ::undefined|binary()
                  , type                    ::atom()
                  , len = 0                 ::integer()
                  , prec = 0                ::integer()
                  , default                 ::any()
                  , readonly = false        ::true|false
                  , func                    ::any()
                  , ptree                   ::any()
                  , btree                   ::any()     
                  , tag = ""                ::any()
                  }                  
       ).

-record(scanSpec,                           %% scanner specification (options default to full table scan)
                  { sspec = []              ::list()              %% scan matchspec for raw scan  [{MatchHead, Guards, [Result]}]
                  , stree = true            ::true|false|undefined|#bind{}  %% scan expression tree, used to calculate sspec and ftree
                  , ftree = true            ::true|false|undefined|#bind{}  %% filter expression tree evaluating to a boolean when bound
                  , tailSpec = true         ::true|false|undefined|ets:comp_match_spec()
                  , filterFun = true        ::true|false|undefined|function()
                  , limit = undefined       ::undefined|integer() %% limit the total number or returned rows approximately
                  }).

-record(statement,                          %% Select statement 
                  { tables = []             ::list({atom(),atom()})  %% {Schema,Name} first one is master table, then optional lookup join tables
                  , blockSize = 100         ::integer()           %% get data in chunks of (approximately) this size
                  , stmtStr = ""            ::string()            %% SQL statement
                  , stmtParse = undefined   ::tuple()             %% SQL parse tree (tuple of atoms, binaries, numbers and and lists)
                  , stmtParams = []         ::list()              %% Proplist with {<<":name">>,<<"type">>,[<<"value">>]}
                  , colMap = []             ::list(#bind{})       %% column map (one expression tree per selected column )
                  , fullMap = []            ::list(#bind{})       %% full map of bind records (meta fields and table fields used in query)
                  , metaFields = []         ::list(atom())        %% list of meta_field names needed by RowFun
                  , rowFun                  ::fun()               %% rendering fun for row {table recs} -> [ResultValues]
                  , sortFun                 ::fun()               %% rendering fun for sorting {table recs} -> SortColumn
                  , sortSpec = []           ::list()              %% how data should be sorted (in the client)
                  , mainSpec = #scanSpec{}  ::#scanSpec{}         %% how to find main table records
                  , joinSpecs = []          ::list(#scanSpec{})   %% how to find joined records list({MatchSpec,Binds})
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

-endif.
