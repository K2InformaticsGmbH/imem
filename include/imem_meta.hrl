-ifndef(IMEM_META_HRL).
-define(IMEM_META_HRL, true).

-compile({parse_transform, imem_rec_pretty_pt}).

-include("imem_exception.hrl").
-include("imem_if.hrl").
-include("imem_if_csv.hrl").
-include("imem_config.hrl").

-define(LOG_TABLE,ddLog_86400@).                    %% 86400 = 1 Day
-define(MONITOR_TABLE,ddMonitor_86400@).            %% 86400 = 1 Day
-define(CACHE_TABLE,ddCache@).
-define(META_ROWNUM, <<"rownum">>).
-define(META_NODE, <<"node">>). 

-record(ddColumn,                           %% column definition    
				  { name                    ::atom()
				  , type = term             ::ddType()
				  , len = undefined 	    ::integer()
				  , prec = undefined        ::integer()
				  , default = undefined     ::any()
				  , opts = []               ::list()
				  }
		).

-type ddEntityId() :: 	integer() | atom().
-type ddType() ::		atom() | tuple() | list().         	% term | list | tuple | integer | float | binary | string | ref | pid | ipaddr                  

-type ddSchema() :: atom() | binary().
-type ddSimpleTable() :: ddMnesiaTable() | ddBinStr() | ddString().	% does not include a schema name
-type ddQualifiedTable() :: {ddSchema(), ddSimpleTable()}.	% does include a schema name
-type ddTable() :: ddSimpleTable() | ddQualifiedTable().	% may or may not include a schema name
-type ddIndex() :: binary() | integer() | ddMnesiaIndex().	% binstr name or integer id or mnesia column (atom) 

-type ddColumnType() :: atom().
-type ddColumnDefault() :: any().
-type ddTypeList() :: [ddColumnType()].
-type ddDefaultList() :: [ddColumnDefault()].
-type ddTableMeta() :: [#ddColumn{}] | {ddColumnList(), ddTypeList(), ddDefaultList()}.

-record(ddIdxDef, %% record definition for index definition              
				  { id      :: integer()    %% index id within the table
				  , name    :: binary()     %% name of the index
				  , type    :: ivk|iv_k|iv_kl|iv_h|ivvk|ivvvk %% Type of index
				  , pl=[]   :: list()       %% list of JSON path expressions as binstr/compiled to term
				  , vnf = <<"fun imem_index:vnf_lcase_ascii_ne/1">> :: binary()    
						  %% value_normalising_fun(Value)  
						  %% applied to each value result of all path scans for given JSON document
						  %% return ?nav = '$not_a_value' if indexing is not wanted, otherwise let iff() decide
				  , iff = <<"fun imem_index:iff_true/1">> :: binary()    
						  %% boolean index_filter_fun({Key,Value}) for the inclusion/exclusion of indexes
						  %% applied to each result of all path scans for given JSON document
				  }     
	   ).

-record(ddIdxPlan, %% record definition plan, combining all index definitions for a table              
				  { def=[]    :: list(#ddIdxDef{})  %% index definitions
				  , jpos=[]   :: list(integer())    %% record positions needing JSON decoding
				  }     
	   ).

-record(ddIndex,  %% record definition for index tables (one per indexed master data table)              
				  { stu     :: tuple()  %% search tuple, cannot be empty
				  , lnk = 0     :: term()   %% Link to key of master data
								%% 0=unused when key is part of search tuple, used in ivk
								%% 0..n  hashed value for the hashmap index (iv_h)
								%% single key of any data type for unique index (iv_k)
								%% list of keys for almost unique index (iv_kl)
				  }     
	   ). 
-define(ddIndex, [tuple,term]).


-record(ddCache,                            %% local kv cache, created empty              
				  { ckey                    :: term()             %% key
				  , cvalue                  :: term()             %% value
				  , opts = []               :: list()             %% options
				  }     
	   ). 
-define(ddCache, [term,term,list]).

-record(ddAlias,                            %% table alias for partitioned tables
				  { qname                   ::{atom(),atom()}   %% {Schema,TableAlias}
				  , columns                 ::list(#ddColumn{})
				  , opts = []               ::list()      
				  , owner=system            ::ddEntityId()          %% AccountId of creator / owner
				  , readonly='false'        ::'true' | 'false'
				  }
	   ).
-define(ddAlias, [tuple,list,list,userid,boolean]).

-record(ddTable,                            %% table    
				  { qname                   ::{atom(),atom()}		%% {Schema,Table}
				  , columns                 ::list(#ddColumn{})
				  , opts = []               ::list()      
				  , owner=system            ::ddEntityId()        	%% AccountId of creator / owner
				  , readonly='false'        ::'true' | 'false'
				  }
	   ).
-define(ddTable, [tuple,list,list,userid,boolean]).

-record(ddLog,                              %% log table    
				  { logTime                 ::ddTimeUID()           % ?TIME_UID
				  , logLevel                ::atom()
				  , pid                     ::pid()      
				  , module                  ::atom()
				  , function                ::atom()
				  , line=0                  ::integer()
				  , node                    ::atom()
				  , fields=[]               ::list()
				  , message= <<"">>         ::binary()
				  , stacktrace=[]           ::list()
				  }
	   ).
-define(ddLog, [timestamp,atom,pid,atom,atom,integer,atom,list,binstr,list]).

-record(ddNode,                             %% node    
				  { name                    ::atom()                    %% erlang node name
				  , wall_clock              ::integer()                 %% erlang:statistics(wall_clock)
				  , time                    ::ddTimestamp()             %% ?TIMESTAMP
				  , extra                   ::list()      
				  }
	   ).
-define(ddNode, [atom, integer, timestamp, list]).

-record(ddSnap,                             %% snapshot
				  { file                    ::binary()                  %% snapshot file name
				  , type                    ::bkp|zip                   %% snapshot file type
				  , size                    ::integer()                 %% snapshot file size in bytes
				  , lastModified            ::ddTimestamp()             %% ?TIMESTAMP
				  }
	   ).
-define(ddSnap, [binstr,atom,integer,datetime]).

-record(ddSchema,                           %% schema node    
				  { schemaNode              ::tuple()                   %% {schema,node}
				  , extra                   ::list()      
				  }
	   ).
-define(ddSchema, [tuple,list]).

-record(ddMonitor,                          %% monitor    
				  { time                    ::ddTimeUID()           	%% ?TIME_UID
				  , node                    ::atom()                    %% erlang node name
				  , memory=0                ::integer()                 %% erlang:memory(total)
				  , process_count=0         ::integer()                 %% erlang:system_info(process_count)          
				  , port_count=0            ::integer()                 %% erlang:system_info(port_count)
				  , run_queue=0             ::integer()                 %% erlang:statistics(run_queue)
				  , wall_clock=0            ::integer()                 %% erlang:statistics(wall_clock)
				  , reductions=0            ::integer()                 %% erlang:statistics(reductions)
				  , input_io=0              ::integer()                 %% erlang:statistics(Item :: io)
				  , output_io=0             ::integer()                 %% erlang:statistics(Item :: io)
				  , extra=[]                ::list()      				%% application dependent proplist
				  }
	   ).
-define(ddMonitor, [timestamp,atom,integer,integer,integer,integer,integer,integer,integer,integer,list]).

-define(nav, '$not_a_value').           %% used as default value which must not be used (not null columns)
-define(navio, <<"'$not_a_value'">>).   %% used as default value which must not be used (not null columns)
-define(nac, '$not_a_column').   %% used as value column name for key only tables

-record(dual,                               %% table    
				  { dummy = "X"             ::list()        % fixed string "X"
				  , nac = ?nav              ::atom()        % not a column
				  }
	   ).
-define(dual, [string,atom]).

-record(ddSize,                             %% table size    
				  { name                    ::atom()
				  , size                    ::integer()
				  , memory                  ::integer()
				  , expiry                  ::ddTimestamp()  %% expiry time (first ts of next partition)
				  , tte                     ::integer()      %% time until expiry (sec)                 
				  }
	   ).
-define(ddSize, [atom, integer, integer, timestamp, integer]).

-record(ddTermDiff,                   		%% term diff    
				  { id                      ::number()      %% record id
				  , left = ?nav        		::binary()      %% left item (text line)
				  , cmp = <<>>              ::binary()      %% comparison token
				  , right = ?nav       		::binary()     	%% right item (text line)
				  }
	   ).
-define(ddTermDiff, [number, binstr, binstr, binstr]).

-record(ddVersion,                           %% code version    
				  { app              		::binary()      %% application
				  , appVsn                  ::binary()		%% application version
				  , file 					::binary()  	%% module / file name
				  , fileVsn					::binary()		%% module / file version
				  , filePath = <<>>	    	::binary()      %% module / file binary path
				  , fileOrigin = <<>>	    ::binary()      %% module / file origin path (e.g. git commit url)
				  }
	   ).
-define(ddVersion, [binstr, binstr, binstr, binstr, binstr, binstr]).

-define(OneWeek, 7.0).                      %% span of  datetime or timestamp (fraction of 1 day)
-define(OneDay, 1.0).                       %% span of  datetime or timestamp (fraction of 1 day)
-define(OneHour, 0.041666666666666664).     %% span of  datetime or timestamp (1.0/24.0 of 1 day)
-define(OneMinute, 6.944444444444444e-4).   %% span of  datetime or timestamp (1.0/24.0 of 1 day)
-define(OneSecond, 1.1574074074074073e-5).  %% span of  datetime or timestamp (fraction of 1 day)

-define(DataTypes,[ 'fun' 
				  , atom
				  , binary
				  , binstr
				  , boolean
				  , datetime
				  , decimal
				  , float
				  , integer
				  , number
				  , ipaddr
				  , list
				  , map
				  , pid
				  , ref
				  , string
				  , term
				  , binterm
				  , timestamp
				  , tuple
				  , userid
				  , json
				  ]).

-define(NumberTypes,[ decimal
					, float
					, integer
					, number
					]).

-define(VirtualTables, [ddSize|?DataTypes]).

-endif.
