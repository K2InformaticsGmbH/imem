-include("imem_if.hrl").

-define(ClientError(__Reason), ?THROW_EXCEPTION('ClientError',__Reason)).
-define(SystemException(__Reason),  ?THROW_EXCEPTION('SystemException',__Reason)).
-define(ConcurrencyException(__Reason),  ?THROW_EXCEPTION('ConcurrencyException',__Reason)).
-define(UnimplementedException(__Reason),  ?THROW_EXCEPTION('UnimplementedException',__Reason)).

-define(CONFIG_TABLE,ddConfig).                    
-define(LOG_TABLE,ddLog_86400@).                    %% 86400 = 1 Day
-define(MONITOR_TABLE,ddMonitor_86400@).            %% 86400 = 1 Day
-define(CACHE_TABLE,ddCache@).

-define(GET_IMEM_CONFIG(__PName,__Context,__Default)
		, imem_meta:get_config_hlk(?CONFIG_TABLE,{imem,?MODULE,__PName},?MODULE,lists:flatten([__Context,node()]),__Default)
	   ).

-type ddEntityId() :: 	reference() | integer() | atom().
-type ddType() ::		atom() | tuple() | list().         %% term | list | tuple | integer | float | binary | string | ref | pid | ipaddr                  

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

-record(ddColumn,                           %% column definition    
				  { name                    ::atom()
				  , type = term             ::ddType()
				  , len = undefined 	      ::integer()
				  , prec = undefined        ::integer()
				  , default = undefined     ::any()
				  , opts = []               ::list()
				  }
		).

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
				  { logTime                 ::ddTimestamp()             %% erlang timestamp {Mega,Sec,Micro}
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
				  , time                    ::ddTimestamp()             %% erlang:now()
				  , extra                   ::list()      
				  }
	   ).
-define(ddNode, [atom,integer,timestamp,list]).

-record(ddSchema,                           %% schema node    
				  { schemaNode              ::tuple()                   %% {schema,node}
				  , extra                   ::list()      
				  }
	   ).
-define(ddSchema, [tuple,list]).

-record(ddConfig,                           %% config record    
				  { hkl                     ::list()                    %% hierarchical key list [item,context1,context2,...]
				  , val                     ::any()
				  , owner                   ::atom()                    %% the module who owns this config
				  , remark= <<"">>          ::binary()                  %% create comments     
				  }
	   ).
-define(ddConfig, [list,term,atom,binstr]).

-record(ddConfigHistory,                    %% config history record    
				  { hkl_time                ::tuple()                   %% {[item,context1,context2,...],erlang:now()}
				  , val                     ::any()                     
				  , remark= <<"">>          ::binary()                  %% comments     
				  , user                    ::integer()                     
				  }
	   ).
-define(ddConfigHistory, [{list,timestamp},term,binstr,integer]).

-record(ddMonitor,                          %% monitor    
				  { time                    ::ddTimestamp()             %% erlang:now()
				  , node                    ::atom()                    %% erlang node name
				  , memory=0                ::integer()                 %% erlang:memory(total)
				  , process_count=0         ::integer()                 %% erlang:system_info(process_count)          
				  , port_count=0            ::integer()                 %% erlang:system_info(port_count)
				  , run_queue=0             ::integer()                 %% erlang:statistics(run_queue)
				  , wall_clock=0            ::integer()                 %% erlang:statistics(wall_clock)
				  , reductions=0            ::integer()                 %% erlang:statistics(reductions)
				  , input_io=0              ::integer()                 %% erlang:statistics(Item :: io)
				  , output_io=0             ::integer()                 %% erlang:statistics(Item :: io)
				  , extra=[]                ::list()      
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

-define(THROW_EXCEPTION(__Ex,__Reason),
	(fun(_Reason) ->
		__Level = case __Ex of
			'UnimplementedException' -> warning;
			'ConcurrencyException' ->   warning;
			'ClientError' ->            warning;
			_ ->                        error
		end,
		_Rsn = _Reason,
		{__Head,__Fields} = case _Rsn of
			__Rsn when is_tuple(__Rsn) ->
				[__H|__R] = tuple_to_list(__Rsn),
				case __R of
					[]  -> {__H,[]};
					__R when is_tuple(__R) ->
						__RL = tuple_to_list(__R),
						{__H, lists:zip([list_to_atom("ep"++integer_to_list(__N)) || __N <- lists:seq(1,length(__RL))], __RL)};
					__R -> {__H,__R}
				end;
			__Else -> {__Level,[{ep1,__Else}]}
		end,            
		__Message = if 
			is_atom(__Head) -> list_to_binary(atom_to_list(__Head));
			is_list(__Head) -> list_to_binary(__Head);
			true            -> <<"invalid exception head">>
		end,
		{_, {_,[_|__ST]}} = (catch erlang:now(1)),
		{__Module,__Function,__Line} = imem_meta:failing_function(__ST),
		__LogRec = #ddLog{logTime=erlang:now(),logLevel=__Level,pid=self()
							,module=__Module,function=__Function,line=__Line
							,node=node(),fields=[{ex,__Ex}|__Fields]
							,message= __Message,stacktrace = __ST},
		__DbResult = {__Level, (catch imem_meta:write_log(__LogRec))},
		case __DbResult of 
			{warning, ok} ->	ok;
			 _ ->				?EXCP_LOG(__LogRec)
		end,
		case __Ex of
			'SecurityViolation' ->  exit({__Ex,_Reason});
			_ ->                    throw({__Ex,_Reason})
		end
	end)(__Reason)
).
