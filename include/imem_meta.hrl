-include("imem_if.hrl").

-define(CONFIG_TABLE,ddConfig).                    
-define(LOG_TABLE,ddLog_86400@).                    %% 86400 = 1 Day
-define(MONITOR_TABLE,ddMonitor_86400@).            %% 86400 = 1 Day

-define(GET_IMEM_CONFIG(__PName,__Context,__Default),
        imem_meta:get_config_hlk(?CONFIG_TABLE,{imem,?MODULE,__PName},?MODULE,lists:flatten([__Context,node()]),__Default)
       ).

-type ddEntityId() :: 	reference() | integer() | atom().
-type ddType() ::		atom() | tuple() | list().         %% term | list | tuple | integer | float | binary | string | ref | pid | ipaddr                  

-record(ddColumn,                           %% column definition    
                  { name                    ::atom()
                  , type = term             ::ddType()
                  , len = undefined 	    ::integer()
                  , prec = undefined        ::integer()
                  , default = undefined     ::any()
                  , opts = []               ::list()
                  }
        ).

-record(ddTable,                            %% table    
                  { qname                   ::{atom(),atom()}		%% {Schema,Table}
                  , columns                 ::list(#ddColumn{})
                  , opts = []               ::list()      
                  , owner=system            ::ddEntityId()        	%% AccountId of creator / owner
                  , readonly='false'        ::'true' | 'false'
                  }
       ).
-define(ddTable, [tuple,list,list,userid,boolean]).

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
                  , timestamp
                  , tuple
                  , userid
                  ]).

-define(NumberTypes,[ decimal
                    , float
                    , integer
                    , number
                    ]).

-define(VirtualTables, [ddSize|?DataTypes]).
