
-type ddTimestamp() :: 'undefined' | {integer(), integer(), integer()}.
-type ddDatetime() :: 'undefined' | {{integer(), integer(), integer()},{integer(), integer(), integer()}}.

-define(MatchAllRecords,[{'$1', [], ['$_']}]).
-define(MatchAllKeys,[{'$1', [], [{element,2,'$1'}]}]).

-define(sot,'$start_of_table').		%% defined in mnesia, signals start of fetch transaction here 
-define(eot,'$end_of_table').		  %% defined in mnesia, signals end of fetch transaction here

-define(ClientError(Reason), imem_meta:throw_exception('ClientError',Reason)).
-define(ClientErrorNoLogging(Reason), throw({'ClientError',Reason})).
-define(SystemException(Reason), imem_meta:throw_exception('SystemException',Reason)).
-define(ConcurrencyException(Reason), imem_meta:throw_exception('ConcurrencyException',Reason)).
-define(UnimplementedException(Reason), imem_meta:throw_exception('UnimplementedException',Reason)).

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


%% HELPER FUNCTIONS (do not export!!) --------------------------------------

-define(binary_to_atom(Bin), list_to_atom(binary_to_list(Bin))).

-define(binary_to_existing_atom(Bin), 
        try
          list_to_existing_atom(binary_to_list(Bin))
        catch
          _:_ -> ?ClientError({"Name does not exist", Bin})
        end
    ).

-define(imem_test_setup, fun() ->
      application:load(imem),
      application:set_env(imem, mnesia_node_type, disc),
      application:start(imem)
    end
    ).

-define(imem_test_teardown, fun() ->
      application:stop(imem)
    end
    ).
