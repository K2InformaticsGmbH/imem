-include("imem.hrl").

-type ddTimestamp() :: 'undefined' | {integer(), integer(), integer()}.
-type ddDatetime() :: 'undefined' | {{integer(), integer(), integer()},{integer(), integer(), integer()}}.

-define(MatchAllRecords,[{'$1', [], ['$_']}]).
-define(MatchAllKeys,[{'$1', [], [{element,2,'$1'}]}]).

-define(sot,'$start_of_table').		%% defined in mnesia, signals start of fetch transaction here 
-define(eot,'$end_of_table').		  %% defined in mnesia, signals end of fetch transaction here

-define(THROW_EXCEPTION(Ex,Reason),
(fun(ST) ->
    Level = case Ex of
        'UnimplementedException' -> warning;
        'ConcurrencyException' ->   warning;
        'ClientError' ->            warning;
        _ ->                        error
    end,
    imem_meta:throw_exception(Ex,Reason,Level,ST)
end)(erlang:get_stacktrace())
).
-define(ClientError(__Reason), ?THROW_EXCEPTION('ClientError',__Reason)).
-define(ClientErrorNoLogging(__Reason), throw({'ClientError',__Reason})).
-define(SystemException(__Reason),  ?THROW_EXCEPTION('SystemException',__Reason)).
-define(SystemExceptionNoLogging(__Reason), throw({'SystemException',__Reason})).
-define(ConcurrencyException(__Reason),  ?THROW_EXCEPTION('ConcurrencyException',__Reason)).
-define(UnimplementedException(__Reason),  ?THROW_EXCEPTION('UnimplementedException',__Reason)).

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

-define(atom_to_binary(__Atom), list_to_binary(atom_to_list(__Atom))).

-define(binary_to_atom(__Bin), list_to_atom(binary_to_list(__Bin))).

-define(binary_to_existing_atom(__Bin), 
        try
          list_to_existing_atom(binary_to_list(__Bin))
        catch
          _:_ -> ?ClientError({"Name does not exist", __Bin})
        end
    ).

-define(imem_test_setup, fun() ->
      application:load(imem),
      application:set_env(imem, mnesia_node_type, disc),
      imem:start()
    end
    ).

-define(imem_test_teardown, fun() ->
      imem:stop()
    end
    ).
