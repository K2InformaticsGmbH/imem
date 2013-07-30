-type ddTimestamp() :: 'undefined' | {integer(), integer(), integer()}.
-type ddDatetime() :: 'undefined' | {{integer(), integer(), integer()},{integer(), integer(), integer()}}.

-record(snap_properties, { table
                         , last_write
                         , last_snap
                         }).
-define(SNAP_ETS_TAB, snap_timer_tab).

-define(MatchAllRecords,[{'$1', [], ['$_']}]).
-define(MatchAllKeys,[{'$1', [], [{element,2,'$1'}]}]).

-define(sot,'$start_of_table').		%% defined in mnesia, signals start of fetch transaction here 
-define(eot,'$end_of_table').		  %% defined in mnesia, signals end of fetch transaction here

-ifdef(TEST).
-define(EXCP_LOG(__Warn),ok).
-else.
-ifdef(LAGER).
-define(EXCP_LOG(__Warn),lager:warning("~p",[__Warn])).
-else. % CONSOLE
-define(EXCP_LOG(__Warn),io:format(user,"~p~n",[__Warn])).
-endif. %LAGER or CONSOLE
-endif. %TEST


-define(THROW_EXCEPTION(__Ex,__Reason),
    (fun() ->
        __Level = case __Ex of
            'UnimplementedException' -> warning;
            'ConcurrencyException' ->   warning;
            'ClientError' ->            warning;
            _ ->                        error
        end,
        _Rsn = __Reason,
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
        {__Module,__Function} = imem_meta:failing_function(__ST),
        __LogRec = #ddLog{logTime=erlang:now(),logLevel=__Level,pid=self()
                            ,module=__Module,function=__Function,node=node()
                            ,fields=[{ex,__Ex}|__Fields],message= __Message
                            ,stacktrace = __ST},
        catch imem_meta:write_log(__LogRec),
        ?EXCP_LOG(__LogRec),
        case __Ex of
            'SecurityViolation' ->  exit({__Ex,__Reason});
            _ ->                    throw({__Ex,__Reason})
        end
    end)()
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
