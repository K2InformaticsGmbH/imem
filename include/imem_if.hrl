-ifndef(IMEM_IF_HRL).
-define(IMEM_IF_HRL, true).

-type ddTimestamp() :: 'undefined' | {integer(), integer(), integer()}.
-type ddDatetime() :: 'undefined' | {{integer(), integer(), integer()},{integer(), integer(), integer()}}.

-record(snap_properties, { table
                         , last_write
                         , last_snap
                         }).
-define(SNAP_ETS_TAB, snap_timer_tab).

-define(NoRec, {}).                      %% Placeholder for nothing where a table record could stand (old record for insert / new record for delete)

-define(TRANS_TIME, imem_if:now()).                                  % Timestamp generator -> {Megas,Secs,Micros},  could be erlhlc:next_now/0)
-define(TRANS_TIME_NAME,imem_if_transaction_time).                  % name of process variable for transaction time
-define(TRANS_TIME_PUT(__TT),erlang:put(?TRANS_TIME_NAME,__TT)).    % store updated transaction time
-define(TRANS_TIME_GET,erlang:get(?TRANS_TIME_NAME)).               % retrieve stored transaction time


-define(MatchAllRecords,[{'$1', [], ['$_']}]).
-define(MatchAllKeys,[{'$1', [], [{element,2,'$1'}]}]).

-define(sot,'$start_of_table').		%% defined in mnesia, signals start of fetch transaction here 
-define(eot,'$end_of_table').		  %% defined in mnesia, signals end of fetch transaction here

-ifdef(TEST).
    -define(EXCP_LOG(__Warn),ok).
-else. %!TEST
    -define(EXCP_LOG(__Warn),lager:warning("~p",[__Warn])).
-endif. %TEST

-define(FP(__RecList, __Pattern), imem_if:field_pick(__RecList,__Pattern)).

-define(ClientErrorNoLogging(__Reason), throw({'ClientError',__Reason})).
-define(SystemExceptionNoLogging(__Reason), throw({'SystemException',__Reason})).
-define(ConcurrencyExceptionNoLogging(__Reason),  throw({'ConcurrencyException',__Reason})).

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


-define(imem_test_setup,
        case erlang:whereis(imem_sup) of
            undefined ->
                application:load(imem),
                application:set_env(imem, mnesia_node_type, disc),
                imem:start();
            _ -> ok
        end
    ).

%-define(imem_test_teardown, imem:stop() ).
-define(imem_test_teardown, ok).

-endif.
