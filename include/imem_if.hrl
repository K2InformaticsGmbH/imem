-ifndef(IMEM_IF_HRL).
-define(IMEM_IF_HRL, true).

-type ddTimeUID() :: 'undefined' | {integer(), integer(), atom(), integer()}.   % {Secs,Micro,Node,UniqueInteger}
-type ddTimestamp() :: 'undefined' | {integer(), integer()}.                    % {Secs,Micro}
-type ddDatetime() :: 'undefined' | {{integer(), integer(), integer()},{integer(), integer(), integer()}}. % {{Y, M, D}, {Hour, Min, Sec}}
-type ddString() :: list().
-type ddBinStr() :: binary().
-type ddIo() :: ddBinStr() | ddString().
-type ddError() :: tuple().
-type ddOption() :: atom() | {atom(), any()}.
-type ddOptions() :: [ddOption()].
-type ddMnesiaTable() :: atom().
-type ddMnesiaIndex() :: atom().
-type ddColumnName() :: atom() | binary().
-type ddColumnList() :: [ddColumnName()].

-record(snap_properties, { table
                         , last_write
                         , last_snap
                         }).
-define(SNAP_ETS_TAB, snap_timer_tab).

-define(NoRec, {}).                     %% Placeholder for nothing where a table record could stand (old record for insert / new record for delete)
-define(ERL_MIN_TERM, -1.0e100).        %% Placeholder for minimum erlang term sorted range initialisations (compromise)            

-define(INTEGER_UID, imem_if_mnesia:integer_uid()).                 % unique integer per imem node and reboot, used in timestamp generator or for other purposes 
-define(TIME_UID, imem_if_mnesia:time_uid()).                       % Unique timestamp generator -> {Secs,Micros,Node,Counter}
-define(TIMESTAMP, imem_if_mnesia:timestamp()).                     % Monotonic (non-unique) timestamp generator per node and reboot -> {Secs,Micros}
-define(TIMESTAMP_DIFF(__A,__B), imem_if_mnesia:timestamp_diff(__A,__B)). % Time difference in Microseconds

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

-define(FP(__RecList, __Pattern), imem_if_mnesia:field_pick(__RecList,__Pattern)).

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

-endif.
