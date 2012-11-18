-define(MatchAllRecords,[{'$1', [], ['$_']}]).
-define(MatchAllKeys,[{'$1', [], [{element,2,'$1'}]}]).

-define(sot,'$start_of_table').		%% defined in mnesia
-define(eot,'$end_of_table').		%% defined in mnesia
-define(more,'$more_rows_exist').	%% defined here

-define(ClientError(Reason), throw({'ClientError',Reason})).
-define(SystemException(Reason), throw({'SystemException',Reason})).
-define(ConcurrencyException(Reason), throw({'ConcurrencyException',Reason})).
-define(UnimplementedException(Reason), throw({'UnimplementedException',Reason})).

