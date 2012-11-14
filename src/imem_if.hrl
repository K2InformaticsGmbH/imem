
-define(ClientError(Reason), throw({'ClientError',Reason})).
-define(SystemException(Reason), throw({'SystemException',Reason})).
-define(MatchAllRecords,[{'$1', [], ['$_']}]).
-define(MatchAllKeys,[{'$1', [], [{element,2,'$1'}]}]).
-define(eot,'$end_of_table').
-define(more,'$more_rows_exist').

