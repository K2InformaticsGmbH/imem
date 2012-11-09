
-define(ClientError(Reason), throw({'ClientError',Reason})).
-define(SystemException(Reason), throw({'SystemException',Reason})).
