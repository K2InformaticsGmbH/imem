
-type ddEntityId() :: reference() | atom().                    

-record(ddTable,                            %% table    
                  { id                      ::atom()
                  , recinfo                 ::list()
                  , opts = []               ::list()      
                  , owner                   ::ddEntityId()        %% binary account.name of creator / owner
                  , readonly='false'        ::'true' | 'false'
                  }
       ).

-define(ClientError(Reason), throw({'ClientError',Reason})).
-define(ConcurrencyException(Reason), throw({'ConcurrencyException',Reason})).
-define(SystemException(Reason), throw({'SystemException',Reason})).
