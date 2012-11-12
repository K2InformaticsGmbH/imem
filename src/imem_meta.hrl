
-include("imem_if.hrl").

-type ddEntityId() :: reference() | atom().                    

-record(ddTable,                            %% table    
                  { id                      ::atom()
                  , recinfo                 ::list()
                  , opts = []               ::list()      
                  , owner                   ::ddEntityId()        %% binary account.name of creator / owner
                  , readonly='false'        ::'true' | 'false'
                  }
       ).

-define(UnimplementedException(Reason), throw({'UnimplementedException',Reason})).
-define(ConcurrencyException(Reason), throw({'ConcurrencyException',Reason})).
