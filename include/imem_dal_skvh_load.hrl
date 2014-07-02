-include("imem_meta.hrl").

-record(loadTest,
                  { state = stopped         ::atom()
                  , keyregex = <<".*">>     ::binary()
                  , limit = 1000 	        ::integer()                  
                  , readdelay = 0           ::integer()
                  , keycounter = 0          ::integer()
                  , totalread = 0           ::integer()
                  , rate = 0                ::integer()                  
                  , lastItem                ::any()
                  , lastValue               ::any()
                  , keys = []               ::list()
                  }
        ).

-define(loadTest, [atom,binstr,integer,integer,integer,integer,integer,term,term,list]).

