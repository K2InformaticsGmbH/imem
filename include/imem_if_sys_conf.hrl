-include("imem_if.hrl").

-record(ddSysConf, %% System Config
                  { item            ::atom()
                  , itemTrm         ::term()
                  , itemStr = <<>>  ::binary()
                  , itemBin = <<>>  ::binary()
                  }
       ).
-define(ddSysConf, [atom,term,binstr,binary]).

