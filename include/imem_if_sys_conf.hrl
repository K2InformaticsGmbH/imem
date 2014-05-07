-include("imem_if.hrl").

-record(ddSysConf, %% System Config
                  { item    ::atom()
                  , itemStr ::binary()
                  , itemBin ::binary()
                  }
       ).
-define(ddSysConf, [atom,binstr,binary]).

