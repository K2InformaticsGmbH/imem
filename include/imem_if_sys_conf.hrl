-ifndef(IMEM_IF_SYS_CONF_HRL).
-define(IMEM_IF_SYS_CONF_HRL, true).

-include("imem_if.hrl").

-record(ddSysConf, %% System Config
                  { item    ::atom()
                  , itemStr ::binary()
                  , itemBin ::binary()
                  }
       ).
-define(ddSysConf, [atom,binstr,binary]).

-endif.
