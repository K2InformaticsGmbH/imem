-include("imem_if.hrl").

-record(ddSysConf, %% System Config
                  { content_str = <<>>      ::binary()
                  , content_bin = <<>>      ::binary()
                  }
       ).
-define(ddSysConf, [binstr,binary]).

