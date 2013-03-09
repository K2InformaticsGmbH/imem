-define(_H,  element(1,erlang:time())).
-define(_M,  element(2,erlang:time())).
-define(_S,  element(3,erlang:time())).
-define(_MS, element(3, erlang:now()) div 1000 rem 1000).

-define(_T, lists:flatten(io_lib:format("~2..0B:~2..0B:~2..0B.~3..0B", [?_H,?_M,?_S,?_MS]))).

-define(Log(__F,__A), io:format(user, ?_T++" [_IMEM_] "++__F, __A)).
