-define(__T,
(fun() ->
    {_,_,__McS} = __Now = erlang:now(),
    {{__YYYY,__MM,__DD},{__H,__M,__S}} = calendar:now_to_local_time(__Now),
    lists:flatten(io_lib:format("~2..0B.~2..0B.~4..0B ~2..0B:~2..0B:~2..0B.~6..0B", [__DD,__MM,__YYYY,__H,__M,__S,__McS rem 1000000]))
end)()).

-define(Log(__F,__A), io:format(user, ?__T++" [_IMEM_] "++__F, __A)).
