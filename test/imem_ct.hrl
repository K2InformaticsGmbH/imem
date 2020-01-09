-define(
    CTPAL(__MSG),
    ct:pal(info, ?MAX_IMPORTANCE, "~p:~p/~p[~p] " ++ __MSG ++ "~n", [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, ?LINE])
).
-define(
    CTPAL(__MSG, __ARGS),
    ct:pal(
        info,
        ?MAX_IMPORTANCE,
        "~p:~p/~p[~p] ~n" ++ __MSG ++ "~n",
        [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, ?LINE | __ARGS]
    )
).
