-module(imem_rec_test).
-compile({parse_transform, imem_rec_pretty_pt}).

-export([test/0]).

-record(rcrd, {a,b}).

test() ->
    io:format("~s ~s~n", [rcrd_pretty(#rcrd{}), rcrd_pretty(#rcrd{a = 1, b = 2})]).
    %ok.
