-module(imem_rec_test).
-compile({parse_transform, imem_rec_pretty_pt}).

-include("imem_rec_test.hrl").

-export([test/0]).

-record(rcrd, {a,b}).
-record(rcrd1, {a,b}).

test() ->
    A = #rcrd2{},
    io:format("~s ~s ~s~n", [rcrd_pretty(#rcrd{a = 1, b = 2}), test1(), ?PP(A)]).
    %io:format("~p~n", [test1()]).
    %ok.

test1() ->
    io:format(""),
    (fun() -> rcrd1_pretty(#rcrd1{a = 1, b = 2}) end)().
