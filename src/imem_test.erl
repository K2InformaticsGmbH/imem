-module(imem_test).

-export([zip/2, fac/1, sort/1, p/1]).

fac(0) -> 1;
fac(N) when N > 0 -> N * fac(N - 1).

zip(L1, L2) -> zip(L1, L2, []).

zip([], [], Acc) -> lists:reverse(Acc);
zip([E1 | R1], [E2 | R2], Acc) -> zip(R1, R2, [{E1, E2} | Acc]).

sort([Pivot | T]) -> sort([X || X <- T, X < Pivot]) ++ [Pivot] ++ sort([X || X <- T, X >= Pivot]);
sort([]) -> [].

p([]) -> [[]];
p(L) -> [[H | T] || H <- L, T <- p(L -- [H])].
