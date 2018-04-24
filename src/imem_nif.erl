-module(imem_nif).
-on_load(init/0).

-export([now/0, tpf/0, qpc/0]).

init() ->
    case os:type() of
        {win32, nt} ->
            erlang:load_nif(imem:priv_dir() ++ "/imem_nif", 0);
        _ -> ok
    end.

now() ->
    exit(nif_library_not_loaded).

% GetSystemTimePreciseAsFileTime
tpf() ->
    exit(nif_library_not_loaded).

% QueryPerformanceCounter
qpc() ->
    exit(nif_library_not_loaded).