-module(imem_win32).
-on_load(init/0).

-export([getSystemTimePreciseAsFileTime/0, queryPerformanceCounter/0]).

init() ->
    case os:type() of
        {win32, nt} ->
            erlang:load_nif(imem:priv_dir() ++ "/imem_win32", 0);
        _ -> ok
    end.

getSystemTimePreciseAsFileTime() ->
    exit(win32_nif_library_not_loaded).

queryPerformanceCounter() ->
    exit(win32_nif_library_not_loaded).