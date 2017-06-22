-module(imem_nif).
-export([now/0]).
-on_load(init/0).

init() ->
    erlang:load_nif(imem:priv_dir() ++ "/imem_nif", 0).

now() ->
    exit(nif_library_not_loaded).
