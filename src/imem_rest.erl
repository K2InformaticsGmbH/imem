-module(imem_rest).

-export([handle/2, handle_event/3]).

-include("../elli/include/elli.hrl").
-behaviour(elli_handler).

-include_lib("kernel/include/file.hrl").

handle(Req, _Args) ->
    %% Delegate to our handler function
    handle(Req#req.method, elli_request:path(Req), Req).

handle(_, _, _Req) ->
    {404, [], <<"Not Found">>}.

handle_event(elli_startup, [], _) -> 
	?Info("Elli server started"),
	ok.