-module(imem_rest).

-export([handle/2, handle_event/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([start_link/1]).

-include_lib("elli/include/elli.hrl").
-include("imem.hrl").
-behaviour(elli_handler).
-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").

-record(state, {pid}).

handle(Req, _Args) ->
    %% Delegate to our handler function
    handle(Req#req.method, elli_request:path(Req), Req).

handle('GET', [Channel], Req) ->
	case elli_request:get_arg_decoded(<<"key">>, Req, undefined) of
		undefined -> {200, [], Channel};
		Key -> 
            case imem_dal_skvh:read(system, Channel, 
                [key_from_json(imem_json:decode(Key))]) of
                [] ->  {404, [], <<"Not Found">>};
    			[#{cvalue := Data}] -> 
                    {200, [{<<"Content-type">>, <<"application/json">>}], Data}
            end
	end;

handle(_, _, _Req) ->
    {404, [], <<"Not Found">>}.

handle_event(elli_startup, [], _) -> 
	ok;

handle_event(_, _, _) ->
	ok.

start_link(Port) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Port], []).

init([Port]) ->
	{ok, Pid} = elli:start_link([{callback, imem_rest}, 
                  {port, Port}]),
    {ok, #state{pid = Pid}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

key_from_json([]) -> [];
key_from_json([B | Key]) when is_binary(B) -> [binary_to_list(B) | key_from_json(Key)];
key_from_json([T | Key]) -> [T | key_from_json(Key)].
