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
    handle(Req#req.method, elli_request:path(Req), Req).

handle('GET', [Channel], Req) ->
    io:format("Parse Args ~p~n", [parse_args(Req)]),
    get_channel(Channel, parse_args(Req));

handle('GET', [Channel, EnKey], _Req) ->
	try decode(EnKey) of
        Key ->
            case imem_dal_skvh:read(system, Channel, [Key]) of
                [] ->  {404, [], <<"Not Found">>};
                [#{cvalue := Data}] ->
                    {200, [{<<"Content-type">>, <<"application/json">>}], Data}
            end
    catch
        _:_ -> {500, [], <<"Not a valid key">>}
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

get_channel(Channel, none) ->
    try imem_meta:read(binary_to_atom(Channel, utf8)) of
        Rows ->
            Keys = [imem_datatype:binterm_to_io(K) || {_, K, _, _} <- Rows],
            {200, [{<<"Content-type">>, <<"application/json">>}], Keys}
    catch
        E1:E2 ->
            io:format("E1 : ~p E2 : ~p~n", [E1, E2]),
            {500, [], <<"Channel does not exist">>}
    end;
get_channel(Channel, #{jp := JP, limit := Limit, keyGT := KeyGT,
    keyLT := KeyLT}) ->
    Rows = imem_dal_skvh:readGELT(system, Channel, KeyGT, KeyLT, Limit),
    % Data = [lists:concat(["{\"key\":", Key, "\"value\": ", Value, "}"]) 
    % || #{ckey := Key, cvalue := Value} <- Rows],
    % {200, [{<<"Content-type">>, <<"application/json">>}], list_to_binary(["[", string:joing(Data, ","), "]"])}.
    Data = [#{key => key_to_json(Key), value => imem_json:decode(Value)} 
    || #{ckey := Key, cvalue := Value} <- Rows],
    {200, [{<<"Content-type">>, <<"application/json">>}], imem_json:encode(Data)}.

parse_args(Req) ->
    case binary:match(elli_request:query_str(Req), 
        [<<"limit">>, <<"jp">>, <<"keyGT">>, <<"keyLT">>]) of
    nomatch -> none;
    _ ->
        JP = elli_request:get_arg_decoded(<<"jp">>, Req, {}),
        Limit = binary_to_integer(elli_request:get_arg_decoded(<<"limit">>, 
            Req, <<"999999999999">>)),
        KeyGT = decode_key(elli_request:get_arg_decoded(<<"keyGT">>, Req, [])),
        KeyLT = case elli_request:get_arg_decoded(<<"keyLT">>, Req, []) of
            [] -> KeyGT ++ <<"255">>;
            Key -> decode_key(Key)
        end,
        #{jp => JP, limit => Limit, keyGT => KeyGT, keyLT => KeyLT}
    end.

decode(Encoded) ->
    decode_key(
        list_to_binary(http_uri:decode(binary_to_list(Encoded)))).

decode_key([]) -> [];
decode_key(EnKey) ->
    key_from_json(imem_json:decode(EnKey)).

key_from_json([]) -> [];
key_from_json([B | Key]) when is_binary(B) -> [binary_to_list(B) | key_from_json(Key)];
key_from_json([T | Key]) -> [T | key_from_json(Key)].

key_to_json([]) -> [];
key_to_json([L | Key]) when is_list(L) -> [list_to_binary(L) | key_to_json(Key)];
key_to_json([T | Key]) -> [T | key_to_json(Key)].