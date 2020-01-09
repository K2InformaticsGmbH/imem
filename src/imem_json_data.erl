-module(imem_json_data).

-include("imem.hrl").

-export([to_strbinterm/1]).

-spec to_strbinterm(term()) -> binary().
to_strbinterm(Term) -> sext:encode(to_str(Term)).

to_str(B) when is_binary(B) -> binary_to_list(B);
to_str(L) when is_list(L) -> [to_str(Elem) || Elem <- L];
to_str(T) when is_tuple(T) -> list_to_tuple(to_str(tuple_to_list(T)));
to_str(Other) -> Other.

%% ===================================================================
%% TESTS
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> ok.

teardown(_) -> ok.

to_strbinterm_test_() -> {setup, fun setup/0, fun teardown/1, {with, [fun strbinterm/1]}}.

strbinterm(_) ->
    %?LogDebug("---TEST---"),
    ?assertEqual(sext:encode(["123456", "IPCON_"]), imem_json_data:to_strbinterm([<<"123456">>, <<"IPCON_">>])),
    ?assertEqual(sext:encode({"123456", "IPCON_"}), imem_json_data:to_strbinterm({<<"123456">>, <<"IPCON_">>})),
    ?assertEqual(sext:encode("123456"), imem_json_data:to_strbinterm(<<"123456">>)),
    ?assertEqual(
        sext:encode(["123456", "789", {"key", 22}]),
        imem_json_data:to_strbinterm([<<"123456">>, <<"789">>, {<<"key">>, 22}])
    ).

-endif.
