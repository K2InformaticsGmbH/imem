-module(imem_rec_pretty_pt).

-export([parse_transform/2]).

-define(L(__F, __A), io:format(user, "{~p:~p} " __F, [?MODULE, ?LINE | __A])).

parse_transform(Forms, _Options) ->
    try
        Functions =
            lists:foldl(
                fun
                    ({attribute, _, record, {Record, RFields}}, Funcs) ->
                        FieldNames =
                            [
                                case R of
                                    {record_field, _, {atom, _, N}} -> N;
                                    {record_field, _, {atom, _, N}, _} -> N;
                                    {typed_record_field, {record_field, _, {atom, _, N}}, _} -> N;
                                    {typed_record_field, {record_field, _, {atom, _, N}, _}, _} -> N
                                end || R <- RFields
                            ],
                        RecFun = list_to_atom(atom_to_list(Record) ++ "_pretty"),
                        Funcs#{RecFun => rf(Record, RecFun, FieldNames)};
                    (_, Acc) -> Acc
                end,
                #{},
                Forms
            ),
        RecFuns = maps:keys(Functions),
        CalledRecFuns = calls(Forms, RecFuns),
        UsedFunctions = maps:values(maps:with(CalledRecFuns, Functions)),
        [{eof, _} = EOF | Rest] = move_eof_to_top(Forms),
        lists:reverse([EOF | add_funs(Rest, UsedFunctions)] ++ Rest)
    catch
        _:Error ->
            ?L("parse transform failed~n~p~n~p~n", [Error, erlang:get_stacktrace()]),
            Forms
    end.

move_eof_to_top(Forms) -> move_eof_to_top(Forms, []).

move_eof_to_top([{eof, _} = EOF | Rest], Acc) -> [EOF | lists:reverse(Rest) ++ Acc];
move_eof_to_top([Form | Rest], Acc) -> move_eof_to_top(Rest, [Form | Acc]).

add_funs(Forms, Funs) -> add_funs(Forms, Funs, []).

add_funs(_, [], Acc) -> lists:reverse(Acc);
add_funs(Forms, [{function, _, Fn, _, _} = F | Funs], Acc) ->
    add_funs(
        Forms,
        Funs,
        case lists:keymember(Fn, 3, Forms) of
            true -> Acc;
            false -> [F | Acc]
        end
    ).

calls(Forms, RecFuns) -> calls(Forms, [], RecFuns).

calls([], Acc, _RecFuns) -> lists:usort(Acc);
calls({call, _, {atom, _, Fn}, _}, Acc, RecFuns) ->
    case lists:member(Fn, RecFuns) of
        true -> [Fn | Acc];
        _ -> Acc
    end;
calls([Head | Rest], Acc, RecFuns) -> calls(Rest, calls(Head, Acc, RecFuns), RecFuns);
calls(Tuple, Acc, RecFuns) when is_tuple(Tuple) -> calls(tuple_to_list(Tuple), Acc, RecFuns);
calls(_, Acc, _RecFuns) -> lists:usort(Acc).

rf(Record, Fun, FieldNames) ->
    Fmt =
        lists:flatten(
            [
                "#",
                atom_to_list(Record),
                "{",
                string:join([atom_to_list(F) ++ " = ~p" || F <- lists:reverse(FieldNames)], ", "),
                "}"
            ]
        ),
    {
        function,
        1,
        Fun,
        1,
        [
            {
                clause,
                1,
                [{var, 1, '_S'}],
                [],
                [
                    {
                        call,
                        1,
                        {remote, 1, {atom, 1, io_lib}, {atom, 1, format}},
                        [{string, 1, Fmt}, f2l(Record, FieldNames)]
                    }
                ]
            }
        ]
    }.

f2l(Record, FieldNames) -> f2l(Record, FieldNames, {nil, 13}).

f2l(_, [], Acc) -> Acc;
f2l(Record, [FieldName | FieldNames], Acc) ->
    f2l(Record, FieldNames, {cons, 1, {record_field, 1, {var, 1, '_S'}, Record, {atom, 1, FieldName}}, Acc}).
