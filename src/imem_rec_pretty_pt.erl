-module(imem_rec_pretty_pt).
-export([parse_transform/2]).

-define(L(__F, __A), io:format(user, "{~p:~p} "__F, [?MODULE, ?LINE | __A])).

parse_transform(Forms, _Options) ->
    try
        {Functions, Exports} =
        lists:foldl(
          fun({attribute,_,record,{Record,RFields}}, {Funcs, Exports}) ->
                  FieldNames =
                  [case R of
                       {record_field,_,{atom,_,N}} -> N;
                       {record_field,_,{atom,_,N},_} -> N;
                       {typed_record_field,{record_field,_,{atom,_,N}},_} -> N;
                       {typed_record_field,{record_field,_,{atom,_,N},_},_} ->
                           N
                   end || R <- RFields],
                  Fun = list_to_atom(atom_to_list(Record)++"_pretty"),
                  {[rf(Record, Fun, FieldNames) | Funcs],
                   [{attribute,1,export,[{Fun,1}]} | Exports]};
             (_, Acc) -> Acc
          end, {[], []}, Forms),
        RecFuns = [F || {attribute,1,export,[{F,1}]} <- Exports],
        io:format(user, "Functions ~p~n", [RecFuns]),
        io:format(user, "Forms ~p~n", [Forms]),
        case ins_exprts(Exports, Forms) of
            Forms -> Forms;
            Forms1 ->
                [{eof,_} = EOF | Rest] = lists:reverse(Forms1),
                lists:reverse([EOF|Functions]++Rest)
        end
    catch
        _:Error ->
            ?L("parse transform failed~n~p~n~p~n",
               [Error, erlang:get_stacktrace()]),
            Forms
    end.

calls(Forms) -> calls(Forms, []).
calls([], Acc) -> Acc;
calls([{attribute,_,_,_}|Rest], Acc) -> calls(Rest, Acc);
calls([{function,_,_,_,FBody}|Rest], Acc) -> calls(Rest, calls(FBody, Acc));
calls([{clause,_,_,_,CBody}|Rest], Acc) -> calls(Rest, calls(CBody, Acc));
calls([{call,_,{remote,_,_,_},CBody}|Rest], Acc) ->
    calls(Rest, calls(CBody, Acc));
calls([{cons,_,Cons,CTail}|Rest], Acc) ->
    calls(Rest, calls(Cons, Acc));

ins_exprts(Exports, [_|_] = Forms) ->
    case lists:usort([lists:member(E, Forms) || E <- Exports]) of
        [false] -> ins_exprts(Exports, {[], Forms});
        _ -> Forms
    end;
ins_exprts([], {Heads,Tail}) -> lists:reverse(Heads)++Tail;
ins_exprts(Exports, {Heads, []}) ->
    [{attribute,_,file,_} = F, {attribute,_,module,_} = M | R] = lists:reverse(Heads),
    [F,M|Exports++R];
ins_exprts(Exports, {Heads, [{attribute,_,export,_} = E | Tail]}) ->
    ins_exprts([], {[E | Exports] ++ Heads, Tail});
ins_exprts(Exports, {Heads, [F | Tail]}) ->
    ins_exprts(Exports, {[F | Heads], Tail}).

rf(Record, Fun, FieldNames) ->
    Fmt =
    lists:flatten(
      ["#",atom_to_list(Record),"{",
       string:join([atom_to_list(F)++" = ~p"
                    || F <- lists:reverse(FieldNames)], ", "),
       "}"]),
    {function,1,Fun,1,
     [{clause,1,
       [{var,1,'_S'}], [],
       [{call,1,{remote,1,{atom,1,io_lib},{atom,1,format}},
         [{string,1,Fmt}, f2l(Record, FieldNames)]}]
      }]}.

f2l(Record, FieldNames) ->
    f2l(Record, FieldNames, {nil,13}).
f2l(_, [], Acc) -> Acc;
f2l(Record, [FieldName|FieldNames], Acc) ->
    f2l(Record, FieldNames,
        {cons,1,{record_field,1,{var,1,'_S'},Record,{atom,1,FieldName}}, Acc}).
