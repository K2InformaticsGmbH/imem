-module(imem_doc_config).
-include("imem.hrl").

-export([get_apps/1, get_app/1, get_mods/1, get_mod/1]).

-export([get_apps_kc/1]).

get_apps_kc(Apps) ->
    maps:fold(
      fun(_, Mods, Acc) ->
              maps:fold(
                fun(_, Configs,IAcc) ->
                        lists:foldl(
                          fun({K,_}, IIAcc) -> [{K,<<>>}|IIAcc];
                             ({K,_,C}, IIAcc) -> [{K,list_to_binary(C)}|IIAcc]
                          end, IAcc, Configs)
                end, Acc, Mods)
      end, [], get_apps(Apps)).

get_apps(Apps) -> get_apps(Apps, #{}).
get_apps([], Confs) -> Confs;
get_apps([App|Apps], Confs) ->
    get_apps(Apps, Confs#{App => get_app(App)}).

get_app(App) ->
    {ok, Mods} = application:get_key(App, modules),
    get_mods(Mods).

get_mods(Modules) -> get_mods(Modules, #{}).
get_mods([], Confs) -> Confs;
get_mods([Mod|Mods], Confs) ->
    get_mods(Mods, case get_mod(Mod) of
                       [] -> Confs;
                       Conf -> Confs#{Mod => Conf}
                   end).

get_mod(Mod) when is_atom(Mod) ->
    case code:get_object_code(Mod) of
        {Mod, ModBin, _ModPath} -> get_mod({Mod, ModBin});
        error -> error({code_get_object_code, Mod})
    end;
get_mod({Mod, ModBin}) when is_binary(ModBin) ->
    case beam_lib:chunks(ModBin, [abstract_code]) of
        {ok, {Mod, [{abstract_code, {_ASTV,AC}}]}} ->
            % io:fwrite("~s~n", [erl_prettypr:format(erl_syntax:form_list(AC))]),
            % AST = erl_syntax:form_list(AC),
            % file:write_file("dump.ast",list_to_binary(io_lib:format("~p", [AST]))),
            find(erl_syntax:form_list(AC));
        Else -> error(Else)
    end.

find({tree,form_list,{attr,0,[],none},Comps}) ->
    find(Comps, []).
find([], Acc) -> Acc;
% get_config_hlk(_, Key, _, Context, Default, Documentation)
% put_config_hlk(_, Key, _, Context, Value, _, Documentation)
find([{call,_,{remote,_,{atom,_,imem_config},{atom,_,get_config_hlk}},
       [_,Key,_,_,Default|Rest]} | Comps], Acc) ->
    find(
      Comps,
      lists:usort(
        [list_to_tuple(
           [ast2term(Key), ast2term(Default)
            | case Rest of
                  [Doc] -> [ast2term(Doc)];
                  _ -> []
              end]) | Acc]
       ));
find([C|Comps], Acc) when is_atom(C); is_integer(C); is_float(C); is_map(C) ->
    find(Comps, Acc);
find([C|Comps], Acc) -> find(Comps, find(C, Acc));
find(C, Acc) when is_tuple(C) -> find(tuple_to_list(C), Acc).

ast2term({var,_,Var}) -> error({unsupported_var,Var});
ast2term(AST) ->
    try
        {value, Value, _} = erl_eval:expr(erl_syntax:revert(AST),[]),
        Value
    catch
        _:_ -> '$unknown'
    end.
