-module(imem_doc_config).
-include("imem.hrl").

-export([get_apps/1, get_app/1, get_mods/1, get_mod/1]).

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
    get_mods(Mods, Confs#{Mod => get_mod(Mod)}).

get_mod(Mod) when is_atom(Mod) ->
    case code:get_object_code(Mod) of
        {Mod, ModBin, _ModPath} -> get_mod({Mod, ModBin});
        error -> error({code_get_object_code, Mod})
    end;
get_mod({Mod, ModBin}) when is_binary(ModBin) ->
    case beam_lib:chunks(ModBin, [abstract_code]) of
        {ok, {Mod, [{abstract_code, {_ASTV,AST}}]}} -> ast_funs(AST);
        Else -> error(Else)
    end.

ast_funs(AST) ->
    lists:foldl(
      fun({function,_,FName,Arity,Body}, Acc) ->
              [{FName,Arity,Body}|Acc];
         (_, Acc) -> Acc
      end, [], AST).
