-module(imem_compiler).
-include("imem_seco.hrl").

-export([compile/1, compile/2, safe/1, nonLocalHFun/2]).

% erlang:_/0
-safe([now/0, date/0, registered/0]).

% erlang:_/1 (default)
-safe(['+','-','bnot',float,size,bit_size,byte_size,md5,throw,hd,tl,round,
       length,whereis,phash2]).

% erlang:_/2
-safe(['+'/2,'-'/2,'/'/2,'*'/2,'=='/2,'/='/2,'=<'/2,'<'/2,'>='/2,'>'/2,'=:='/2,
       '=/='/2,'++'/2,'--'/2,'band'/2,'bor'/2,'bxor'/2,'bsl'/2,'bsr'/2,'div'/2,
       'rem'/2,abs/2,min/2,max/2,binary_part/2,element/2,phash2/2,
       process_info/2]).

% erlang:_/3
-safe([setelement/3]).

% external erlang modules (all exported functions)
-safe([#{m => math}, #{m => lists}, #{m => proplists}, #{m => re},
       #{m => maps}, #{m => binary}, #{m => string}, #{m => erl_epmd},
       #{m => rand}]).

% external {M,F,A} s
-safe([#{m => io, f => [format/2]},
       #{m => io_lib, f => [format/2]},
       #{m => erlang, f => [node/0, integer_to_binary/2, map_size/1]},
       #{m => os, f => [getenv/1,getpid,system_time,timestamp,type,version]}]).

% external match {M,F,A} s
-safe([#{m => erlang, f => [{"^is_", 1},{"_to_",1}]}]).

safe() -> safe(?MODULE).
safe(Mod) when is_atom(Mod) ->
    DefMod = if Mod == ?MODULE -> erlang; true -> Mod end,
    lists:usort(
      lists:flatten(
        lists:foldl(
          fun({safe, [all]}, Acc) -> check_export(Mod) ++ Acc;
             ({safe, SL}, Acc) when is_list(SL) ->
                  lists:map(
                    fun(F) when is_atom(F) ->
                            if DefMod /= erlang -> check_export(DefMod, [F]);
                               true -> {DefMod, F, 1}
                            end;
                       ({F,A}) when is_atom(F), is_integer(A) ->
                            if DefMod /= erlang -> check_export(DefMod, [{F,A}]);
                               true -> {DefMod, F, A}
                            end;
                       ({M,F,A}) when is_atom(M), is_atom(F), is_integer(A) ->
                            check_export(M, F, A);
                       (#{m := M, f := F}) when is_atom(M) ->
                            check_export(M, F);
                       (#{m := M}) when is_atom(M) ->
                            check_export(M)
                    end, SL) ++ Acc;
             (_, Acc) -> Acc
          end, [], Mod:module_info(attributes)))).

check_export(M) ->
    case lists:map(fun({F, A}) -> {M, F, A} end, M:module_info(exports)) of
        [] -> ?ClientErrorNoLogging({"Nothing exported", M});
        MFAs -> MFAs
    end.

check_export(_, []) -> [];
check_export(M, [F|Fs]) ->
    check_export(M, F) ++ check_export(M, Fs);
check_export(M, {R, A}) when is_list(R), is_integer(A) ->
    [{M, Fn, A} || {Fn, Art} <- M:module_info(exports),
                   re:run(atom_to_list(Fn),R) /= nomatch, Art == A];
check_export(M, {F, A}) when is_atom(F), is_integer(A) ->
    check_export(M, F, A);
check_export(M, F) when is_atom(F) ->
    case lists:filtermap(
           fun({Fun, A}) when Fun == F -> {true, {M, F, A}};
              (_) -> false
           end, M:module_info(exports)) of
        [] -> ?ClientErrorNoLogging({"Not exported", {M, F}});
        MFAs -> MFAs
    end.

check_export(M, F, A) ->
    case erlang:function_exported(M, F, A) of
        true -> [{M, F, A}];
        false -> ?ClientErrorNoLogging({"Not exported", {M, F, A}})
    end.

compile(String) when is_list(String) -> compile(String,[]);
compile(String) when is_binary(String) -> compile(binary_to_list(String)).

compile(String,Bindings) when is_binary(String), is_list(Bindings) ->
    compile(binary_to_list(String),Bindings);
compile(String,Bindings) when is_list(String), is_list(Bindings) ->
    Code = case [lists:last(string:strip(String))] of
        "." -> String;
        _ -> String ++ "."
    end,
    {ok,ErlTokens,_} = erl_scan:string(Code),
    {ok,ErlAbsForm} = erl_parse:parse_exprs(ErlTokens),
    case catch erl_eval:exprs(ErlAbsForm, Bindings, none,
                              {value, fun ?MODULE:nonLocalHFun/2}) of
        {value,Value,_} -> Value;
        {Ex, Exception} when Ex == 'SystemException'; Ex == 'SecurityException' ->
            ?SecurityException({"Potentially harmful code", Exception});
        {'EXIT', Error} -> ?ClientErrorNoLogging({"Term compile error", Error})
    end.

nonLocalHFun(FSpec, Args) ->
    nonLocalHFun(FSpec, Args, safe()).

% @doc callback function used as 'Non-local Function Handler' in
% erl_eval:exprs/4 to restrict code injection. This callback function will
% throw '{restricted,{M,F}}' exception. If the exprassion is evaluated to
% an erlang fun, the fun will throw the same expection at runtime.
nonLocalHFun({Mod, Fun} = FSpec, Args, SafeFuns) ->
    ArgsLen = length(Args),
    case lists:member({Mod, Fun, ArgsLen}, SafeFuns) of
        true -> apply(Mod, Fun, Args);
        false ->
            case lists:keymember(Mod, 1, SafeFuns) of
                false ->
                    case safe(Mod) of
                        [] -> ?SecurityException({restricted, FSpec, ArgsLen});
                        ModSafe ->
                            nonLocalHFun(FSpec, Args, SafeFuns ++ ModSafe)
                    end;
                true -> ?SecurityException({restricted, FSpec, ArgsLen})
            end
    end.

%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

erl_value_test_() ->
    {inparallel,
     [{C, case O of
              'SystemException' ->
                  ?_assertException(throw, {'SecurityException', _}, compile(C));
              'ClientError' ->
                  ?_assertException(throw, {'ClientError', _}, compile(C));
              runtime ->
                  Fun = compile(C),
                  ?_assertException(throw, {'SecurityException', _}, Fun());
              _ ->
                  ?_assertEqual(O, compile(C))
          end}
      || {C,O} <-
         [
          {"{1,2}", {1,2}},
          {"(fun() -> 1 + 2 end)()", 3},
          {"(fun() -> A end)()", 'ClientError'},
          {"os:cmd(\"pwd\")", 'SystemException'},
          {"(fun() -> apply(filelib, ensure_dir, [\"pwd\"]) end)()",'SystemException'},
          {"fun() -> os:cmd(\"pwd\") end", runtime}
         ]
     ]}.

-endif.
