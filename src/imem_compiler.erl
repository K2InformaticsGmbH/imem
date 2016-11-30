-module(imem_compiler).
-include("imem_seco.hrl").

-export([compile/1, compile/2, ctx/0]).

-safe(['==','/=','=<','>','>=','>','=:=','=/=','+','-','*','/','++','--',
       'bnot','band','bor','bxor','bsl','bsr','abs','div','rem','min','max',
       'float','now','date','element','size','bit_size','byte_size',
       'binary_part','phash2','md5','throw','hd','tl','setelement','round']).

-unsafe(['list_to_atom','binary_to_atom','list_to_pid','binary_to_term',
         'is_pid','is_port','is_process_alive']).

-unsafesrv([start,start_link,init,handle_info,handle_call,handle_cast,
            terminate,code_change]).

ctx() ->
    #{safe =>
      lists:foldl(
        fun({safe, SL}, Acc) -> SL ++ Acc;
           (_, Acc) -> Acc
        end, [], module_info(attributes)),
      unsafe =>
      lists:foldl(
        fun({unsafe, SL}, Acc) -> SL ++ Acc;
           (_, Acc) -> Acc
        end, [], module_info(attributes)),
      unsafesrv =>
      lists:foldl(
        fun({unsafesrv, SL}, Acc) -> SL ++ Acc;
           (_, Acc) -> Acc
        end, [], module_info(attributes))}.

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
                              {value, nonLocalHFun()}) of
        {value,Value,_} -> Value;
        {Ex, Exception} when Ex == 'SystemException'; Ex == 'SecurityException' ->
            ?SecurityException({"Potentially harmful code", Exception});
        {'EXIT', Error} -> ?ClientErrorNoLogging({"Term compile error", Error})
    end.

nonLocalHFun() ->
    Ctx = ctx(),
    fun(FSpec, Args) ->
            nonLocalHFun(FSpec, Args, Ctx)
    end.

% @doc callback function used as 'Non-local Function Handler' in
% erl_eval:exprs/4 to restrict code injection. This callback function will
% exit with '{restricted,{M,F}}' exit value. If the exprassion is evaluated to
% an erlang fun, the fun will throw the same expection at runtime.
nonLocalHFun({erlang, Fun} = FSpec, Args,
             #{safe := SafeFuns, unsafe := UnsafeFuns}) ->
    case lists:member(Fun, SafeFuns) of
        true ->
            apply(erlang, Fun, Args);
        false ->
            case lists:member(Fun, UnsafeFuns) of
                true ->
                    ?SecurityException({restricted, FSpec});
                false ->
                    case re:run(atom_to_list(Fun),"^is_") of
                        nomatch ->
                            case re:run(atom_to_list(Fun),"_to_") of
                                nomatch ->  ?SecurityException({restricted, FSpec});
                                _ ->        apply(erlang, Fun, Args)
                            end;
                        _ ->
                            apply(erlang, Fun, Args)
                    end
            end
    end;
nonLocalHFun({Mod, Fun}, Args, _) when Mod==math;Mod==lists;Mod==imem_datatype;Mod==imem_json ->
    apply(Mod, Fun, Args);
nonLocalHFun({io_lib, Fun}, Args, _) when Fun==format ->
    apply(io_lib, Fun, Args);
nonLocalHFun({imem_meta, Fun}, Args, _) when Fun==log_to_db;Fun==update_index;Fun==dictionary_trigger ->
    apply(imem_meta, Fun, Args);
nonLocalHFun({imem_domain, Fun}, Args, Ctx) ->
    nonLocalServerFun({imem_domain, Fun}, Args, Ctx);
nonLocalHFun({imem_dal_skvh, Fun}, Args, _) ->
    apply(imem_dal_skvh, Fun, Args);            % TODO: restrict to subset of functions
nonLocalHFun({imem_index, Fun}, Args, _) ->
    apply(imem_index, Fun, Args);               % TODO: restrict to subset of functions
nonLocalHFun({Mod, Fun}, Args, _) ->
    apply(imem_meta, secure_apply, [Mod, Fun, Args]).

nonLocalServerFun({Mod, Fun}, Args, #{unsafesrv := UnsafeServerFuns}) ->
    case lists:member(Fun,UnsafeServerFuns) of
        true ->     ?SecurityException({restricted, {Mod, Fun}});
        false ->    apply(Mod, Fun, Args)
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
                  ?_assertException(throw, {'SystemException', _}, Fun());
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
