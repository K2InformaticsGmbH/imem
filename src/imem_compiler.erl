-module(imem_compiler).
-include("imem_seco.hrl").

-export([compile/1, compile/2, compile_mod/1, compile_mod/2, compile_mod/3,
         safe/1, format_error/1]).

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
       #{m => maps}, #{m => binary}, #{m => string}, #{m => erl_epmd}]).

% external {M,F,A} s
-safe([#{m => io, f => [format/2]},
       #{m => io_lib, f => [format/2]},
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
                              {value, nonLocalHFun()}) of
        {value,Value,_} -> Value;
        {Ex, Exception} when Ex == 'SystemException';
                             Ex == 'SecurityException' ->
            ?SecurityException({"Potentially harmful code", Exception});
        {'EXIT', Error} -> ?ClientErrorNoLogging({"Term compile error", Error})
    end.

nonLocalHFun() ->
    Safe = safe(),
    fun(FSpec, Args) ->
            nonLocalHFun(FSpec, Args, Safe)
    end.

% @doc callback function used as 'Non-local Function Handler' in
% erl_eval:exprs/4 to restrict code injection. This callback function will
% exit with '{restricted,{M,F}}' exit value. If the exprassion is evaluated to
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

compile_mod(ModuleCodeBinStr) -> compile_mod(ModuleCodeBinStr, [], []).
compile_mod(ModuleCodeBinStr, Opts) -> compile_mod(ModuleCodeBinStr, [], Opts).
compile_mod(ModuleCodeBinStr, Restrict, Opts) when is_binary(ModuleCodeBinStr) ->
    case tokenize(ModuleCodeBinStr) of
        {ok, TokenGroups} ->
            case lists:foldl(
                    fun(TokenGroup, Acc) when is_list(Acc) ->
                            case erl_parse:parse_form(TokenGroup) of
                                {ok, AbsForm} -> [AbsForm | Acc];
                                {error, ErrorInfo} ->
                                    {error, [error_info(error, ErrorInfo)]}
                            end;
                        (_, Error) -> Error
                    end, [], TokenGroups) of
                Forms when is_list(Forms) ->
                    case security_check(Forms, Restrict) of
                        List when is_list(List) ->
                            case compile:forms(Forms, [return | Opts]) of
                                error -> {error, #{error => <<"unknown">>}};
                                {ok, _Module, Bin} -> {ok, Bin};
                                {ok, _Module, Bin, []} -> {ok, Bin};
                                {ok, _Module, Bin, Warnings} ->
                                    {warning, Bin, error_info(warning, Warnings)};
                                {error, Errors, []} ->
                                    {error, error_info(error, Errors)};
                                {error, Errors, Warnings} ->
                                    {error, error_info(error, Errors) ++ error_info(warning, Warnings)}
                            end;
                        {error, Errors} ->
                            {error, error_info(error, Errors)}
                    end;
                Error -> Error
            end;
        Error -> Error
    end.

tokenize(ModuleCodeBinStr) ->
    case erl_scan:string(binary_to_list(ModuleCodeBinStr), {0,1}) of
        {ok, RawTokens, _} ->
            case catch aleppo:process_tokens(RawTokens) of
                {ok, TokensEOF} ->
                    [{eof,_} | RevTokens] = lists:reverse(TokensEOF),
                    Tokens = lists:reverse(RevTokens),
                    {ok, cut_dot(Tokens)};
                {error, Error} ->
                    {error, {preprocess, {{0, 1}, ?MODULE, Error}, {0, 1}}};
                {'EXIT', Error} ->
                    {error, {preprocess, {{0, 1}, ?MODULE, Error}, {0, 1}}}
            end;
        {error, ErrorInfo, ErrorLocation} ->
            {error, {scan, ErrorInfo, ErrorLocation}}
    end.

cut_dot(Tokens) -> cut_dot(Tokens, [[]]).
cut_dot([], [[]|Acc]) -> cut_dot([], Acc);
cut_dot([], Acc) -> Acc;
cut_dot([{dot,_} = Dot | Tokens], [A | Rest]) ->
    cut_dot(Tokens, [[], lists:reverse([Dot | A]) | Rest]);
cut_dot([T | Tokens], [A | Rest]) -> cut_dot(Tokens, [[T | A] | Rest]).

error_info(_Type, []) -> [];
error_info(Type, [{_, _, _} = ErrorInfo | ErrorInfos]) ->
    [error_info(Type, ErrorInfo) | error_info(Type, ErrorInfos)];
error_info(Type, [{_,ErrorInfos}|Tail]) ->
    error_info(Type, ErrorInfos) ++ error_info(Type, Tail);
error_info(Type, {{Line, Column}, Module, ErrorDesc}) ->
    #{  
        type => Type,
        row => Line,
        col => Column,
        text => list_to_binary(Module:format_error(ErrorDesc))
    }.

format_error([]) -> [];
format_error([H | T]) when is_list(H) -> [H | format_error(T)];
format_error([H | T]) -> [io_lib:format("~p", [H]) | format_error(T)].

security_check(Forms, Restrict) ->
    Safe = lists:usort(
            safe(?MODULE) ++
            [{'$local_mod', Fun, Arity}
             || {function, _, Fun, Arity, _Body} <- Forms]),
    security_check(Forms, Safe, Restrict).
security_check(_, {error, _} = Error, _) -> Error;
security_check([], Safe, _) -> Safe;
security_check([{attribute, _, _, _} | Forms], Safe, Restrict) ->
    security_check(Forms, Safe, Restrict);
security_check([{function, _, _Fun, _Arity, Body} | Forms], Safe, Restrict) ->
    security_check(Forms, security_check(Body, Safe, Restrict), Restrict);
security_check([Form | Forms], Safe, Restrict) ->
    security_check(Forms, security_check(Form, Safe, Restrict), Restrict);
security_check(Form, Safe, Restrict) when is_tuple(Form) ->
    case Form of
        {call, Line, {remote,_,{atom,_,Mod},{atom,_,Fun}}, Args} ->
            safety_check(Form, Line, Mod, Fun, Args, Safe, Restrict);
        {call, Line, {atom,_,Fun}, Args} ->
            safety_check(Form, Line, '$local_mod', Fun, Args, Safe, Restrict);
        _ ->
            security_check(tuple_to_list(Form), Safe, Restrict)
    end;
security_check(_, Safe, _) -> Safe.

safety_check(Form, Line, Mod, Fun, Args, Safe, Restrict) ->
    case is_safe(Mod, Fun, Args, Safe) and
         not restrict(Mod, Fun, Args, Restrict) of
        true ->
            security_check(
                tuple_to_list(Form),
                lists:usort([Mod, Fun, length(Args) | Safe])
            );
        false ->
            NewMod = if Mod == '$local_mod' -> erlang; true -> Mod end,
            case {catch safe(NewMod), lists:keymember(NewMod, 1, Safe)} of
                {{'EXIT', {undef, _}}, _} -> Safe;
                {ModSafe, false} when is_list(ModSafe), length(ModSafe) > 0 ->
                    safety_check(Form, Line, NewMod, Fun, Args,
                                 lists:usort(ModSafe ++ Safe), Restrict);
                _ ->
                    {error, [{Line, ?MODULE,
                             ["unsafe function call ",
                              NewMod, ":", Fun, "/", length(Args)]}]}
            end
    end.

is_safe(_, _, _, []) -> false;
is_safe(M, F, Args, [{M, F, Arity} | _]) when length(Args) == Arity -> true;
is_safe(M, F, A, [_ | Safe]) -> is_safe(M, F, A, Safe).

restrict(_, _, _, []) -> false;
restrict(M, F, _Args, [{M, F} | _]) -> true;
restrict(M, F, A, [_ | Restricted]) -> restrict(M, F, A, Restricted).

%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

compile_test_() ->
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

-define(TEST_MODULES, [
{"simple",
<<"
-module(test).
-export([test/0]).
test() ->
    ok.
">>, ok},
{"macro",
<<"
-module(test).
-export([test/0]).
-define(XXX, true).
test() ->
    ?XXX.
">>, ok},
{"behavior",
<<"
-module(test).
-behavior(gen_server).
-export([handle_call/3,handle_cast/2,handle_info/2,init/1,terminate/2]).
handle_call(_, _, _) -> ok.
handle_cast(_, _) -> ok.
handle_info(_, _) -> ok.
init(_) -> ok.
terminate(_, _) -> ok.
">>, ok},
{"restricted",
<<"
-module(test).
-export([test/0]).
test() ->
    io:format(\"~p\", [123]),
    ok.
">>,
[#{type => error, row => 5, text => <<"unsafe function call io:format/2">>}]},
{"error",
<<"
-module(test).
-export([test/0, test/1]).
test() ->
    ok.
">>,
[#{type => error, row => 3, text => <<"function test/1 undefined">>}]},
{"warning",
<<"
-module(test).
-export([test/0]).
test() ->
    X = 0,
    ok.
">>,
[#{type => warning, row => 5, text => <<"variable 'X' is unused">>}]},
{"error and warning",
<<"
-module(test).
-export([test/0, test/1]).
test() ->
    X = 0,
    ok.
">>,
[#{type => error, row => 3, text => <<"function test/1 undefined">>},
 #{type => warning, row => 5, text => <<"variable 'X' is unused">>}]},
{"unsafe",
<<"
-module(test).

-export([test/0]).

test() ->
    bikram:call(bnot 1),
    binary_to_atom(<<\"1\">>, utf8),
    ok.
">>,
[#{type => error, row => 8,
   text => <<"unsafe function call erlang:binary_to_atom/2">>}]}
]).

compile_mod_test_() ->
    {inparallel,
     [{T,
        case {O, compile_mod(C, [{io, format}], [])} of
            {O, {warning, _, Warning}} -> ?_assertEqual(O, Warning);
            {O, {error, Error}} -> ?_assertEqual(O, Error);
            {ok, Output} -> ?_assertMatch({ok, _}, Output)
        end} || {T, C, O} <- ?TEST_MODULES]}.

-endif.
