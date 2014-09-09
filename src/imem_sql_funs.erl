-module(imem_sql_funs).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define( FilterFuns, 
            [ safe, concat, is_nav 
            , is_member, is_like, is_regexp_like, to_name, to_text
            , add_dt, add_ts, diff_dt, diff_ts
            , to_atom, to_string, to_binstr, to_integer, to_float, to_number
            , to_tuple, to_list, to_term, to_binterm, from_binterm
            , to_decimal, from_decimal
            , byte_size, bit_size, nth, sort, usort, reverse, last
            , remap
            ]).

-export([ filter_funs/0
        , expr_fun/1
        , filter_fun/1
        ]).

-export([ unary_fun_bind_type/1
        , unary_fun_result_type/1
        , binary_fun_bind_type1/1
        , binary_fun_bind_type2/1
        , binary_fun_result_type/1
        , ternary_not/1
        , ternary_and/2
        , ternary_or/2
        , mod_op_1/3
        , mod_op_2/4
        , mod_op_3/5
        , math_plus/1
        , math_minus/1
        , is_nav/1
        ]).

-export([ re_compile/1
        , re_match/2
        , like_compile/1
        , like_compile/2
        ]).

-export([ to_integer/1
        , to_float/1
        , to_number/1
        , to_string/1
        , to_binstr/1
        , to_atom/1
        , to_existing_atom/1
        , to_tuple/1
        , to_list/1
        , to_term/1
        , to_binterm/1
        , to_name/1
        , to_text/1
        ]).

-export([ concat/2
        , add_dt/2
        , add_ts/2
        , diff_dt/2
        , diff_ts/2
        , to_decimal/2
        , from_decimal/2
        , from_binterm/1
        , is_member/2
        , remap/3
        ]).


filter_funs() -> ?FilterFuns.

unary_fun_bind_type(B) when is_binary(B) ->     unary_fun_bind_type(binary_to_list(B));
unary_fun_bind_type([$t,$o,$_|_]) ->            #bind{type=binstr,default= <<>>};
unary_fun_bind_type("is_integer") ->            #bind{type=integer,default=?nav};
unary_fun_bind_type("is_float") ->              #bind{type=float,default=?nav};
unary_fun_bind_type("is_boolean") ->            #bind{type=boolean,default=?nav};
unary_fun_bind_type("is_binstr") ->             #bind{type=binstr,default= <<>>};
unary_fun_bind_type("is_string") ->             #bind{type=string,default=[]};
unary_fun_bind_type("is_tuple") ->              #bind{type=tuple,default=undefined};
unary_fun_bind_type("is_list") ->               #bind{type=list,default=[]};
unary_fun_bind_type("is_ipaddr") ->             #bind{type=ipaddr,default=undefined};
unary_fun_bind_type("is_datetime") ->           #bind{type=datetime,default=undefined};
unary_fun_bind_type("is_timestamp") ->          #bind{type=timestamp,default=undefined};
unary_fun_bind_type([$i,$s,$_|_]) ->            #bind{type=term,default=undefined};
unary_fun_bind_type("length") ->                #bind{type=list,default=[]};
unary_fun_bind_type("hd") ->                    #bind{type=list,default=[]};
unary_fun_bind_type("tl") ->                    #bind{type=list,default=[]};
unary_fun_bind_type("last") ->                  #bind{type=list,default=[]};
unary_fun_bind_type("sort") ->                  #bind{type=list,default=[]};
unary_fun_bind_type("usort") ->                 #bind{type=list,default=[]};
unary_fun_bind_type("reverse") ->               #bind{type=list,default=[]};
unary_fun_bind_type("size") ->                  #bind{type=tuple,default=undefined};
unary_fun_bind_type("tuple_size") ->            #bind{type=tuple,default=undefined};
unary_fun_bind_type("byte_size") ->             #bind{type=binary,default= <<>>};
unary_fun_bind_type("bit_size") ->              #bind{type=binary,default= <<>>};
unary_fun_bind_type("from_binterm") ->          #bind{type=binterm,default= ?nav};
unary_fun_bind_type(_) ->                       #bind{type=number,default= ?nav}.

unary_fun_result_type(B) when is_binary(B) ->   unary_fun_result_type(binary_to_list(B));
unary_fun_result_type([$i,$s,$_|_]) ->          #bind{type=boolean,default=?nav};
unary_fun_result_type("hd") ->                  #bind{type=term,default=undefined};
unary_fun_result_type("last") ->                #bind{type=term,default=undefined};
unary_fun_result_type("tl") ->                  #bind{type=list,default=[]};
unary_fun_result_type("sort") ->                #bind{type=list,default=[]};
unary_fun_result_type("usort") ->               #bind{type=list,default=[]};
unary_fun_result_type("reverse") ->             #bind{type=list,default=[]};
unary_fun_result_type("length") ->              #bind{type=integer,default=?nav};
unary_fun_result_type("size") ->                #bind{type=integer,default=?nav};
unary_fun_result_type("tuple_size") ->          #bind{type=integer,default=?nav};
unary_fun_result_type("byte_size") ->           #bind{type=integer,default=?nav};
unary_fun_result_type("bit_size") ->            #bind{type=integer,default=?nav};
unary_fun_result_type("from_decimal") ->        #bind{type=float,default=?nav};
unary_fun_result_type("from_binterm") ->        #bind{type=term,default=?nav};
unary_fun_result_type(String) ->            
    case re:run(String,"to_(.*)$",[{capture,[1],list}]) of
        {match,["binstr"]}->                    #bind{type=binstr,default=?nav};
        {match,["binterm"]}->                   #bind{type=binterm,default=?nav};
        {match,["boolean"]}->                   #bind{type=boolean,default=?nav};
        {match,["decimal"]}->                   #bind{type=decimal,default=?nav};
        {match,["float"]}->                     #bind{type=float,default=?nav};
        {match,["integer"]}->                   #bind{type=integer,default=?nav};
        {match,["list"]}->                      #bind{type=list,default=[]};
        {match,["name"]}->                      #bind{type=binstr,default=?nav};
        {match,["string"]}->                    #bind{type=string,default=?nav};
        {match,["text"]}->                      #bind{type=binstr,default=?nav};
        {match,[Name]}->                        #bind{type=list_to_existing_atom(Name),default=undefined};
        nomatch ->                              #bind{type=number,default=?nav}
    end.

binary_fun_bind_type1(B) when is_binary(B) ->   binary_fun_bind_type1(binary_to_list(B));
binary_fun_bind_type1("element") ->             #bind{type=integer,default=?nav};
binary_fun_bind_type1("nth") ->                 #bind{type=integer,default=?nav};
binary_fun_bind_type1("is_like") ->             #bind{type=binstr,default=?nav};
binary_fun_bind_type1("is_regexp_like") ->      #bind{type=binstr,default=?nav};
binary_fun_bind_type1("to_decimal") ->          #bind{type=binstr,default=?nav};
binary_fun_bind_type1("from_decimal") ->        #bind{type=decimal,default=?nav};
binary_fun_bind_type1(_) ->                     #bind{type=number,default=?nav}.

binary_fun_bind_type2(B) when is_binary(B) ->   binary_fun_bind_type2(binary_to_list(B));
binary_fun_bind_type2("element") ->             #bind{type=tuple,default=?nav};
binary_fun_bind_type2("nth") ->                 #bind{type=list,default=[]};
binary_fun_bind_type2("is_like") ->             #bind{type=binstr,default=?nav};
binary_fun_bind_type2("is_regexp_like") ->      #bind{type=binstr,default=?nav};
binary_fun_bind_type2("to_decimal") ->          #bind{type=integer,default=0};
binary_fun_bind_type2("from_decimal") ->        #bind{type=integer,default=0};
binary_fun_bind_type2(_) ->                     #bind{type=number,default=?nav}.

binary_fun_result_type(B) when is_binary(B) ->  binary_fun_result_type(binary_to_list(B));
binary_fun_result_type("element") ->            #bind{type=term,default=?nav};
binary_fun_result_type("nth") ->                #bind{type=term,default=?nav};
binary_fun_result_type("is_like") ->            #bind{type=boolean,default=?nav};
binary_fun_result_type("is_regexp_like") ->     #bind{type=boolean,default=?nav};
binary_fun_result_type("to_decimal") ->         #bind{type=decimal,default=?nav};
binary_fun_result_type("from_decimal") ->       #bind{type=float,default=?nav};
binary_fun_result_type(_) ->                    #bind{type=number,default=?nav}.

re_compile(?nav) -> ?nav;
re_compile(S) when is_list(S);is_binary(S) ->
    case (catch re:compile(S))  of
        {ok, MP} -> MP;
        _ ->        ?nav
    end;
re_compile(_) ->    ?nav.

like_compile(S) -> like_compile(S, <<>>).

like_compile(_, ?nav) -> ?nav;
like_compile(?nav, _) -> ?nav;
like_compile(S, Esc) when is_list(S); is_binary(S)  -> re_compile(transform_like(S, Esc));
like_compile(_,_)     -> ?nav.

transform_like(S, Esc) ->
    E = if
        Esc =:= "" ->       "";
        Esc =:= <<>> ->     "";
        is_list(Esc) ->     Esc;
        is_binary(Esc) ->   binary_to_list(Esc);
        true ->             ""
    end,
    Escape = if E =:= "" -> ""; true -> "["++E++"]" end,
    NotEscape = if E =:= "" -> ""; true -> "(^|[^"++E++"])" end,
    S0 = re:replace(S, "([\\\\^$.\\[\\]|()?*+\\-{}])", "\\\\\\1", [global, {return, binary}]),
    S1 = re:replace(S0, NotEscape++"%", "\\1.*", [global, {return, binary}]),
    S2 = re:replace(S1, NotEscape++"_", "\\1.", [global, {return, binary}]),
    S3 = re:replace(S2, Escape++"%", "%", [global, {return, binary}]),
    S4 = re:replace(S3, Escape++"_", "_", [global, {return, binary}]),
    list_to_binary(["^",S4,"$"]).

re_match(?nav, _) -> ?nav;
re_match(_, ?nav) -> ?nav;
re_match(RE, S) when is_list(S);is_binary(S) ->
    case re:run(S, RE) of
        nomatch ->  false;
        _ ->        true
    end;
re_match(RE, S) ->
    case re:run(io_lib:format("~p", [S]), RE) of
        nomatch ->  false;
        _ ->        true
    end.

filter_fun(FTree) ->
    fun(X) -> 
        case expr_fun(FTree) of
            true ->     true;
            false ->    false;
            ?nav ->     false;
            F when is_function(F,1) ->
                case F(X) of
                    true ->     true;
                    false ->    false;
                    ?nav ->     false
                    %% Other ->    ?ClientError({"Filter function evaluating to non-boolean term",Other})
                end
        end
    end.

%% Constant tuple expressions
expr_fun({const, A}) when is_tuple(A) -> A;
%% Select field Expression header
expr_fun(#bind{tind=0,cind=0,btree=BTree}) -> expr_fun(BTree);
%% Comparison expressions
% expr_fun({'==', Same, Same}) -> true;        %% TODO: Is this always true? (what if Same evaluates to ?nav)
% expr_fun({'/=', Same, Same}) -> false;       %% TODO: Is this always true? (what if Same evaluates to ?nav)
expr_fun({Op, A, B}) when Op=='==';Op=='>';Op=='>=';Op=='<';Op=='=<';Op=='/=' ->
    comp_fun({Op, A, B}); 
%% Mathematical expressions    
expr_fun({'pi'}) -> math:pi();
expr_fun({Op, A}) when Op=='+';Op=='-' ->
    math_fun({Op, A}); 
expr_fun({Op, A}) when Op=='sqrt';Op=='log';Op=='log10';Op=='exp';Op=='erf';Op=='erfc' ->
    module_fun('math', {Op, A});
expr_fun({Op, A}) when Op=='sin';Op=='cos';Op=='tan';Op=='asin';Op=='acos';Op=='atan' ->
    module_fun('math', {Op, A});
expr_fun({Op, A}) when Op=='sinh';Op=='cosh';Op=='tanh';Op=='asinh';Op=='acosh';Op=='atanh' ->
    module_fun('math', {Op, A});
expr_fun({Op, A, B}) when Op=='+';Op=='-';Op=='*';Op=='/';Op=='div';Op=='rem' ->
    math_fun({Op, A, B});
expr_fun({Op, A, B}) when Op=='pow';Op=='atan2' ->
    module_fun('math', {Op, A, B});
%% Erlang module
expr_fun({Op, A}) when Op=='abs';Op=='length';Op=='hd';Op=='tl';Op=='size';Op=='tuple_size';Op=='round';Op=='trunc' ->
    module_fun('erlang', {Op, A});
expr_fun({Op, A}) when Op=='atom_to_list';Op=='binary_to_float';Op=='binary_to_integer';Op=='binary_to_list' ->
    module_fun('erlang', {Op, A});
expr_fun({Op, A}) when Op=='bitstring_to_list';Op=='binary_to_term';Op=='bit_size';Op=='byte_size';Op=='crc32' ->
    module_fun('erlang', {Op, A});
expr_fun({Op, A}) when Op=='float';Op=='float_to_binary';Op=='float_to_list';Op=='fun_to_list';Op=='tuple_to_list' ->
    module_fun('erlang', {Op, A});
expr_fun({Op, A}) when Op=='integer_to_binary';Op=='integer_to_list';Op=='fun_to_list';Op=='list_to_float' ->
    module_fun('erlang', {Op, A});
expr_fun({Op, A}) when Op=='list_to_integer';Op=='list_to_pid';Op=='list_to_tuple';Op=='phash2';Op=='pid_to_list' ->
    module_fun('erlang', {Op, A});
expr_fun({Op, A}) when Op=='is_atom';Op=='is_binary';Op=='is_bitstring';Op=='is_boolean' ->
    module_fun('erlang', {Op, A});
expr_fun({Op, A}) when Op=='is_float';Op=='is_function';Op=='is_integer';Op=='is_list';Op=='is_number' ->
    module_fun('erlang', {Op, A});
expr_fun({Op, A}) when Op=='is_pid';Op=='is_port';Op=='is_reference';Op=='is_tuple' ->
    module_fun('erlang', {Op, A});
expr_fun({Op, A, B}) when Op=='is_function';Op=='is_record';Op=='atom_to_binary';Op=='binary_part' ->
    module_fun('erlang', {Op, A, B});
expr_fun({Op, A, B}) when  Op=='integer_to_binary';Op=='integer_to_list';Op=='list_to_binary';Op=='list_to_bitstring' ->
    module_fun('erlang', {Op, A, B});
expr_fun({Op, A, B}) when  Op=='list_to_integer';Op=='max';Op=='min';Op=='phash2' ->
    module_fun('erlang', {Op, A, B});
expr_fun({Op, A, B}) when Op=='crc32';Op=='float_to_binary';Op=='float_to_list' ->
    module_fun('erlang', {Op, A, B});
expr_fun({Op, A, B}) when Op=='atom_to_binary';Op=='binary_to_integer';Op=='binary_to_integer';Op=='binary_to_term' ->
    module_fun('erlang', {Op, A, B});
%% Lists module
expr_fun({Op, A}) when Op=='last';Op=='reverse';Op=='sort';Op=='usort' ->
    module_fun('lists', {Op, A});
expr_fun({Op, A, B}) when Op=='nth';Op=='member';Op=='merge';Op=='nthtail';Op=='seq';Op=='sublist';Op=='subtract';Op=='usort' ->
    module_fun('lists', {Op, A, B});
%% Logical expressions
expr_fun({'not', A}) ->
    case expr_fun(A) of
        F when is_function(F) ->    fun(X) -> ternary_not(F(X)) end;
        V ->                        ternary_not(V)
    end;                       
expr_fun({'and', A, B}) ->
    Fa = expr_fun(A),
    Fb = expr_fun(B),
    case {Fa,Fb} of
        {true,true} ->  true;
        {false,_} ->    false;
        {_,false} ->    false;
        {true,_} ->     Fb;         %% may be ?nav or a fun evaluating to ?nav
        {_,true} ->     Fa;         %% may be ?nav or a fun evaluating to ?nav
        {_,_} ->        fun(X) -> ternary_and(Fa(X),Fb(X)) end
    end;
expr_fun({'or', A, B}) ->
    Fa = expr_fun(A),
    Fb = expr_fun(B),
    case {Fa,Fb} of
        {false,false}-> false;
        {true,_} ->     true;
        {_,true} ->     true;
        {false,_} ->    Fb;         %% may be ?nav or a fun evaluating to ?nav
        {_,false} ->    Fa;         %% may be ?nav or a fun evaluating to ?nav
        {_,_} ->        fun(X) -> ternary_or(Fa(X),Fb(X)) end
    end;
%% Unary custom filters
expr_fun({'safe', A}) ->
    safe_fun(A);
expr_fun({Op, A}) when Op=='to_string';Op=='to_binstr';Op=='to_binterm';Op=='from_binterm';Op=='to_integer';Op=='to_float';Op=='to_number'->
    unary_fun({Op, A});
expr_fun({Op, A}) when Op=='to_atom';Op=='to_tuple';Op=='to_list';Op=='to_term';Op=='to_name';Op=='to_text';Op=='is_nav' ->
    unary_fun({Op, A});
expr_fun({Op, A}) ->
    ?UnimplementedException({"Unsupported expression operator", {Op, A}});
%% Binary custom filters
expr_fun({Op, A, B}) when Op=='is_member';Op=='is_like';Op=='is_regexp_like';Op=='element';Op=='concat' ->
    binary_fun({Op, A, B});
expr_fun({Op, A, B}) when Op=='to_decimal';Op=='from_decimal';Op=='add_dt';Op=='add_ts' ->
    binary_fun({Op, A, B});
expr_fun({Op, A, B}) ->
    ?UnimplementedException({"Unsupported expression operator", {Op, A, B}});
%% Ternary custom filters
expr_fun({Op, A, B, C}) when Op=='remap' ->
    ternary_fun({Op, A, B, C});
expr_fun({Op, A, B, C}) ->
    ?UnimplementedException({"Unsupported function arity 3", {Op, A, B, C}});
expr_fun({Op, A, B, C, D}) ->
    ?UnimplementedException({"Unsupported function arity 4", {Op, A, B, C, D}});
expr_fun(Value)  -> Value.

bind_action(P) when is_function(P) -> true;     %% parameter already bound to function
bind_action(#bind{tind=0,cind=0}=P) ->          ?SystemException({"Unexpected expression binding",P});
bind_action(#bind{}=P) -> P;                    %% find bind by tag name or return false for value prameter
bind_action(_) -> false. 

safe_fun(A) ->
    Fa = expr_fun(A),
    safe_fun_final(Fa).

safe_fun_final(A) ->
    case bind_action(A) of 
        false ->            A;        
        true ->             fun(X) -> try A(X) catch _:_ -> ?nav end end;       
        ABind ->            fun(X) -> try ?BoundVal(ABind,X) catch _:_ -> ?nav end end
    end.

module_fun(Mod, {Op, {const,A}}) when is_tuple(A) ->
    module_fun_final(Mod, {Op, A});
module_fun(Mod, {Op, A}) ->
    module_fun_final(Mod, {Op, expr_fun(A)});
module_fun(Mod, {Op, {const,A}, {const,B}}) when is_tuple(A),is_tuple(B)->
    module_fun_final(Mod, {Op, A, B});
module_fun(Mod, {Op, {const,A}, B}) when is_tuple(A) ->
    module_fun_final(Mod, {Op, A, expr_fun(B)});
module_fun(Mod, {Op, A, {const,B}}) when is_tuple(B) ->
    module_fun_final(Mod, {Op, expr_fun(A), B});
module_fun(Mod, {Op, A, B}) ->
    Fa = expr_fun(A),
    Fb = expr_fun(B),
    module_fun_final(Mod, {Op, Fa, Fb}).

module_fun_final(Mod, {Op, A}) -> 
    case bind_action(A) of 
        false ->        mod_op_1(Mod,Op,A);
        true ->         fun(X) -> mod_op_1(Mod,Op,A(X)) end;
        ABind ->        fun(X) -> mod_op_1(Mod,Op,?BoundVal(ABind,X)) end
    end;
module_fun_final(Mod, {Op, A, B}) -> 
    case {bind_action(A),bind_action(B)} of 
        {false,false} ->        mod_op_2(Mod,Op,A,B);
        {false,true} ->         fun(X) -> mod_op_2(Mod,Op,A,B(X)) end;
        {false,BBind} ->        fun(X) -> mod_op_2(Mod,Op,A,?BoundVal(BBind,X)) end;
        {true,false} ->         fun(X) -> mod_op_2(Mod,Op,A(X),B) end;
        {true,true} ->          fun(X) -> mod_op_2(Mod,Op,A(X),B(X)) end; 
        {true,BBind} ->         fun(X) -> mod_op_2(Mod,Op,A(X),?BoundVal(BBind,X)) end; 
        {ABind,false} ->        fun(X) -> mod_op_2(Mod,Op,?BoundVal(ABind,X),B) end; 
        {ABind,true} ->         fun(X) -> mod_op_2(Mod,Op,?BoundVal(ABind,X),B(X)) end; 
        {ABind,BBind} ->        fun(X) -> mod_op_2(Mod,Op,?BoundVal(ABind,X),?BoundVal(BBind,X)) end 
    end.

mod_op_1(_,_,?nav) -> ?nav;
mod_op_1(Mod,Op,A) -> Mod:Op(A).

mod_op_2(_,_,_,?nav) -> ?nav;
mod_op_2(_,_,?nav,_) -> ?nav;
mod_op_2(Mod,Op,A,B) -> Mod:Op(A,B).

mod_op_3(_,_,_,_,?nav) -> ?nav;
mod_op_3(_,_,_,?nav,_) -> ?nav;
mod_op_3(_,_,?nav,_,_) -> ?nav;
mod_op_3(Mod,Op,A,B,C) -> Mod:Op(A,B,C).

math_fun({Op, A}) ->
    math_fun_unary({Op, expr_fun(A)});
math_fun({Op, A, B}) ->
    Fa = expr_fun(A),
    Fb = expr_fun(B),
    math_fun_binary({Op, Fa, Fb}).

math_fun_unary({'+', A}) ->
    case bind_action(A) of 
        false ->            math_plus(A);        
        true ->             fun(X) -> math_plus(A(X)) end;       
        ABind ->            fun(X) -> math_plus(?BoundVal(ABind,X)) end
    end;
math_fun_unary({'-', A}) ->
    case bind_action(A) of 
        false ->            math_minus(A);        
        true ->             fun(X) -> math_minus(A(X)) end;
        ABind ->            fun(X) -> math_minus(?BoundVal(ABind,X)) end
    end.

math_plus(?nav) ->                  ?nav;
math_plus(A) when is_number(A) ->   A;
math_plus(_) ->                     ?nav.

math_minus(?nav) ->                 ?nav;
math_minus(A) when is_number(A) ->  (-A);
math_minus(_) ->                    ?nav.

-define(MathOpBlockBinary(__Op,__A,__B), 
        case __Op of
            _ when (is_number(__A)==false);(is_number(__B)==false) -> ?nav;
            '+'  ->      (__A + __B);
            '-'  ->      (__A - __B);
            '*'  ->      (__A * __B);
            '/'  ->      (__A / __B);
            'div'  ->    (__A div __B);
            'rem'  ->    (__A rem __B)
        end).

math_fun_binary({Op, A, B}) ->
    case {bind_action(A),bind_action(B)} of 
        {false,false} ->    ?MathOpBlockBinary(Op,A,B);
        {false,true} ->     fun(X) -> Bb=B(X),?MathOpBlockBinary(Op,A,Bb) end;
        {false,BBind} ->    fun(X) -> Bb=?BoundVal(BBind,X),?MathOpBlockBinary(Op,A,Bb) end;
        {true,false} ->     fun(X) -> Ab=A(X),?MathOpBlockBinary(Op,Ab,B) end;
        {true,true} ->      fun(X) -> Ab=A(X),Bb=B(X),?MathOpBlockBinary(Op,Ab,Bb) end;  
        {true,BBind} ->     fun(X) -> Ab=A(X),Bb=?BoundVal(BBind,X),?MathOpBlockBinary(Op,Ab,Bb) end;  
        {ABind,false} ->    fun(X) -> Ab=?BoundVal(ABind,X),?MathOpBlockBinary(Op,Ab,B) end;  
        {ABind,true} ->     fun(X) -> Ab=?BoundVal(ABind,X),Bb=B(X),?MathOpBlockBinary(Op,Ab,Bb) end;  
        {ABind,BBind} ->    fun(X) -> Ab=?BoundVal(ABind,X),Bb=?BoundVal(BBind,X),?MathOpBlockBinary(Op,Ab,Bb) end
    end.

comp_fun({Op, {const,A}, {const,B}}) when is_tuple(A),is_tuple(B)->
    comp_fun_final({Op, A, B});
comp_fun({Op, {const,A}, B}) when is_tuple(A) ->
    comp_fun_final({Op, A, expr_fun(B)});
comp_fun({Op, A, {const,B}}) when is_tuple(B) ->
    comp_fun_final({Op, expr_fun(A), B});
comp_fun({Op, A, B}) ->
    Fa = expr_fun(A),
    Fb = expr_fun(B),
    comp_fun_final({Op, Fa, Fb}).


-define(CompOpBlock(__Op,__A,__B), 
        case __Op of
             _   when __A==?nav;__B==?nav -> ?nav;
            '==' ->  (__A==__B);
            '>'  ->  (__A>__B);
            '>=' ->  (__A>=__B);
            '<'  ->  (__A<__B);
            '=<' ->  (__A=<__B);
            '/=' ->  (__A/=__B)
        end).

comp_fun_final({Op, A, B}) ->
    case {bind_action(A),bind_action(B)} of 
        {false,false} ->    ?CompOpBlock(Op,A,B);
        {false,true} ->     fun(X) -> Bb=B(X),?CompOpBlock(Op,A,Bb) end;
        {false,BBind} ->    fun(X) -> Bb=?BoundVal(BBind,X),?CompOpBlock(Op,A,Bb) end;
        {true,false} ->     fun(X) -> Ab=A(X),?CompOpBlock(Op,Ab,B) end;
        {true,true} ->      fun(X) -> Ab=A(X),Bb=B(X),?CompOpBlock(Op,Ab,Bb) end;  
        {true,BBind} ->     fun(X) -> Ab=A(X),Bb=?BoundVal(BBind,X),?CompOpBlock(Op,Ab,Bb) end;  
        {ABind,false} ->    fun(X) -> Ab=?BoundVal(ABind,X),?CompOpBlock(Op,Ab,B) end;  
        {ABind,true} ->     fun(X) -> Ab=?BoundVal(ABind,X),Bb=B(X),?CompOpBlock(Op,Ab,Bb) end;  
        {ABind,BBind} ->    fun(X) -> Ab=?BoundVal(ABind,X),Bb=?BoundVal(BBind,X),?CompOpBlock(Op,Ab,Bb) end
    end.

unary_fun({Op, {const,A}}) when is_tuple(A) ->
    unary_fun_final({Op, A});
unary_fun({Op, A}) ->
    unary_fun_final( {Op, expr_fun(A)});
unary_fun(Value) -> Value.

unary_fun_final({'is_nav', A}) -> 
    case bind_action(A) of 
        false ->        is_nav(A);
        true ->         fun(X) -> Ab=A(X),is_nav(Ab) end;
        ABind ->        fun(X) -> Ab=?BoundVal(ABind,X),is_nav(Ab) end
    end;
unary_fun_final({Op, A}) -> 
    case bind_action(A) of 
        false ->        mod_op_1(?MODULE,Op,A);
        true ->         fun(X) -> Ab=A(X),mod_op_1(?MODULE,Op,Ab) end;
        ABind ->        fun(X) -> Ab=?BoundVal(ABind,X),mod_op_1(?MODULE,Op,Ab) end
    end.

is_nav(?nav) -> true;
is_nav(_) -> false.

to_atom(A) when is_atom(A) -> A;
to_atom(B) when is_binary(B) -> ?binary_to_atom(B);
to_atom(L) when is_list(L) -> list_to_atom(L).

to_name(T) when is_tuple(T) ->
    imem_datatype:io_to_binstr(string:join([imem_datatype:strip_squotes(to_string(E)) || E <- tuple_to_list(T)],"."));
to_name(E) -> imem_datatype:strip_squotes(to_binstr(E)).

to_text(T) when is_binary(T) ->
    to_text(binary_to_list(T));
to_text(T) when is_list(T) ->
    try
        Mask=fun(X) ->
                case unicode:characters_to_list([X], unicode) of
                    [X] when (X<16#20) ->   $.;
                    [X]  ->   X;
                     _ -> 
                        case unicode:characters_to_list([X], latin1) of
                            [Y] -> Y;
                             _ ->  $.
                        end
                end
            end,
        unicode:characters_to_binary(lists:map(Mask,T),unicode)
    catch
        _:_ -> imem_datatype:term_to_io(T)
    end;
to_text(T) ->
    imem_datatype:term_to_io(T).

to_tuple(B) when is_binary(B) -> imem_datatype:io_to_tuple(B,0).

to_list(B) when is_binary(B) -> imem_datatype:io_to_list(B,0).

to_term(B) when is_binary(B) -> imem_datatype:io_to_term(B).

to_existing_atom(A) when is_atom(A) -> A;
to_existing_atom(B) when is_binary(B) -> ?binary_to_existing_atom(B);
to_existing_atom(L) when is_list(L) -> list_to_existing_atom(L).

to_integer(B) when is_binary(B) -> to_integer(binary_to_list(B));
to_integer(I) when is_integer(I) -> I;
to_integer(F) when is_float(F) -> erlang:round(F);
to_integer(L) when is_list(L) -> list_to_integer(L).

to_float(B) when is_binary(B) -> to_float(binary_to_list(B));
to_float(F) when is_float(F) -> F;
to_float(I) when is_integer(I) -> I + 1.0;
to_float(L) when is_list(L) -> 
    case (catch list_to_integer(L)) of
        I when is_integer(I) -> float(I);
        _ -> list_to_float(L)
    end.

to_number(B) when is_binary(B) -> to_number(binary_to_list(B));
to_number(F) when is_float(F) -> F;
to_number(I) when is_integer(I) -> I;
to_number(L) when is_list(L) -> 
    case (catch list_to_integer(L)) of
        I when is_integer(I) -> I;
        _ -> list_to_float(L)
    end.

to_string(B) when is_binary(B) ->   binary_to_list(B);
to_string(I) when is_integer(I) -> integer_to_list(I);
to_string(F) when is_float(F) -> float_to_list(F);
to_string(A) when is_atom(A) -> atom_to_list(A);
to_string(X) -> io_lib:format("~p", [X]).

to_binstr(B) when is_binary(B) ->   B;
to_binstr(I) when is_integer(I) -> list_to_binary(integer_to_list(I));
to_binstr(F) when is_float(F) -> list_to_binary(float_to_list(F));
to_binstr(A) when is_atom(A) -> list_to_binary(atom_to_list(A));
to_binstr(X) -> list_to_binary(io_lib:format("~p", [X])).

to_binterm(B) when is_binary(B) ->   imem_datatype:io_to_binterm(B);
to_binterm(T) ->                     imem_datatype:term_to_binterm(T).

from_binterm(B)  ->                  imem_datatype:binterm_to_term(B).

binary_fun({Op, {const,A}, {const,B}}) when is_tuple(A), is_tuple(B) ->
    binary_fun_final({Op, A, B});
binary_fun({Op, {const,A}, B}) when is_tuple(A) ->
    binary_fun_final({Op, A, expr_fun(B)});
binary_fun({Op, A, {const,B}}) when is_tuple(B) ->
    binary_fun_final({Op, expr_fun(A), B});
binary_fun({Op, A, B}) ->
    FA = expr_fun(A),
    FB = expr_fun(B),
    binary_fun_final( {Op, FA, FB});
binary_fun(Value) -> Value.

-define(ElementOpBlock(__A,__B), 
    if 
        (not is_number(__A)) -> ?nav; 
        (not is_tuple(__B)) -> ?nav;
        (not tuple_size(__B) >= __A) -> ?nav;
        true -> element(__A,__B)
    end).

binary_fun_final({'element', A, B})  ->
    case {bind_action(A),bind_action(B)} of 
        {false,false} ->    ?ElementOpBlock(A,B);
        {false,true} ->     fun(X) -> Bb=B(X),?ElementOpBlock(A,Bb) end;
        {false,BBind} ->    fun(X) -> Bb=?BoundVal(BBind,X),?ElementOpBlock(A,Bb) end;
        {true,false} ->     fun(X) -> Ab=A(X),?ElementOpBlock(Ab,B) end;
        {true,true} ->      fun(X) -> Ab=A(X),Bb=B(X),?ElementOpBlock(Ab,Bb) end;
        {true,BBind} ->     fun(X) -> Ab=A(X),Bb=?BoundVal(BBind,X),?ElementOpBlock(Ab,Bb) end;
        {ABind,false} ->    fun(X) -> Ab=?BoundVal(ABind,X),?ElementOpBlock(Ab,B) end;
        {ABind,true} ->     fun(X) -> Ab=?BoundVal(ABind,X),Bb=B(X),?ElementOpBlock(Ab,Bb) end;
        {ABind,BBind} ->    fun(X) -> Ab=?BoundVal(ABind,X),Bb=?BoundVal(BBind,X),?ElementOpBlock(Ab,Bb) end
    end;
binary_fun_final({'is_like', A, B})  ->
    case {bind_action(A),bind_action(B)} of 
        {false,false} ->    re_match(like_compile(B),A);
        {false,true} ->     fun(X) -> Bb=B(X),re_match(like_compile(Bb),A) end;
        {false,BBind} ->    fun(X) -> Bb=?BoundVal(BBind,X),re_match(like_compile(Bb),A) end;
        {true,false} ->     RE = like_compile(B),fun(X) -> Ab=A(X),re_match(RE,Ab) end;
        {true,true} ->      fun(X) -> Ab=A(X),Bb=B(X),re_match(like_compile(Bb),Ab) end;
        {true,BBind} ->     fun(X) -> Ab=A(X),Bb=?BoundVal(BBind,X),re_match(like_compile(Ab),Bb) end;
        {ABind,false} ->    RE = like_compile(B),fun(X) -> Bb=?BoundVal(ABind,X),re_match(RE,Bb) end;
        {ABind,true} ->     fun(X) -> Ab=?BoundVal(ABind,X),Bb=B(X),re_match(like_compile(Bb),Ab) end;
        {ABind,BBind} ->    fun(X) -> Ab=?BoundVal(ABind,X),Bb=?BoundVal(BBind,X),re_match(like_compile(Bb),Ab) end
    end;
binary_fun_final({'is_regexp_like', A, B})  ->
    case {bind_action(A),bind_action(B)} of 
        {false,false} ->    re_match(re_compile(B),A);
        {false,true} ->     fun(X) -> Bb=B(X),re_match(re_compile(Bb),A) end;
        {false,BBind} ->    fun(X) -> Bb=?BoundVal(BBind,X),re_match(re_compile(Bb),A) end;
        {true,false} ->     RE = re_compile(B),fun(X) -> Ab=A(X),re_match(RE,Ab) end;
        {true,true} ->      fun(X) -> Ab=A(X),Bb=B(X),re_match(re_compile(Bb),Ab) end;
        {true,BBind} ->     fun(X) -> Ab=A(X),Bb=?BoundVal(BBind,X),re_match(re_compile(Ab),Bb) end;
        {ABind,false} ->    RE = re_compile(B),fun(X) -> Ab=?BoundVal(ABind,X),re_match(RE,Ab) end;
        {ABind,true} ->     fun(X) -> Ab=?BoundVal(ABind,X),Bb=B(X),re_match(re_compile(Bb),Ab) end;
        {ABind,BBind} ->    fun(X) -> Ab=?BoundVal(ABind,X),Bb=?BoundVal(BBind,X),re_match(re_compile(Bb),Ab) end
    end;
binary_fun_final({Op, A, B}) when Op=='to_decimal';Op=='from_decimal';Op=='add_dt';Op=='add_ts';Op=='is_member';Op=='concat' ->
    case {bind_action(A),bind_action(B)} of 
        {false,false} ->    mod_op_2(?MODULE,Op,A,B);        
        {false,true} ->     fun(X) -> Bb=B(X),mod_op_2(?MODULE,Op,A,Bb) end;
        {false,BBind} ->    fun(X) -> Bb=?BoundVal(BBind,X),mod_op_2(?MODULE,Op,A,Bb) end;
        {true,false} ->     fun(X) -> Ab=A(X),mod_op_2(?MODULE,Op,Ab,B) end;
        {true,true} ->      fun(X) -> Ab=A(X),Bb=B(X),mod_op_2(?MODULE,Op,Ab,Bb) end;
        {true,BBind} ->     fun(X) -> Ab=A(X),Bb=?BoundVal(BBind,X),mod_op_2(?MODULE,Op,Ab,Bb) end;
        {ABind,false} ->    fun(X) -> Ab=?BoundVal(ABind,X),mod_op_2(?MODULE,Op,Ab,B) end;
        {ABind,true} ->     fun(X) -> Ab=?BoundVal(ABind,X),Bb=B(X),mod_op_2(?MODULE,Op,Ab,Bb) end;
        {ABind,BBind} ->    fun(X) -> Ab=?BoundVal(ABind,X),Bb=?BoundVal(BBind,X),mod_op_2(?MODULE,Op,Ab,Bb) end
    end;
binary_fun_final(BTree) ->
    ?UnimplementedException({"Unsupported filter function",{BTree}}).

add_dt(DT, Offset) when is_tuple(DT),is_number(Offset) -> 
    imem_datatype:offset_datetime('+',DT,Offset).   %% Offset in (fractions of) days

add_ts(TS, Offset) when is_tuple(TS),is_number(Offset) -> 
    imem_datatype:offset_timestamp('+',TS,Offset).  %% Offset in (fractions of) days

concat(A, B) when is_list(A),is_list(B) -> A ++ B;
concat(A, B) when is_binary(A),is_binary(B) -> <<A/binary,B/binary>>.

diff_dt(A,B) when is_tuple(A),is_tuple(B) -> 
    (calendar:datetime_to_gregorian_seconds(A)-calendar:datetime_to_gregorian_seconds(B))/86400.0.

diff_ts({AM,AS,AMicro},{BM,BS,BMicro}) -> 
    (1000000*(AM-BM)+AS-BS+0.000001*(AMicro-BMicro))/86400.0.

from_decimal(I,0) when is_integer(I) -> I; 
from_decimal(I,P) when is_integer(I),is_integer(P),(P>0) -> 
    Str = integer_to_list(I),
    Len = length(Str),
    if 
        P-Len+1 > 0 -> 
            {Whole,Frac} = lists:split(1,lists:duplicate(P-Len+1,$0) ++ Str),
            to_float(io_lib:format("~s.~s",[Whole,Frac]));
        true ->
            {Whole,Frac} = lists:split(Len-P,Str),
            to_float(io_lib:format("~s.~s",[Whole,Frac]))
    end;
from_decimal(I,P) -> ?ClientError({"Invalid conversion from_decimal",{I,P}}).

is_member(A, B) when is_list(B) ->     lists:member(A,B);
is_member(A, B) when is_tuple(B) ->    lists:member(A,tuple_to_list(B));
is_member(_, _) ->                     false.

ternary_not(?nav) ->        ?nav;
ternary_not(true) ->        false;
ternary_not(false) ->       true.

ternary_and(?nav,_)->       ?nav;
ternary_and(_,?nav)->       ?nav;
ternary_and(A,B)->          (A and B).

ternary_or(_,true) ->       true;
ternary_or(true,_) ->       true;
ternary_or(A,?nav) ->       A;
ternary_or(?nav,B) ->       B;
ternary_or(A,false) ->      A;
ternary_or(false,B) ->      B;
ternary_or(A,B) ->          (A or B).

to_decimal(B,0) -> erlang:round(to_number(B));
to_decimal(B,P) when is_integer(P),(P>0) ->
    erlang:round(math:pow(10, P) * to_number(B)).

ternary_fun({Op, {const,A}, B, C}) when is_tuple(A) ->
    ternary_fun({Op, A, B, C});
ternary_fun({Op, A, {const,B}, C}) when is_tuple(B) ->
    ternary_fun({Op, A, B, C});
ternary_fun({Op, A, B, {const,C}}) when is_tuple(C) ->
    ternary_fun({Op, A, B, C});
ternary_fun({Op, A, B, C}) ->
    FA = expr_fun(A),
    FB = expr_fun(B),
    FC = expr_fun(C),
    ternary_fun_final( {Op, FA, FB, FC});
ternary_fun(Value) -> Value.

ternary_fun_final({Op, A, B, C}) when Op=='remap' ->
    case {bind_action(A),bind_action(B),bind_action(C)} of 
        {false,false,false} ->  mod_op_3(?MODULE,Op,A,B,C);        
        {false,true,false} ->   fun(X) -> Bb=B(X),mod_op_3(?MODULE,Op,A,Bb,C) end;
        {false,BBind,false} ->  fun(X) -> Bb=?BoundVal(BBind,X),mod_op_3(?MODULE,Op,A,Bb,C) end;
        {true,false,false} ->   fun(X) -> Ab=A(X),mod_op_3(?MODULE,Op,Ab,B,C) end;
        {true,true,false} ->    fun(X) -> Ab=A(X),Bb=B(X),mod_op_3(?MODULE,Op,Ab,Bb,C) end;
        {true,BBind,false} ->   fun(X) -> Ab=A(X),Bb=?BoundVal(BBind,X),mod_op_3(?MODULE,Op,Ab,Bb,C) end;
        {ABind,false,false} ->  fun(X) -> Ab=?BoundVal(ABind,X),mod_op_3(?MODULE,Op,Ab,B,C) end;
        {ABind,true,false} ->   fun(X) -> Ab=?BoundVal(ABind,X),Bb=B(X),mod_op_3(?MODULE,Op,Ab,Bb,C) end;
        {ABind,BBind,false} ->  fun(X) -> Ab=?BoundVal(ABind,X),Bb=?BoundVal(BBind,X),mod_op_3(?MODULE,Op,Ab,Bb,C) end;

        {false,false,true} ->   fun(X) -> Cb=C(X),mod_op_3(?MODULE,Op,A,B,Cb) end;        
        {false,true,true} ->    fun(X) -> Cb=C(X),Bb=B(X),mod_op_3(?MODULE,Op,A,Bb,Cb) end;
        {false,BBind,true} ->   fun(X) -> Cb=C(X),Bb=?BoundVal(BBind,X),mod_op_3(?MODULE,Op,A,Bb,Cb) end;
        {true,false,true} ->    fun(X) -> Cb=C(X),Ab=A(X),mod_op_3(?MODULE,Op,Ab,B,Cb) end;
        {true,true,true} ->     fun(X) -> Cb=C(X),Ab=A(X),Bb=B(X),mod_op_3(?MODULE,Op,Ab,Bb,Cb) end;
        {true,BBind,true} ->    fun(X) -> Cb=C(X),Ab=A(X),Bb=?BoundVal(BBind,X),mod_op_3(?MODULE,Op,Ab,Bb,Cb) end;
        {ABind,false,true} ->   fun(X) -> Cb=C(X),Ab=?BoundVal(ABind,X),mod_op_3(?MODULE,Op,Ab,B,Cb) end;
        {ABind,true,true} ->    fun(X) -> Cb=C(X),Ab=?BoundVal(ABind,X),Bb=B(X),mod_op_3(?MODULE,Op,Ab,Bb,Cb) end;
        {ABind,BBind,true} ->   fun(X) -> Cb=C(X),Ab=?BoundVal(ABind,X),Bb=?BoundVal(BBind,X),mod_op_3(?MODULE,Op,Ab,Bb,Cb) end;

        {false,false,CBind} ->  fun(X) -> Cb=?BoundVal(CBind,X),mod_op_3(?MODULE,Op,A,B,Cb) end;        
        {false,true,CBind} ->   fun(X) -> Cb=?BoundVal(CBind,X),Bb=B(X),mod_op_3(?MODULE,Op,A,Bb,Cb) end;
        {false,BBind,CBind} ->  fun(X) -> Cb=?BoundVal(CBind,X),Bb=?BoundVal(BBind,X),mod_op_3(?MODULE,Op,A,Bb,Cb) end;
        {true,false,CBind} ->   fun(X) -> Cb=?BoundVal(CBind,X),Ab=A(X),mod_op_3(?MODULE,Op,Ab,B,Cb) end;
        {true,true,CBind} ->    fun(X) -> Cb=?BoundVal(CBind,X),Ab=A(X),Bb=B(X),mod_op_3(?MODULE,Op,Ab,Bb,Cb) end;
        {true,BBind,CBind} ->   fun(X) -> Cb=?BoundVal(CBind,X),Ab=A(X),Bb=?BoundVal(BBind,X),mod_op_3(?MODULE,Op,Ab,Bb,Cb) end;
        {ABind,false,CBind} ->  fun(X) -> Cb=?BoundVal(CBind,X),Ab=?BoundVal(ABind,X),mod_op_3(?MODULE,Op,Ab,B,Cb) end;
        {ABind,true,CBind} ->   fun(X) -> Cb=?BoundVal(CBind,X),Ab=?BoundVal(ABind,X),Bb=B(X),mod_op_3(?MODULE,Op,Ab,Bb,Cb) end;
        {ABind,BBind,CBind} ->  fun(X) -> Cb=?BoundVal(CBind,X),Ab=?BoundVal(ABind,X),Bb=?BoundVal(BBind,X),mod_op_3(?MODULE,Op,Ab,Bb,Cb) end
    end;
ternary_fun_final(BTree) ->
    ?UnimplementedException({"Unsupported filter function",{BTree}}).


remap(Val,From,To) ->
    if 
        Val == From ->  To;
        true ->         Val
    end.

%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) ->
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun test_without_sec/1
            % , fun test_with_sec/1
        ]}
    }.
    
test_without_sec(_) -> 
    test_with_or_without_sec(false).

% test_with_sec(_) ->
%     test_with_or_without_sec(true).

test_with_or_without_sec(IsSec) ->
    try
        % ClEr = 'ClientError',
        % SyEx = 'SystemException',    %% difficult to test
        % SeEx = 'SecurityException',
        ?Info("----------------------------------~n"),
        ?Info("---TEST--- ~p ----Security ~p", [?MODULE, IsSec]),
        ?Info("----------------------------------~n"),

        ?assertEqual(<<"Imem.ddTable">>, to_name({'Imem',ddTable})),
        ?assertEqual(<<"imem.ddTable">>, to_name({'imem',ddTable})),
        ?assertEqual(<<"undefined.ddTable">>, to_name({undefined,ddTable})),
        ?assertEqual(<<"ddTable">>, to_name(<<"ddTable">>)),
        % ?assertEqual(<<"ddTable">>, to_name("ddTable")),
        ?assertEqual(<<"imem.ddäöü"/utf8>>, to_name({<<"imem">>,<<"ddäöü">>})),
        ?Info("to_name success~n", []),

        ?assertEqual(<<"">>, to_text([])),
        ?assertEqual(<<"SomeText1234">>, to_text("SomeText1234")),
        ?assertEqual(<<"SomeText1234">>, to_text(<<"SomeText1234">>)),
        ?assertEqual(<<".SomeText1234.">>, to_text([2|"SomeText1234"]++[3])),
        ?assertEqual(<<"ddäöü"/utf8>>, to_text(<<"ddäöü">>)),
        ?assertEqual(<<".ddäöü."/utf8>>, to_text(<<2,"ddäöü",3>>)),
        ?assertEqual(<<"{'Imem',ddTable}">>, to_text({'Imem',ddTable})),
        ?Info("to_text success~n", []),

    %% Like strig to Regex string
        ?assertEqual(<<"^Sm.th$">>, transform_like(<<"Sm_th">>, <<>>)),
        ?assertEqual(<<"^.*Sm.th.*$">>, transform_like(<<"%Sm_th%">>, <<>>)),
        ?assertEqual(<<"^.A.*Sm.th.*$">>, transform_like(<<"_A%Sm_th%">>, <<>>)),
        ?assertEqual(<<"^.A.*S\\$m.t\\*\\[h.*$">>, transform_like(<<"_A%S$m_t*[h%">>, <<>>)),
        ?assertEqual(<<"^.A.*S\\^\\$\\.\\[\\]\\|\\(\\)\\?\\*\\+\\-\\{\\}m.th.*$">>, transform_like(<<"_A%S^$.[]|()?*+-{}m_th%">>, <<>>)),
        ?assertEqual(<<"^Sm_th.$">>, transform_like(<<"Sm@_th_">>, <<"@">>)),
        ?assertEqual(<<"^Sm%th.*$">>, transform_like(<<"Sm@%th%">>, <<"@">>)),
        ?assertEqual(<<"^.m_th.$">>, transform_like(<<"_m@_th_">>, <<"@">>)),
        ?assertEqual(<<"^.*m%th.*$">>, transform_like(<<"%m@%th%">>, <<"@">>)),
        ?Info("success ~p~n", [transform_like]),

    %% Regular Expressions
        ?Info("testing regular expressions: ~p~n", ["like_compile"]),
        RE1 = like_compile("abc_123%@@"),
        ?assertEqual(true,re_match(RE1,"abc_123%@@")),         
        ?assertEqual(true,re_match(RE1,<<"abc_123jhhsdhjhj@@">>)),         
        ?assertEqual(true,re_match(RE1,"abc_123%%@@@")),         
        ?assertEqual(true,re_match(RE1,"abc0123@@")),         
        ?assertEqual(false,re_match(RE1,"abc_123%@")),         
        ?assertEqual(false,re_match(RE1,"abc_123%@")),         
        ?assertEqual(false,re_match(RE1,"")),         
        ?assertEqual(false,re_match(RE1,<<"">>)),         
        ?assertEqual(false,re_match(RE1,<<"abc_@@">>)),         
        RE2 = like_compile(<<"%@@">>,<<>>),
        ?assertEqual(true,re_match(RE2,"abc_123%@@")),         
        ?assertEqual(true,re_match(RE2,<<"123%@@">>)),         
        ?assertEqual(true,re_match(RE2,"@@")),
        ?assertEqual(true,re_match(RE2,"@@@")),
        ?assertEqual(false,re_match(RE2,"abc_123%@")),         
        ?assertEqual(false,re_match(RE2,"@.@")),         
        ?assertEqual(false,re_match(RE2,"@_@")),         
        RE3 = like_compile(<<"text_in%">>),
        ?assertEqual(true,re_match(RE3,<<"text_in_text">>)),         
        ?assertEqual(true,re_match(RE3,"text_in_text")),         
        ?assertEqual(true,re_match(RE3,<<"text_in_quotes\"">>)),         
        ?assertEqual(false,re_match(RE3,<<"\"text_in_quotes">>)),         
        ?assertEqual(false,re_match(RE3,"\"text_in_quotes\"")),         
        ?assertEqual(false,re_match(RE3,<<"\"text_in_quotes\"">>)),         
        RE4 = like_compile(<<"%12">>),
        ?assertEqual(true,re_match(RE4,12)),         
        ?assertEqual(true,re_match(RE4,112)),         
        ?assertEqual(true,re_match(RE4,012)),         
        ?assertEqual(false,re_match(RE4,122)),         
        ?assertEqual(false,re_match(RE4,1)),         
        ?assertEqual(false,re_match(RE4,11)),         
        RE5 = like_compile(<<"12.5%">>),
        ?assertEqual(true,re_match(RE5,12.51)),         
        ?assertEqual(true,re_match(RE5,12.55)),         
        ?assertEqual(true,re_match(RE5,12.50)),         
        ?assertEqual(false,re_match(RE5,12)),         
        ?assertEqual(false,re_match(RE5,12.4)),         
        ?assertEqual(false,re_match(RE5,12.49999)),         

        %% ToDo: implement and test patterns involving regexp reserved characters

        % ?Info("success ~p~n", [replace_match]),
        % L = {like,'$6',"%5%"},
        % NL = {not_like,'$7',"1%"},
        % ?assertEqual( true, replace_match(L,L)),
        % ?assertEqual( NL, replace_match(NL,L)),
        % ?assertEqual( {'and',true,NL}, replace_match({'and',L,NL},L)),
        % ?assertEqual( {'and',L,true}, replace_match({'and',L,NL},NL)),
        % ?assertEqual( {'and',{'and',true,NL},{a,b,c}}, replace_match({'and',{'and',L,NL},{a,b,c}},L)),
        % ?assertEqual( {'and',{'and',L,{a,b,c}},true}, replace_match({'and',{'and',L,{a,b,c}},NL},NL)),
        % ?assertEqual( {'and',{'and',true,NL},{'and',L,NL}}, replace_match({'and',{'and',L,NL},{'and',L,NL}},L)),
        % ?assertEqual( {'and',NL,{'and',true,NL}}, replace_match({'and',NL,{'and',L,NL}},L)),
        % ?assertEqual( {'and',NL,{'and',NL,NL}}, replace_match({'and',NL,{'and',NL,NL}},L)),
        % ?assertEqual( {'and',NL,{'and',NL,true}}, replace_match({'and',NL,{'and',NL,L}},L)),
        % ?assertEqual( {'and',{'and',{'and',{'=<',5,'$1'},L},true},{'==','$1','$6'}}, replace_match({'and',{'and',{'and',{'=<',5,'$1'},L},NL},{'==','$1','$6'}},NL)),
        % ?assertEqual( {'and',{'and',{'and',{'=<',5,'$1'},true},NL},{'==','$1','$6'}}, replace_match({'and',{'and',{'and',{'=<',5,'$1'},L},NL},{'==','$1','$6'}},L)),

    %% expr_fun
        ?assertEqual(true, expr_fun(true)),
        ?assertEqual(false, expr_fun(false)),
        ?assertEqual(true, expr_fun({'not', false})),
        ?assertEqual(false, expr_fun({'not', true})),
        ?assertEqual(12, expr_fun(12)),
        ?assertEqual(a, expr_fun(a)),
        ?assertEqual({a,b}, expr_fun({const,{a,b}})),
        ?assertEqual(true, expr_fun({'==', 10,10})),
        ?assertEqual(true, expr_fun({'==', {const,{a,b}}, {const,{a,b}}})), 
        ?assertEqual(false, expr_fun({'==', {const,{a,b}}, {const,{a,1}}})), 
        ?assertEqual(true, expr_fun({'is_member', a, [a,b]})),
        ?assertEqual(true, expr_fun({'is_member', 1, [a,b,1]})),
        ?assertEqual(false, expr_fun({'is_member', 1, [a,b,c]})),
        ?assertEqual(true, expr_fun({'is_member', 1, {const,{a,b,1}}})),
        ?assertEqual(false, expr_fun({'is_member', 1, {const,{a,b,c}}})),
        ?assertEqual(true, expr_fun({'is_like', "12345", "%3%"})),
        ?assertEqual(true, expr_fun({'is_like', <<"12345">>, "%3__"})),
        ?assertEqual(true, expr_fun({'is_like', "12345", <<"1%">>})),
        ?assertEqual(true, expr_fun({'is_like', {'+',12300,45}, <<"%45">>})),
        ?assertEqual(false, expr_fun({'is_like', "12345", "%6%"})),
        ?assertEqual(false, expr_fun({'is_like', <<"12345">>, "%7__"})),
        ?assertEqual(33, expr_fun({'*',{'+',10,1},3})),
        ?assertEqual(10, expr_fun({'abs',{'-',10,20}})),
        ?Info("success ~p~n", ["expr_fun constants"]),

        X1 = {{1,2,3},{2,2,2}},
        B1a = #bind{tag='$1',tind=1,cind=2},    % = 2
        B1b = #bind{tag='$2',tind=1,cind=2},    % = 2
        F1 = expr_fun({'==', B1a,B1b}),
        ?assertEqual(true, F1(X1)),

        B1c = #bind{tag='$2',tind=2,cind=2},    % = 2
        F1a = expr_fun({'==', B1a,B1c}),
        ?assertEqual(true, F1a(X1)),

        B2 = #bind{tag='$3',tind=1,cind=3},     % = 3
        F2 = expr_fun({'is_member', a, B2}),
        ?assertEqual(false,F2({{1,2,[3,4,5]},{2,2,2}})),        
        ?assertEqual(true, F2({{1,2,[c,a,d]},{2,2,2}})),        

        F3 = expr_fun({'is_member', B1c, B2}),
        ?assertEqual(false, F3({{1,d,[c,a,d]},{2,2,2}})),        
        ?assertEqual(true, F3({{1,c,[2,a,d]},{2,2,2}})),        
        ?assertEqual(true, F3({{1,a,[c,2,2]},{2,2,2}})),        
        ?assertEqual(true, F3({{1,3,{3,4,2}},{2,2,2}})),        
        ?assertEqual(false,F3({{1,2,{3,4,5}},{2,2,2}})),        
        ?assertEqual(false,F3({{1,[a],[3,4,5]},{2,2,2}})),        
        ?assertEqual(false,F3({{1,3,[]},{2,2,2}})),        

        F4 = expr_fun({'is_member', {'+',B1c,1}, B2}),
        ?assertEqual(true, F4({{1,2,[3,4,5]},{2,2,2}})),        
        ?assertEqual(false,F4({{1,2,[c,4,d]},{2,2,2}})),        

        ?assert(true)
    catch
        Class:Reason ->  ?Info("Exception~n~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
