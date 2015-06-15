%% -*- coding: utf-8 -*-
-module(imem_datatype).
-compile(inline).
-compile({inline_size,1000}).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(rawTypeIo,binary).
-define(emptyIo,<<>>).


-define(SAFE_ERLANG_FUNCTIONS,['+','-','*','/','++','--','abs','div','rem','min','max','float','now','date','element','size','bit_size','byte_size','binary_part','phash2','md5','throw']).
-define(UNSAFE_ERLANG_FUNCTIONS,['list_to_atom','binary_to_atom','list_to_pid','binary_to_term','is_pid','is_port','is_process_alive']).

-define(ROWFUN_EXTENSIONS,[{<<"nodef">>,1}
                          ,{<<"item1">>,1},{<<"item2">>,1},{<<"item3">>,1},{<<"item4">>,1}
                          ,{<<"item5">>,1},{<<"item6">>,1},{<<"item7">>,1},{<<"item8">>,1},{<<"item9">>,1}
                          ]).

-define(H(X), (hex(X)):16).

-define(BinaryMaxLen,250).          %% more represented by "..." suffix

-export([ raw_type/1
        , imem_type/1
        , type_check/5
        , is_datatype/1
        , is_number_type/1
        , is_unicode_binary/1
        , is_rowfun_extension/2
        , is_term_or_fun_text/1
        , to_term_or_fun/1
        ]).

-export([ is_binterm/1
        , term_to_binterm/1
        , binterm_to_term/1
        ]).

-export([ add_squotes/1
        , add_dquotes/1
        , strip_squotes/1           %% strip single quotes
        , strip_dquotes/1           %% strip double quotes
        , strip_quotes/1            %% strip both quotes in any order
        , binary_to_hex/1
        ]).

-export([ offset_datetime/3
        , offset_timestamp/3
        , musec_diff/1              %% UTC time difference in microseconds towards erlang:now()
        , musec_diff/2              %% UTC time difference in microseconds
        , msec_diff/1               %% UTC time difference in milliseconds towards erlang:now()
        , msec_diff/2               %% UTC time difference in milliseconds
        , sec_diff/1                %% UTC time difference in milliseconds towards erlang:now()
        , sec_diff/2                %% UTC time difference in milliseconds
        ]).

%   datatypes
%   internal    aliases (synonyms)
%   --------------------------------------------
%   atom
%   binary      raw(L), blob(L), rowid
%   binstr      clob(L), nclob(L), varchar2(L), nvarchar2(L), char(L), nchar(L)
%   boolean     bool
%   datetime    date
%   decimal     number(Len,Prec)
%   float
%   fun
%   integer     int
%   ipaddr
%   list
%   map
%   number      (virtual= integer|float)
%   pid
%   ref
%   string
%   term
%   binterm
%   timestamp
%   tuple
%   userid
-define(CLM_TYPES,
        [atom, binary,  raw,  blob,  rowid,  binstr, clob, nclob, varchar2,
         nvarchar2, char, nchar, boolean, datetime, decimal, float, 'fun',
         integer, ipaddr, list, map, number, pid, ref, string, term, binterm,
         timestamp, tuple, userid]).

-export([bind_arg_types/0]).

-export([ io_to_db/8
        , io_to_binary/2
        , io_to_binstr/1
        , io_to_boolean/1
        , io_to_datetime/1
        , io_to_decimal/3
        , io_to_float/2
        , io_to_fun/1, io_to_fun/2, io_to_fun/3
        , io_to_integer/1
        , io_to_integer/2
        , io_to_integer/3
        , io_to_ipaddr/1
        , io_to_ipaddr/2
        , io_to_list/2
        , io_to_map/1        
        , io_to_string/1
        , io_to_pid/1
        , io_to_term/1
        , io_to_binterm/1
        , io_to_timestamp/1, io_to_timestamp/2
        , io_to_tuple/2
        , io_to_userid/1
        ]).

-export([ db_to_io/6
        , atom_to_io/1
        , binary_to_io/1
        , binstr_to_io/1
        , boolean_to_io/1
        , datetime_to_io/1, datetime_to_io/2
        , decimal_to_io/2
        , float_to_io/3
        , integer_to_io/1
        , ipaddr_to_io/1
        , string_to_io/1
        , timestamp_to_io/1, timestamp_to_io/2, timestamp_to_io/3
        , userid_to_io/1
        , term_to_io/1
        , binterm_to_io/1
        ]).

-export([ field_value/6
        , field_value_type/6
        ]).

-export([ item1/1
        , item2/1
        , item3/1
        , item4/1
        , item5/1
        , item6/1
        , item7/1
        , item8/1
        , item9/1
        ]).


-export([ select_rowfun_raw/1   %% return rows in raw erlang db format
        , select_rowfun_str/4   %% convert all rows to string
        ]).

bind_arg_types() ->
    [atom_to_binary(T,utf8) || T <- ?CLM_TYPES].

select_rowfun_raw(ColMap) ->
    fun(Recs) ->
        select_rowfun_raw(Recs, ColMap, [])
    end.

select_rowfun_raw(_Recs, [], Acc) ->
    lists:reverse(Acc);
select_rowfun_raw(Recs, [#bind{tind=0,cind=0,func=F}|ColMap], Acc) ->
    Fld = case is_function(F) of
        true ->     F(Recs);
        false ->    F
    end,
    select_rowfun_raw(Recs, ColMap, [Fld|Acc]);
select_rowfun_raw(Recs, [#bind{tind=Ti,cind=Ci,func=undefined}|ColMap], Acc) ->
    Fld = case element(Ti,Recs) of
        undefined ->    undefined;
        Rec ->          case Ci of
                            0 ->    Rec;
                            _ ->    element(Ci,Rec)
                        end
    end,
    select_rowfun_raw(Recs, ColMap, [Fld|Acc]);
select_rowfun_raw(Recs, [#bind{tind=Ti,cind=Ci,func=F}|ColMap], Acc) ->
    Fld = case element(Ti,Recs) of
        undefined ->
            undefined;
        Rec ->
            try
                case Ci of
                    0 ->    apply(F,[Rec]);
                    _ ->    apply(F,[element(Ci,Rec)])
                end
            catch
                _:Reason ->  ?UnimplementedException({"Failed row function",{F,Reason}})
            end
    end,
    select_rowfun_raw(Recs, ColMap, [Fld|Acc]).


select_rowfun_str(ColMap, DateFmt, NumFmt, StrFmt) ->
    fun(Recs) ->
        select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, [])
    end.

select_rowfun_str(_Recs, [], _DateFmt, _NumFmt, _StrFmt, Acc) ->
    lists:reverse(Acc);
select_rowfun_str(Recs, [#bind{type=T,prec=P,tind=0,cind=0,func=F}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    Str = case is_function(F) of
        true -> db_to_io(T, P, DateFmt, NumFmt, StrFmt, F(Recs));
        false -> db_to_io(T, P, DateFmt, NumFmt, StrFmt, F)
    end,
    select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]);
select_rowfun_str(Recs, [#bind{type=T,prec=P,tind=Ti,cind=Ci,func=F,default=D}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    Str = case element(Ti,Recs) of
        undefined ->
            ?emptyIo;
        Rec ->
            X = case Ci of
                0 ->    Rec;
                _ ->    element(Ci,Rec)
            end,
            try
                case F of
                    undefined ->        db_to_io(T, P, DateFmt, NumFmt, StrFmt, X);
                    nodef when X==D ->  ?emptyIo;
                    nodef ->            db_to_io(T, P, DateFmt, NumFmt, StrFmt, X);
                    item1 ->            item1(X);
                    item2 ->            item2(X);
                    item3 ->            item3(X);
                    item4 ->            item4(X);
                    item5 ->            item5(X);
                    item6 ->            item6(X);
                    item7 ->            item7(X);
                    item8 ->            item8(X);
                    item9 ->            item9(X);
                    Name ->             ?UnimplementedException({"Unimplemented row function",Name})
                end
            catch
                _:Reason ->  ?SystemException({"Failed row function",{F,X,Reason}})
            end
    end,
    select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]).


%% ----- DATA TYPES    --------------------------------------------


is_number_type(Type) when is_atom(Type) -> lists:member(Type,?NumberTypes).

is_datatype([]) -> false;
is_datatype({}) -> false;
is_datatype(Type) when is_binary(Type) ->
    case (catch ?binary_to_existing_atom(Type)) of
        T when is_atom(T) ->    is_datatype(T);
        _ ->                    false
    end;
is_datatype(Type) when is_atom(Type) -> lists:member(Type,?DataTypes);
is_datatype(Types) when is_list(Types) ->
    (not lists:member(false,[is_datatype(T) || T <- Types]));
is_datatype(Type) when is_tuple(Type) -> is_datatype(tuple_to_list(Type));
is_datatype(_) -> false.

is_rowfun_extension(Func,Arity) when is_atom(Func) -> is_rowfun_extension(atom_to_binary(Func, utf8),Arity);
is_rowfun_extension(Func,Arity) -> lists:member({Func,Arity},?ROWFUN_EXTENSIONS).

imem_type(raw) -> binary;
imem_type(blob) -> binary;
imem_type(rowid) -> rowid;
imem_type(clob) -> binstr;
imem_type(nclob) -> binstr;
imem_type(bool) -> boolean;
imem_type(date) -> datetime;
imem_type(number) -> decimal;
imem_type(int) -> integer;
imem_type(varchar2) -> binstr;
imem_type(nvarchar2) -> binstr;
imem_type(char) -> binstr;
imem_type(nchar) -> binstr;
imem_type(Type) -> Type.

raw_type(userid) -> integer;
raw_type(binstr) -> binary;
raw_type(binterm) -> binary;
raw_type(string) -> ?rawTypeIo;
raw_type(decimal) -> integer;
raw_type(datetime) -> tuple;
raw_type(timestamp) -> tuple;
raw_type(ipaddr) -> tuple;
raw_type(boolean) -> atom;
raw_type(Type) -> Type.

%% ----- Value Type Tests and Conversions ---------------------------------

type_check(V,_,_,_,V) -> ok;
type_check(V,binary,Len,_,_) when is_binary(V) -> length_check(binary,Len,byte_size(V));
type_check(V,binstr,Len,_,_) when is_binary(V) -> length_check(binstr,Len,byte_size(V));
type_check(V,Type,_,_,Def) -> type_check(V,Type,Def).

type_check(V,atom,_) when is_atom(V),V/=?nav -> ok;
type_check(V,boolean,_) when is_boolean(V) -> ok;
type_check({{Y,M,D},{Hh,Mm,Ss}},datetime,_) when
            is_integer(Y), is_integer(M), is_integer(D),
            is_integer(Hh), is_integer(Mm), is_integer(Ss) -> ok;
type_check(V,decimal,_) when is_integer(V) -> ok;
type_check(V,float,_) when is_float(V) -> ok;
type_check(V,'fun',_) when is_function(V) -> ok;
type_check(V,integer,_) when is_integer(V) -> ok;
type_check({A,B,C,D},ipaddr,_) when
            is_integer(A), is_integer(B),
            is_integer(C), is_integer(D) -> ok;
type_check({A,B,C,D,E,F,G,H},ipaddr,_) when
            is_integer(A), is_integer(B), is_integer(C), is_integer(D),
            is_integer(E), is_integer(F), is_integer(G), is_integer(H) -> ok;
type_check(V,list,_) when is_list(V) -> ok;
type_check(V,map,_) when is_map(V) -> ok;
type_check(V,number,_) when is_integer(V);is_float(V) -> ok;
type_check(V,pid,_) when is_pid(V) -> ok;
type_check(V,ref,_) when is_reference(V) -> ok;
type_check(V,string,_) when is_list(V) -> ok;
type_check(V,term,_) when V/=?nav -> ok;
type_check(V,binterm,Def) when is_binary(V);is_list(V);is_tuple(V) ->
    try
        case binterm_to_term(V) of
            ?nav -> {error,{"Wrong data type for value, expecting type or default",{V,binterm,Def}}};
            _ ->    ok
        end
    catch
        _:_ -> {error,{"Wrong data type for value, expecting type or default",{V,binterm,Def}}}
    end;
type_check({D,T,M},timestamp,_) when
            is_integer(D), is_integer(T), is_integer(M) -> ok;
type_check(V,tuple,_) when is_tuple(V) -> ok;
type_check(V,userid,_) when is_integer(V) -> ok;
type_check(V,Type,Def) ->
    {error,{"Wrong data type for value, expecting type or default",{V,Type,Def}}}.

length_check(_,0,_) -> ok;
length_check(_,undefined,_) -> ok;
length_check(Type,Max,Len) ->
    if
        Len=<Max -> ok;
        true -> {error,{"Data exceeds maximum byte length",{Type,Len}}}
    end.

is_binterm(B) when is_binary(B) ->
    try
        _ = sext:decode(B),
        true
    catch
        _:_ -> false
    end;
is_binterm(_) -> false.

term_to_binterm(T) -> sext:encode(T).

binterm_to_term(B) when is_binary(B) ->
    sext:decode(B);
binterm_to_term(L) when is_list(L) ->
    [binterm_to_term(I) || I <- L];
binterm_to_term(T) when is_tuple(T) ->
    list_to_tuple([binterm_to_term(I) || I <- tuple_to_list(T)]).

is_unicode_binary(B) when is_binary(B) ->
    case unicode:characters_to_binary(B,utf8,utf8) of
        B ->    true;
        _ ->    false
    end;
is_unicode_binary(_) ->
    false.

is_term_or_fun_text(F) when is_function(F) -> false;
is_term_or_fun_text(B) when is_binary(B) ->
    is_term_or_fun_text(binary_to_list(B));
is_term_or_fun_text([$f,$u,$n|_]=Str) ->
    try
        case re:run(Str, "fun\\((.*)\\)[ ]*\->(.*)end.", [global, {capture, [1,2], list}]) of
            {match,[[_Params,_Body]]} ->
                is_function((catch io_to_fun(Str,undefined)));
            nomatch ->
                true
        end
    catch
        throw:{'ClientError',Reason} -> ?ClientError(Reason);
        _:_ -> true    %% some term, not a string
    end;
is_term_or_fun_text(_) -> true.

to_term_or_fun(B) when is_binary(B) ->
    to_term_or_fun(B, binary_to_list(B));
to_term_or_fun(L) when is_list(L) ->
    to_term_or_fun(L, L);
to_term_or_fun(T) -> T.

to_term_or_fun(T, [$f,$u,$n|_]=Str) ->
    try
        case re:run(Str, "fun\\((.*)\\)[ ]*\->(.*)end.", [global, {capture, [1,2], list}]) of
            {match,[[_Params,_Body]]} ->    io_to_fun(Str,undefined);
            nomatch ->                      T
        end
    catch
        throw:{'ClientError',Reason} -> ?ClientError(Reason);
        _:_ -> T        %% not a valid string representation of a fun
    end;
to_term_or_fun(T, _) -> T.

%% ----- CAST Data to become compatible with DB  ------------------

io_to_db(_Item,Old,_Type,_Len,_Prec,_Def,true,_) -> Old;
io_to_db(Item,Old,Type,Len,Prec,Def,false,Val) when is_function(Def,0) ->
    io_to_db(Item,Old,Type,Len,Prec,Def(),false,Val);
io_to_db(_Item,_Old,_Type,_Len,_Prec,Def,false,?emptyIo) -> Def;
io_to_db(Item,Old,Type,Len,Prec,Def,false,Val) when is_binary(Val);is_list(Val) ->
    try
        {DefAsStr,OldAsStr} = case Type of
            binterm ->  { io_to_binstr(io_lib:format("~10000p", [Def]))
                        , io_to_binstr(io_lib:format("~10000p", [term_to_binterm(Old)]))};
            _ ->        { io_to_binstr(io_lib:format("~10000p", [Def]))
                        , io_to_binstr(io_lib:format("~10000p", [Old]))}
        end,
        % ?LogDebug("DefAsStr ~10000tp ~ts~n",[<<DefAsStr/binary,1>>,<<DefAsStr/binary,1>>]),
        % ?LogDebug("OldAsStr ~10000tp ~ts~n",[<<OldAsStr/binary,1>>,<<OldAsStr/binary,1>>]),
        ValAsStr = io_to_binstr(io_lib:format("~ts", [Val])),
        % ?LogDebug("ValAsStr ~tp ~ts~n",[<<ValAsStr/binary,1>>,<<ValAsStr/binary,1>>]),
        if
            (DefAsStr == ValAsStr) ->   Def;
            (OldAsStr == ValAsStr) ->   Old;
            (Type == 'fun') ->          io_to_fun(Val,Len);
            (Type == atom) ->           io_to_atom(Val);
            (Type == binary) ->         io_to_binary(Val,Len);
            (Type == binstr) ->         io_to_binstr(Val,Len);
            (Type == boolean) ->        io_to_boolean(Val);
            (Type == datetime) ->       io_to_datetime(Val);
            (Type == decimal) ->        io_to_decimal(Val,Len,Prec);
            (Type == float) ->          io_to_float(Val,Prec);
            (Type == integer) ->        io_to_integer(Val,Len,Prec);
            (Type == ipaddr) ->         io_to_ipaddr(Val,Len);
            (Type == list) ->           io_to_list(Val,Len);
            (Type == map) ->            io_to_map(Val);            
            (Type == pid) ->            io_to_pid(Val);
            (Type == ref) ->            Old;    %% cannot convert back
            (Type == string) ->         io_to_string(Val,Len);
            (Type == term) ->           io_to_term(Val);
            (Type == binterm) ->        io_to_binterm(Val);
            (Type == timestamp) ->      io_to_timestamp(Val,Prec);
            (Type == tuple) ->          io_to_tuple(Val,Len);
            (Type == userid) ->         io_to_userid(Val);
            true ->                     io_to_term(Val)
        end
    catch
        _:{'UnimplementedException',_} ->       ?ClientError({"Unimplemented data type conversion",{Item,{Type,Val}}});
        _:{'ClientError', {Text, Reason}} ->    ?ClientError({Text, {Item,Reason}});
        _:_ ->                                  ?ClientError({"Data conversion format error",{Item,{Type,Val}}})
    end.

add_squotes(<<>>) -> <<"''">>;
add_squotes(B) when is_binary(B) -> <<$',B/binary,$'>>;
add_squotes([]) -> "''";
add_squotes(String) when is_list(String) -> "'" ++ String ++ "'".

add_dquotes(<<>>) -> <<"\"\"">>;
add_dquotes(B) when is_binary(B) -> <<$",B/binary,$">>;
add_dquotes([]) -> "\"\"";
add_dquotes(String) when is_list(String) -> "\"" ++ String ++ "\"".

strip_dquotes(<<>>) -> ?emptyIo;
strip_dquotes(<<H:8>>) -> <<H:8>>;
strip_dquotes(B) when is_binary(B) ->
    F = binary:first(B),
    L = binary:last(B),
    if
        (F == $") andalso (L == $") ->
            binary:part(B, 1, size(B)-2);
        true ->
            B
    end;
strip_dquotes([]) -> [];
strip_dquotes([H]) -> [H];
strip_dquotes([H|T]=Str) ->
    L = lists:last(Str),
    if
        H == $" andalso L == $" ->
            lists:sublist(T, length(T)-1);
        true ->
            Str
    end.

strip_squotes(<<>>) -> ?emptyIo;
strip_squotes(<<H:8>>) -> <<H:8>>;
strip_squotes(B) when is_binary(B) ->
    F = binary:first(B),
    L = binary:last(B),
    if
        (F == $') andalso (L == $') ->
            binary:part(B, 1, size(B)-2);
        true ->
            B
    end;
strip_squotes([]) -> [];
strip_squotes([H]) -> [H];
strip_squotes([H|T]=Str) ->
    L = lists:last(Str),
    if
        H == $' andalso L == $' ->  lists:sublist(T, length(T)-1);
        true ->                     Str
    end.

strip_quotes(<<>>) -> ?emptyIo;
strip_quotes(<<H:8>>) -> <<H:8>>;
strip_quotes(B) when is_binary(B) ->
    F = binary:first(B),
    L = binary:last(B),
    if
        (F == $') andalso (L == $') ->
            strip_dquotes(binary:part(B, 1, size(B)-2));
        (F == $") andalso (L == $") ->
            strip_squotes(binary:part(B, 1, size(B)-2));
        true ->
            B
    end;
strip_quotes([]) -> [];
strip_quotes([H]) -> [H];
strip_quotes([H|T]=Str) ->
    L = lists:last(Str),
    if
        H == $' andalso L == $' ->
            strip_dquotes(lists:sublist(T, length(T)-1));
        H == $" andalso L == $" ->
            strip_squotes(lists:sublist(T, length(T)-1));
        true ->
            Str
    end.


io_to_atom(Val) when is_binary(Val) ->
    binary_to_atom(strip_quotes(Val), utf8);
io_to_atom(Val) when is_list(Val) ->
    list_to_atom(strip_quotes(Val)).

io_to_pid(Val) when is_binary(Val) ->
    list_to_pid(binary_to_list(Val));
io_to_pid(Val) when is_list(Val) ->
    list_to_pid(Val).

io_to_integer(Val) ->
    io_to_integer(Val,undefined,0).

io_to_integer(Val,Len) ->
    io_to_integer(Val,Len,0).

io_to_integer(Val,0,Prec) ->
    io_to_integer(Val,undefined,Prec);
io_to_integer(Val,Len,undefined) ->
    io_to_integer(Val,Len,0);
io_to_integer(Val,Len,Prec) ->
    Value = case io_to_term(Val) of
        V when is_integer(V) -> V;
        V when is_float(V) ->   erlang:round(V);
        _ ->                    ?ClientError({"Data conversion format error",{integer,Len,Prec,Val}})
    end,
    Result = if
        Prec == undefined ->    Value;
        Prec <  0 ->            erlang:round(erlang:round(math:pow(10, Prec) * Value) * math:pow(10,-Prec));
        true ->                 Value
    end,
    RLen = length(integer_to_list(Result)),
    if
        Len == undefined ->     Result;
        RLen > Len ->           ?ClientError({"Data conversion format error",{integer,Len,Prec,Val}});
        true ->                 Result
    end.

io_to_float(Val,Prec) ->
    Value = case io_to_term(Val) of
        V when is_float(V) ->   V;
        V when is_integer(V) -> float(V);
        _ ->                    ?ClientError({"Data conversion format error",{float,Prec,Val}})
    end,
    if
        Prec == undefined ->    Value;
        true ->                 erlang:round(math:pow(10, Prec) * Value) * math:pow(10,-Prec)
    end.

io_to_binary(<<>>,_Len) -> ?emptyIo;
io_to_binary([],_Len) -> ?emptyIo;
io_to_binary(Val,Len) when is_binary(Val) ->
    S = size(Val),
    F = binary:first(Val),
    L = binary:last(Val),
    if
        (F < $0) orelse (F > $F) ->
            ?ClientError({"Invalid hex string starts with",{binary,[F]}});
        (F > $9) andalso (F < $A) ->
            ?ClientError({"Invalid hex string starts with",{binary,[F]}});
        (L < $0) orelse (L > $F) ->
            ?ClientError({"Invalid hex string starts with",{binary,[L]}});
        (L > $9) andalso (L < $A) ->
            ?ClientError({"Invalid hex string starts with",{binary,[L]}});
        (Len /= undefined) andalso (S > Len+Len) ->
            ?ClientError({"Binary data is too long",{binary,Len}});
        (S rem 2) == 1 ->
            ?ClientError({"Hex string must have even number of characters",{binary,S}});
        true ->
            hexstr_to_bin(Val)
    end;
io_to_binary(Val,Len) when is_list(Val) ->
    L = length(Val),
    FirstOK = lists:member(hd(Val),"0123456789ABCDEF"),
    LastOK = lists:member(lists:last(Val),"0123456789ABCDEF"),
    if
        (FirstOK == false) ->
            ?ClientError({"Invalid hex string starts with",{binary,[hd(Val)]}});
        (LastOK == false) ->
            ?ClientError({"Invalid hex string ends with",{binary,[lists:last(Val)]}});
        (Len /= undefined) andalso (L > Len+Len) ->
            ?ClientError({"Binary data is too long",{binary,Len}});
        (L rem 2) == 1 ->
            ?ClientError({"Hex string must have even number of characters",{binary,L}});
        true ->
            hexstr_to_bin(Val)
    end.

hexstr_to_bin(B) when is_binary(B) ->
    hexstr_to_bin(binary_to_list(B));
hexstr_to_bin(S) when is_list(S) ->
    hexstr_to_bin(S, []).

hexstr_to_bin([], Acc) ->
    list_to_binary(lists:reverse(Acc));
hexstr_to_bin([X,Y|T], Acc) ->
    {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
    hexstr_to_bin(T, [V | Acc]).

io_to_userid(<<"system">>) -> system;
io_to_userid(<<"unknown">>) -> unknown;
io_to_userid(Id) when is_binary(Id) ->
    try
        list_to_integer(binary_to_list(Id))
    catch
        _:_ -> io_to_atom(Id)
    end;
io_to_userid(Id) when is_list(Id) ->
    io_to_userid(list_to_binary(Id)).

io_to_timestamp(TS) ->
    io_to_timestamp(TS,undefined).

io_to_timestamp(TS,undefined) ->
    io_to_timestamp(TS,6);
io_to_timestamp(B,Prec) when is_binary(B) ->
    io_to_timestamp(binary_to_list(B),Prec);
io_to_timestamp("systime",Prec) ->
    io_to_timestamp("now",Prec);
io_to_timestamp("sysdate",Prec) ->
    io_to_timestamp("now",Prec);
io_to_timestamp("now",Prec) ->
    {Megas,Secs,Micros} = erlang:now(),
    {Megas,Secs,erlang:round(erlang:round(math:pow(10, Prec-6) * Micros) * erlang:round(math:pow(10,6-Prec)))};
io_to_timestamp([${|_]=Val,_Prec) ->
    case io_to_tuple(Val,3) of
        {D,T,M} when is_integer(D), is_integer(T), is_integer(M) -> {D,T,M}
    end;
io_to_timestamp(Val0,6) ->
    Val = re:replace(re:replace(Val0, "T", " ", [{return, list}]), "Z$", "", [{return, list}]),
    try
        {Date,Time,Micro} = case re:run(lists:sublist(Val,5),"[\/\.\-]+",[{capture,all,list}]) of
            {match,["/"]} ->
                case string:tokens(Val, " ") of
                    [D,T] ->  case re:split(T,"[.]",[{return,list}]) of
                                        [Hms,M] ->
                                            {parse_date_us(D),parse_time(Hms),parse_micro(M)};
                                        [Hms] ->
                                            {parse_date_us(D),parse_time(Hms),0.0}
                                    end;
                    [D] ->       {parse_date_us(D),{0,0,0},0.0}
                end;
            {match,["-"]} ->
                case string:tokens(Val, " ") of
                    [D,T] ->    case re:split(T,"[.]",[{return,list}]) of
                                    [Hms,M] ->  {parse_date_int(D),parse_time(Hms),parse_micro(M)};
                                    [Hms] ->    {parse_date_int(D),parse_time(Hms),0.0}
                                end;
                    [D] ->      {parse_date_int(D),{0,0,0},0.0}
                end;
            {match,["."]} ->
                case string:tokens(Val, " ") of
                    [D,T] ->  case re:split(T,"[.]",[{return,list}]) of
                                        [Hms,M] ->
                                            {parse_date_eu(D),parse_time(Hms),parse_micro(M)};
                                        [Hms] ->
                                            {parse_date_eu(D),parse_time(Hms),0.0}
                                    end;
                    [D] ->       {parse_date_eu(D),{0,0,0},0.0}
                end;
            _ ->
                case string:tokens(Val, " ") of
                    [D,T,M] ->            {parse_date_raw(D),parse_time(T),parse_micro(M)};
                    [D,T] ->              {parse_date_raw(D),parse_time(T),0.0};
                    [DT] when length(DT)>14 ->  {D,T} = lists:split(8,DT),
                                                {Hms,M} = lists:split(6,T),
                                                {parse_date_raw(D),parse_time(Hms),parse_micro(M)};
                    [DT] when length(DT)>8 ->   {D,T} = lists:split(8,DT),
                                                {parse_date_raw(D),parse_time(T),0.0};
                    [D] ->                      {parse_date_raw(D),{0,0,0},0.0}
                end
        end,
        {Meg,Sec, 0} = utc_seconds_to_now(local_datetime_to_utc_seconds({Date, Time})),
        {Meg,Sec,round(1000000*Micro)}
    catch
        _:{'ClientError',Reason} -> ?ClientError({"Data conversion format error",{timestamp,Val,Reason}});
        _:Reason ->  ?ClientError({"Data conversion format error",{timestamp,Val,Reason}})
    end;
io_to_timestamp(Val,Prec) when Prec == 0 ->
    {Megas,Secs,Micros} = io_to_timestamp(Val,6),
    if
        (Micros >= 500000) and (Secs == 999999) ->
            {Megas+1,0,0};
        Micros >= 500000 ->
            {Megas,Secs+1,0};
        true ->
            {Megas,Secs,0}
    end;
io_to_timestamp(Val,Prec) when Prec > 0 ->
    {Megas,Secs,Micros} = io_to_timestamp(Val,6),
    {Megas,Secs,erlang:round(erlang:round(math:pow(10, Prec-6) * Micros) * erlang:round(math:pow(10,6-Prec)))};
io_to_timestamp(Val,Prec) when Prec =< 0 ->
    {Megas,Secs,_} = io_to_timestamp(Val),
    {Megas,erlang:round(erlang:round(math:pow(10, -Prec) * Secs) * erlang:round(math:pow(10,Prec))),0}.

io_to_datetime(B) when is_binary(B) ->
    io_to_datetime(binary_to_list(B));
io_to_datetime("today") ->
    {Date,_} = io_to_datetime("localtime"),
    {Date,{0,0,0}};
io_to_datetime("systime") ->
    io_to_datetime("localtime");
io_to_datetime("sysdate") ->
    io_to_datetime("localtime");
io_to_datetime("now") ->
    io_to_datetime("localtime");
io_to_datetime("localtime") ->
    erlang:localtime();
io_to_datetime([${|_]=Val) ->
    case io_to_tuple(Val,2) of
        {{Y,M,D},{Hh,Mm,Ss}} when
            is_integer(Y), is_integer(M), is_integer(D),
            is_integer(Hh), is_integer(Mm), is_integer(Ss) ->
                {{Y,M,D},{Hh,Mm,Ss}}
    end;
io_to_datetime(Val0) ->
    Val = re:replace(re:replace(Val0, "T", " ", [{return, list}]),
                     "\.[0-9]*Z$", "", [{return, list}]),
    try
        case re:run(lists:sublist(Val,5),"[\/\.\-]+",[{capture,all,list}]) of
            {match,["."]} ->
                case string:tokens(Val, " ") of
                    [Date,Time] ->              {parse_date_eu(Date),parse_time(Time)};
                    [Date] ->                   {parse_date_eu(Date),{0,0,0}}
                end;
            {match,["/"]} ->
                case string:tokens(Val, " ") of
                    [Date,Time] ->              {parse_date_us(Date),parse_time(Time)};
                    [Date] ->                   {parse_date_us(Date),{0,0,0}}
                end;
            {match,["-"]} ->
                case string:tokens(Val, " ") of
                    [Date,Time] ->              {parse_date_int(Date),parse_time(Time)};
                    [Date] ->                   {parse_date_int(Date),{0,0,0}}
                end;
            _ ->
                case string:tokens(Val, " ") of
                    [Date,Time] ->              {parse_date_raw(Date),parse_time(Time)};
                    [DT] when length(DT) > 8 -> {Date,Time} = lists:split(8,DT),
                                                {parse_date_raw(Date),parse_time(Time)};
                    [Date] ->                   {parse_date_raw(Date),{0,0,0}}
                end
        end
    catch
        _:_ ->  ?ClientError({"Data conversion format error",{datetime,Val}})
    end.

parse_date_eu(Val) ->
    case string:tokens(Val, ".") of
        [Day,Month,Year] ->     validate_date({parse_year(Year),parse_month(Month),parse_day(Day)});
        _ ->                    ?ClientError({"parse_date_eu",Val})
    end.

parse_date_us(Val) ->
    case string:tokens(Val, "/") of
        [Month,Day,Year] ->     validate_date({parse_year(Year),parse_month(Month),parse_day(Day)});
        _ ->                    ?ClientError({"parse_date_us",Val})
    end.

parse_date_int(Val) ->
    case string:tokens(Val, "-") of
        [Year,Month,Day] ->     validate_date({parse_year(Year),parse_month(Month),parse_day(Day)});
        _ ->                    ?ClientError({"parse_date_int",Val})
    end.

parse_date_raw(Val) ->
    case length(Val) of
        8 ->    validate_date({parse_year(lists:sublist(Val,1,4)),parse_month(lists:sublist(Val,5,2)),parse_day(lists:sublist(Val,7,2))});
        6 ->    validate_date({parse_year(lists:sublist(Val,1,2)),parse_month(lists:sublist(Val,3,2)),parse_day(lists:sublist(Val,5,2))});
        _ ->    ?ClientError({"parse_date_raw",Val})
    end.

parse_year(Val) ->
    case length(Val) of
        4 ->    list_to_integer(Val);
        2 ->    Year2 = list_to_integer(Val),
                if
                   Year2 < 50 ->    2000+Year2;
                   true ->          1900+Year2
                end;
        _ ->    ?ClientError({"parse_year",Val})
    end.

parse_month(Val) ->
    case length(Val) of
        1 ->                    list_to_integer(Val);
        2 ->                    list_to_integer(Val);
        _ ->                    ?ClientError({"parse_month",Val})
    end.

parse_day(Val) ->
    case length(Val) of
        1 ->                    list_to_integer(Val);
        2 ->                    list_to_integer(Val);
        _ ->                    ?ClientError({"parse_day",Val})
    end.

parse_time(Val) ->
    case string:tokens(Val, ":") of
        [H,M,S] ->      {parse_hour(H),parse_minute(M),parse_second(S)};
        [H,M] ->        {parse_hour(H),parse_minute(M),0};
        _ ->
            case length(Val) of
                6 ->    {parse_hour(lists:sublist(Val,1,2)),parse_minute(lists:sublist(Val,3,2)),parse_minute(lists:sublist(Val,5,2))};
                4 ->    {parse_hour(lists:sublist(Val,1,2)),parse_minute(lists:sublist(Val,3,2)),0};
                2 ->    {parse_hour(lists:sublist(Val,1,2)),0,0};
                0 ->    {0,0,0};
                _ ->    ?ClientError({"parse_time",Val})
            end
    end.

parse_hour(Val) ->
    H = list_to_integer(Val),
    if
        H >= 0 andalso H < 25 ->    H;
        true ->                     ?ClientError({"parse_hour",Val})
    end.

parse_minute(Val) ->
    M = list_to_integer(Val),
    if
        M >= 0 andalso M < 60 ->    M;
        true ->                     ?ClientError({"parse_minute",Val})
    end.

parse_second(Val) ->
    S = list_to_integer(Val),
    if
        S >= 0 andalso S < 60 ->    S;
        true ->                     ?ClientError({"parse_second",Val})
    end.

parse_micro(Val) ->
    list_to_float("0." ++ Val).


-spec utc_seconds_to_now(integer()) -> {integer(),integer(),0}.
utc_seconds_to_now(SecondsUtc) ->
%%  DateTime1970 = calendar:datetime_to_gregorian_seconds({{1970, 01, 01}, {00, 00, 00}}),
%%  DateTime1900 = calendar:datetime_to_gregorian_seconds({{1900, 01, 01}, {00, 00, 00}}),
%%  Seconds1970 = SecondsUtc - (DateTime1970 - DateTime1900),
    Seconds1970 = SecondsUtc - 2208988800,
    {Seconds1970 div 1000000, Seconds1970 rem 1000000, 0}.

local_datetime_to_utc_seconds({{Year,_,_}, _}) when Year < 1970 ->
    ?ClientError({"Cannot handle dates before 1970"});
local_datetime_to_utc_seconds({Date, Time}) ->
%%  DateTime1900 = calendar:datetime_to_gregorian_seconds({{1900, 01, 01}, {00, 00, 00}}),
%%  calendar:datetime_to_gregorian_seconds({Date, Time}) - DateTime1900.
    case calendar:local_time_to_universal_time_dst({Date, Time}) of
        [DateTimeUTC] ->
            calendar:datetime_to_gregorian_seconds(DateTimeUTC) - 59958230400;
        [DstDateTimeUTC, _] ->
            calendar:datetime_to_gregorian_seconds(DstDateTimeUTC) - 59958230400
    end.

validate_date(Date) ->
    case calendar:valid_date(Date) of
        true ->     Date;
        false ->    ?ClientError({"validate_date",Date})
    end.

io_to_ipaddr(Val) ->
    io_to_ipaddr(Val,undefined).

io_to_ipaddr(Val,0) ->
    io_to_ipaddr(Val,undefined);
io_to_ipaddr(Val,Len) when is_binary(Val) ->
    io_to_ipaddr(binary_to_list(Val),Len);
io_to_ipaddr([${|_]=Val,Len) ->
    case io_to_term(Val) of
        {A,B,C,D} when is_integer(A), is_integer(B),
            is_integer(C), is_integer(D) ->
                if
                    Len==undefined ->   {A,B,C,D};
                    Len==4 ->           {A,B,C,D};
                    true ->             ?ClientError({"Data conversion format error",{ipaddr,Len,Val}})
                end;
        {A,B,C,D,E,F,G,H} when
            is_integer(A), is_integer(B), is_integer(C), is_integer(D),
            is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
                if
                    Len==undefined ->   {A,B,C,D,E,F,G,H};
                    Len==8 ->           {A,B,C,D,E,F,G,H};
                    true ->             ?ClientError({"Data conversion format error",{ipaddr,Len,Val}})
                end;
        _ -> ?ClientError({"Data conversion format error",{ipaddr,Len,Val}})
    end;
io_to_ipaddr(Val,Len) ->
    Result = try
        {ok,Ip} = inet_parse:address(Val),
        Ip
    catch
        _:_ -> ?ClientError({"Data conversion format error",{ipaddr,Len,Val}})
    end,
    RLen = size(Result),
    if
        Len == undefined andalso RLen == 4 ->   Result;
        Len == undefined andalso RLen == 8 ->   Result;
        RLen == Len ->                          Result;
        true ->                                 ?ClientError({"Data conversion format error",{ipaddr,Len,Val}})
    end.

io_to_decimal(Val,Len,Prec) when is_binary(Val) ->
    io_to_decimal(binary_to_list(Val),Len,Prec);
io_to_decimal(Val,Len,undefined) ->
    io_to_decimal(Val,Len,0);
io_to_decimal(Val,Len,0) ->         %% use fixed point arithmetic with implicit scaling factor
    io_to_integer(Val,Len,0);
io_to_decimal(Val,Len,Prec) ->
    case Val of
        [$-|Positive] -> Sign = [$-];
        Positive -> Sign = []
    end,
    case string:chr(Positive, $.) of
        0 -> PointPos = length(Positive) + 1;
        PointPos -> PointPos
    end,
    DotRemoved = lists:delete($., Positive),
    case string:to_integer(DotRemoved) of
        {Valid, []} when Valid >= 0->
            ZerosToAdd = Prec + PointPos - length(DotRemoved) - 1,
            NewLength = Prec + PointPos - 1,
            case io_to_decimal(DotRemoved, NewLength, Sign, ZerosToAdd) of
                {Accumulator, []} ->
                    Result = Accumulator,
                    ResultLength = 1;
                {Accumulator, ResultList} ->
                    Result = list_to_integer(ResultList) + Accumulator,
                    ResultLength = length(ResultList)
            end,
            if
                Len == undefined         ->  Result;
                ResultLength > Len       ->  ?ClientError({"Data conversion format error",{decimal,Len,Prec,Val}});
                true                     ->  Result
            end;
        _NotInteger ->
            ?ClientError({"Data conversion format error",{decimal,Len,Prec,Val}})
    end.


io_to_decimal(DotRemoved, _NewLength, Sign, 0) ->
    {0, Sign ++ DotRemoved};
io_to_decimal(DotRemoved, _NewLength, Sign, ZerosToAdd) when ZerosToAdd > 0 ->
    {0, lists:flatten([Sign, DotRemoved, lists:duplicate(ZerosToAdd, $0)])};
io_to_decimal(_DotRemoved, NewLength, _Sign, _ZerosToAdd) when NewLength < 0 ->
    {0, []};
io_to_decimal(DotRemoved, NewLength, Sign, _ZerosToAdd) ->
    NextDigit = lists:nth(NewLength + 1, DotRemoved),
    if
        NextDigit < $5 -> Accumulator = 0;
        true -> Accumulator = 1 - length(Sign) * 2
    end,
    {Accumulator, lists:flatten([Sign, string:substr(DotRemoved, 1, NewLength)])}.

io_to_binstr(Val) ->
    io_to_binstr(Val,undefined).

io_to_binstr(Val,Len) ->
    Bin = unicode:characters_to_binary(Val, utf8, utf8),
    if
        Len == undefined ->     Bin;
        Len == 0 ->             Bin;
        size(Bin) =< Len  ->    Bin;
        true ->                 ?ClientError({"String is too long",{Val,Len}})
    end.

io_to_string(Bin) ->
    io_to_string(Bin,undefined).

io_to_string(Bin,Len) when is_binary(Bin) ->
    case strip_dquotes(Bin) of
        Bin ->  ?ClientError({"Missing double quotes for list string format",{Bin,Len}});
        B ->
            List = case unicode:characters_to_list(B, utf8) of
                L when is_list(L) ->
                    lists:flatten(io_lib:format("~s",[un_escape_io(L)]));   %% Bin was utf8 encoded
                _ ->
                    lists:flatten(io_lib:format("~s",[un_escape_io(B)]))    %% Bin is bytewise encoded
            end,
            if
                Len == undefined ->     List;
                Len == 0 ->             List;
                length(List) =< Len  -> List;
                true ->                 ?ClientError({"String is too long",{B,Len}})
            end
    end.

io_to_list(Val,Len) ->
    case io_to_term(Val) of
        V when is_list(V) ->
            if
                Len == undefined -> V;
                Len == 0 ->         V;
                length(V) == Len -> V;
                true ->             ?ClientError({"Data conversion format error",{list,Len,Val}})
            end;
        _ ->
            ?ClientError({"Data conversion format error",{list,Len,Val}})
    end.

io_to_map(Val) ->
    case io_to_term(Val) of
        V when is_map(V) -> V;
        _ -> ?ClientError({"Data conversion format error",{map,Val}})
    end.

io_to_tuple(Val,Len) ->
    case io_to_term(Val) of
        V when is_tuple(V) ->
            if
                Len == undefined -> V;
                Len == 0 ->         V;
                size(V) == Len ->   V;
                true ->             ?ClientError({"Data conversion format error",{tuple,Len,Val}})
            end;
        _ ->
            ?ClientError({"Data conversion format error",{tuple,Len,Val}})
    end.

io_to_boolean(Val) ->
    case io_to_term(Val) of
        V when is_boolean(V) ->     V;
        _ ->
            ?ClientError({"Data conversion format error",{boolean,Val}})
    end.

io_to_term(Val) ->
    try
        erl_value(Val)
    catch
        _:_ -> 
            ?LogDebug("Cannot convert this to erlang term: ~10000p", [Val]),
            ?ClientError({})
    end.

io_to_binterm(Val) ->
    try
        sext:encode(erl_value(Val))
    catch
        _:_ -> ?ClientError({})
    end.

io_to_fun(Str) ->
    Fun = erl_value(Str),
    if
        is_function(Fun) ->
            Fun;
        true ->
            ?ClientError({"Data conversion format error",{'fun',Str}})
    end.

io_to_fun(Str,Len) ->
    Fun = erl_value(Str),
    if
        Len == undefined ->     Fun;
        is_function(Fun,Len) -> Fun;
        true ->
            ?ClientError({"Data conversion format error",{'fun',Len,Str}})
    end.

io_to_fun(Str,Len,Bindings) ->
    Fun = erl_value(Str,Bindings),
    if
        Len == undefined ->     Fun;
        is_function(Fun,Len) -> Fun;
        true ->
            ?ClientError({"Data conversion format error",{'fun',Len,Str,Bindings}})
    end.

erl_value(String) when is_binary(String) -> erl_value(binary_to_list(String),[]);
erl_value(String) when is_list(String) -> erl_value(String,[]).

erl_value(String,Bindings) when is_binary(String), is_list(Bindings) ->
    erl_value(binary_to_list(String),Bindings);
erl_value(String,Bindings) when is_list(String), is_list(Bindings) ->
    Code = case [lists:last(string:strip(String))] of
        "." -> String;
        _ -> String ++ "."
    end,
    {ok,ErlTokens,_} = erl_scan:string(Code),
    {ok,ErlAbsForm} = erl_parse:parse_exprs(ErlTokens),
    case catch erl_eval:exprs(ErlAbsForm, Bindings, none,
                              {value, fun nonLocalHFun/2}) of
        {value,Value,_} -> Value;
        {Ex, Exception} when Ex == 'SystemException'; Ex == 'SecurityException' ->
            ?SecurityException({"Potentially harmful code", Exception});
        {'EXIT', Error} -> ?ClientError({"Term compile error", Error})
    end.

% @doc callback function used as 'Non-local Function Handler' in
% erl_eval:exprs/4 to restrict code injection. This callback function will
% exit with '{restricted,{M,F}}' exit value. If the exprassion is evaluated to
% an erlang fun, the fun will throw the same expection at runtime.
nonLocalHFun({erlang, Fun} = FSpec, Args) ->
    case lists:member(Fun,?SAFE_ERLANG_FUNCTIONS) of
        true ->
            apply(erlang, Fun, Args);
        false ->
            case lists:member(Fun,?UNSAFE_ERLANG_FUNCTIONS) of
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
nonLocalHFun({Mod, Fun}, Args) when Mod==math;Mod==lists;Mod==imem_datatype;Mod==imem_json ->
    apply(Mod, Fun, Args);
nonLocalHFun({io_lib, Fun}, Args) when Fun==format ->
    apply(io_lib, Fun, Args);
nonLocalHFun({imem_meta, Fun}, Args) when Fun==log_to_db;Fun==update_index;Fun==dictionary_trigger ->
    apply(imem_meta, Fun, Args);
nonLocalHFun({imem_dal_skvh, Fun}, Args) ->
    apply(imem_dal_skvh, Fun, Args);   % TODO: restrict to subset of functions
nonLocalHFun({imem_index, Fun}, Args) ->
    apply(imem_index, Fun, Args);   % TODO: restrict to subset of functions
nonLocalHFun({Mod, Fun}, Args) ->
    apply(imem_meta, secure_apply, [Mod, Fun, Args]).

%% ----- CAST Data from DB to string ------------------

db_to_io(Type, Prec, DateFmt, NumFmt, _StringFmt, Val) ->
    try
        if
            (Type == atom) andalso is_atom(Val) ->          atom_to_io(Val);
            (Type == binary) andalso is_binary(Val) ->      binary_to_io(Val);
            (Type == binstr) andalso is_binary(Val) ->      binstr_to_io(Val);
            (Type == boolean) andalso is_boolean(Val) ->    boolean_to_io(Val);
            (Type == datetime) andalso is_tuple(Val) ->     datetime_to_io(Val,DateFmt);
            (Type == decimal) andalso is_integer(Val) ->    decimal_to_io(Val,Prec);
            (Type == float) andalso is_float(Val) ->        float_to_io(Val,Prec,NumFmt);
            (Type == integer) andalso is_integer(Val) ->    integer_to_io(Val);
            (Type == ipaddr) andalso is_tuple(Val) ->       ipaddr_to_io(Val);
            (Type == string) andalso is_list(Val) ->        string_to_io(Val);
            (Type == timestamp) andalso is_tuple(Val) ->    timestamp_to_io(Val,Prec,DateFmt);
            (Type == userid) andalso is_atom(Val) ->        atom_to_io(Val);
            (Type == userid) ->                             userid_to_io(Val);
            (Type == binterm) andalso is_binary(Val) ->     binterm_to_io(Val);
            (Type == json) andalso is_binary(Val) ->        Val;
            (Type == json) andalso is_integer(Val) ->       integer_to_io(Val);
            (Type == json) andalso is_float(Val) ->         float_to_io(Val,Prec,NumFmt);
            (Type == json) andalso is_boolean(Val) ->       boolean_to_io(Val);
            (Type == json) andalso (Val==null) ->           atom_to_io(Val);
            (Type == json) andalso is_map(Val) ->           imem_json:encode(Val);
            (Type == json) andalso is_list(Val) ->          imem_json:encode(Val);
            true -> term_to_io(Val)
        end
    catch
        _:_ -> io_lib:format("~10000tp",[Val])
    end.

atom_to_io(Val) ->
    list_to_binary(io_lib:format("~tp",[Val])).

datetime_to_io(Datetime) ->
    datetime_to_io(Datetime, eu).

datetime_to_io({{Year,Month,Day},{Hour,Min,Sec}},eu) ->
    list_to_binary(io_lib:format("~2.10.0B.~2.10.0B.~4.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Day, Month, Year, Hour, Min, Sec]));
datetime_to_io(Datetime,erlang) ->
    term_to_io(Datetime);
datetime_to_io({{Year,Month,Day},{Hour,Min,Sec}},raw) ->
    list_to_binary(io_lib:format("~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",
        [Year, Month, Day, Hour, Min, Sec]));
datetime_to_io({{Year,Month,Day},{Hour,Min,Sec}},iso) ->
    list_to_binary(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Year, Month, Day, Hour, Min, Sec]));
datetime_to_io({{Year,Month,Day},{Hour,Min,Sec}},us) ->
    list_to_binary(io_lib:format("~2.10.0B/~2.10.0B/~4.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Month, Day, Year, Hour, Min, Sec]));
datetime_to_io(Datetime, Fmt) ->
    ?ClientError({"Data conversion format error",{datetime,Fmt,Datetime}}).

timestamp_to_io(TS) ->
    timestamp_to_io(TS,6,eu).

timestamp_to_io(TS,undefined) ->
    timestamp_to_io(TS,6,eu);
timestamp_to_io(TS,Prec) ->
    timestamp_to_io(TS,Prec,eu).

timestamp_to_io(TS,undefined,Fmt) ->
    timestamp_to_io(TS,6,Fmt);
timestamp_to_io({Megas,Secs,Micros},_Prec,raw) ->
    list_to_binary(io_lib:format("~6.6.0w~6.6.0w~6.6.0w",[Megas,Secs,Micros]));
timestamp_to_io(TS,_Prec,erlang) ->
    term_to_io(TS);
timestamp_to_io({Megas,Secs,Micros},Prec,Fmt) when Prec >= 6 ->
    list_to_binary(io_lib:format("~s.~6.6.0w",[datetime_to_io(calendar:now_to_local_time({Megas,Secs,0}),Fmt), Micros]));
timestamp_to_io({Megas,Secs,Micros},Prec,Fmt) when Prec > 0 ->
    [MStr0] = io_lib:format("~6.6.0w",[Micros]),
    % ?Info("----MStr0 ~p~n", [MStr0]),
    % ?Info("----Prec ~p~n", [Prec]),
    MStr1 = case list_to_integer(lists:sublist(MStr0, Prec+1, 6-Prec)) of
        0 ->    [$.|lists:sublist(MStr0, Prec)];
        _ ->    [$.|MStr0]
    end,
    list_to_binary(io_lib:format("~s~s",[datetime_to_io(calendar:now_to_local_time({Megas,Secs,0}),Fmt),MStr1]));
timestamp_to_io({Megas,Secs,0},_,Fmt) ->
    datetime_to_io(calendar:now_to_local_time({Megas,Secs,0}),Fmt);
timestamp_to_io({Megas,Secs,Micros},_,Fmt) ->
    timestamp_to_io({Megas,Secs,Micros},6,Fmt).



decimal_to_io(Val,undefined) ->
    decimal_to_io(Val,0);
decimal_to_io(Val,0) ->
    list_to_binary(io_lib:format("~p",[Val]));
decimal_to_io(Val,Prec) when Val < 0 ->
    list_to_binary(io_lib:format("-~s",[decimal_to_io(-Val,Prec)]));
decimal_to_io(Val,Prec) when Prec > 0 ->
    Str = integer_to_list(Val),
    Len = length(Str),
    if
        Prec-Len+1 > 0 ->
            {Whole,Frac} = lists:split(1,lists:duplicate(Prec-Len+1,$0) ++ Str),
            list_to_binary(io_lib:format("~s.~s",[Whole,Frac]));
        true ->
            {Whole,Frac} = lists:split(Len-Prec,Str),
            list_to_binary(io_lib:format("~s.~s",[Whole,Frac]))
    end;
decimal_to_io(Val,Prec) ->
    list_to_binary(io_lib:format("~s~s",[integer_to_list(Val),lists:duplicate(-Prec,$0)])).

binstr_to_io(BinStr) -> BinStr.

binary_to_io(Val) ->
    if
        byte_size(Val) =< ?BinaryMaxLen ->
            list_to_binary(io_lib:format("~s",[binary_to_hex(Val)]));
        true ->
            list_to_binary(io_lib:format("~s...",[binary_to_hex(binary:part(Val, 0, ?BinaryMaxLen))]))
    end.

userid_to_io(A) when is_atom(A) ->  list_to_binary(atom_to_list(A));
userid_to_io(Val) ->                integer_to_binary(Val).

offset_datetime('-', DT, Offset) ->
    offset_datetime('+', DT, -Offset);
offset_datetime('+', {{Y,M,D},{HH,MI,SS}}, Offset) ->
    GregSecs = calendar:datetime_to_gregorian_seconds({{Y,M,D},{HH,MI,SS}}),  %% for local time we should use calendar:local_time_to_universal_time_dst(DT)
    calendar:gregorian_seconds_to_datetime(GregSecs + round(Offset*86400.0)); %% calendar:universal_time_to_local_time(
offset_datetime(OP, DT, Offset) ->
    ?ClientError({"Illegal datetime offset operation",{OP,DT,Offset}}).

offset_timestamp('+', TS, Offset) when Offset < 0.0 ->
    offset_timestamp('-', TS, -Offset);
offset_timestamp('-', TS, Offset) when Offset < 0.0 ->
    offset_timestamp('+', TS, -Offset);
offset_timestamp(_, TS, Offset) when Offset < 5.787e-12 ->
    TS;
offset_timestamp('+', {Mega,Sec,Micro}, Offset) ->
    NewMicro = Micro + round(Offset*8.64e10),
    NewSec = Sec + NewMicro div 1000000,
    NewMega = Mega + NewSec div 1000000,
    {NewMega, NewSec rem 1000000, NewMicro rem 1000000};
offset_timestamp('-', {Mega,Sec,Micro}, Offset) ->
    NewMicro = Micro - round(Offset*8.64e10) + Sec * 1000000 + Mega * 1000000000000,
    Mi = NewMicro rem 1000000,
    NewSec = (NewMicro-Mi) div 1000000,
    Se = NewSec rem 1000000,
    NewMega = (NewSec-Se) div 1000000,
    {NewMega, Se, Mi};
offset_timestamp(OP, TS, Offset) ->
    ?ClientError({"Illegal timestamp offset operation",{OP,TS,Offset}}).

musec_diff(TS1) -> musec_diff(TS1,erlang:now()).

musec_diff({Mega1,Sec1,Micro1},{Mega2,Sec2,Micro2}) ->
    Micro2 - Micro1 + 1000000 *(Sec2 - Sec1) + 1000000000000 * (Mega2 - Mega1).

msec_diff(TS1) -> msec_diff(TS1,erlang:now()).

msec_diff({Mega1,Sec1,Micro1},{Mega2,Sec2,Micro2}) ->
    Micro2 div 1000 - Micro1 div 1000 + 1000 *(Sec2 - Sec1) + 1000000000 * (Mega2 - Mega1).

sec_diff(TS1) -> sec_diff(TS1,erlang:now()).

sec_diff({Mega1,Sec1,_},{Mega2,Sec2,_}) ->
    Sec2 - Sec1 + 1000000 * (Mega2 - Mega1).

ipaddr_to_io(IpAddr) ->
    list_to_binary(inet_parse:ntoa(IpAddr)).

float_to_io(Val,_Prec,_NumFmt) ->
    list_to_binary(float_to_list(Val)).                    %% ToDo: implement rounding to db precision

boolean_to_io(T) ->
    list_to_binary(io_lib:format("~p",[T])).

term_to_io(T) ->
    %list_to_binary(io_lib:format("~w",[T])).
    list_to_binary(t2s(T)).

t2s(T) when is_tuple(T) ->
    ["{"
    ,string:join([t2s(Te) || Te <- tuple_to_list(T)], ",")
    ,"}"];
t2s(T) when is_list(T) ->
    case io_lib:printable_list(T) of
        true -> io_lib:format("~p", [T]);
        _    -> ["["
                ,string:join([t2s(Te) || Te <- T], ",")
                ,"]"]
    end;
t2s(T) -> io_lib:format("~p", [T]).

binterm_to_io(B) when is_binary(B) ->
    list_to_binary(t2s(sext:decode(B))).


% escape_io(Str) when is_list(Str) ->
%     re:replace(Str, "(\")", "(\\\\\")", [global, {return, list}]);
% escape_io(Bin) when is_binary(Bin) ->
%     re:replace(Bin, "(\")", "(\\\\\")", [global, {return, binary}]).

un_escape_io(Str) when is_list(Str) ->
    re:replace(Str, "(\\\\\")", "\"", [global, {return, list}]);
un_escape_io(Bin) when is_binary(Bin) ->
    re:replace(Bin, "(\\\\\")", "\"", [global, {return, binary}]).

string_to_io(Val) when is_list(Val) ->
    case io_lib:printable_unicode_list(Val) of
        true ->
            unicode:characters_to_binary(io_lib:format("~p",[Val]));
        false ->
            list_to_binary(lists:flatten(io_lib:format("\"~tp\"",[Val])))
    end;
string_to_io(Val) ->
    list_to_binary(lists:flatten(io_lib:format("~tp",[Val]))).      %% "\"~tp\""

integer_to_io(Val) ->
    list_to_binary(integer_to_list(Val)).

%% ----- Helper Functions ---------------------------------------------

field_value(Tag,Type,Len,Prec,Def,Val) when is_binary(Val);is_list(Val) ->
    case io_to_db(Tag,?nav,Type,Len,Prec,Def,false,imem_sql:un_escape_sql(Val)) of
        T when is_tuple(T) ->   {const,T};
        V ->                    V
    end;
field_value(_,_,_,_,_,Val) when is_tuple(Val) -> {const,Val};
field_value(_,_,_,_,_,Val) -> Val.

%% @doc Convert string value to a proposed datatype with best effort and fallback to binstr.
%% Tag:     Label which can be used in conversion error log
%% Type:    Proposed data type
%% Len:     Proposed data length limit (0=any, undefined=any)
%% Prec:    Proposed precision, used for decimals
%% Def:     Proposed default value, alternative to NULL value
%% Val:     Input binstr without quotes
-spec field_value_type(any(),atom(),undefined|integer(),undefined|integer(),any(),list()|binary()) -> {any(),any(),atom(),integer()}.
%% throws ?ClientError, ?UnimplementedException
field_value_type(Tag,Type,Len,Prec,Def,Val) when is_binary(Val) ->
    try
        case io_to_db(Tag,?nav,Type,Len,Prec,Def,false,Val) of
            true ->                                     {true,true,boolean,0};
            false ->                                    {false,false,boolean,0};
            A when is_atom(A) ->                        {A,A,atom,0};
            B when is_binary(B),(Type==binstr) ->       {B,B,binstr,0};
            B when is_binary(B),(Type==binary) ->       {B,B,binary,0};
            B when is_binary(B),(Type==binterm) ->      {B,B,binary,0};
            T when is_tuple(T),(Type==datetime) ->      {T,{const,T},datetime,0};
            T when is_tuple(T),(Type==timestamp) ->     {T,{const,T},timestamp,0};
            T when is_tuple(T),(Type==ipaddr) ->        {T,{const,T},ipaddr,0};
            T when is_tuple(T) ->                       {T,{const,T},tuple,0};
            D when is_integer(D),(Type==decimal)->      {D,D,decimal,Prec};
            I when is_integer(I)->                      {I,I,integer,0};
            N when is_float(N)->                        {N,N,float,0};
            F when is_function(F) ->                    {F,F,'fun',0};
            S when is_list(S),(Type==string) ->         {S,S,string,0};
            L when is_list(L) ->                        {L,L,list,0};
            X ->                                        {X,X,term,0}
        end
    catch
        _:_ -> {Val,Val,binstr,0}
    end.

item(I,T) when is_tuple(T) ->
    if
        size(T) >= I ->
            term_to_io(element(I,T));
        true ->
            ?emptyIo        %% ?ClientError({"Tuple too short",{T,I}})
    end;
item(I,L) when is_list(L) ->
    if
        length(L) >= I ->
            term_to_io(lists:nth(I,L));
        true ->
            ?emptyIo        %% ?ClientError({"List too short",{L,I}})
    end;
item(I,B) when is_binary(B) ->
    try
        case sext:decode(B) of
            T when is_tuple(T) ->   item(I,T);
            L when is_list(L) ->    item(I,L);
            _ ->                    ?emptyIo
        end
    catch _:_ -> ?emptyIo
    end;
item(_,_) -> ?emptyIo.      %% ?ClientError({"Tuple or list expected",T}).

item1(T) -> item(1,T).
item2(T) -> item(2,T).
item3(T) -> item(3,T).
item4(T) -> item(4,T).
item5(T) -> item(5,T).
item6(T) -> item(6,T).
item7(T) -> item(7,T).
item8(T) -> item(8,T).
item9(T) -> item(9,T).

binary_to_hex(B) when is_binary(B) ->
  binary_to_hex(B, <<>>).

binary_to_hex(<<>>, Acc) -> Acc;
binary_to_hex(Bin, Acc) when byte_size(Bin) band 7 =:= 0 ->
  binary_to_hex_(Bin, Acc);
binary_to_hex(<<X:8, Rest/binary>>, Acc) ->
  binary_to_hex(Rest, <<Acc/binary, ?H(X)>>).

binary_to_hex_(<<>>, Acc) -> Acc;
binary_to_hex_(<<A:8, B:8, C:8, D:8, E:8, F:8, G:8, H:8, Rest/binary>>, Acc) ->
  binary_to_hex_(
    Rest,
    <<Acc/binary,
      ?H(A), ?H(B), ?H(C), ?H(D), ?H(E), ?H(F), ?H(G), ?H(H)>>).

% -compile({inline, [hex/1]}).
hex(X) ->
  element(
    X+1, {16#3030, 16#3031, 16#3032, 16#3033, 16#3034, 16#3035, 16#3036,
          16#3037, 16#3038, 16#3039, 16#3041, 16#3042, 16#3043, 16#3044,
          16#3045, 16#3046, 16#3130, 16#3131, 16#3132, 16#3133, 16#3134,
          16#3135, 16#3136, 16#3137, 16#3138, 16#3139, 16#3141, 16#3142,
          16#3143, 16#3144, 16#3145, 16#3146, 16#3230, 16#3231, 16#3232,
          16#3233, 16#3234, 16#3235, 16#3236, 16#3237, 16#3238, 16#3239,
          16#3241, 16#3242, 16#3243, 16#3244, 16#3245, 16#3246, 16#3330,
          16#3331, 16#3332, 16#3333, 16#3334, 16#3335, 16#3336, 16#3337,
          16#3338, 16#3339, 16#3341, 16#3342, 16#3343, 16#3344, 16#3345,
          16#3346, 16#3430, 16#3431, 16#3432, 16#3433, 16#3434, 16#3435,
          16#3436, 16#3437, 16#3438, 16#3439, 16#3441, 16#3442, 16#3443,
          16#3444, 16#3445, 16#3446, 16#3530, 16#3531, 16#3532, 16#3533,
          16#3534, 16#3535, 16#3536, 16#3537, 16#3538, 16#3539, 16#3541,
          16#3542, 16#3543, 16#3544, 16#3545, 16#3546, 16#3630, 16#3631,
          16#3632, 16#3633, 16#3634, 16#3635, 16#3636, 16#3637, 16#3638,
          16#3639, 16#3641, 16#3642, 16#3643, 16#3644, 16#3645, 16#3646,
          16#3730, 16#3731, 16#3732, 16#3733, 16#3734, 16#3735, 16#3736,
          16#3737, 16#3738, 16#3739, 16#3741, 16#3742, 16#3743, 16#3744,
          16#3745, 16#3746, 16#3830, 16#3831, 16#3832, 16#3833, 16#3834,
          16#3835, 16#3836, 16#3837, 16#3838, 16#3839, 16#3841, 16#3842,
          16#3843, 16#3844, 16#3845, 16#3846, 16#3930, 16#3931, 16#3932,
          16#3933, 16#3934, 16#3935, 16#3936, 16#3937, 16#3938, 16#3939,
          16#3941, 16#3942, 16#3943, 16#3944, 16#3945, 16#3946, 16#4130,
          16#4131, 16#4132, 16#4133, 16#4134, 16#4135, 16#4136, 16#4137,
          16#4138, 16#4139, 16#4141, 16#4142, 16#4143, 16#4144, 16#4145,
          16#4146, 16#4230, 16#4231, 16#4232, 16#4233, 16#4234, 16#4235,
          16#4236, 16#4237, 16#4238, 16#4239, 16#4241, 16#4242, 16#4243,
          16#4244, 16#4245, 16#4246, 16#4330, 16#4331, 16#4332, 16#4333,
          16#4334, 16#4335, 16#4336, 16#4337, 16#4338, 16#4339, 16#4341,
          16#4342, 16#4343, 16#4344, 16#4345, 16#4346, 16#4430, 16#4431,
          16#4432, 16#4433, 16#4434, 16#4435, 16#4436, 16#4437, 16#4438,
          16#4439, 16#4441, 16#4442, 16#4443, 16#4444, 16#4445, 16#4446,
          16#4530, 16#4531, 16#4532, 16#4533, 16#4534, 16#4535, 16#4536,
          16#4537, 16#4538, 16#4539, 16#4541, 16#4542, 16#4543, 16#4544,
          16#4545, 16#4546, 16#4630, 16#4631, 16#4632, 16#4633, 16#4634,
          16#4635, 16#4636, 16#4637, 16#4638, 16#4639, 16#4641, 16#4642,
          16#4643, 16#4644, 16#4645, 16#4646}).



%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

erl_value_test_() ->
    {inparallel,
     [{C, case O of
              'SystemException' ->
                  ?_assertException(throw, {'SecurityException', _},
                                    erl_value(C));
              'ClientError' ->
                  ?_assertException(throw, {'ClientError', _},
                                    erl_value(C));
              runtime ->
                  Fun = erl_value(C),
                  ?_assertException(throw, {'SystemException', _},
                                    Fun());
              _ ->
                  ?_assertEqual(O, erl_value(C))
          end}
      || {C,O} <-
         [
          {"{1,2}", {1,2}},
          {"(fun() -> 1 + 2 end)()", 3},
          {"(fun() -> A end)()", 'ClientError'},
          {"os:cmd(\"pwd\")", 'SystemException'},
          {"(fun() -> apply(filelib, ensure_dir, [\"pwd\"]) end)()",
           'SystemException'},
          {"fun() -> os:cmd(\"pwd\") end", runtime}
         ]
     ]}.

db_test_() ->
    {
        setup,
        fun() -> ok end,
        fun(_) -> ok end,
        {with, [
              fun data_types/1
        ]}}.

data_types(_) ->
    try
        ClEr = 'ClientError',
        %% SyEx = 'SystemException',    %% difficult to test

        ?LogDebug("----------------------------------~n"),
        ?LogDebug("---TEST--- ~p ----Security ~p", [?MODULE, all]),
        ?LogDebug("----------------------------------~n"),

        ?assertEqual(<<"'Imem'">>, item1({'Imem',ddTable})),
        ?assertEqual(<<"'Imem'">>, item(1,{'Imem',ddTable})),
        ?assertEqual(<<"ddTable">>, item2({'Imem',ddTable})),
        ?assertEqual(<<"12.45">>, item2({'Imem',12.45})),
        ?assertEqual(<<"\"ddTable\"">>, item2({'Imem',"ddTable"})),
        ?assertEqual(<<"{1,2,3,4}">>, item2({'Imem',{1,2,3,4}})),
        ?assertEqual(<<"<<\"abcd\">>">>, item(2,{'Imem',<<"abcd">>})),
        %% ?assertEqual(<<"<<\"ddäöü\">>/utf8">>, item(2,{'Imem',<<"ddäöü"/utf8>>})),
        ?LogDebug("item success~n", []),

        ?assertEqual(<<"\"abcde\"">>, string_to_io("abcde")),
        ?assertEqual(<<"\"123\\\"abc\"">>, string_to_io("123\"abc")),

        ?assertEqual("abcde", io_to_string(<<"\"abcde\"">>)),
        ?assertEqual("123\"abc", io_to_string(<<"\"123\\\"abc\"">>)),

        ?LogDebug("Str0 äöü -> ~p~n",[[0|"äöü"]]),

        Str1 = "äöü",                       ?LogDebug("Str1 äöü -> ~p~n",[[<<?H(I)>> || I <- Str1]]),
        Utf8 = unicode:characters_to_binary("äöü",unicode,utf8), ?LogDebug("Utf8 äöü -> ~p~n",[binary_to_hex(Utf8)]),
        Exp1 = <<"\"äöü\""/utf8>>,          ?LogDebug("Exp1 äöü -> ~p~n",[binary_to_hex(Exp1)]),
        Ios1 = string_to_io(Str1),          ?LogDebug("Ios1 äöü -> ~p~n",[binary_to_hex(Ios1)]),
        ?assertEqual(Exp1, Ios1),

        ?assertEqual("abcde", io_to_string(string_to_io("abcde"))),
        ?assertEqual("123\"abc", io_to_string(string_to_io("123\"abc"))),
        ?assertEqual(Str1, io_to_string(string_to_io(Str1))),

        ?assertEqual(<<"\"abcde\"">>, string_to_io(io_to_string(<<"\"abcde\"">>))),
        ?assertEqual(<<"\"123\\\"abc\"">>, string_to_io(io_to_string(<<"\"123\\\"abc\"">>))),
        ?assertEqual(Exp1, string_to_io(io_to_string(Exp1))),

        ?assertEqual(<<"123">>, decimal_to_io(123,0)),
        ?assertEqual(<<"-123">>, decimal_to_io(-123,0)),
        ?assertEqual(<<"12.3">>, decimal_to_io(123,1)),
        ?assertEqual(<<"-12.3">>, decimal_to_io(-123,1)),
        ?assertEqual(<<"0.123">>, decimal_to_io(123,3)),
        ?assertEqual(<<"-0.123">>, decimal_to_io(-123,3)),
        ?assertEqual(<<"0.00123">>, decimal_to_io(123,5)),
        ?assertEqual(<<"-0.00123">>, decimal_to_io(-123,5)),
        ?assertEqual(<<"-0.00123">>, decimal_to_io(-123,5)),
        ?assertEqual(<<"12300000">>, decimal_to_io(123,-5)),
        ?assertEqual(<<"-12300000">>, decimal_to_io(-123,-5)),
        ?LogDebug("decimal_to_io success~n", []),

        ?assertEqual(<<"0.0.0.0">>, ipaddr_to_io({0,0,0,0})),
        ?assertEqual(<<"1.2.3.4">>, ipaddr_to_io({1,2,3,4})),
        ?LogDebug("ipaddr_to_io success~n", []),

        ?assertEqual({1900,2,1}, parse_date_eu("01.02.1900")),
        ?assertEqual({1900,2,1}, parse_date_us("02/01/1900")),
        ?assertEqual({1900,2,1}, parse_date_us("02/01/1900")),
        ?assertEqual({1900,2,1}, parse_date_int("1900-02-01")),
        ?assertException(throw,{ClEr,{"parse_month","1900"}}, parse_date_us("1900/02/01")),
        ?assertException(throw,{ClEr,{"Cannot handle dates before 1970"}},local_datetime_to_utc_seconds({{1900, 01, 01}, {00, 00, 00}})),
        % ?assertEqual(2208985200,local_datetime_to_utc_seconds({{1970, 01, 01}, {00, 00, 00}})),
        % ?assertEqual(<<"01.01.1970 01:00:00.123456">>, timestamp_to_io({0,0,123456},0)),  %% with DLS offset wintertime CH
        % ?assertEqual(<<"01.01.1970 01:00:00">>, timestamp_to_io({0,0,0},0)),  %% with DLS offset wintertime CH
        % ?assertEqual(<<"01.01.1970 01:00:00.123">>, timestamp_to_io({0,0,123000},3)),  %% with DLS offset wintertime CH
        % ?assertEqual(<<"12.01.1970 14:46:42.123456">>, timestamp_to_io({1,2,123456},6)),  %% with DLS offset wintertime CH
        ?assertEqual(<<"{1,2,1234}">>, timestamp_to_io({1,2,1234},3,erlang)),
        ?assertEqual(<<"000001000002001234">>, timestamp_to_io({1,2,1234},3,raw)),
        ?LogDebug("timestamp_to_io success~n", []),
        % ?assertEqual({0,0,0}, io_to_timestamp(<<"01.01.1970 01:00:00.000000">>,0)),  %% with DLS offset wintertime CH
        % ?assertEqual({0,-3600,0}, io_to_timestamp(<<"01.01.1970">>,0)),                  %% with DLS offset wintertime CH
        % ?assertEqual({0,-3600,0}, io_to_timestamp(<<"1970-01-01">>)),                  %% with DLS offset wintertime CH
        % ?assertEqual({162,946800,0}, io_to_timestamp(<<"1975-03-02">>)),                  %% with DLS offset wintertime CH
        % ?assertEqual({1,2,123456}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,undefined)),  %% with DLS offset wintertime CH
        % ?assertEqual({1,2,123456}, io_to_timestamp(<<"12.01.70 14:46:42.123456">>,undefined)),  %% with DLS offset wintertime CH
        % ?assertEqual({1,2,123456}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,6)),  %% with DLS offset wintertime CH
        % ?assertEqual({1,2,123000}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,3)),  %% with DLS offset wintertime CH
        % ?assertEqual({1,2,123456}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,undefined)),  %% with DLS offset wintertime CH
        % ?assertEqual({1,3,0}, io_to_timestamp(<<"12.01.1970 14:46:42.654321">>,0)),  %% with DLS offset wintertime CH
        % ?assertEqual({1,2,123000}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,3)),  %% with DLS offset wintertime CH
        % ?assertEqual({1,2,100000}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,1)),  %% with DLS offset wintertime CH
        % ?assertEqual({587,863439,585000}, io_to_timestamp(<<"1988-8-18T01:23:59.585Z">>)),
        ?assertEqual({1,2,12345}, io_to_timestamp(<<"{1,2,12345}">>,0)),
        ?LogDebug("io_to_timestamp success~n", []),

        ?assertEqual(list_to_pid("<0.44.0>"), io_to_pid("<0.44.0>")),
        ?assertEqual(list_to_pid("<0.44.555>"), io_to_pid(<<"<0.44.555>">>)),
        ?assertEqual(list_to_pid("<0.55.0>"), io_to_pid(<<"<0.55.0>">>)),

        ?LogDebug("io_to_db success 5~n", []),

        LocalTime = erlang:localtime(),
        {Date,_Time} = LocalTime,
        ?assertEqual({{2004,3,1},{0,0,0}}, io_to_datetime(<<"1.3.2004">>)),
        ?assertEqual({{2004,3,1},{3,45,0}}, io_to_datetime(<<"1.3.2004 3:45">>)),
        ?assertEqual({{2004,3,1},{3,45,0}}, io_to_datetime(<<"{{2004,3,1},{3,45,0}}">>)),
        ?assertEqual({{2012,12,10},{8,44,7}}, io_to_datetime(<<"10.12.12 08:44:07">>)),
        ?assertEqual({{1999,12,10},{8,44,7}}, io_to_datetime(<<"10.12.99 08:44:07">>)),
        ?assertEqual({Date,{0,0,0}}, io_to_datetime(<<"today">>)),
        ?assertEqual(LocalTime, io_to_datetime(<<"sysdate">>)),
        ?assertEqual(LocalTime, io_to_datetime(<<"systime">>)),
        ?assertEqual(LocalTime, io_to_datetime(<<"now">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"18.8.1888 1:23:59">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"1888-08-18 1:23:59">>)),
        ?assertEqual({{2018,8,18},{1,23,59}}, io_to_datetime(<<"18-08-18 1:23:59">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"8/18/1888 1:23:59">>)),
        ?assertEqual({{1988,8,18},{1,23,59}}, io_to_datetime(<<"8/18/88 1:23:59">>)),
        ?assertEqual({{1888,8,18},{1,23,0}}, io_to_datetime(<<"8/18/1888 1:23">>)),
        ?assertEqual({{1888,8,18},{1,0,0}}, io_to_datetime(<<"8/18/1888 01">>)),
        ?assertException(throw,{ClEr,{"Data conversion format error",{datetime,"8/18/1888 1"}}}, io_to_datetime(<<"8/18/1888 1">>)),
        ?assertException(throw,{ClEr,{"Data conversion format error",{datetime,"8/18/1888 1"}}}, io_to_datetime(<<"8/18/1888 1">>)),
        ?assertEqual({{1888,8,18},{0,0,0}}, io_to_datetime(<<"8/18/1888 ">>)),
        ?assertEqual({{1888,8,18},{0,0,0}}, io_to_datetime(<<"8/18/1888">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"18880818012359">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"18880818 012359">>)),
        ?assertEqual({{1988,8,18},{1,23,59}}, io_to_datetime(<<"880818 012359">>)),
        ?assertEqual({{1988,8,18},{1,23,59}}, io_to_datetime(<<"1988-8-18T01:23:59.585Z">>)),
        ?LogDebug("io_to_datetime success~n", []),

        ?assertEqual({1,23,59}, parse_time("01:23:59")),
        ?assertEqual({1,23,59}, parse_time("1:23:59")),
        ?assertEqual({1,23,0}, parse_time("01:23")),
        ?assertEqual({1,23,59}, parse_time("012359")),
        ?assertEqual({1,23,0}, parse_time("0123")),
        ?assertEqual({1,0,0}, parse_time("01")),
        ?assertEqual({0,0,0}, parse_time("")),
        ?LogDebug("parse_time success~n", []),

        ?assertEqual({1,2,3,4},io_to_ipaddr(<<"1.2.3.4">>,0)),
        ?assertEqual({1,2,3,4},io_to_ipaddr(<<"{1,2,3,4}">>,undefined)),
        ?assertEqual({1,2,3,4},io_to_ipaddr(<<"1.2.3.4">>,0)),
        ?assertEqual({1,2,3,4},io_to_ipaddr(<<"{1,2,3,4}">>,undefined)),
        ?assertEqual({1,2,3,4},io_to_db(0,"",ipaddr,0,0,undefined,false,<<"1.2.3.4">>)),
        ?assertEqual({1,2,3,4},io_to_db(0,"",ipaddr,0,undefined,undefined,false,<<"1.2.3.4">>)),

        Item = 0,
        OldString = io_to_string(<<"\"OldäString\"">>),
        Len = 3,
        Prec = 1,
        Def = default,
        RW = false,
        DefFun = fun() -> [{},{}] end,
        ?assertEqual(OldString, io_to_db(Item,OldString,string,Len,Prec,Def,true,<<"NewVal">>)),
        ?LogDebug("io_to_db success 1~n", []),
        ?assertEqual(io_to_string(<<"\"NöVal\"">>), io_to_db(Item,OldString,string,6,Prec,Def,RW,<<"\"NöVal\"">>)),
        ?assertEqual(default, io_to_db(Item,OldString,string,Len,Prec,Def,RW,?emptyIo)),
        ?assertEqual([], io_to_db(Item,OldString,string,Len,Prec,[],RW,?emptyIo)),
        ?assertEqual("{}", io_to_db(Item,OldString,string,Len,Prec,Def,RW,<<"\"{}\"">>)),
        ?assertEqual("[atom,atom]", io_to_db(Item,OldString,string,30,Prec,Def,RW,<<"\"[atom,atom]\"">>)),
        ?assertEqual("12", io_to_db(Item,OldString,string,Len,Prec,Def,RW,<<"\"12\"">>)),
        ?assertEqual("-3.14", io_to_db(Item,OldString,string,5,Prec,Def,RW,<<"\"-3.14\"">>)),
        ?assertEqual("-3.14", io_to_db(Item,OldString,string,undefined,undefined,Def,RW,<<"\"-3.14\"">>)),
        ?LogDebug("io_to_db success 2~n", []),

        ?assertException(throw,{ClEr,{"String is too long",{0,{<<"NewVal">>,3}}}}, io_to_db(Item,OldString,string,Len,Prec,Def,RW,<<"\"NewVal\"">>)),
        ?assertEqual("NewVal", io_to_db(Item,OldString,string,6,Prec,Def,RW,<<"\"NewVal\"">>)),
        ?assertEqual("[NewVal]", io_to_db(Item,OldString,string,8,Prec,Def,RW,<<"\"[NewVal]\"">>)),
        ?assertEqual("default", io_to_db(Item,OldString,string,7,Prec,Def,RW,<<"\"default\"">>)),
        ?assertEqual(default, io_to_db(Item,OldString,string,Len,Prec,Def,RW,<<"default">>)),
        ?LogDebug("io_to_db success 3~n", []),

        ?assertEqual([{},{}], io_to_db(Item,OldString,string,Len,Prec,DefFun,RW,<<"[{},{}]">>)),
        ?assertEqual(oldValue, io_to_db(Item,oldValue,string,Len,Prec,Def,RW,<<"oldValue">>)),
        ?assertEqual('OldValue', io_to_db(Item,'OldValue',string,Len,Prec,Def,RW,<<"'OldValue'">>)),
        ?assertEqual(-15, io_to_db(Item,-15,string,Len,Prec,Def,RW,<<"-15">>)),
        ?LogDebug("io_to_db success 3a~n", []),

        ?assertEqual(OldString, io_to_db(Item,OldString,binstr,Len,Prec,Def,true,<<"NewVal">>)),
        ?LogDebug("io_to_db success 4a~n", []),
        ?assertEqual(<<"NöVal"/utf8>>, io_to_db(Item,OldString,binstr,6,undefined,Def,RW,<<"NöVal"/utf8>>)),
        ?assertEqual(default, io_to_db(Item,OldString,binstr,Len,Prec,Def,RW,?emptyIo)),
        ?assertEqual(<<>>, io_to_db(Item,OldString,binstr,Len,Prec,<<>>,RW,?emptyIo)),
        ?assertEqual(<<"{}">>, io_to_db(Item,OldString,binstr,Len,Prec,Def,RW,<<"{}">>)),
        ?assertEqual(<<"[atom,atom]">>, io_to_db(Item,OldString,binstr,30,Prec,Def,RW,<<"[atom,atom]">>)),
        ?assertEqual(<<"12">>, io_to_db(Item,OldString,binstr,Len,Prec,Def,RW,<<"12">>)),
        ?assertEqual(<<"-3.14">>, io_to_db(Item,OldString,binstr,5,Prec,Def,RW,<<"-3.14">>)),
        ?LogDebug("io_to_db success 4b~n", []),

        ?assertEqual(<<"OldäString"/utf8>>, io_to_db(Item,<<"OldäString"/utf8>>,binstr,11,Prec,Def,RW,<<"OldäString"/utf8>>)),
        ?assertException(throw,{ClEr,{"String is too long",{0,{<<"OldäString"/utf8>>,3}}}},  io_to_db(Item,<<"OldäString"/utf8>>,binstr,Len,Prec,Def,RW,<<"OldäString"/utf8>>)),
        ?assertException(throw,{ClEr,{"String is too long",{0,{<<"NewVal">>,3}}}}, io_to_db(Item,<<"OldäString"/utf8>>,binstr,Len,Prec,Def,RW,<<"NewVal">>)),
        ?assertEqual(<<"NewVal">>, io_to_db(Item,OldString,binstr,6,Prec,Def,RW,<<"NewVal">>)),
        ?assertEqual(<<"[NewVal]">>, io_to_db(Item,OldString,binstr,8,Prec,Def,RW,<<"[NewVal]">>)),
        ?assertEqual(default, io_to_db(Item,OldString,binstr,Len,Prec,Def,RW,<<"default">>)),
        ?assertEqual(default, io_to_db(Item,OldString,binstr,Len,Prec,Def,RW,<<"default">>)),
        ?LogDebug("io_to_db success 4c~n", []),


        OldInteger = 17,
    %     ?assertEqual(OldString, io_to_db(Item,OldString,integer,Len,Prec,Def,RW,<<"OldäString">>)),
        ?assertEqual(OldInteger, io_to_db(Item,OldInteger,integer,Len,Prec,Def,RW,<<"17">>)),
        ?assertEqual(default, io_to_db(Item,OldInteger,integer,Len,Prec,Def,RW,<<"default">>)),
        ?assertEqual(18, io_to_db(Item,OldInteger,integer,Len,Prec,Def,RW,<<"18">>)),
        ?assertEqual(-18, io_to_db(Item,OldInteger,integer,undefined,undefined,Def,RW,<<"-18">>)),
        ?assertEqual(-18, io_to_db(Item,OldInteger,integer,Len,Prec,Def,RW,<<"-18">>)),
        ?assertEqual(100, io_to_db(Item,OldInteger,integer,Len,-2,Def,RW,<<"149">>)),
        ?assertEqual(200, io_to_db(Item,OldInteger,integer,Len,-2,Def,RW,<<"150">>)),
        ?assertEqual(-100, io_to_db(Item,OldInteger,integer,4,-2,Def,RW,<<"-149">>)),
        ?assertEqual(-200, io_to_db(Item,OldInteger,integer,4,-2,Def,RW,<<"-150">>)),
        ?assertEqual(-200, io_to_db(Item,OldInteger,integer,100,0,Def,RW,<<"300-500">>)),
        ?assertEqual(12, io_to_db(Item,OldInteger,integer,20,0,Def,RW,<<"120/10.0">>)),
        ?assertEqual(12, io_to_db(Item,OldInteger,integer,20,0,Def,RW,<<"120/10.0001">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,<<"1234">>}}}}, io_to_db(Item,OldInteger,integer,Len,Prec,Def,RW,<<"1234">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,<<"-">>}}}}, io_to_db(Item,OldInteger,integer,Len,Prec,Def,RW,<<"-">>)),
        ?assertEqual(default, io_to_db(Item,OldInteger,integer,Len,Prec,Def,RW,?emptyIo)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,<<"-100">>}}}}, io_to_db(Item,OldInteger,integer,Len,Prec,Def,RW,<<"-100">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,<<"9999">>}}}}, io_to_db(Item,OldInteger,integer,Len,Prec,Def,RW,<<"9999">>)),

        ?LogDebug("io_to_db success 5~n", []),

        OldFloat = -1.2,
        ?assertEqual(8.1, io_to_db(Item,OldFloat,float,undefined,undefined,Def,RW,<<"8.1">>)),
        ?assertEqual(8.1, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,<<"8.1">>)),
        ?assertEqual(18.0, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,<<"18">>)),
        ?assertEqual(1.1, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,<<"1.12">>)),
        ?assertEqual(-1.1, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,<<"-1.14">>)),
        ?assertEqual(-1.1, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,<<"-1.1234567">>)),
        ?assertEqual(-1.12, io_to_db(Item,OldFloat,float,Len,2,Def,RW,<<"-1.1234567">>)),
        ?assertEqual(-1.123, io_to_db(Item,OldFloat,float,undefined,3,Def,RW,<<"-1.1234567">>)),
        ?assertEqual(-1.1235, io_to_db(Item,OldFloat,float,Len,4,Def,RW,<<"-1.1234567">>)),
        ?LogDebug("io_to_db success 6~n", []),
        %% ?assertEqual(-1.12346, io_to_db(Item,OldFloat,float,Len,5,Def,RW,"-1.1234567")),  %% fails due to single precision math
        %% ?assertEqual(-1.123457, io_to_db(Item,OldFloat,float,Len,6,Def,RW,"-1.1234567")), %% fails due to single precision math
        ?assertEqual(100.0, io_to_db(Item,OldFloat,float,Len,-2,Def,RW,<<"149">>)),
        ?assertEqual(-100.0, io_to_db(Item,OldFloat,float,undefined,-2,Def,RW,<<"-149">>)),
        ?assertEqual(-200.0, io_to_db(Item,OldFloat,float,Len,-2,Def,RW,<<"-150">>)),
        %% ?assertEqual(0.6, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,"0.56")),         %% rounding not supported any more for floats
        %% ?assertEqual(0.6, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,"0.5678")),
        %% ?assertEqual(0.6, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,"0.5678910111")),
        %% ?assertEqual(0.6, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,"0.56789101112131415")),
        ?assertEqual(1234.5, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,<<"1234.5">>)),   %% rounding not supported any more for floats
        %% ?assertEqual(1234.6, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,"1234.56")),
        %% ?assertEqual(1234.6, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,"1234.5678")),
        %% ?assertEqual(1234.6, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,"1234.5678910111")),
        %% ?assertEqual(1234.6, io_to_db(Item,OldFloat,float,Len,Prec,Def,RW,"1234.56789101112131415")),
        ?LogDebug("io_to_db success 7~n", []),

        OldDecimal = -123,
        ?assertEqual(81, io_to_db(Item,OldDecimal,decimal,Len,Prec,Def,RW,<<"8.1">>)),
        ?assertEqual(86, io_to_db(Item,OldDecimal,decimal,Len,Prec,Def,RW,<<"8.6">>)),
        ?assertEqual(-86, io_to_db(Item,OldDecimal,decimal,Len,Prec,Def,RW,<<"-8.6">>)),
        ?assertEqual(8, io_to_db(Item,OldDecimal,decimal,Len,undefined,Def,RW,<<"8.1">>)),
        ?assertEqual(180, io_to_db(Item,OldDecimal,decimal,Len,Prec,Def,RW,<<"18.001">>)),
        ?assertEqual(1, io_to_db(Item,OldDecimal,decimal,1,0,Def,RW,<<"1.12">>)),
        ?assertEqual(123, io_to_db(Item,OldDecimal,decimal,Len,2,Def,RW,<<"1.23456">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,5,"1.123"}}}}, io_to_db(Item,OldDecimal,decimal,5,5,Def,RW,<<"1.123">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,6,5,"-1.123"}}}}, io_to_db(Item,OldDecimal,decimal,6,5,Def,RW,<<"-1.123">>)),
        ?assertEqual(-112300, io_to_db(Item,OldDecimal,decimal,7,5,Def,RW,<<"-1.123">>)),
        ?assertEqual(-112346, io_to_db(Item,OldDecimal,decimal,7,5,Def,RW,<<"-1.1234567">>)),
        ?assertEqual(3123, io_to_db(Item,OldDecimal,decimal,10,3,Def,RW,<<"3.1226">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,Prec,"1234567.89"}}}}, io_to_db(Item,OldDecimal,decimal,5,Prec,Def,RW,<<"1234567.89">>)),
        ?assertEqual(1234500, io_to_db(Item,OldDecimal,decimal,7,2,Def,RW,<<"12345">>)),
        ?assertEqual(1234500, io_to_db(Item,OldDecimal,decimal,7,2,Def,RW,<<"12345.0000">>)),
        ?assertEqual(-1234500, io_to_db(Item,OldDecimal,decimal,8,2,Def,RW,<<"-12345">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,2,"12345"}}}}, io_to_db(Item,OldDecimal,decimal,5,2,Def,RW,<<"12345">>)),
        ?assertEqual(300000000000000000000000000000000000000, io_to_db(Item,OldDecimal,decimal,undefined,38,Def,RW,<<"3">>)),
        ?assertEqual(2, io_to_db(Item,OldDecimal,decimal,undefined,0,Def,RW,<<"2">>)),
        ?assertEqual(2, io_to_db(Item,OldDecimal,decimal,undefined,-3,Def,RW,<<"2000">>)),
        ?assertEqual(0, io_to_db(Item,OldDecimal,decimal,undefined,-4,Def,RW,<<"4999">>)),
        ?assertEqual(0, io_to_db(Item,OldDecimal,decimal,undefined,-5,Def,RW,<<"9000">>)),
        ?assertEqual(16, io_to_db(Item,OldDecimal,decimal,undefined,-2,Def,RW,<<"1590">>)),
        ?assertEqual(1, io_to_db(Item,OldDecimal,decimal,undefined,-4,Def,RW,<<"5000">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,-2,"123bb"}}}}, io_to_db(Item,OldDecimal,decimal,5,-2,Def,RW,<<"123bb">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,-1,"--123"}}}}, io_to_db(Item,OldDecimal,decimal,5,-1,Def,RW,<<"--123">>)),
        ?LogDebug("io_to_db success 8~n", []),

        OldTerm = {-1.2,[a,b,c]},
        ?assertEqual(OldTerm, io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"{-1.2,[a,b,c]}">>)),
        ?assertEqual(Def, io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"default">>)),
        ?assertEqual("default", io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"\"default\"">>)),
        ?assertEqual("'default'", io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"\"'default'\"">>)),
        ?assertEqual('default', io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"'default'">>)),
        ?assertEqual([a,b], io_to_db(Item,OldTerm,term,undefined,undefined,Def,RW,<<"[a,b]">>)),
        ?assertEqual([a,b], io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"[a,b]">>)),
        ?assertEqual(-1.1234567, io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"-1.1234567">>)),
        ?assertEqual("'-1.1234567'", io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"\"'-1.1234567'\"">>)),
        ?assertEqual('-1.1234567', io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"'-1.1234567'">>)),
        ?assertEqual("-1.1234567", io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"\"-1.1234567\"">>)),
        ?assertEqual({[1,2,3]}, io_to_db(Item,OldTerm,term,undefined,Prec,Def,RW,<<"{[1,2,3]}">>)),
        ?assertEqual("{[1,2,3]}", io_to_db(Item,OldTerm,term,Len,undefined,Def,RW,<<"\"{[1,2,3]}\"">>)),
        ?assertEqual({1,2,3}, io_to_db(Item,?nav,term,0,0,?nav,false,<<"{1,2,3}">>)),
        ?assertEqual("{1,2,3}", io_to_db(Item,?nav,term,0,0,?nav,false,<<"\"{1,2,3}\"">>)),
        ?assertEqual([1,2,3], io_to_db(Item,?nav,term,0,0,?nav,false,<<"[1,2,3]">>)),
        ?assertEqual("[1,2,3]", io_to_db(Item,?nav,term,0,0,?nav,false,<<"\"[1,2,3]\"">>)),
        ?assertEqual('$_', io_to_db(Item,?nav,term,0,0,?nav,false,<<"'$_'">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{term,<<"[a|]">>}}}}, io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"[a|]">>)),
        ?LogDebug("io_to_db success 9~n", []),

        OldBinTerm = term_to_binterm({-1.2,[a,b,c]}),
        ?assertEqual(OldBinTerm, io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"{-1.2,[a,b,c]}">>)),
        ?assertEqual(Def, io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"default">>)),
        ?assertEqual(term_to_binterm("default"), io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"\"default\"">>)),
        ?assertEqual(term_to_binterm("'default'"), io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"\"'default'\"">>)),
        ?assertEqual(term_to_binterm('default'), io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"'default'">>)),
        ?assertEqual(term_to_binterm([a,b]), io_to_db(Item,OldBinTerm,binterm,undefined,undefined,Def,RW,<<"[a,b]">>)),
        ?assertEqual(term_to_binterm([a,b]), io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"[a,b]">>)),
        ?assertEqual(term_to_binterm(-1.1234567), io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"-1.1234567">>)),
        ?assertEqual(term_to_binterm("'-1.1234567'"), io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"\"'-1.1234567'\"">>)),
        ?assertEqual(term_to_binterm('-1.1234567'), io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"'-1.1234567'">>)),
        ?assertEqual(term_to_binterm("-1.1234567"), io_to_db(Item,OldBinTerm,binterm,Len,Prec,Def,RW,<<"\"-1.1234567\"">>)),
        ?assertEqual(term_to_binterm({[1,2,3]}), io_to_db(Item,OldBinTerm,binterm,undefined,Prec,Def,RW,<<"{[1,2,3]}">>)),
        ?assertEqual(term_to_binterm("{[1,2,3]}"), io_to_db(Item,OldBinTerm,binterm,Len,undefined,Def,RW,<<"\"{[1,2,3]}\"">>)),
        ?assertEqual(term_to_binterm({1,2,3}), io_to_db(Item,?nav,binterm,0,0,?nav,false,<<"{1,2,3}">>)),
        ?assertEqual(term_to_binterm("{1,2,3}"), io_to_db(Item,?nav,binterm,0,0,?nav,false,<<"\"{1,2,3}\"">>)),
        ?assertEqual(term_to_binterm([1,2,3]), io_to_db(Item,?nav,binterm,0,0,?nav,false,<<"[1,2,3]">>)),
        ?assertEqual(term_to_binterm("[1,2,3]"), io_to_db(Item,?nav,binterm,0,0,?nav,false,<<"\"[1,2,3]\"">>)),
        ?assertEqual(term_to_binterm('$_'), io_to_db(Item,?nav,binterm,0,0,?nav,false,<<"'$_'">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{term,<<"[a|]">>}}}}, io_to_db(Item,OldTerm,term,Len,Prec,Def,RW,<<"[a|]">>)),
        ?LogDebug("io_to_db success 9a~n", []),

        ?assertEqual(true, io_to_db(Item,OldTerm,boolean,undefined,Prec,Def,RW,<<"true">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{boolean,<<"\"false\"">>}}}}, io_to_db(Item,OldTerm,boolean,Len,undefined,Def,RW,<<"\"false\"">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{boolean,<<"something">>}}}}, io_to_db(Item,OldTerm,boolean,Len,Prec,Def,RW,<<"something">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{boolean,<<"TRUE">>}}}}, io_to_db(Item,OldTerm,boolean,Len,Prec,Def,RW,<<"TRUE">>)),
        ?LogDebug("io_to_db success 10~n", []),


        ?assertEqual({1,2,3}, io_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,<<"{1,2,3}">>)),
        ?assertEqual({}, io_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,<<"{}">>)),
        ?assertEqual({1}, io_to_db(Item,OldTerm,tuple,undefined,undefined,Def,RW,<<"{1}">>)),
        ?assertEqual({1,2}, io_to_db(Item,OldTerm,tuple,undefined,Prec,Def,RW,<<"{1,2}">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{tuple,Len,<<"[a]">>}}}}, io_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,<<"[a]">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{tuple,Len,<<"{a}">>}}}}, io_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,<<"{a}">>)),
        ?LogDebug("io_to_db success 11~n", []),

        ?assertEqual([a,b,c], io_to_db(Item,OldTerm,list,Len,Prec,Def,RW,<<"[a,b,c]">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{list,Len,<<"[a]">>}}}}, io_to_db(Item,OldTerm,list,Len,Prec,Def,RW,<<"[a]">>)),
        ?assertEqual([], io_to_db(Item,[],list,undefined,Prec,Def,RW,<<"[]">>)),
        ?assertEqual([], io_to_db(Item,OldTerm,list,undefined,undefined,[],RW,<<"[]">>)),
        ?assertEqual([], io_to_db(Item,OldTerm,list,0,Prec,Def,RW,<<"[]">>)),
        ?assertEqual([a], io_to_db(Item,OldTerm,list,undefined,Prec,Def,RW,<<"[a]">>)),
        ?assertEqual([a,b], io_to_db(Item,OldTerm,list,undefined,Prec,Def,RW,<<"[a,b]">>)),
        ?assertEqual("123", io_to_db(Item,OldTerm,list,0,Prec,Def,RW,<<"\"123\"">>)),
        ?LogDebug("io_to_db success 12~n", []),

        ?assertEqual(#{}, io_to_db(Item,OldTerm,map,Len,Prec,Def,RW,<<"#{}">>)),
        ?assertEqual(#{a=>1,b=>2}, io_to_db(Item,OldTerm,map,Len,Prec,Def,RW,<<"#{a=>1,b=>2}">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{map,<<"[a]">>}}}}, io_to_db(Item,OldTerm,map,Len,Prec,Def,RW,<<"[a]">>)),

        ?assertEqual({10,132,7,92}, io_to_db(Item,OldTerm,ipaddr,0,Prec,Def,RW,<<"10.132.7.92">>)),
        ?assertEqual({0,0,0,0}, io_to_db(Item,OldTerm,ipaddr,4,undefined,Def,RW,<<"0.0.0.0">>)),
        ?assertEqual({255,255,255,255}, io_to_db(Item,OldTerm,ipaddr,undefined,Prec,Def,RW,<<"255.255.255.255">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,undefined,"1.2.3.4.5"}}}}, io_to_db(Item,OldTerm,ipaddr,0,0,Def,RW,<<"1.2.3.4.5">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,4,"1.2.-1.4"}}}}, io_to_db(Item,OldTerm,ipaddr,4,0,Def,RW,<<"1.2.-1.4">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,6,"1.256.1.4"}}}}, io_to_db(Item,OldTerm,ipaddr,6,0,Def,RW,<<"1.256.1.4">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,8,"1.2.1.4"}}}}, io_to_db(Item,OldTerm,ipaddr,8,0,Def,RW,<<"1.2.1.4">>)),
        ?LogDebug("io_to_db success 13~n", []),

        OldPid = self(),
        ?assertEqual(OldPid, io_to_db(Item,OldPid,pid,Len,Prec,Def,RW,pid_to_list(OldPid))),
        ?assertEqual(OldPid, io_to_db(Item,OldPid,pid,Len,Prec,Def,RW,list_to_binary(pid_to_list(OldPid)))),
        ?assertEqual(list_to_pid("<0.55.0>"), io_to_db(Item,OldPid,pid,Len,Prec,Def,RW,"<0.55.0>")),
        ?assertEqual(list_to_pid("<0.55.66>"), io_to_db(Item,OldPid,pid,Len,Prec,Def,RW,<<"<0.55.66>">>)),

        AdminId1 = io_to_db('Item','OldTerm',userid,undefined,undefined,undefined,RW,<<"1234">>),
        ?assert(is_integer(AdminId1)),
        AdminId2 = io_to_db('Item','OldTerm',userid,undefined,undefined,undefined,RW,<<"test_user">>),
        ?assert(is_atom(AdminId2)),
        % ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,8,"1.2.1.4"}}}}, io_to_db(Item,OldTerm,ipaddr,8,0,Def,RW,"1.2.1.4")),
        ?LogDebug("io_to_db success 12a~n", []),

        Fun = fun(X) -> X*X end,
        ?LogDebug("Fun ~p~n", [Fun]),
        Res = io_to_db(Item,OldTerm,'fun',1,Prec,Def,RW,<<"fun(X) -> X*X end">>),
        ?LogDebug("Run ~p~n", [Res]),
        ?assertEqual(Fun(4), Res(4)),
        ?LogDebug("io_to_db success 13~n", []),

        ?assertEqual(<<>>, binary_to_hex(<<>>)),
        ?assertEqual(<<"41">>, binary_to_hex(<<"A">>)),
        ?assertEqual(<<"4142434445464748">>, binary_to_hex(<<"ABCDEFGH">>)),
        ?LogDebug("binary_to_hex success~n", []),

        ?assertEqual(<<>>, io_to_binary(<<>>,0)),
        ?assertEqual(<<0:8>>, io_to_binary(<<"00">>,undefined)),
        ?assertEqual(<<1:8>>, io_to_binary(<<"01">>,1)),
        ?assertEqual(<<9:8>>, io_to_binary(<<"09">>,1)),
        ?assertEqual(<<10:8>>, io_to_binary(<<"0A">>,1)),
        ?assertEqual(<<15:8>>, io_to_binary(<<"0F">>,200)),
        ?assertEqual(<<255:8>>, io_to_binary(<<"FF">>,1)),
        ?assertEqual(<<1:8,1:8>>, io_to_binary(<<"0101">>,2)),
        ?assertException(throw, {'ClientError',{"Binary data is too long",{binary,1}}}, io_to_binary(<<"0101">>,1)),
        ?assertEqual(<<"ABCDEFGH">>, io_to_binary(<<"4142434445464748">>,undefined)),

        ?LogDebug("io_to_binary success~n", []),

        RF1 = select_rowfun_str([#bind{type=integer,tind=1,cind=2}], eu, undefined, undefined),
        ?assert(is_function(RF1)),
        ?assertEqual([<<"5">>],RF1({{dummy,5},{}})),
        ?LogDebug("rowfun success~n", []),

        ?assertEqual({{2000,1,29},{12,13,14}}, offset_datetime('+', {{2000,1,28},{12,13,14}}, 1.0)),
        ?assertEqual({{2000,1,27},{12,13,14}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0)),
        ?assertEqual({{2000,1,28},{12,13,14}}, offset_datetime('+', {{2000,1,28},{12,13,14}}, 1.0e-10)),
        ?assertEqual({{2000,1,28},{12,13,14}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0e-10)),
        ?assertEqual({{2000,1,28},{11,13,14}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0/24.0)),
        ?assertEqual({{2000,1,28},{12,12,14}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0/24.0/60.0)),
        ?assertEqual({{2000,1,28},{12,13,13}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0/24.0/3600.0)),

        ENow = erlang:now(),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('+', ENow, 1.0),-1.0)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0),1.0)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.1),0.1)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.01),0.01)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.001),0.001)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.0001),0.0001)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.00001),0.00001)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.000001),0.000001)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-6),1.0e-6)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-7),1.0e-7)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-8),1.0e-8)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-9),1.0e-9)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-10),1.0e-10)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-11),1.0e-11)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-12),1.0e-12)),

        ?LogDebug("ErlangNow: ~p~n", [ENow]),
        OneSec = 1.0/86400.0,
        ?LogDebug("Now-  1us: ~p~n", [offset_timestamp('-', ENow, 0.000001 * OneSec)]),
        ?LogDebug("Now- 10us: ~p~n", [offset_timestamp('-', ENow, 0.00001 * OneSec)]),
        ?LogDebug("Now-100us: ~p~n", [offset_timestamp('-', ENow, 0.0001 * OneSec)]),
        ?LogDebug("Now-  1ms: ~p~n", [offset_timestamp('-', ENow, 0.001 * OneSec)]),
        ?LogDebug("Now- 10ms: ~p~n", [offset_timestamp('-', ENow, 0.01 * OneSec)]),
        ?LogDebug("Now-100ms: ~p~n", [offset_timestamp('-', ENow, 0.1 * OneSec)]),
        ?LogDebug("Now-   1s: ~p~n", [offset_timestamp('-', ENow, OneSec)]),
        ?LogDebug("Now-  10s: ~p~n", [offset_timestamp('-', ENow, 10.0*OneSec)]),
        ?LogDebug("Now- 100s: ~p~n", [offset_timestamp('-', ENow, 100.0*OneSec)]),
        ?LogDebug("Now-1000s: ~p~n", [offset_timestamp('-', ENow, 1000.0*OneSec)]),

        ?assertEqual(true, is_term_or_fun_text(a)),
        ?assertEqual(true, is_term_or_fun_text([1,2,3])),
        ?assertEqual(true, is_term_or_fun_text("fun")),
        ?assertEqual(true, is_term_or_fun_text("fun()-> ok end.")),
        ?assertEqual(true, is_term_or_fun_text(<<"fun(X,Y)-> erlang:now() end.">>)),
        % TODO: Behavior inconsistant between erl_eval:exprs/2 and erl_eval:exprs/3,4
        %?assertEqual(false, is_term_or_fun_text(<<"fun()-> A end.">>)),
        ?assertEqual(true, is_term_or_fun_text("fun()-> A end")),
        ?assertEqual(true, is_term_or_fun_text("fun()- A end.")),

        ?assertEqual(true, true)
    catch
        Class:Reason ->
            timer:sleep(1000),
            ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
            throw ({Class, Reason})
    end,
    ok.

-endif.
