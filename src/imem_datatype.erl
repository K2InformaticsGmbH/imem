-module(imem_datatype).
-compile(inline).
-compile({inline_size,1000}).

-include_lib("eunit/include/eunit.hrl").

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(rawTypeStr,binary).
-define(emptyStr,<<>>).

-define(H(X), (hex(X)):16).

-define(BinaryMaxLen,250).  %% more represented by "..." suffix

-export([ raw_type/1
        , imem_type/1
        , is_datatype/1
        , is_unicode_binary/1 
        ]).

-export([ add_squotes/1
        , add_dquotes/1
        , strip_squotes/1           %% strip single quotes
        , strip_dquotes/1           %% strip double quotes
        , strip_quotes/1            %% strip both quotes in any order
        , binary_to_hex/1
        ]).

%   datatypes
%   internal    aliases (synonyms)
%   --------------------------------------------
%   atom
%   binary      raw(L), blob(L), rowid
%   binstr      clob(L), nclob(L)
%   boolean     bool
%   datetime    date
%   decimal     number(Len,Prec)
%   float
%   fun
%   integer     int
%   ipaddr
%   list
%   pid
%   ref
%   string      varchar2(L), nvarchar2(L), char(L), nchar(L)
%   term
%   timestamp
%   tuple
%   userid


-export([ io_to_db/8
        , io_to_binary/2
        , io_to_binstr/1
        , io_to_boolean/1
        , io_to_datetime/1
        , io_to_decimal/3
        , io_to_float/2
        , io_to_fun/2
        , io_to_integer/3
        , io_to_ipaddr/1        
        , io_to_ipaddr/2
        , io_to_list/2
        , io_to_string/1
        , io_to_term/1
        , io_to_timestamp/1
        , io_to_timestamp/2
        , io_to_tuple/2
        , io_to_userid/1
        ]).

-export([ db_to_io/6
        , atom_to_io/1
        , binary_to_io/1
        , binstr_to_io/1
        , boolean_to_io/1
        , datetime_to_io/1
        , datetime_to_io/2
        , decimal_to_io/2
        , float_to_io/3
        , integer_to_io/1
        , ipaddr_to_io/1
        , string_to_io/1
        , timestamp_to_io/1
        , timestamp_to_io/2
        , timestamp_to_io/3
        , userid_to_io/1
        , term_to_io/1
        ]).

-export([ map/3
        , name/1
        , item/2
        , item1/1
        , item2/1
        , item3/1
        , item4/1
        , item5/1
        , item6/1
        , item7/1
        , item8/1
        , item9/1
        , concat/1
        , concat/2
        , concat/3
        , concat/4
        , concat/5
        , concat/6
        , concat/7
        , concat/8
        , concat/9
        ]).


-export([ select_rowfun_raw/1   %% return rows in raw erlang db format
        , select_rowfun_str/4   %% convert all rows to string
        ]).

select_rowfun_raw(ColMap) ->
    fun(Recs) -> 
        select_rowfun_raw(Recs, ColMap, []) 
    end.

select_rowfun_raw(_Recs, [], Acc) ->
    lists:reverse(Acc);
select_rowfun_raw(Recs, [#ddColMap{tind=Ti,cind=Ci,func=undefined}|ColMap], Acc) ->
    Fld = case element(Ti,Recs) of
        undefined ->    undefined;
        Rec ->          element(Ci,Rec)
    end,
    select_rowfun_raw(Recs, ColMap, [Fld|Acc]);
select_rowfun_raw(Recs, [#ddColMap{tind=Ti,cind=Ci,func=F}|ColMap], Acc) ->
    Fld = case element(Ti,Recs) of
        undefined ->    
            undefined;
        Rec ->
            try
                apply(F,[element(Ci,Rec)])
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
select_rowfun_str(Recs, [#ddColMap{type=T,prec=P,tind=Ti,cind=Ci,func=undefined}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    Str = case element(Ti,Recs) of
        undefined ->
            ?emptyStr;
        Rec ->
            db_to_io(T, P, DateFmt, NumFmt, StrFmt, element(Ci,Rec))
    end,
    select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]);
select_rowfun_str(Recs, [#ddColMap{tind=Ti,cind=Ci,func=F}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    Str = case element(Ti,Recs) of
        undefined ->    
            ?emptyStr;
        Rec ->
            X = element(Ci,Rec),
            try
                case F of
                    name ->     name(X);
                    item1 ->    item1(X);
                    item2 ->    item2(X);
                    item3 ->    item3(X);
                    item4 ->    item4(X);
                    item5 ->    item5(X);
                    item6 ->    item6(X);
                    item7 ->    item7(X);
                    item8 ->    item8(X);
                    item9 ->    item9(X);
                    Name ->     ?UnimplementedException({"Unimplemented row function",Name})
                end
            catch
                _:Reason ->  ?SystemException({"Failed row function",{F,X,Reason}})
            end
    end,
    select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]).


%% ----- DATA TYPES    --------------------------------------------

is_unicode_binary(B) when is_binary(B) ->
    case unicode:characters_to_binary(B,utf8,utf8) of
        B ->    true;
        _ ->    false
    end;
is_unicode_binary(_) ->
    false.    

is_datatype([]) -> false;
is_datatype({}) -> false;
is_datatype(Type) when is_atom(Type) -> lists:member(Type,?DataTypes);
is_datatype(Types) when is_list(Types) ->
    (not lists:member(false,[is_datatype(T) || T <- Types]));
is_datatype(Type) when is_tuple(Type) -> is_datatype(tuple_to_list(Type));
is_datatype(_) -> false.

imem_type(raw) -> binary; 
imem_type(blob) -> binary; 
imem_type(rowid) -> rowid; 
imem_type(clob) -> binstr; 
imem_type(nclob) -> binstr; 
imem_type(bool) -> boolean; 
imem_type(date) -> datetime; 
imem_type(number) -> decimal; 
imem_type(int) -> integer; 
imem_type(varchar2) -> string; 
imem_type(nvarchar2) -> string; 
imem_type(char) -> string; 
imem_type(nchar) -> string; 
imem_type(Type) -> Type. 

raw_type(userid) -> integer;
raw_type(binstr) -> binary;
raw_type(string) -> ?rawTypeStr;
raw_type(decimal) -> integer;
raw_type(datetime) -> tuple;
raw_type(timestamp) -> tuple;
raw_type(ipaddr) -> tuple;
raw_type(boolean) -> atom;
raw_type(Type) -> Type.

%% ----- CAST Data to become compatible with DB  ------------------

io_to_db(_Item,Old,_Type,_Len,_Prec,_Def,true,_) -> Old;
io_to_db(_Item,Old,_Type,_Len,_Prec,_Def,_RO, Old) -> Old;
io_to_db(_Item,_Old,_Type,_Len,_Prec,Def,_RO, Def) -> Def;
io_to_db(Item,Old,Type,Len,Prec,Def,_RO,Val) when is_function(Def,0) ->
    io_to_db(Item,Old,Type,Len,Prec,Def(),_RO,Val);
io_to_db(Item,Old,Type,Len,Prec,Def,_RO,Val) when is_binary(Val);is_list(Val) ->
    try
        DefAsStr = strip_dquotes(io_to_binstr(io_lib:format("~tp", [Def]))),
        % ?Log("DefAsStr ~tp ~ts~n",[<<DefAsStr/binary,1>>,<<DefAsStr/binary,1>>]),
        OldAsStr = strip_dquotes(io_to_binstr(io_lib:format("~tp", [Old]))),
        % ?Log("OldAsStr ~tp ~ts~n",[<<OldAsStr/binary,1>>,<<OldAsStr/binary,1>>]),
        ValAsStr = io_to_binstr(io_lib:format("~ts", [Val])),
        % ?Log("ValAsStr ~tp ~ts~n",[<<ValAsStr/binary,1>>,<<ValAsStr/binary,1>>]),
        Squoted = strip_dquotes(io_to_binstr(Val)), %% may still be single quoted
        % ?Log("Squoted  ~tp ~ts~n",[<<Squoted/binary,1>>,<<Squoted/binary,1>>]),
        Unquoted = strip_squotes(Squoted),                      %% only explicit quotes remaining
        % ?Log("Unquoted ~tp ~ts~n",[<<Unquoted/binary,1>>,<<Unquoted/binary,1>>]),
        if 
            (DefAsStr == Unquoted) ->   Def;
            (DefAsStr == Squoted) ->    Def;
            (DefAsStr == ValAsStr) ->   Def;
            (OldAsStr == Unquoted) ->   Old;
            (OldAsStr == Squoted) ->    Old;
            (OldAsStr == ValAsStr) ->   Old;
            (Type == 'fun') ->          io_to_fun(Unquoted,Len);
            (Type == atom) ->           io_to_atom(Unquoted);
            (Type == binary) ->         io_to_binary(Unquoted,Len);
            (Type == binstr) ->         io_to_binstr(Unquoted,Len);
            (Type == boolean) ->        io_to_boolean(Unquoted);
            (Type == datetime) ->       io_to_datetime(Unquoted);
            (Type == decimal) ->        io_to_decimal(Unquoted,Len,Prec);
            (Type == float) ->          io_to_float(Unquoted,Prec);
            (Type == integer) ->        io_to_integer(Unquoted,Len,Prec);
            (Type == ipaddr) ->         io_to_ipaddr(Unquoted,Len);
            (Type == list) ->           io_to_list(Unquoted,Len);
            (Type == pid) ->            io_to_pid(Unquoted);
            (Type == ref) ->            Old;    %% cannot convert back
            (Type == string) ->         io_to_string(Unquoted,Len);
            (Type == term) ->
                case Squoted of
                    Unquoted ->         io_to_term(Unquoted);       %% not single quoted, use erlang term
                    %% ValAsStr ->         io_to_string(Unquoted);     %% single quoted only, unquote to string
                    ValAsStr ->         io_to_term(Squoted);     %% single quoted only, unquote to string
                    %% _ ->                io_to_term(ValAsStr)     %% double and single quoted, make an atom
                    _ ->                io_to_term(Squoted)         %% double and single quoted, make an atom
                end;        
            (Type == timestamp) ->      io_to_timestamp(Unquoted,Prec); 
            (Type == tuple) ->          io_to_tuple(Unquoted,Len);
            (Type == userid) ->         io_to_userid(Unquoted);
            true ->                     io_to_term(Unquoted)   
        end
    catch
        _:{'UnimplementedException',_} ->       ?ClientError({"Unimplemented data type conversion",{Item,{Type,Val}}});
        _:{'ClientError', {Text, Reason}} ->    ?ClientError({Text, {Item,Reason}});
        _:_ ->                                  ?ClientError({"Data conversion format error",{Item,{Type,Val}}})
    end.
% io_to_db(_Item,_Old,_Type,_Len,_Prec,_Def,_RO,Val) -> Val.    

add_squotes(<<>>) -> <<"''">>;
add_squotes(B) when is_binary(B) -> <<$',B/binary,$'>>;
add_squotes([]) -> "''";
add_squotes(String) when is_list(String) -> "'" ++ String ++ "'".

add_dquotes(<<>>) -> <<"\"\"">>;
add_dquotes(B) when is_binary(B) -> <<$",B/binary,$">>;
add_dquotes([]) -> "\"\"";
add_dquotes(String) when is_list(String) -> "\"" ++ String ++ "\"".

strip_dquotes(<<>>) -> <<>>;
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

strip_squotes(<<>>) -> <<>>;
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

strip_quotes(<<>>) -> <<>>;
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
    binary_to_atom(Val, utf8);
io_to_atom(Val) when is_list(Val) ->
    list_to_atom(Val).

io_to_pid(Val) when is_binary(Val) ->
    list_to_pid(binary_to_list(Val));
io_to_pid(Val) when is_list(Val) ->
    list_to_pid(Val).

io_to_integer(Val,Len,Prec) ->
    Value = case io_to_term(Val) of
        V when is_integer(V) -> V;
        V when is_float(V) ->   erlang:round(V);
        _ ->                    ?ClientError({"Data conversion format error",{integer,Len,Prec,Val}})
    end,
    Result = if 
        Len == 0 andalso Prec == 0 ->   Value;
        Prec <  0 ->                    erlang:round(erlang:round(math:pow(10, Prec) * Value) * math:pow(10,-Prec));
        true ->                         Value
    end,
    RLen = length(integer_to_list(Result)),
    if 
        Len == 0 ->     Result;
        RLen > Len ->   ?ClientError({"Data conversion format error",{integer,Len,Prec,Val}});
        true ->         Result
    end.

io_to_float(Val,Prec) ->
    Value = case io_to_term(Val) of
        V when is_float(V) ->   V;
        V when is_integer(V) -> float(V);
        _ ->                    ?ClientError({"Data conversion format error",{float,Prec,Val}})
    end,
    if 
        Prec == 0 ->    Value;
        true ->         erlang:round(math:pow(10, Prec) * Value) * math:pow(10,-Prec)
    end.

io_to_binary(<<>>,_Len) -> <<>>;
io_to_binary([],_Len) -> <<>>;
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
        (Len > 0) andalso (S > Len+Len) ->
            ?ClientError({"Binary data string is too long",{binary,Len}});
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
        (Len > 0) andalso (L > Len+Len) ->
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
io_to_userid("system") -> system;
io_to_userid(Name) when is_binary(Name) ->
    % ?Log("UserName: ~p~n", [Name]),
    MatchHead = #ddAccount{id='$1', name='$2', _='_'},
    Guard = {'==', '$2', Name},
    % ?Log("UserGuard: ~p~n", [Guard]),
    case imem_if:select(ddAccount, [{MatchHead, [Guard], ['$1']}]) of
        {[],true} ->    ?ClientError({"Account does not exist",Name});
        {[Id],true} ->  Id;
        Else ->         ?SystemException({"Account lookup error",{Name,Else}})
    end;
io_to_userid(Name) when is_list(Name) ->
    io_to_userid(list_to_binary(Name)).

io_to_timestamp(TS) ->
    io_to_timestamp(TS,6).

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
io_to_timestamp(Val,6) ->
    try 
        {Date,Time,Micro} = case re:run(lists:sublist(Val,5),"[\/\.\-]+",[{capture,all,list}]) of
            {match,["/"]} ->    
                case string:tokens(Val, " ") of
                    [D,T] ->  case re:split(T,"[$.]",[{return,list}]) of
                                        [Hms,M] -> 
                                            {parse_date_us(D),parse_time(Hms),parse_micro(M)};
                                        [Hms] ->
                                            {parse_date_us(D),parse_time(Hms),0.0}
                                    end;
                    [D] ->       {parse_date_us(D),{0,0,0},0.0}
                end;
            {match,["-"]} ->    
                case string:tokens(Val, " ") of
                    [D,T] ->  case re:split(T,"[$.]",[{return,list}]) of
                                        [Hms,M] -> 
                                            {parse_date_int(D),parse_time(Hms),parse_micro(M)};
                                        [Hms] ->
                                            {parse_date_int(D),parse_time(Hms),0.0}
                                    end;
                    [D] ->       {parse_date_int(D),{0,0,0},0.0}
                end;
            {match,["."]} ->    
                case string:tokens(Val, " ") of
                    [D,T] ->  case re:split(T,"[$.]",[{return,list}]) of
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
        _:_ ->  ?ClientError({"Data conversion format error",{timestamp,Val}})
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
io_to_datetime(Val) ->
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
        _ ->                    ?ClientError({})
    end.    

parse_date_us(Val) ->
    case string:tokens(Val, "/") of
        [Month,Day,Year] ->     validate_date({parse_year(Year),parse_month(Month),parse_day(Day)});
        _ ->                    ?ClientError({})
    end.    

parse_date_int(Val) ->
    case string:tokens(Val, "-") of
        [Year,Month,Day] ->     validate_date({parse_year(Year),parse_month(Month),parse_day(Day)});
        _ ->                    ?ClientError({})
    end.    

parse_date_raw(Val) ->
    case length(Val) of
        8 ->    validate_date({parse_year(lists:sublist(Val,1,4)),parse_month(lists:sublist(Val,5,2)),parse_day(lists:sublist(Val,7,2))});
        6 ->    Year2 = parse_year(lists:sublist(Val,1,2)),
                Year = if 
                   Year2 < 40 ->    2000+Year2;
                   true ->          1900+Year2
                end,     
                validate_date({Year,parse_month(lists:sublist(Val,3,2)),parse_day(lists:sublist(Val,5,2))});
        _ ->    ?ClientError({})
    end.    

parse_year(Val) ->
    case length(Val) of
        4 ->    list_to_integer(Val);
        2 ->    Year2 = parse_year(lists:sublist(Val,1,2)),
                if 
                   Year2 < 40 ->    2000+Year2;
                   true ->          1900+Year2
                end;   
        _ ->    ?ClientError({})
    end.    

parse_month(Val) ->
    case length(Val) of
        1 ->                    list_to_integer(Val);
        2 ->                    list_to_integer(Val);
        _ ->                    ?ClientError({})
    end.    

parse_day(Val) ->
    case length(Val) of
        1 ->                    list_to_integer(Val);
        2 ->                    list_to_integer(Val);
        _ ->                    ?ClientError({})
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
                _ ->    ?ClientError({})
            end
    end.    

parse_hour(Val) ->
    H = list_to_integer(Val),
    if
        H >= 0 andalso H < 25 ->    H;
        true ->                     ?ClientError({})
    end.

parse_minute(Val) ->
    M = list_to_integer(Val),
    if
        M >= 0 andalso M < 60 ->    M;
        true ->                     ?ClientError({})
    end.

parse_second(Val) ->
    S = list_to_integer(Val),
    if
        S >= 0 andalso S < 60 ->    S;
        true ->                     ?ClientError({})
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
        false ->    ?ClientError({})
    end.    

io_to_ipaddr(Val) ->
    io_to_ipaddr(Val,undefined).
    
io_to_ipaddr(Val,Len) when is_binary(Val) ->
    io_to_ipaddr(binary_to_list(Val),Len);
io_to_ipaddr(Val,undefined) -> 
    io_to_ipaddr(Val,0);
io_to_ipaddr([${|_]=Val,Len) ->
    case io_to_term(Val) of
        {A,B,C,D} when is_integer(A), is_integer(B), 
            is_integer(C), is_integer(D) -> 
                if 
                    Len==4 ->   {A,B,C,D};
                    Len==0 ->   {A,B,C,D};
                    true ->     ?ClientError({"Data conversion format error",{ipaddr,Len,Val}})
                end;
        {A,B,C,D,E,F,G,H} when 
            is_integer(A), is_integer(B), is_integer(C), is_integer(D), 
            is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
                if 
                    Len==6 ->   {A,B,C,D,E,F,G,H};
                    Len==8 ->   {A,B,C,D,E,F,G,H};
                    Len==0 ->   {A,B,C,D,E,F,G,H};
                    true ->     ?ClientError({"Data conversion format error",{ipaddr,Len,Val}})
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
        Len == 0 andalso RLen == 4 ->   Result;
        Len == 0 andalso RLen == 8 ->   Result;
        RLen == Len ->                  Result;
        true ->                         ?ClientError({"Data conversion format error",{ipaddr,Len,Val}})
    end.

io_to_decimal(Val,Len,Prec) when is_binary(Val) ->
    io_to_decimal(binary_to_list(Val),Len,Prec);
io_to_decimal(Val,Len,undefined) ->
    io_to_decimal(Val,Len,0);
io_to_decimal(Val,undefined,Prec) ->
    io_to_decimal(Val,0,Prec);
io_to_decimal(Val,Len,0) ->         %% use fixed point arithmetic with implicit scaling factor
    io_to_integer(Val,Len,0);  
io_to_decimal(Val,Len,Prec) when Prec > 0 -> 
    Result = erlang:round(math:pow(10, Prec) * io_to_float(Val,0)),
    RLen = length(integer_to_list(Result)),
    if 
        Len == 0 ->     Result;
        RLen > Len ->   ?ClientError({"Data conversion format error",{decimal,Len,Prec,Val}});
        true ->         Result
    end;
io_to_decimal(Val,Len,Prec) ->
    ?ClientError({"Data conversion format error",{decimal,Len,Prec,Val}}).

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

io_to_string(Bin,Len) ->
    List = case unicode:characters_to_list(Bin, utf8) of
      L when is_list(L) ->  L;                      %% Bin was utf8 encoded
      _ ->                  binary_to_list(Bin)     %% Bin is bytewise encoded
    end,
    if
        Len == undefined ->     List;
        Len == 0 ->             List;
        length(List) =< Len  -> List;
        true ->                 ?ClientError({"String is too long",{Bin,Len}})
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
        erl_value(io_to_string(Val))
    catch
        _:_ -> ?ClientError({})
    end.

erl_value(String) when is_list(String) -> 
    Code = case [lists:last(string:strip(String))] of
        "." -> String;
        _ -> String ++ "."
    end,
    {ok,ErlTokens,_}=erl_scan:string(Code),    
    {ok,ErlAbsForm}=erl_parse:parse_exprs(ErlTokens),    
    {value,Value,_}=erl_eval:exprs(ErlAbsForm,[]),    
    Value.

io_to_fun(Val,Len) ->
    Fun = erl_value(io_to_string(Val)),
    if
        Len == undefined ->     Fun; 
        is_function(Fun,Len) -> Fun;
        true ->                 
            ?ClientError({"Data conversion format error",{'fun',Len,Val}})
    end.


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
            true -> term_to_io(Val)   
        end
    catch
        _:_ -> io_lib:format("~tp",[Val]) 
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
    % ?Log("----MStr0 ~p~n", [MStr0]),
    % ?Log("----Prec ~p~n", [Prec]),
    MStr1 = case list_to_integer(lists:sublist(MStr0, Prec+1, 6-Prec)) of
        0 ->    [$.|lists:sublist(MStr0, Prec)];
        _ ->    [$.|MStr0]
    end,    
    list_to_binary(io_lib:format("~s~s",[datetime_to_io(calendar:now_to_local_time({Megas,Secs,0}),Fmt),MStr1]));
timestamp_to_io({Megas,Secs,0},_,Fmt) ->
    datetime_to_io(calendar:now_to_local_time({Megas,Secs,0}),Fmt);
timestamp_to_io({Megas,Secs,Micros},_,Fmt) ->
    timestamp_to_io({Megas,Secs,Micros},6,Fmt).

   

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


userid_to_io(Val) ->
    case imem_if:read(ddAccount,Val) of
        [] ->           ?ClientError({"Account does not exist",Val});
        [Account] ->    Name=element(3,Account),
                        if 
                            is_binary(Name) ->  binstr_to_io(Name);
                            is_atom(Name) ->    atom_to_io(Name);
                            true ->             list_to_binary(io_lib:format("~tp",[Name]))
                        end;
        Else ->         ?SystemException({"Account lookup error",{Val,Else}})
    end.

ipaddr_to_io(IpAddr) -> 
    list_to_binary(inet_parse:ntoa(IpAddr)).

float_to_io(Val,_Prec,_NumFmt) ->
    list_to_binary(float_to_list(Val)).                    %% ToDo: implement rounding to db precision

boolean_to_io(T) ->
    list_to_binary(io_lib:format("~p",[T])).

term_to_io(T) ->
    list_to_binary(io_lib:format("~w",[T])).
    
string_to_io(Val) when is_list(Val) ->
    IsString = io_lib:printable_unicode_list(Val),
    if 
        IsString ->     list_to_binary(io_lib:format("~ts",[Val]));   %% ToDo: handle escaping and quoting etc.
        true ->         list_to_binary(lists:flatten(io_lib:format("~tp",[Val])))
    end;                                    
string_to_io(Val) ->
    list_to_binary(lists:flatten(io_lib:format("~tp",[Val]))).

integer_to_io(Val) ->
    list_to_binary(integer_to_list(Val)).

%% ----- Helper Functions ---------------------------------------------

map(Val,From,To) ->
    if 
        Val == From ->  To;
        true ->         Val
    end.

name(T) when is_tuple(T) ->
    io_to_binstr(string:join([name(E) || E <- tuple_to_list(T)],"."));
name(N) when is_atom(N) -> atom_to_list(N);
name(N) when is_binary(N) -> binary_to_list(N);
name(N) when is_list(N) -> lists:flatten(N);
name(N) -> lists:flatten(io_lib:format("~tp",[N])).

item(I,T) when is_tuple(T) ->
    if 
        size(T) >= I ->
            io_to_binstr(name(element(I,T)));
        true ->
            <<>>        %% ?ClientError({"Tuple too short",{T,I}})
    end;
item(I,L) when is_list(L) ->
    if 
        length(L) >= I ->
            io_to_binstr(name(lists:nth(I,L)));
        true ->
            <<>>        %% ?ClientError({"List too short",{L,I}})
    end;
item(_,_) -> <<>>.      %% ?ClientError({"Tuple or list expected",T}).

item1(T) -> item(1,T).
item2(T) -> item(2,T).
item3(T) -> item(3,T).
item4(T) -> item(4,T).
item5(T) -> item(5,T).
item6(T) -> item(6,T).
item7(T) -> item(7,T).
item8(T) -> item(8,T).
item9(T) -> item(9,T).

concat(S1)-> concat_list([S1]).
concat(S1,S2)-> concat_list([S1,S2]).
concat(S1,S2,S3)-> concat_list([S1,S2,S3]).
concat(S1,S2,S3,S4)-> concat_list([S1,S2,S3,S4]).
concat(S1,S2,S3,S4,S5)-> concat_list([S1,S2,S3,S4,S5]).
concat(S1,S2,S3,S4,S5,S6)-> concat_list([S1,S2,S3,S4,S5,S6]).
concat(S1,S2,S3,S4,S5,S6,S7)-> concat_list([S1,S2,S3,S4,S5,S6,S7]).
concat(S1,S2,S3,S4,S5,S6,S7,S8)-> concat_list([S1,S2,S3,S4,S5,S6,S7,S8]).
concat(S1,S2,S3,S4,S5,S6,S7,S8,S9)-> concat_list([S1,S2,S3,S4,S5,S6,S7,S8,S9]).

concat_list(L) when is_list(L) ->
    io_to_binstr(string:join([name(I) || I <- L],[])).


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
              fun data_types/1
        ]}}.    


data_types(_) ->
    try 
        ClEr = 'ClientError',
        %% SyEx = 'SystemException',    %% difficult to test

        ?Log("----TEST--~p:test_data_types~n", [?MODULE]),

        ?assertEqual(<<"Imem.ddTable">>, name({'Imem',ddTable})),
        ?assertEqual(<<"Imem.ddäöü"/utf8>>, name({'Imem',<<"ddäöü">>})),
        ?assertEqual(<<"Imem">>, item1({'Imem',ddTable})),
        ?assertEqual(<<"Imem">>, item(1,{'Imem',ddTable})),
        ?assertEqual(<<"ddTable">>, item2({'Imem',ddTable})),
        ?assertEqual(<<"ddäöü"/utf8>>, item(2,{'Imem',<<"ddäöü">>})),
        ?Log("name success~n", []),
        ?assertEqual(<<"ABC">>, concat("A","B","C")),
        ?assertEqual(<<"aabbcc">>, concat(aa,bb,cc)),
        ?assertEqual(<<"123">>, concat(1,2,3)),
        ?assertEqual(<<"1.a.A">>, concat(1,<<".">>,a,".",<<"A">>)),
        ?Log("concat success~n", []),

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
        ?Log("decimal_to_io success~n", []),

        ?assertEqual(<<"0.0.0.0">>, ipaddr_to_io({0,0,0,0})),
        ?assertEqual(<<"1.2.3.4">>, ipaddr_to_io({1,2,3,4})),
        ?Log("ipaddr_to_io success~n", []),

        ?assertEqual(<<"01.01.1970 01:00:00.123456">>, timestamp_to_io({0,0,123456},0)),  %% with DLS offset wintertime CH
        ?assertEqual(<<"01.01.1970 01:00:00">>, timestamp_to_io({0,0,0},0)),  %% with DLS offset wintertime CH
        ?assertEqual(<<"01.01.1970 01:00:00.123">>, timestamp_to_io({0,0,123000},3)),  %% with DLS offset wintertime CH
        ?assertEqual(<<"12.01.1970 14:46:42.123456">>, timestamp_to_io({1,2,123456},6)),  %% with DLS offset wintertime CH
        ?assertEqual(<<"{1,2,1234}">>, timestamp_to_io({1,2,1234},3,erlang)),  %% with DLS offset wintertime CH
        ?assertEqual(<<"000001000002001234">>, timestamp_to_io({1,2,1234},3,raw)),  %% with DLS offset wintertime CH
        ?Log("timestamp_to_io success~n", []),
        ?assertEqual({0,0,0}, io_to_timestamp(<<"01.01.1970 01:00:00.000000">>,0)),  %% with DLS offset wintertime CH
        ?assertEqual({1,2,123456}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,6)),  %% with DLS offset wintertime CH
        ?assertEqual({1,2,123000}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,3)),  %% with DLS offset wintertime CH
        ?assertEqual({1,2,0}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,0)),  %% with DLS offset wintertime CH
        ?assertEqual({1,3,0}, io_to_timestamp(<<"12.01.1970 14:46:42.654321">>,0)),  %% with DLS offset wintertime CH
        ?assertEqual({1,2,123000}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,3)),  %% with DLS offset wintertime CH
        ?assertEqual({1,2,100000}, io_to_timestamp(<<"12.01.1970 14:46:42.123456">>,1)),  %% with DLS offset wintertime CH
        ?assertEqual({1,2,12345}, io_to_timestamp(<<"{1,2,12345}">>,0)),  %% with DLS offset wintertime CH
        ?Log("io_to_timestamp success~n", []),

        LocalTime = erlang:localtime(),
        {Date,_Time} = LocalTime,
        ?assertEqual({{2004,3,1},{0,0,0}}, io_to_datetime(<<"1.3.2004">>)),
        ?assertEqual({{2004,3,1},{3,45,0}}, io_to_datetime(<<"1.3.2004 3:45">>)),
        ?assertEqual({{2004,3,1},{3,45,0}}, io_to_datetime(<<"{{2004,3,1},{3,45,0}}">>)),
        ?assertEqual({Date,{0,0,0}}, io_to_datetime(<<"today">>)),
        ?assertEqual(LocalTime, io_to_datetime(<<"sysdate">>)),
        ?assertEqual(LocalTime, io_to_datetime(<<"systime">>)),
        ?assertEqual(LocalTime, io_to_datetime(<<"now">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"18.8.1888 1:23:59">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"1888-08-18 1:23:59">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"8/18/1888 1:23:59">>)),
        ?assertEqual({{1888,8,18},{1,23,0}}, io_to_datetime(<<"8/18/1888 1:23">>)),
        ?assertEqual({{1888,8,18},{1,0,0}}, io_to_datetime(<<"8/18/1888 01">>)),
        ?assertException(throw,{ClEr,{"Data conversion format error",{datetime,"8/18/1888 1"}}}, io_to_datetime(<<"8/18/1888 1">>)),
        ?assertException(throw,{ClEr,{"Data conversion format error",{datetime,"8/18/1888 1"}}}, io_to_datetime(<<"8/18/1888 1">>)),
        ?assertEqual({{1888,8,18},{0,0,0}}, io_to_datetime(<<"8/18/1888 ">>)),
        ?assertEqual({{1888,8,18},{0,0,0}}, io_to_datetime(<<"8/18/1888">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"18880818012359">>)),
        ?assertEqual({{1888,8,18},{1,23,59}}, io_to_datetime(<<"18880818 012359">>)),
        ?Log("io_to_datetime success~n", []),

        ?assertEqual({1,23,59}, parse_time("01:23:59")),        
        ?assertEqual({1,23,59}, parse_time("1:23:59")),        
        ?assertEqual({1,23,0}, parse_time("01:23")),        
        ?assertEqual({1,23,59}, parse_time("012359")),        
        ?assertEqual({1,23,0}, parse_time("0123")),        
        ?assertEqual({1,0,0}, parse_time("01")),        
        ?assertEqual({0,0,0}, parse_time("")),        
        ?Log("parse_time success~n", []),

        ?assertEqual({1,2,3,4},io_to_ipaddr(<<"1.2.3.4">>,0)),
        ?assertEqual({1,2,3,4},io_to_ipaddr(<<"{1,2,3,4}">>,0)),
        ?assertEqual({1,2,3,4},io_to_ipaddr(<<"1.2.3.4">>,0)),
        ?assertEqual({1,2,3,4},io_to_ipaddr(<<"{1,2,3,4}">>,0)),
        ?assertEqual({1,2,3,4},io_to_db(0,"",ipaddr,0,0,undefined,false,<<"1.2.3.4">>)),
        ?assertEqual({1,2,3,4},io_to_db(0,"",ipaddr,0,0,undefined,false,<<"'1.2.3.4'">>)),
        ?assertEqual({1,2,3,4},io_to_db(0,"",ipaddr,0,0,undefined,false,<<"1.2.3.4">>)),
        ?assertEqual({1,2,3,4},io_to_db(0,"",ipaddr,0,0,undefined,false,<<"'1.2.3.4'">>)),

        Item = 0,
        OldString = io_to_string(<<"OldäString">>),
        StringType = string,
        Len = 3,
        Prec = 1,
        Def = default,
        RO = true,
        RW = false,
        DefFun = fun() -> [{},{}] end,
        ?assertEqual(OldString, io_to_db(Item,OldString,StringType,Len,Prec,Def,RO,<<"NewVal">>)),
        ?Log("io_to_db success 1~n", []),
        ?assertEqual(io_to_string(<<"NöVal">>), io_to_db(Item,OldString,StringType,6,Prec,Def,RW,<<"NöVal">>)),
        ?assertEqual([], io_to_db(Item,OldString,StringType,Len,Prec,Def,RW,<<>>)),
        ?assertEqual("{}", io_to_db(Item,OldString,StringType,Len,Prec,Def,RW,<<"{}">>)),
        ?assertEqual("[atom,atom]", io_to_db(Item,OldString,StringType,30,Prec,Def,RW,<<"[atom,atom]">>)),
        ?assertEqual("12", io_to_db(Item,OldString,StringType,Len,Prec,Def,RW,<<"12">>)),
        ?assertEqual("-3.14", io_to_db(Item,OldString,StringType,5,Prec,Def,RW,<<"-3.14">>)),
        ?Log("io_to_db success 2~n", []),

        ?assertEqual(OldString, io_to_db(Item,OldString,StringType,Len,Prec,Def,RW,<<"OldäString">>)),
        ?assertException(throw,{ClEr,{"String is too long",{0,{<<"NewVal">>,3}}}}, io_to_db(Item,OldString,StringType,Len,Prec,Def,RW,<<"NewVal">>)),
        ?assertEqual("NewVal", io_to_db(Item,OldString,StringType,6,Prec,Def,RW,<<"NewVal">>)),
        ?assertEqual("[NewVal]", io_to_db(Item,OldString,StringType,8,Prec,Def,RW,<<"[NewVal]">>)),
        ?assertEqual(default, io_to_db(Item,OldString,StringType,Len,Prec,Def,RW,<<"default">>)),
        ?assertEqual(default, io_to_db(Item,OldString,StringType,Len,Prec,Def,RW,<<"default">>)),
        ?Log("io_to_db success 3~n", []),

        ?assertEqual([{},{}], io_to_db(Item,OldString,StringType,Len,Prec,DefFun,RW,<<"[{},{}]">>)),
        ?assertEqual(oldValue, io_to_db(Item,oldValue,StringType,Len,Prec,Def,RW,<<"oldValue">>)),
        ?assertEqual('OldValue', io_to_db(Item,'OldValue',StringType,Len,Prec,Def,RW,<<"'OldValue'">>)),
        ?assertEqual(-15, io_to_db(Item,-15,StringType,Len,Prec,Def,RW,<<"-15">>)),
        ?Log("io_to_db success 3a~n", []),

        BinStrType = binstr,
        ?assertEqual(OldString, io_to_db(Item,OldString,BinStrType,Len,Prec,Def,RO,<<"NewVal">>)),
        ?Log("io_to_db success 4a~n", []),
        ?assertEqual(<<"NöVal">>, io_to_db(Item,OldString,BinStrType,6,Prec,Def,RW,<<"NöVal">>)),
        ?assertEqual(<<>>, io_to_db(Item,OldString,BinStrType,Len,Prec,Def,RW,<<>>)),
        ?assertEqual(<<"{}">>, io_to_db(Item,OldString,BinStrType,Len,Prec,Def,RW,<<"{}">>)),
        ?assertEqual(<<"[atom,atom]">>, io_to_db(Item,OldString,BinStrType,30,Prec,Def,RW,<<"[atom,atom]">>)),
        ?assertEqual(<<"12">>, io_to_db(Item,OldString,BinStrType,Len,Prec,Def,RW,<<"12">>)),
        ?assertEqual(<<"-3.14">>, io_to_db(Item,OldString,BinStrType,5,Prec,Def,RW,<<"-3.14">>)),
        ?Log("io_to_db success 4b~n", []),

        ?assertEqual(<<"OldäString">>, io_to_db(Item,<<"OldäString">>,BinStrType,Len,Prec,Def,RW,<<"OldäString">>)),
        ?assertException(throw,{ClEr,{"String is too long",{0,{<<"NewVal">>,3}}}}, io_to_db(Item,<<"OldäString">>,BinStrType,Len,Prec,Def,RW,<<"NewVal">>)),
        ?assertEqual(<<"NewVal">>, io_to_db(Item,OldString,BinStrType,6,Prec,Def,RW,<<"NewVal">>)),
        ?assertEqual(<<"[NewVal]">>, io_to_db(Item,OldString,BinStrType,8,Prec,Def,RW,<<"[NewVal]">>)),
        ?assertEqual(default, io_to_db(Item,OldString,BinStrType,Len,Prec,Def,RW,<<"default">>)),
        ?assertEqual(default, io_to_db(Item,OldString,BinStrType,Len,Prec,Def,RW,<<"default">>)),
        ?Log("io_to_db success 4c~n", []),


        IntegerType = integer,
        OldInteger = 17,
        ?assertEqual(OldString, io_to_db(Item,OldString,IntegerType,Len,Prec,Def,RW,<<"OldäString">>)),
        ?assertEqual(OldInteger, io_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,<<"17">>)),
        ?assertEqual(default, io_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,<<"default">>)),
        ?assertEqual(18, io_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,<<"18">>)),
        ?assertEqual(-18, io_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,<<"-18">>)),
        ?assertEqual(100, io_to_db(Item,OldInteger,IntegerType,Len,-2,Def,RW,<<"149">>)),
        ?assertEqual(200, io_to_db(Item,OldInteger,IntegerType,Len,-2,Def,RW,<<"150">>)),
        ?assertEqual(-100, io_to_db(Item,OldInteger,IntegerType,4,-2,Def,RW,<<"-149">>)),
        ?assertEqual(-200, io_to_db(Item,OldInteger,IntegerType,4,-2,Def,RW,<<"-150">>)),
        ?assertEqual(-200, io_to_db(Item,OldInteger,IntegerType,100,0,Def,RW,<<"300-500">>)),
        ?assertEqual(12, io_to_db(Item,OldInteger,IntegerType,20,0,Def,RW,<<"120/10.0">>)),
        ?assertEqual(12, io_to_db(Item,OldInteger,IntegerType,20,0,Def,RW,<<"120/10.0001">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,<<"1234">>}}}}, io_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,<<"1234">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,<<"-">>}}}}, io_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,<<"-">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,<<>>}}}}, io_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,<<>>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,<<"-100">>}}}}, io_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,<<"-100">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,<<"9999">>}}}}, io_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,<<"9999">>)),

        ?Log("io_to_db success 5~n", []),

        FloatType = float,
        OldFloat = -1.2,
        ?assertEqual(8.1, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,<<"8.1">>)),
        ?assertEqual(18.0, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,<<"18">>)),
        ?assertEqual(1.1, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,<<"1.12">>)),
        ?assertEqual(-1.1, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,<<"-1.14">>)),
        ?assertEqual(-1.1, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,<<"-1.1234567">>)),
        ?assertEqual(-1.12, io_to_db(Item,OldFloat,FloatType,Len,2,Def,RW,<<"-1.1234567">>)),
        ?assertEqual(-1.123, io_to_db(Item,OldFloat,FloatType,Len,3,Def,RW,<<"-1.1234567">>)),
        ?assertEqual(-1.1235, io_to_db(Item,OldFloat,FloatType,Len,4,Def,RW,<<"-1.1234567">>)),
        ?Log("io_to_db success 6~n", []),
        %% ?assertEqual(-1.12346, io_to_db(Item,OldFloat,FloatType,Len,5,Def,RW,"-1.1234567")),  %% fails due to single precision math
        %% ?assertEqual(-1.123457, io_to_db(Item,OldFloat,FloatType,Len,6,Def,RW,"-1.1234567")), %% fails due to single precision math
        ?assertEqual(100.0, io_to_db(Item,OldFloat,FloatType,Len,-2,Def,RW,<<"149">>)),
        ?assertEqual(-100.0, io_to_db(Item,OldFloat,FloatType,Len,-2,Def,RW,<<"-149">>)),
        ?assertEqual(-200.0, io_to_db(Item,OldFloat,FloatType,Len,-2,Def,RW,<<"-150">>)),
        %% ?assertEqual(0.6, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.56")),
        %% ?assertEqual(0.6, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.5678")),
        %% ?assertEqual(0.6, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.5678910111")),
        %% ?assertEqual(0.6, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.56789101112131415")),
        ?assertEqual(1234.5, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,<<"1234.5">>)),
        %% ?assertEqual(1234.6, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.56")),
        %% ?assertEqual(1234.6, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.5678")),
        %% ?assertEqual(1234.6, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.5678910111")),
        %% ?assertEqual(1234.6, io_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.56789101112131415")),
        ?Log("io_to_db success 7~n", []),

        DecimalType = decimal,
        OldDecimal = -123,
        ?assertEqual(81, io_to_db(Item,OldDecimal,DecimalType,Len,Prec,Def,RW,<<"8.1">>)),
        ?assertEqual(180, io_to_db(Item,OldDecimal,DecimalType,Len,Prec,Def,RW,<<"18.001">>)),
        ?assertEqual(1, io_to_db(Item,OldDecimal,DecimalType,1,0,Def,RW,<<"1.12">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,5,"-1.123"}}}}, io_to_db(Item,OldDecimal,DecimalType,5,5,Def,RW,<<"-1.123">>)),
        ?assertEqual(-112300, io_to_db(Item,OldDecimal,DecimalType,7,5,Def,RW,<<"-1.123">>)),
        ?assertEqual(-112346, io_to_db(Item,OldDecimal,DecimalType,7,5,Def,RW,<<"-1.1234567">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,Prec,"1234567.89"}}}}, io_to_db(Item,OldDecimal,DecimalType,5,Prec,Def,RW,<<"1234567.89">>)),
        ?Log("io_to_db success 8~n", []),

        TermType = term,
        OldTerm = {-1.2,[a,b,c]},
        ?assertEqual(OldTerm, io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"{-1.2,[a,b,c]}">>)),
        ?assertEqual(default, io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"default">>)),
        ?assertEqual(default, io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"\"default\"">>)),
        ?assertEqual(default, io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"\"'default'\"">>)),
    %    ?assertEqual("default", io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"'default'">>)),
        ?assertEqual([a,b], io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"[a,b]">>)),
        ?assertEqual(-1.1234567, io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"-1.1234567">>)),
        ?assertEqual('-1.1234567', io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"\"'-1.1234567'\"">>)),
        ?assertEqual('-1.1234567', io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"'-1.1234567'">>)),
        ?assertEqual(-1.1234567, io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"\"-1.1234567\"">>)),
        ?assertEqual({[1,2,3]}, io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"{[1,2,3]}">>)),
        ?assertEqual({[1,2,3]}, io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"\"{[1,2,3]}\"">>)),
        ?assertEqual({1,2,3}, io_to_db(Item,?nav,term,0,0,?nav,false,<<"{1,2,3}">>)),
        ?assertEqual({1,2,3}, io_to_db(Item,?nav,term,0,0,?nav,false,<<"\"{1,2,3}\"">>)),
        ?assertEqual([1,2,3], io_to_db(Item,?nav,term,0,0,?nav,false,<<"[1,2,3]">>)),
        ?assertEqual([1,2,3], io_to_db(Item,?nav,term,0,0,?nav,false,<<"\"[1,2,3]\"">>)),
        ?assertEqual('$_', io_to_db(Item,?nav,term,0,0,?nav,false,<<"'$_'">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{term,<<"[a|]">>}}}}, io_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,<<"[a|]">>)),
        ?Log("io_to_db success 9~n", []),

        ?assertEqual(true, io_to_db(Item,OldTerm,boolean,Len,Prec,Def,RW,<<"true">>)),
        ?assertEqual(false, io_to_db(Item,OldTerm,boolean,Len,Prec,Def,RW,<<"\"false\"">>)),
        ?assertEqual(false, io_to_db(Item,OldTerm,boolean,Len,Prec,Def,RW,<<"\"'false'\"">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{boolean,<<"something">>}}}}, io_to_db(Item,OldTerm,boolean,Len,Prec,Def,RW,<<"something">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{boolean,<<"TRUE">>}}}}, io_to_db(Item,OldTerm,boolean,Len,Prec,Def,RW,<<"TRUE">>)),
        ?Log("io_to_db success 10~n", []),

        ?assertEqual({1,2,3}, io_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,<<"{1,2,3}">>)),
        ?assertEqual({}, io_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,<<"{}">>)),
        ?assertEqual({1}, io_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,<<"{1}">>)),
        ?assertEqual({1,2}, io_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,<<"{1,2}">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{tuple,Len,<<"[a]">>}}}}, io_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,<<"[a]">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{tuple,Len,<<"{a}">>}}}}, io_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,<<"{a}">>)),
        ?Log("io_to_db success 11~n", []),

        ?assertEqual([a,b,c], io_to_db(Item,OldTerm,elist,Len,Prec,Def,RW,<<"[a,b,c]">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{list,Len,<<"[a]">>}}}}, io_to_db(Item,OldTerm,list,Len,Prec,Def,RW,<<"[a]">>)),
        ?assertEqual([], io_to_db(Item,[],list,Len,Prec,Def,RW,<<"[]">>)),
        ?assertEqual([], io_to_db(Item,OldTerm,list,Len,Prec,[],RW,<<"[]">>)),
        ?assertEqual([], io_to_db(Item,OldTerm,list,0,Prec,Def,RW,<<"[]">>)),
        ?assertEqual([a], io_to_db(Item,OldTerm,list,0,Prec,Def,RW,<<"[a]">>)),
        ?assertEqual([a,b], io_to_db(Item,OldTerm,list,0,Prec,Def,RW,<<"[a,b]">>)),
        ?Log("io_to_db success 12~n", []),

        ?assertEqual({10,132,7,92}, io_to_db(Item,OldTerm,ipaddr,0,Prec,Def,RW,<<"10.132.7.92">>)),
        ?assertEqual({0,0,0,0}, io_to_db(Item,OldTerm,ipaddr,4,Prec,Def,RW,<<"0.0.0.0">>)),
        ?assertEqual({255,255,255,255}, io_to_db(Item,OldTerm,ipaddr,4,Prec,Def,RW,<<"255.255.255.255">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,0,"1.2.3.4.5"}}}}, io_to_db(Item,OldTerm,ipaddr,0,0,Def,RW,<<"1.2.3.4.5">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,4,"1.2.-1.4"}}}}, io_to_db(Item,OldTerm,ipaddr,4,0,Def,RW,<<"1.2.-1.4">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,6,"1.256.1.4"}}}}, io_to_db(Item,OldTerm,ipaddr,6,0,Def,RW,<<"1.256.1.4">>)),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,8,"1.2.1.4"}}}}, io_to_db(Item,OldTerm,ipaddr,8,0,Def,RW,<<"1.2.1.4">>)),
        ?Log("io_to_db success 13~n", []),

        AdminId = io_to_db('Item','OldTerm',userid,0,0,0,RW,<<"admin">>),
        ?assert(is_integer(AdminId)),
        % ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,8,"1.2.1.4"}}}}, io_to_db(Item,OldTerm,ipaddr,8,0,Def,RW,"1.2.1.4")),
        ?Log("Admin Id: ~p~n", [AdminId]),
        ?Log("io_to_db success 12a~n", []),

        Fun = fun(X) -> X*X end,
        ?Log("Fun ~p~n", [Fun]),
        Res = io_to_db(Item,OldTerm,'fun',1,Prec,Def,RW,<<"fun(X) -> X*X end">>),
        ?Log("Run ~p~n", [Res]),
        ?assertEqual(Fun(4), Res(4)),
        ?Log("io_to_db success 13~n", []),

        ?assertEqual(<<>>, binary_to_hex(<<>>)),
        ?assertEqual(<<"41">>, binary_to_hex(<<"A">>)),
        ?assertEqual(<<"4142434445464748">>, binary_to_hex(<<"ABCDEFGH">>)),
        ?Log("binary_to_hex success~n", []),

        ?assertEqual(<<>>, io_to_binary(<<>>,0)),
        ?assertEqual(<<0:8>>, io_to_binary(<<"00">>,0)),
        ?assertEqual(<<1:8>>, io_to_binary(<<"01">>,0)),
        ?assertEqual(<<9:8>>, io_to_binary(<<"09">>,0)),
        ?assertEqual(<<10:8>>, io_to_binary(<<"0A">>,0)),
        ?assertEqual(<<15:8>>, io_to_binary(<<"0F">>,0)),
        ?assertEqual(<<255:8>>, io_to_binary(<<"FF">>,0)),
        ?assertEqual(<<1:8,1:8>>, io_to_binary(<<"0101">>,0)),
        ?assertEqual(<<"ABCDEFGH">>, io_to_binary(<<"4142434445464748">>,0)),

        ?Log("io_to_binary success~n", []),

        RF1 = select_rowfun_str([#ddColMap{type=integer,tind=1,cind=2}], eu, undefined, undefined),
        ?assert(is_function(RF1)), 
        ?assertEqual([<<"5">>],RF1({{dummy,5},{}})), 
        ?Log("rowfun success~n", []),

        ?assertEqual(true, true)
    catch
        Class:Reason ->  ?Log("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
