-module(imem_datatype).

-include_lib("eunit/include/eunit.hrl").

-include("imem_meta.hrl").
-include("imem_sql.hrl").

-define(H(X), (hex(X)):16).

-define(BinaryMaxLen,250).  %% more represented by "..." suffix

-export([ value_to_db/8
        , db_to_string/6
        , db_to_gui/6
        , strip_squotes/1           %% strip single quotes
        , strip_dquotes/1           %% strip double quotes
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


-export([ raw_type/1
        , imem_type/1
        , string_to_binary/2
        , string_to_datetime/1
        , string_to_decimal/3
        , string_to_float/2
        , string_to_fun/2
        , string_to_integer/3
        , string_to_ipaddr/2
        , string_to_list/2
        , string_to_term/1
        , string_to_timestamp/1
        , string_to_timestamp/2
        , string_to_tuple/2
        , string_to_userid/1
        ]).

-export([ atom_to_string/1
        , binary_to_string/1
        , binstr_to_string/2
        , binary_to_hex/1
        , datetime_to_string/1
        , datetime_to_string/2
        , decimal_to_string/2
        , float_to_string/3
        , integer_to_string/2
        , ipaddr_to_string/1
        , string_to_string/2
        , timestamp_to_string/1
        , timestamp_to_string/2
        , timestamp_to_string/3
        , userid_to_string/1
        ]).

-export([ map/3
        , name/1
        , name/2
        , name1/1
        , name2/1
        , name3/1
        , name4/1
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
        , select_rowfun_gui/4   %% convert rows to string when necessary (where io_lib(~p) will not do)
        ]).

select_rowfun_raw(ColMap) ->
    fun(Recs) -> 
        select_rowfun_raw(Recs, ColMap, []) 
    end.

select_rowfun_raw(_Recs, [], Acc) ->
    lists:reverse(Acc);
select_rowfun_raw(Recs, [#ddColMap{tind=Ti,cind=Ci,func=undefined}|ColMap], Acc) ->
    select_rowfun_raw(Recs, ColMap, [element(Ci,element(Ti,Recs))|Acc]);
select_rowfun_raw(Recs, [#ddColMap{tind=Ti,cind=Ci,func=F}|ColMap], Acc) ->
    Str = try
        apply(F,[element(Ci,element(Ti,Recs))])
    catch
        _:Reason ->  ?UnimplementedException({"Failed row function",{F,Reason}})
    end,    
    select_rowfun_raw(Recs, ColMap, [Str|Acc]).


select_rowfun_str(ColMap, DateFmt, NumFmt, StrFmt) ->
    fun(Recs) -> 
        select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, []) 
    end.

select_rowfun_str(_Recs, [], _DateFmt, _NumFmt, _StrFmt, Acc) ->
    lists:reverse(Acc);
select_rowfun_str(Recs, [#ddColMap{type=T,prec=P,tind=Ti,cind=Ci,func=undefined}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    Str = db_to_string(T, P, DateFmt, NumFmt, StrFmt, element(Ci,element(Ti,Recs))),
    select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]);
select_rowfun_str(Recs, [#ddColMap{tind=Ti,cind=Ci,func=F}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    X = element(Ci,element(Ti,Recs)),
    Str = try
        case F of
            name ->     name(X);
            name1 ->    name1(X);
            name2 ->    name2(X);
            name3 ->    name3(X);
            name4 ->    name4(X);
            Name ->     ?UnimplementedException({"Unimplemented row function",Name})
        end
    catch
        _:Reason ->  ?SystemException({"Failed row function",{F,X,Reason}})
    end,    
    select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]).

select_rowfun_gui(ColMap, DateFmt, NumFmt, StrFmt) ->
    fun(Recs) -> 
        select_rowfun_gui(Recs, ColMap, DateFmt, NumFmt, StrFmt, []) 
    end.

select_rowfun_gui(_Recs, [], _DateFmt, _NumFmt, _StrFmt, Acc) ->
    lists:reverse(Acc);
select_rowfun_gui(Recs, [#ddColMap{type=T,prec=P,tind=Ti,cind=Ci,func=undefined}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    Str = db_to_gui(T, P, DateFmt, NumFmt, StrFmt, element(Ci,element(Ti,Recs))),
    select_rowfun_gui(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]);
select_rowfun_gui(Recs, [#ddColMap{tind=Ti,cind=Ci,func=F}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    X = element(Ci,element(Ti,Recs)),
    Str = try
        case F of
            name ->     name(X);
            name1 ->    name1(X);
            name2 ->    name2(X);
            name3 ->    name3(X);
            name4 ->    name4(X);
            Name ->     ?UnimplementedException({"Unimplemented row function",Name})
        end
    catch
        _:Reason ->  ?SystemException({"Failed row function",{F,X,Reason}})
    end,    
    select_rowfun_gui(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]).


%% ----- DATA TYPES    --------------------------------------------

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
raw_type(string) -> list;
raw_type(decimal) -> integer;
raw_type(datetime) -> tuple;
raw_type(timestamp) -> tuple;
raw_type(ipaddr) -> tuple;
raw_type(boolean) -> atom;
raw_type(Type) -> Type.

%% ----- CAST Data to become compatible with DB  ------------------

value_to_db(_Item,Old,_Type,_Len,_Prec,_Def,true,_) -> Old;
value_to_db(_Item,Old,_Type,_Len,_Prec,_Def,_RO, Old) -> Old;
value_to_db(_Item,_Old,_Type,_Len,_Prec,Def,_RO, Def) -> Def;
value_to_db(Item,Old,Type,Len,Prec,Def,_RO,Val) when is_function(Def,0) ->
    value_to_db(Item,Old,Type,Len,Prec,Def(),_RO,Val);
value_to_db(Item,Old,Type,Len,Prec,Def,_RO,Val) when is_list(Val) ->
    try
        IsString = io_lib:printable_unicode_list(Val),
        if 
            IsString ->
                DefAsStr = lists:flatten(io_lib:format("~p", [Def])),
                OldAsStr = lists:flatten(io_lib:format("~p", [Old])),
                Squoted = strip_dquotes(Val),       %% may still be single quoted
                Unquoted = strip_squotes(Squoted),  %% only explicit quotes remaining
                if 
                    (DefAsStr == Unquoted) ->   Def;
                    (DefAsStr == Squoted) ->    Def;
                    (DefAsStr == Val) ->        Def;
                    (OldAsStr == Unquoted) ->   Old;
                    (OldAsStr == Squoted) ->    Old;
                    (OldAsStr == Val) ->        Old;
                    (Type == 'fun') ->          string_to_fun(Unquoted,Len);
                    (Type == atom) ->           list_to_atom(Unquoted);
                    (Type == binary) ->         string_to_binary(Unquoted,Len);
                    (Type == binstr) ->         list_to_binary(Unquoted);
                    (Type == boolean) ->        string_to_term(Unquoted);
                    (Type == datetime) ->       string_to_datetime(Unquoted);
                    (Type == decimal) ->        string_to_decimal(Val,Len,Prec);
                    (Type == float) ->          string_to_float(Val,Prec);
                    (Type == integer) ->        string_to_integer(Val,Len,Prec);
                    (Type == ipaddr) ->         string_to_ipaddr(Unquoted,Len);
                    (Type == list) ->           string_to_list(Unquoted,Len);
                    (Type == pid) ->            list_to_pid(Unquoted);
                    (Type == ref) ->            Old;    %% cannot convert back
                    (Type == string) ->         string_to_limited_string(Unquoted,Len);
                    (Type == term) ->
                        case Squoted of
                            Unquoted ->         string_to_term(Unquoted);   %% not single quoted, use erlang term
                            Val ->              Unquoted;                   %% single quoted only, unquote to string
                            _ ->                string_to_term(Squoted)     %% double and single quoted, make an atom
                        end;        
                    (Type == timestamp) ->      string_to_timestamp(Unquoted,Prec); 
                    (Type == tuple) ->          string_to_tuple(Unquoted,Len);
                    (Type == userid) ->         string_to_userid(Unquoted);
                    true ->                     string_to_term(Unquoted)   
                end;
            true -> Val
        end
    catch
        _:{'UnimplementedException',_} ->       ?ClientError({"Unimplemented data type conversion",{Item,{Type,Val}}});
        _:{'ClientError', {Text, Reason}} ->    ?ClientError({Text, {Item,Reason}});
        _:_ ->                                  ?ClientError({"Data conversion format error",{Item,{Type,Val}}})
    end;
value_to_db(_Item,_Old,_Type,_Len,_Prec,_Def,_RO,Val) -> Val.    

strip_dquotes([]) -> [];
strip_dquotes([H]) -> [H];
% strip_dquotes([92,34,92,34]) -> [];
% strip_dquotes([92,34,_,_,_|_]=Str) ->
%     L = lists:last(Str),
%     SL = lists:nth(length(Str)-2,Str),
%     if 
%         L == 34 andalso SL == 92 -> lists:sublist(Str, 3, length(Str)-4);
%         true ->                     Str
%     end;
strip_dquotes([H|T]=Str) ->
    L = lists:last(Str),
    if 
        H == $" andalso L == $" ->  lists:sublist(T, length(T)-1);
        true ->                     Str
    end.

strip_squotes([]) -> [];
strip_squotes([H]) -> [H];
strip_squotes([H|T]=Str) ->
    L = lists:last(Str),
    if 
        H == $' andalso L == $' ->  lists:sublist(T, length(T)-1);
        true ->                     Str
    end.

string_to_integer(Val,Len,Prec) ->
    Value = case string_to_term(Val) of
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

string_to_float(Val,Prec) ->
    Value = case string_to_term(Val) of
        V when is_float(V) ->   V;
        V when is_integer(V) -> float(V);
        _ ->                    ?ClientError({"Data conversion format error",{float,Prec,Val}})
    end,
    if 
        Prec == 0 ->    Value;
        true ->         erlang:round(math:pow(10, Prec) * Value) * math:pow(10,-Prec)
    end.

string_to_binary([],_Len) -> <<>>;
string_to_binary(Val,Len) ->
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

hexstr_to_bin(S) ->
    hexstr_to_bin(S, []).
hexstr_to_bin([], Acc) ->
    list_to_binary(lists:reverse(Acc));
hexstr_to_bin([X,Y|T], Acc) ->
    {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
    hexstr_to_bin(T, [V | Acc]).

string_to_userid("system") -> system;
string_to_userid(Name) ->
    Guard = {'==', '$1', list_to_binary(Name)},
    case imem_if:select(ddAccount, [{'$1', [Guard], [{element,2,'$1'}]}]) of
        [] ->   ?ClientError({"Account does not exist",list_to_binary(Name)});
        [Id] -> Id;
        Else -> ?SystemException({"Account lookup error",{list_to_binary(Name),Else}})
    end.

string_to_timestamp(TS) ->
    string_to_timestamp(TS,6).

string_to_timestamp("systime",Prec) ->
    string_to_timestamp("now",Prec);
string_to_timestamp("sysdate",Prec) ->
    string_to_timestamp("now",Prec);
string_to_timestamp("now",Prec) ->
    {Megas,Secs,Micros} = erlang:now(),    
    {Megas,Secs,erlang:round(erlang:round(math:pow(10, Prec-6) * Micros) * erlang:round(math:pow(10,6-Prec)))};  
string_to_timestamp(Val,6) ->
    string_to_timestamp(Val); 
string_to_timestamp(Val,0) ->
    {Megas,Secs,_} = string_to_timestamp(Val),
    {Megas,Secs,0};
string_to_timestamp(Val,Prec) when Prec > 0 ->
    {Megas,Secs,Micros} = string_to_timestamp(Val),
    {Megas,Secs,erlang:round(erlang:round(math:pow(10, Prec-6) * Micros) * erlang:round(math:pow(10,6-Prec)))};
string_to_timestamp(Val,Prec) when Prec < 0 ->
    {Megas,Secs,_} = string_to_timestamp(Val),
    {Megas,erlang:round(erlang:round(math:pow(10, -Prec) * Secs) * erlang:round(math:pow(10,Prec))),0}.  

string_to_datetime("today") ->
    {Date,_} = string_to_datetime("localtime"),
    {Date,{0,0,0}}; 
string_to_datetime("systime") ->
    string_to_datetime("localtime"); 
string_to_datetime("sysdate") ->
    string_to_datetime("localtime"); 
string_to_datetime("now") ->
    string_to_datetime("localtime"); 
string_to_datetime("localtime") ->
    erlang:localtime(); 
string_to_datetime(Val) ->
    try 
        case re:run(Val,"[\/\.\-]+",[{capture,all,list}]) of
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

validate_date(Date) ->
    case calendar:valid_date(Date) of
        true ->     Date;
        false ->    ?ClientError({})
    end.    

string_to_ipaddr(Val,undefined) -> 
    string_to_ipaddr(Val,0);
string_to_ipaddr(Val,Len) ->
    Result = try 
        [ip_item(Item) || Item <- string:tokens(Val, ".")]
    catch
        _:_ -> ?ClientError({"Data conversion format error",{ipaddr,Len,Val}})
    end,
    RLen = length(lists:flatten(Result)),
    if 
        Len == 0 andalso RLen == 4 ->   list_to_tuple(Result);
        Len == 0 andalso RLen == 6 ->   list_to_tuple(Result);
        Len == 0 andalso RLen == 8 ->   list_to_tuple(Result);
        RLen /= Len ->                  ?ClientError({"Data conversion format error",{ipaddr,Len,Val}});
        true ->                         list_to_tuple(Result)
    end.

ip_item(Val) ->
    case string_to_term(Val) of
        V when is_integer(V), V >= 0, V < 256  ->   V;
        _ ->                                        ?ClientError({})
    end.

string_to_decimal(Val,Len,undefined) ->
    string_to_decimal(Val,Len,0);
string_to_decimal(Val,undefined,Prec) ->
    string_to_decimal(Val,0,Prec);
string_to_decimal(Val,Len,0) ->         %% use fixed point arithmetic with implicit scaling factor
    string_to_integer(Val,Len,0);  
string_to_decimal(Val,Len,Prec) when Prec > 0 -> 
    Result = erlang:round(math:pow(10, Prec) * string_to_float(Val,0)),
    RLen = length(integer_to_list(Result)),
    if 
        Len == 0 ->     Result;
        RLen > Len ->   ?ClientError({"Data conversion format error",{decimal,Len,Prec,Val}});
        true ->         Result
    end;
string_to_decimal(Val,Len,Prec) ->
    ?ClientError({"Data conversion format error",{decimal,Len,Prec,Val}}).

string_to_limited_string(Val,0) ->
    Val;
string_to_limited_string(Val,undefined) ->
    Val;
string_to_limited_string(Val,Len) ->
    if
        length(Val) =< Len  ->  Val;
        true ->                 ?ClientError({"String is too long",{Val,Len}})
    end.

string_to_list(Val, 0) -> 
    case string_to_term(Val) of
        V when is_list(V) ->    V;
        _ ->                    ?ClientError({"Data conversion format error",{list,0,Val}})
    end;
string_to_list(Val, undefined) -> 
    case string_to_term(Val) of
        V when is_list(V) ->    V;
        _ ->                    ?ClientError({"Data conversion format error",{list,undefined,Val}})
    end;
string_to_list(Val,Len) -> 
    case string_to_term(Val) of
        V when is_list(V) ->
            if 
                length(V) == Len -> V;
                true ->             ?ClientError({"Data conversion format error",{list,Len,Val}})
            end;
        _ ->
            ?ClientError({"Data conversion format error",{list,Len,Val}})
    end.

string_to_tuple(Val,0) -> 
    case string_to_term(Val) of
        V when is_tuple(V) ->   V;
        _ ->                    ?ClientError({"Data conversion format error",{tuple,0,Val}})
    end;
string_to_tuple(Val,undefined) -> 
    case string_to_term(Val) of
        V when is_tuple(V) ->   V;
        _ ->                    ?ClientError({"Data conversion format error",{tuple,undefined,Val}})
    end;
string_to_tuple(Val,Len) -> 
    case string_to_term(Val) of
        V when is_tuple(V) ->
            if 
                size(V) == Len ->   V;
                true ->             ?ClientError({"Data conversion format error",{tuple,Len,Val}})
            end;
        _ ->
            ?ClientError({"Data conversion format error",{tuple,Len,Val}})
    end.

string_to_term(Val) -> 
    try
        erl_value(Val)
    catch
        _:_ -> ?ClientError({})
    end.

erl_value(Val) -> 
    Code = case [lists:last(string:strip(Val))] of
        "." -> Val;
        _ -> Val ++ "."
    end,
    {ok,ErlTokens,_}=erl_scan:string(Code),    
    {ok,ErlAbsForm}=erl_parse:parse_exprs(ErlTokens),    
    {value,Value,_}=erl_eval:exprs(ErlAbsForm,[]),    
    Value.

string_to_fun(Val,undefined) ->
    string_to_fun(Val,0);
string_to_fun(Val,Len) ->
    Fun = erl_value(Val),
    if 
        is_function(Fun,Len) -> 
            Fun;
        true ->                 
            ?ClientError({"Data conversion format error",{eFun,Len,Val}})
    end.


%% ----- CAST Data from DB to string ------------------

db_to_gui(Type, Prec, DateFmt, NumFmt, StringFmt, Val) ->
    try
        if 
            (Type == binary) andalso is_binary(Val) ->      binary_to_string(Val);
            (Type == binstr) andalso is_binary(Val) ->      binstr_to_string(Val,StringFmt);
            (Type == datetime) andalso is_tuple(Val)->      datetime_to_string(Val,DateFmt);
            (Type == decimal) andalso is_integer(Val) ->    decimal_to_string(Val,Prec);
            (Type == float) andalso is_float(Val) ->        float_to_string(Val,Prec,NumFmt);
            (Type == ipaddr) andalso is_tuple(Val) ->       ipaddr_to_string(Val);
            (Type == timestamp) andalso is_tuple(Val) ->    timestamp_to_string(Val,Prec,DateFmt);
            (Type == userid) andalso is_atom(Val) ->        atom_to_list(Val);
            (Type == userid) ->                             userid_to_string(Val);
            true ->                 Val   
        end
    catch
        _:_ -> Val
    end.

db_to_string(Type, Prec, DateFmt, NumFmt, StringFmt, Val) ->
    try
        if 
            (Type == atom) andalso is_atom(Val) ->          atom_to_string(Val);
            (Type == binary) andalso is_binary(Val) ->      binary_to_string(Val);
            (Type == binstr) andalso is_binary(Val) ->      binstr_to_string(Val,StringFmt);
            (Type == datetime) andalso is_tuple(Val) ->     datetime_to_string(Val,DateFmt);
            (Type == decimal) andalso is_integer(Val) ->    decimal_to_string(Val,Prec);
            (Type == float) andalso is_float(Val) ->        float_to_string(Val,Prec,NumFmt);
            (Type == integer) andalso is_integer(Val) ->    integer_to_string(Val,StringFmt);
            (Type == ipaddr) andalso is_tuple(Val) ->       ipaddr_to_string(Val);
            (Type == string) andalso is_list(Val) ->        string_to_string(Val,StringFmt);
            (Type == timestamp) andalso is_tuple(Val) ->    timestamp_to_string(Val,Prec,DateFmt);
            (Type == userid) andalso is_atom(Val) ->        atom_to_list(Val);
            (Type == userid) ->                             userid_to_string(Val);
            true -> lists:flatten(io_lib:format("~w",[Val]))   
        end
    catch
        _:_ -> lists:flatten(io_lib:format("~p",[Val])) 
    end.

atom_to_string(Val) ->
    lists:flatten(io_lib:format("~p",[Val])).


datetime_to_string(Datetime) ->
    datetime_to_string(Datetime, eu).

datetime_to_string({{Year,Month,Day},{Hour,Min,Sec}},eu) ->
    lists:flatten(io_lib:format("~2.10.0B.~2.10.0B.~4.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Day, Month, Year, Hour, Min, Sec]));
datetime_to_string({{Year,Month,Day},{Hour,Min,Sec}},raw) ->
    lists:flatten(io_lib:format("~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",
        [Year, Month, Day, Hour, Min, Sec]));
datetime_to_string({{Year,Month,Day},{Hour,Min,Sec}},iso) ->
    lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Year, Month, Day, Hour, Min, Sec]));
datetime_to_string({{Year,Month,Day},{Hour,Min,Sec}},us) ->
    lists:flatten(io_lib:format("~2.10.0B/~2.10.0B/~4.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Month, Day, Year, Hour, Min, Sec]));
datetime_to_string(Datetime, Fmt) ->
    ?ClientError({"Data conversion format error",{datetime,Fmt,Datetime}}).

timestamp_to_string(TS) ->
    timestamp_to_string(TS,6,eu).

timestamp_to_string(TS,Prec) ->
    timestamp_to_string(TS,Prec,eu).

timestamp_to_string({Megas,Secs,Micros},_Prec,raw) ->
    lists:concat([integer_to_list(Megas),":",integer_to_list(Secs),":",integer_to_list(Micros)]);   
timestamp_to_string({Megas,Secs,Micros},_Prec,Fmt) ->
    lists:flatten(datetime_to_string(calendar:now_to_local_time({Megas,Secs,0}),Fmt) ++ io_lib:format(".~6.6.0w",[Micros])).
   

decimal_to_string(Val,0) ->
    lists:flatten(io_lib:format("~p",[Val]));   
decimal_to_string(Val,Prec) when Val < 0 ->
    lists:flatten(lists:concat(["-",decimal_to_string(-Val,Prec)]));
decimal_to_string(Val,Prec) when Prec > 0 ->
    Str = integer_to_list(Val),
    Len = length(Str),
    if 
        Prec-Len+1 > 0 -> 
            {Whole,Frac} = lists:split(1,lists:duplicate(Prec-Len+1,$0) ++ Str),
            lists:flatten(lists:concat([Whole,".",Frac]));
        true ->
            {Whole,Frac} = lists:split(Len-Prec,Str),
            lists:flatten(lists:concat([Whole,".",Frac]))
    end;
decimal_to_string(Val,Prec) ->
    lists:flatten(lists:concat([integer_to_list(Val),lists:duplicate(-Prec,$0)])).

binary_to_string(Val) -> 
    if
        byte_size(Val) =< ?BinaryMaxLen ->
            lists:flatten(io_lib:format("~s",[binary_to_hex(Val)]));
        true ->
            B = binary:part(Val, 1, ?BinaryMaxLen), 
            lists:flatten(io_lib:format("~s",[<<B/binary,46:8,46:8,46:8>>]))
    end.


userid_to_string(Val) ->
    case imem_if:read(ddAccount,Val) of
        [] ->           ?ClientError({"Account does not exist",Val});
        [Account] ->    Name=element(3,Account),
                        if 
                            is_binary(Name) ->  binary_to_list(Name);
                            is_atom(Name) ->    atom_to_list(Name);
                            true ->             lists:flatten(io_lib:format("~p",[Name]))
                        end;
        Else ->         ?SystemException({"Account lookup error",{Val,Else}})
    end.

ipaddr_to_string({A,B,C,D}) ->
    lists:concat([integer_to_list(A),".",integer_to_list(B),".",integer_to_list(C),".",integer_to_list(D)]);
ipaddr_to_string(_IpAddr) -> ?UnimplementedException({}).

float_to_string(Val,_Prec,_NumFmt) ->
    float_to_list(Val).                     %% ToDo: implement rounding to db precision

binstr_to_string(Val, StringFmt) ->
    string_to_string(binary_to_list(Val), StringFmt).      %% ToDo: handle escaping and quoting etc.
    
string_to_string(Val, _StringFmt) when is_list(Val) ->
    IsString = io_lib:printable_unicode_list(Val),
    if 
        IsString ->     lists:flatten(io_lib:format("~s",[Val]));          %% ToDo: handle escaping and quoting etc.
        true ->         io_lib:format("~p",[Val])
    end;                                    
string_to_string(Val, _StringFmt) ->
    lists:flatten(io_lib:format("~p",[Val])).

integer_to_string(Val, _StringFmt) ->
    lists:flatten(io_lib:format("~p",[Val])).

%% ----- Helper Functions ---------------------------------------------

map(Val,From,To) ->
    if 
        Val == From ->  To;
        true ->         Val
    end.

name(T) when is_tuple(T) ->
    string:join([name(E) || E <- tuple_to_list(T)],".");
name(N) when is_atom(N) -> atom_to_list(N);
name(N) when is_binary(N) -> binary_to_list(N);
name(N) when is_list(N) -> lists:flatten(N);
name(N) -> lists:flatten(io_lib:format("~p",[N])).

name(T,I) when is_tuple(T) ->
    if 
        size(T) >= I ->
            name(element(I,T));
        true ->
            ?ClientError({"Tuple too short",{T,I}})
    end;
name(T,_) ->
    ?ClientError({"Tuple expected",T}).

name1(T) -> name(T,1).
name2(T) -> name(T,2).
name3(T) -> name(T,3).
name4(T) -> name(T,4).

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
    string:join([name(I) || I <- L],[]).


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

        io:format(user, "----TEST--~p:test_data_types~n", [?MODULE]),

        ?assertEqual("Imem.ddTable", name({'Imem',ddTable})),
        ?assertEqual("Imem", name1({'Imem',ddTable})),
        ?assertEqual("Imem", name({'Imem',ddTable},1)),
        ?assertEqual("ddTable", name2({'Imem',ddTable})),
        ?assertEqual("ddTable", name({'Imem',ddTable},2)),
        io:format(user, "name success~n", []),
        ?assertEqual("ABC", concat("A","B","C")),
        ?assertEqual("abc", concat(a,b,c)),
        ?assertEqual("123", concat(1,2,3)),
        io:format(user, "concat success~n", []),

        ?assertEqual("123", decimal_to_string(123,0)),
        ?assertEqual("-123", decimal_to_string(-123,0)),
        ?assertEqual("12.3", decimal_to_string(123,1)),
        ?assertEqual("-12.3", decimal_to_string(-123,1)),
        ?assertEqual("0.123", decimal_to_string(123,3)),
        ?assertEqual("-0.123", decimal_to_string(-123,3)),
        ?assertEqual("0.00123", decimal_to_string(123,5)),
        ?assertEqual("-0.00123", decimal_to_string(-123,5)),
        ?assertEqual("-0.00123", decimal_to_string(-123,5)),
        ?assertEqual("12300000", decimal_to_string(123,-5)),
        ?assertEqual("-12300000", decimal_to_string(-123,-5)),
        io:format(user, "decimal_to_string success~n", []),

        ?assertEqual("0.0.0.0", ipaddr_to_string({0,0,0,0})),
        ?assertEqual("1.2.3.4", ipaddr_to_string({1,2,3,4})),
        io:format(user, "ipaddr_to_string success~n", []),

        ?assertEqual("01.01.1970 01:00:00.000000", timestamp_to_string({0,0,0},0)),  %% with DLS offset wintertime CH
        ?assertEqual("12.01.1970 14:46:42.000003", timestamp_to_string({1,2,3},0)),  %% with DLS offset wintertime CH
        io:format(user, "timestamp_to_string success~n", []),

        LocalTime = erlang:localtime(),
        {Date,_Time} = LocalTime,
        ?assertEqual({{2004,3,1},{0,0,0}}, string_to_datetime("1.3.2004")),
        ?assertEqual({{2004,3,1},{3,45,0}}, string_to_datetime("1.3.2004 3:45")),
        ?assertEqual({Date,{0,0,0}}, string_to_datetime("today")),
        ?assertEqual(LocalTime, string_to_datetime("sysdate")),
        ?assertEqual(LocalTime, string_to_datetime("systime")),
        ?assertEqual(LocalTime, string_to_datetime("now")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_datetime("18.8.1888 1:23:59")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_datetime("1888-08-18 1:23:59")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_datetime("8/18/1888 1:23:59")),
        ?assertEqual({{1888,8,18},{1,23,0}}, string_to_datetime("8/18/1888 1:23")),
        ?assertEqual({{1888,8,18},{1,0,0}}, string_to_datetime("8/18/1888 01")),
        ?assertException(throw,{ClEr,{"Data conversion format error",{datetime,"8/18/1888 1"}}}, string_to_datetime("8/18/1888 1")),
        ?assertEqual({{1888,8,18},{0,0,0}}, string_to_datetime("8/18/1888 ")),
        ?assertEqual({{1888,8,18},{0,0,0}}, string_to_datetime("8/18/1888")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_datetime("18880818012359")),
        io:format(user, "string_to_datetime success~n", []),

        ?assertEqual({1,23,59}, parse_time("01:23:59")),        
        ?assertEqual({1,23,59}, parse_time("1:23:59")),        
        ?assertEqual({1,23,0}, parse_time("01:23")),        
        ?assertEqual({1,23,59}, parse_time("012359")),        
        ?assertEqual({1,23,0}, parse_time("0123")),        
        ?assertEqual({1,0,0}, parse_time("01")),        
        ?assertEqual({0,0,0}, parse_time("")),        
        io:format(user, "parse_time success~n", []),

        ?assertEqual({1,2,3,4},string_to_ipaddr("1.2.3.4",0)),
        ?assertEqual({1,2,3,4},value_to_db(0,"",ipaddr,0,0,undefined,false,"1.2.3.4")),
        ?assertEqual({1,2,3,4},value_to_db(0,"",ipaddr,0,0,undefined,false,"\"1.2.3.4\"")),

        Item = 0,
        OldString = "OldString",
        StringType = string,
        Len = 3,
        Prec = 1,
        Def = default,
        RO = true,
        RW = false,
        DefFun = fun() -> [{},{}] end,
        ?assertEqual(OldString, value_to_db(Item,OldString,StringType,Len,Prec,Def,RO,"NewVal")),
        io:format(user, "value_to_db success 1~n", []),

        ?assertEqual(<<"NewVal">>, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,<<"NewVal">>)),
        ?assertEqual([], value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,"")),
        ?assertEqual({}, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,{})),
        ?assertEqual([atom,atom], value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,[atom,atom])),
        ?assertEqual(12, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,12)),
        ?assertEqual(-3.14, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,-3.14)),
        io:format(user, "value_to_db success 2~n", []),

        ?assertEqual(OldString, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,OldString)),
        ?assertException(throw,{ClEr,{"String is too long",{0,{"NewVal",3}}}}, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,"NewVal")),
        ?assertEqual("NewVal", value_to_db(Item,OldString,StringType,6,Prec,Def,RW,"NewVal")),
        ?assertEqual("[NewVal]", value_to_db(Item,OldString,StringType,8,Prec,Def,RW,"[NewVal]")),
        ?assertEqual(default, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,"default")),
        io:format(user, "value_to_db success 3~n", []),

        ?assertEqual([{},{}], value_to_db(Item,OldString,StringType,Len,Prec,DefFun,RW,"[{},{}]")),

        ?assertEqual(oldValue, value_to_db(Item,oldValue,StringType,Len,Prec,Def,RW,"oldValue")),
        ?assertEqual('OldValue', value_to_db(Item,'OldValue',StringType,Len,Prec,Def,RW,"'OldValue'")),
        ?assertEqual(-15, value_to_db(Item,-15,StringType,Len,Prec,Def,RW,"-15")),
        io:format(user, "value_to_db success 4~n", []),

        IntegerType = integer,
        OldInteger = 17,
        ?assertEqual(OldString, value_to_db(Item,OldString,IntegerType,Len,Prec,Def,RW,"OldString")),
        ?assertEqual(OldInteger, value_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"17")),
        ?assertEqual(default, value_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"default")),
        ?assertEqual(18, value_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"18")),
        ?assertEqual(-18, value_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"-18")),
        ?assertEqual(100, value_to_db(Item,OldInteger,IntegerType,Len,-2,Def,RW,"149")),
        ?assertEqual(200, value_to_db(Item,OldInteger,IntegerType,Len,-2,Def,RW,"150")),
        ?assertEqual(-100, value_to_db(Item,OldInteger,IntegerType,4,-2,Def,RW,"-149")),
        ?assertEqual(-200, value_to_db(Item,OldInteger,IntegerType,4,-2,Def,RW,"-150")),
        ?assertEqual(-200, value_to_db(Item,OldInteger,IntegerType,100,0,Def,RW,"300-500")),
        ?assertEqual(12, value_to_db(Item,OldInteger,IntegerType,20,0,Def,RW,"120/10.0")),
        ?assertEqual(12, value_to_db(Item,OldInteger,IntegerType,20,0,Def,RW,"120/10.0001")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,"1234"}}}}, value_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"1234")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,"-"}}}}, value_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"-")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,""}}}}, value_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,"-100"}}}}, value_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"-100")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,"9999"}}}}, value_to_db(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"9999")),

        ?assertEqual({1,2,3}, value_to_db(Item,?nav,term,0,0,?nav,false,"{1,2,3}")),
        ?assertEqual([1,2,3], value_to_db(Item,?nav,term,0,0,?nav,false,"[1,2,3]")),
        ?assertEqual('$_', value_to_db(Item,?nav,term,0,0,?nav,false,"\"'$_'\"")),

        io:format(user, "value_to_db success 5~n", []),

        FloatType = float,
        OldFloat = -1.2,
        ?assertEqual(8.1, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"8.1")),
        ?assertEqual(18.0, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"18")),
        ?assertEqual(1.1, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1.12")),
        ?assertEqual(-1.1, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"-1.14")),
        ?assertEqual(-1.1, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"-1.1234567")),
        ?assertEqual(-1.12, value_to_db(Item,OldFloat,FloatType,Len,2,Def,RW,"-1.1234567")),
        ?assertEqual(-1.123, value_to_db(Item,OldFloat,FloatType,Len,3,Def,RW,"-1.1234567")),
        ?assertEqual(-1.1235, value_to_db(Item,OldFloat,FloatType,Len,4,Def,RW,"-1.1234567")),
        io:format(user, "value_to_db success 6~n", []),
        %% ?assertEqual(-1.12346, value_to_db(Item,OldFloat,FloatType,Len,5,Def,RW,"-1.1234567")),  %% fails due to single precision math
        %% ?assertEqual(-1.123457, value_to_db(Item,OldFloat,FloatType,Len,6,Def,RW,"-1.1234567")), %% fails due to single precision math
        ?assertEqual(100.0, value_to_db(Item,OldFloat,FloatType,Len,-2,Def,RW,"149")),
        ?assertEqual(-100.0, value_to_db(Item,OldFloat,FloatType,Len,-2,Def,RW,"-149")),
        ?assertEqual(-200.0, value_to_db(Item,OldFloat,FloatType,Len,-2,Def,RW,"-150")),
        %% ?assertEqual(0.6, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.56")),
        %% ?assertEqual(0.6, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.5678")),
        %% ?assertEqual(0.6, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.5678910111")),
        %% ?assertEqual(0.6, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.56789101112131415")),
        ?assertEqual(1234.5, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.5")),
        %% ?assertEqual(1234.6, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.56")),
        %% ?assertEqual(1234.6, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.5678")),
        %% ?assertEqual(1234.6, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.5678910111")),
        %% ?assertEqual(1234.6, value_to_db(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.56789101112131415")),
        io:format(user, "value_to_db success 7~n", []),

        DecimalType = decimal,
        OldDecimal = -123,
        ?assertEqual(81, value_to_db(Item,OldDecimal,DecimalType,Len,Prec,Def,RW,"8.1")),
        ?assertEqual(180, value_to_db(Item,OldDecimal,DecimalType,Len,Prec,Def,RW,"18.001")),
        ?assertEqual(1, value_to_db(Item,OldDecimal,DecimalType,1,0,Def,RW,"1.12")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,5,"-1.123"}}}}, value_to_db(Item,OldDecimal,DecimalType,5,5,Def,RW,"-1.123")),
        ?assertEqual(-112300, value_to_db(Item,OldDecimal,DecimalType,7,5,Def,RW,"-1.123")),
        ?assertEqual(-112346, value_to_db(Item,OldDecimal,DecimalType,7,5,Def,RW,"-1.1234567")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,Prec,"1234567.89"}}}}, value_to_db(Item,OldDecimal,DecimalType,5,Prec,Def,RW,"1234567.89")),
        io:format(user, "value_to_db success 8~n", []),

        TermType = term,
        OldTerm = {-1.2,[a,b,c]},
        ?assertEqual(OldTerm, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,OldTerm)),
        ?assertEqual(default, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"default")),
        ?assertEqual(default, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"\"default\"")),
        ?assertEqual(default, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"\"'default'\"")),
    %    ?assertEqual("default", value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"'default'")),
        ?assertEqual([a,b], value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"[a,b]")),
        ?assertEqual(-1.1234567, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"-1.1234567")),
        ?assertEqual("-1.1234567", value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"'-1.1234567'")),
        ?assertEqual(-1.1234567, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"\"-1.1234567\"")),
        ?assertEqual({[1,2,3]}, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"{[1,2,3]}")),
        ?assertEqual({[1,2,3]}, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"\"{[1,2,3]}\"")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{term,"[a|]"}}}}, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"[a|]")),
        io:format(user, "value_to_db success 9~n", []),

        ?assertEqual({1,2,3}, value_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,"{1,2,3}")),
        ?assertEqual({}, value_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,"{}")),
        ?assertEqual({1}, value_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,"{1}")),
        ?assertEqual({1,2}, value_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,"{1,2}")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{tuple,Len,"[a]"}}}}, value_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,"[a]")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{tuple,Len,"{a}"}}}}, value_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,"{a}")),
        io:format(user, "value_to_db success 10~n", []),

        ?assertEqual([a,b,c], value_to_db(Item,OldTerm,elist,Len,Prec,Def,RW,"[a,b,c]")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{list,Len,"[a]"}}}}, value_to_db(Item,OldTerm,list,Len,Prec,Def,RW,"[a]")),
        ?assertEqual([], value_to_db(Item,[],list,Len,Prec,Def,RW,"[]")),
        ?assertEqual([], value_to_db(Item,OldTerm,list,Len,Prec,[],RW,"[]")),
        ?assertEqual([], value_to_db(Item,OldTerm,list,0,Prec,Def,RW,"[]")),
        ?assertEqual([a], value_to_db(Item,OldTerm,list,0,Prec,Def,RW,"[a]")),
        ?assertEqual([a,b], value_to_db(Item,OldTerm,list,0,Prec,Def,RW,"[a,b]")),
        io:format(user, "value_to_db success 11~n", []),

        ?assertEqual({10,132,7,92}, value_to_db(Item,OldTerm,ipaddr,0,Prec,Def,RW,"10.132.7.92")),
        ?assertEqual({0,0,0,0}, value_to_db(Item,OldTerm,ipaddr,4,Prec,Def,RW,"0.0.0.0")),
        ?assertEqual({255,255,255,255}, value_to_db(Item,OldTerm,ipaddr,4,Prec,Def,RW,"255.255.255.255")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,0,"1.2.3.4.5"}}}}, value_to_db(Item,OldTerm,ipaddr,0,0,Def,RW,"1.2.3.4.5")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,4,"1.2.-1.4"}}}}, value_to_db(Item,OldTerm,ipaddr,4,0,Def,RW,"1.2.-1.4")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,6,"1.256.1.4"}}}}, value_to_db(Item,OldTerm,ipaddr,6,0,Def,RW,"1.256.1.4")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,8,"1.2.1.4"}}}}, value_to_db(Item,OldTerm,ipaddr,8,0,Def,RW,"1.2.1.4")),
        io:format(user, "value_to_db success 12~n", []),

        Fun = fun(X) -> X*X end,
        io:format(user, "Fun ~p~n", [Fun]),
        Res = value_to_db(Item,OldTerm,'fun',1,Prec,Def,RW,"fun(X) -> X*X end"),
        io:format(user, "Run ~p~n", [Res]),
        ?assertEqual(Fun(4), Res(4)),
        io:format(user, "value_to_db success 13~n", []),

        ?assertEqual(<<>>, binary_to_hex(<<>>)),
        ?assertEqual(<<"41">>, binary_to_hex(<<"A">>)),
        ?assertEqual(<<"4142434445464748">>, binary_to_hex(<<"ABCDEFGH">>)),
        io:format(user, "binary_to_hex success~n", []),

        ?assertEqual(<<>>, string_to_binary([],0)),
        ?assertEqual(<<0:8>>, string_to_binary("00",0)),
        ?assertEqual(<<1:8>>, string_to_binary("01",0)),
        ?assertEqual(<<9:8>>, string_to_binary("09",0)),
        ?assertEqual(<<10:8>>, string_to_binary("0A",0)),
        ?assertEqual(<<15:8>>, string_to_binary("0F",0)),
        ?assertEqual(<<255:8>>, string_to_binary("FF",0)),
        ?assertEqual(<<1:8,1:8>>, string_to_binary("0101",0)),
        ?assertEqual(<<"ABCDEFGH">>, string_to_binary("4142434445464748",0)),

        io:format(user, "string_to_binary success~n", []),

        
        ?assertEqual(true, true)
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
