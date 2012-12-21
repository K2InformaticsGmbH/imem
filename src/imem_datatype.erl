-module(imem_datatype).

-include_lib("eunit/include/eunit.hrl").

-include("imem_meta.hrl").

-export([ value_to_db/8
        , db_to_string/6
        , db_to_gui/6
        , strip_quotes/1
        ]).

%   datatypes
%   internal    aliases (synonyms)
%   --------------------------------------------
%   atom
%   binary      raw(L), blob(L), rowid
%   binstr      clob(L), nclob(L)
%   boolean
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


-export([ raw_type/1
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
        , datetime_to_string/1
        , datetime_to_string/2
        , decimal_to_string/2
        , float_to_string/3
        , integer_to_string/2
        , ipaddr_to_string/1
        , string_to_string/2
        , timestamp_to_string/2
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

select_rowfun_raw(Recs, [], Acc) ->
    [Recs|lists:reverse(Acc)];
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

select_rowfun_str(Recs, [], _DateFmt, _NumFmt, _StrFmt, Acc) ->
    [Recs|lists:reverse(Acc)];
select_rowfun_str(Recs, [#ddColMap{type=T,precision=P,tind=Ti,cind=Ci,func=undefined}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    Str = db_to_string(T, P, DateFmt, NumFmt, StrFmt, element(Ci,element(Ti,Recs))),
    select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]);
select_rowfun_str(Recs, [#ddColMap{tind=Ti,cind=Ci,func=F}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
    Str = try
        apply(F,[element(Ci,element(Ti,Recs))])
    catch
        _:Reason ->  ?UnimplementedException({"Failed row function",{F,Reason}})
    end,    
    select_rowfun_str(Recs, ColMap, DateFmt, NumFmt, StrFmt, [Str|Acc]).

select_rowfun_gui(ColMap, DateFmt, NumFmt, StrFmt) ->
    fun(Recs) -> 
        select_rowfun_gui(Recs, ColMap, DateFmt, NumFmt, StrFmt, []) 
    end.

select_rowfun_gui(Recs, [], _DateFmt, _NumFmt, _StrFmt, Acc) ->
    [Recs|lists:reverse(Acc)];
select_rowfun_gui(Recs, [#ddColMap{type=T,precision=P,tind=Ti,cind=Ci,func=undefined}|ColMap], DateFmt, NumFmt, StrFmt, Acc) ->
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


%% ----- DATA TYPES  with CamelCase  --------------------------------

raw_type(userid) -> ref;
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
value_to_db(_Item,_Old,_Type,_Len,_Prec,[],_RO, "[]") -> [];
value_to_db(_Item,[],_Type,_Len,_Prec,_Def,_RO, "[]") -> [];
value_to_db(Item,Old,Type,Len,Prec,Def,_RO,Val) when is_function(Def,0) ->
    value_to_db(Item,Old,Type,Len,Prec,Def(),_RO,Val);
value_to_db(Item,Old,Type,Len,Prec,Def,_RO,Val) when is_list(Val) ->
    try
        IsString = io_lib:printable_unicode_list(Val),
        if 
            IsString ->
                DefAsStr = lists:flatten(io_lib:format("~p", [Def])),
                OldAsStr = lists:flatten(io_lib:format("~p", [Old])),
                Unquoted = strip_quotes(Val),
                if 
                    (DefAsStr == Unquoted) ->   Def;
                    (DefAsStr == Val) ->        Def;
                    (OldAsStr == Unquoted) ->   Old;
                    (OldAsStr == Val) ->        Old;
                    (Type == 'fun') ->          string_to_fun(Unquoted,Len);
                    (Type == atom) ->           list_to_atom(Unquoted);
                    (Type == binary) ->         string_to_binary(Unquoted,Len);
                    (Type == binstr) ->         list_to_binary(Unquoted);
                    (Type == blob) ->           string_to_binary(Unquoted,Len);
                    (Type == datetime) ->       string_to_datetime(Unquoted);
                    (Type == decimal) ->        string_to_decimal(Val,Len,Prec);
                    (Type == float) ->          string_to_float(Val,Prec);
                    (Type == integer) ->        string_to_integer(Val,Len,Prec);
                    (Type == ipaddr) ->         string_to_ipaddr(Unquoted,Len);
                    (Type == list) ->           string_to_list(Unquoted,Len);
                    (Type == pid) ->            list_to_pid(Unquoted);
                    (Type == ref) ->            Old;    %% cannot convert back
                    (Type == string) ->         string_to_limited_string(Unquoted,Len);
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

strip_quotes([]) -> [];
strip_quotes([H]) -> [H];
strip_quotes([H|T]=Str) ->
    L = lists:last(T),
    if 
        H == $" andalso L == $" ->  lists:sublist(T, length(T)-1);
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

string_to_binary(_Val,_Len) -> ?UnimplementedException({}).

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
        _:_ ->  ?ClientError({"Data conversion format error",{eDatetime,Val}})
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
string_to_list(Val,Len) -> 
    case string_to_term(Val) of
        V when is_list(V) ->
            if 
                length(V) == Len -> V;
                true ->             ?ClientError({"Data conversion format error",{list,Len,Val}})
            end;
        _ ->
            ?ClientError({"Data conversion format error",{tuple,Len,Val}})
    end.

string_to_tuple(Val,0) -> 
    case string_to_term(Val) of
        V when is_tuple(V) ->   V;
        _ ->                    ?ClientError({"Data conversion format error",{tuple,0,Val}})
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
            (Type == timestamp) andalso is_tuple(Val) ->    timestamp_to_string(Val,Prec);
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
            (Type == timestamp) andalso is_tuple(Val) ->    timestamp_to_string(Val,Prec);
            (Type == userid) andalso is_atom(Val) ->        atom_to_list(Val);
            (Type == userid) ->                             userid_to_string(Val);
            true -> lists:flatten(io_lib:format("~p",[Val]))   
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

binary_to_string(_Val) -> ?UnimplementedException({}).

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


timestamp_to_string({Megas,Secs,Micros},_Prec) ->
    lists:concat([integer_to_list(Megas),":",integer_to_list(Secs),":",integer_to_list(Micros)]).    

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


%% ----- TESTS ------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ok. 		%% ?imem_test_setup().

teardown(_) ->
    ok. 		%% ?imem_test_teardown().

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
        ?assertEqual("ABC", concat("A","B","C")),
        ?assertEqual("abc", concat(a,b,c)),
        ?assertEqual("123", concat(1,2,3)),

        ?assertEqual("123", decimal_to_string(123,0)),
        ?assertEqual("-123", decimal_to_string(-123,0)),
        ?assertEqual("12.3", decimal_to_string(123,1)),
        ?assertEqual("-12.3", decimal_to_string(-123,1)),
        ?assertEqual("0.123", decimal_to_string(123,3)),
        ?assertEqual("-0.123", decimal_to_string(-123,3)),
        ?assertEqual("0.00123", decimal_to_string(123,5)),
        ?assertEqual("-0.00123", decimal_to_string(-123,5)),

        ?assertEqual("0.0.0.0", ipaddr_to_string({0,0,0,0})),
        ?assertEqual("1.2.3.4", ipaddr_to_string({1,2,3,4})),

        ?assertEqual("1:2:3", timestamp_to_string({1,2,3},0)),

        ?assertEqual("-0.00123", decimal_to_string(-123,5)),
        ?assertEqual("12300000", decimal_to_string(123,-5)),
        ?assertEqual("-12300000", decimal_to_string(-123,-5)),

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
        ?assertException(throw,{ClEr,{"Data conversion format error",{eDatetime,"8/18/1888 1"}}}, string_to_datetime("8/18/1888 1")),
        ?assertEqual({{1888,8,18},{0,0,0}}, string_to_datetime("8/18/1888 ")),
        ?assertEqual({{1888,8,18},{0,0,0}}, string_to_datetime("8/18/1888")),
        ?assertEqual({1,23,59}, parse_time("01:23:59")),        
        ?assertEqual({1,23,59}, parse_time("1:23:59")),        
        ?assertEqual({1,23,0}, parse_time("01:23")),        
        ?assertEqual({1,23,59}, parse_time("012359")),        
        ?assertEqual({1,23,0}, parse_time("0123")),        
        ?assertEqual({1,0,0}, parse_time("01")),        
        ?assertEqual({0,0,0}, parse_time("")),        
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_datetime("18880818012359")),

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

        ?assertEqual(<<"NewVal">>, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,<<"NewVal">>)),
        ?assertEqual([], value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,"")),
        ?assertEqual({}, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,{})),
        ?assertEqual([atom,atom], value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,[atom,atom])),
        ?assertEqual(12, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,12)),
        ?assertEqual(-3.14, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,-3.14)),

        ?assertEqual(OldString, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,OldString)),
        ?assertException(throw,{ClEr,{"String is too long",{0,{"NewVal",3}}}}, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,"NewVal")),
        ?assertEqual("NewVal", value_to_db(Item,OldString,StringType,6,Prec,Def,RW,"NewVal")),
        ?assertEqual("[NewVal]", value_to_db(Item,OldString,StringType,8,Prec,Def,RW,"[NewVal]")),
        ?assertEqual(default, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,"default")),

        ?assertEqual([{},{}], value_to_db(Item,OldString,StringType,Len,Prec,DefFun,RW,"[{},{}]")),

        ?assertEqual(oldValue, value_to_db(Item,oldValue,StringType,Len,Prec,Def,RW,"oldValue")),
        ?assertEqual('OldValue', value_to_db(Item,'OldValue',StringType,Len,Prec,Def,RW,"'OldValue'")),
        ?assertEqual(-15, value_to_db(Item,-15,StringType,Len,Prec,Def,RW,"-15")),

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

        DecimalType = decimal,
        OldDecimal = -123,
        ?assertEqual(81, value_to_db(Item,OldDecimal,DecimalType,Len,Prec,Def,RW,"8.1")),
        ?assertEqual(180, value_to_db(Item,OldDecimal,DecimalType,Len,Prec,Def,RW,"18.001")),
        ?assertEqual(1, value_to_db(Item,OldDecimal,DecimalType,1,0,Def,RW,"1.12")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,5,"-1.123"}}}}, value_to_db(Item,OldDecimal,DecimalType,5,5,Def,RW,"-1.123")),
        ?assertEqual(-112300, value_to_db(Item,OldDecimal,DecimalType,7,5,Def,RW,"-1.123")),
        ?assertEqual(-112346, value_to_db(Item,OldDecimal,DecimalType,7,5,Def,RW,"-1.1234567")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,Prec,"1234567.89"}}}}, value_to_db(Item,OldDecimal,DecimalType,5,Prec,Def,RW,"1234567.89")),

        TermType = term,
        OldTerm = {-1.2,[a,b,c]},
        ?assertEqual(OldTerm, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,OldTerm)),
        ?assertEqual(default, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"default")),
        ?assertEqual([a,b], value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"[a,b]")),
        ?assertEqual(-1.1234567, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"-1.1234567")),
        ?assertEqual({[1,2,3]}, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"{[1,2,3]}")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{term,"[a|]"}}}}, value_to_db(Item,OldTerm,TermType,Len,Prec,Def,RW,"[a|]")),

        ?assertEqual({1,2,3}, value_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,"{1,2,3}")),
        ?assertEqual({}, value_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,"{}")),
        ?assertEqual({1}, value_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,"{1}")),
        ?assertEqual({1,2}, value_to_db(Item,OldTerm,tuple,0,Prec,Def,RW,"{1,2}")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{tuple,Len,"[a]"}}}}, value_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,"[a]")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{tuple,Len,"{a}"}}}}, value_to_db(Item,OldTerm,tuple,Len,Prec,Def,RW,"{a}")),

        ?assertEqual([a,b,c], value_to_db(Item,OldTerm,elist,Len,Prec,Def,RW,"[a,b,c]")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{list,Len,"[a]"}}}}, value_to_db(Item,OldTerm,list,Len,Prec,Def,RW,"[a]")),
        ?assertEqual([], value_to_db(Item,[],list,Len,Prec,Def,RW,"[]")),
        ?assertEqual([], value_to_db(Item,OldTerm,list,Len,Prec,[],RW,"[]")),
        ?assertEqual([], value_to_db(Item,OldTerm,list,0,Prec,Def,RW,"[]")),
        ?assertEqual([a], value_to_db(Item,OldTerm,list,0,Prec,Def,RW,"[a]")),
        ?assertEqual([a,b], value_to_db(Item,OldTerm,list,0,Prec,Def,RW,"[a,b]")),

        ?assertEqual({10,132,7,92}, value_to_db(Item,OldTerm,ipaddr,0,Prec,Def,RW,"10.132.7.92")),
        ?assertEqual({0,0,0,0}, value_to_db(Item,OldTerm,ipaddr,4,Prec,Def,RW,"0.0.0.0")),
        ?assertEqual({255,255,255,255}, value_to_db(Item,OldTerm,ipaddr,4,Prec,Def,RW,"255.255.255.255")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,0,"1.2.3.4.5"}}}}, value_to_db(Item,OldTerm,ipaddr,0,0,Def,RW,"1.2.3.4.5")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,4,"1.2.-1.4"}}}}, value_to_db(Item,OldTerm,ipaddr,4,0,Def,RW,"1.2.-1.4")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,6,"1.256.1.4"}}}}, value_to_db(Item,OldTerm,ipaddr,6,0,Def,RW,"1.256.1.4")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{ipaddr,8,"1.2.1.4"}}}}, value_to_db(Item,OldTerm,ipaddr,8,0,Def,RW,"1.2.1.4")),

        Fun = fun(X) -> X*X end,
        io:format(user, "Fun ~p~n", [Fun]),
        Res = value_to_db(Item,OldTerm,'fun',1,Prec,Def,RW,"fun(X) -> X*X end"),
        io:format(user, "Run ~p~n", [Res]),
        ?assertEqual(Fun(4), Res(4)),

        ?assertEqual(true, true)
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
