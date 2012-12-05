-module(imem_datatype).

-include_lib("eunit/include/eunit.hrl").

-include("imem_meta.hrl").

-export([ value_to_db/8
        , db_to_string/4
        ]).

-export([ pretty_type/1
        , string_to_date/1
        , string_to_decimal/3
        , string_to_double/2
        , string_to_ebinary/2
        , string_to_edatetime/1
        , string_to_eipaddr/2
        , string_to_elist/2
        , string_to_enum/2
        , string_to_eterm/1
        , string_to_etimestamp/1
        , string_to_etuple/2
        , string_to_float/2
        , string_to_fun/2
        , string_to_integer/3
        , string_to_number/3
        , string_to_set/2
        , string_to_time/1
        , string_to_timestamp/2
        , string_to_year/1
        ]).

-export([ date_to_string/1
        , date_to_string/2
        , decimal_to_string/2
        , ebinary_to_string/1
        , edatetime_to_string/1
        , edatetime_to_string/2
        , eipaddr_to_string/1
        , etimestamp_to_string/1
        , number_to_string/2
        , time_to_string/1
        , timestamp_to_string/1
        , year_to_string/1
        ]).


-export([ localTimeToSysDate/1
        , nowToSysTimeStamp/1
        , map/3
        ]).


-export([ select_rowfun_raw/1
        , select_rowfun_str/1
        ]).


select_rowfun_raw(ColMap) ->
    ColPointers = [{C#ddColMap.tind, C#ddColMap.cind} || C <- ColMap],
    fun(X) -> 
        [X|[element(Cind,element(Tind,X))|| {Tind,Cind} <- ColPointers]] 
    end.

select_rowfun_str(ColMap) ->
    fun(X) -> 
        select_rowfun_str_(ColMap, []) 
    end.

select_rowfun_str_([#ddColMap{type=T,length=L,precision=P,tind=Ti,cind=Ci}|ColMap], Acc) ->
    
ok.


%% ----- DATA TYPES  with CamelCase  --------------------------------

pretty_type(etuple) -> eTuple;
pretty_type(efun) -> eFun;
pretty_type(ebinary) -> eBinary;
pretty_type(elist) -> eList;
pretty_type(eipaddr) -> eIpaddr;
pretty_type(etimestamp) -> eTimestamp;
pretty_type(edatetime) -> eDatetime;
pretty_type(eatom) -> eAtom;
pretty_type(ebinstr) -> eBinstr;
pretty_type(eref) -> eRef;
pretty_type(epid) -> ePid;
pretty_type(estring) -> eString;
pretty_type(eterm) -> eTerm;
pretty_type(Type) -> Type.


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
                if 
                    (DefAsStr == Val) ->    Def;
                    (OldAsStr == Val) ->    Old;
                    (Type == bigint) ->     string_to_integer(Val,Len,Prec);
                    (Type == blob) ->       string_to_ebinary(Val,Len);
                    (Type == character) ->  string_to_list(Val,map(Len,0,1));
                    (Type == decimal) ->    string_to_decimal(Val,Len,Prec);
                    (Type == date) ->       string_to_date(Val);
                    (Type == double) ->     string_to_double(Val,Prec);
                    (Type == eatom) ->      list_to_atom(Val);
                    (Type == ebinary) ->    string_to_ebinary(Val,Len);
                    (Type == ebinstr) ->    list_to_binary(Val);
                    (Type == edatetime) ->  string_to_edatetime(Val);
                    (Type == efun) ->       string_to_fun(Val,Len);
                    (Type == eipaddr) ->    string_to_eipaddr(Val,Len);
                    (Type == elist) ->      string_to_elist(Val,Len);
                    (Type == enum) ->       string_to_enum(Val,Len);
                    (Type == epid) ->       list_to_pid(Val);
                    (Type == eref) ->       Old;    %% cannot convert back
                    (Type == estring) ->    string_to_list(Val,Len);
                    (Type == etimestamp) -> string_to_etimestamp(Val); 
                    (Type == etuple) ->     string_to_etuple(Val,Len);
                    (Type == float) ->      string_to_float(Val,Prec);
                    (Type == int) ->        string_to_integer(Val,Len,Prec);
                    (Type == integer) ->    string_to_integer(Val,Len,Prec);
                    (Type == longblob) ->   string_to_ebinary(Val,Len);
                    (Type == longtext) ->   string_to_list(Val,map(Len,0,4294967295));
                    (Type == mediumint) ->  string_to_integer(Val,Len,Prec);
                    (Type == mediumtext) -> string_to_list(Val,map(Len,0,16777215));
                    (Type == number) ->     string_to_number(Val,Len,Prec);
                    (Type == numeric) ->    string_to_number(Val,Len,Prec);
                    (Type == set) ->        string_to_set(Val,Len);
                    (Type == smallint) ->   string_to_integer(Val,Len,Prec);
                    (Type == text) ->       string_to_list(Val,map(Len,0,65535));
                    (Type == time) ->       string_to_time(Val);
                    (Type == timestamp) ->  string_to_timestamp(Val,Prec);
                    (Type == tinyint) ->    string_to_integer(Val,Len,Prec);
                    (Type == tinytext) ->   string_to_list(Val,map(Len,0,255));
                    (Type == varchar) ->    string_to_list(Val,map(Len,0,255));
                    (Type == varchar2) ->   string_to_list(Val,map(Len,0,4000));
                    (Type == year) ->       string_to_year(Val);                    
                    true ->                 string_to_eterm(Val)   
                end;
            true -> Val
        end
    catch
        _:{'UnimplementedException',_} ->       ?ClientError({"Unimplemented data type conversion",{Item,{Type,Val}}});
        _:{'ClientError', {Text, Reason}} ->    ?ClientError({Text, {Item,Reason}});
        _:_ ->                                  ?ClientError({"Data conversion format error",{Item,{pretty_type(Type),Val}}})
    end;
value_to_db(_Item,_Old,_Type,_Len,_Prec,_Def,_RO,Val) -> Val.    


string_to_list(Val,Len) ->
    if 
        Len == 0 ->             Val;
        length(Val) > Len ->    ?ClientError({"Data conversion format error",{string,Len,Val}});
        true ->                 Val
    end.

string_to_integer(Val,Len,Prec) ->
    Value = case string_to_eterm(Val) of
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
    Value = case string_to_eterm(Val) of
        V when is_float(V) ->   V;
        V when is_integer(V) -> float(V);
        _ ->                    ?ClientError({"Data conversion format error",{float,Prec,Val}})
    end,
    if 
        Prec == 0 ->    Value;
        true ->         erlang:round(math:pow(10, Prec) * Value) * math:pow(10,-Prec)
    end.

string_to_set(_Val,_Len) -> ?UnimplementedException({}).
string_to_enum(_Val,_Len) -> ?UnimplementedException({}).
string_to_double(_Val,_Prec) -> ?UnimplementedException({}).  %% not in erlang:math ??
string_to_ebinary(_Val,_Len) -> ?UnimplementedException({}).

string_to_date(Val) -> 
    string_to_edatetime(Val).

string_to_time("systime") ->
    string_to_time("localtime");
string_to_time("sysdate") ->
    string_to_time("localtime");
string_to_time("now") ->
    string_to_time("localtime");
string_to_time("localtime") -> 
    {_,Time} = string_to_edatetime("localtime"),
    Time;
string_to_time(Val) -> 
    try
        parse_time(Val)
    catch
        _:_ ->  ?ClientError({"Data conversion format error",{eTime,Val}})
    end.

string_to_timestamp("systime",Prec) ->
    string_to_timestamp("now",Prec);
string_to_timestamp("sysdate",Prec) ->
    string_to_timestamp("now",Prec);
string_to_timestamp("now",Prec) ->
    {Megas,Secs,Micros} = erlang:now(),    
    {Megas,Secs,erlang:round(erlang:round(math:pow(10, Prec-6) * Micros) * erlang:round(math:pow(10,6-Prec)))};  
string_to_timestamp(Val,6) ->
    string_to_etimestamp(Val); 
string_to_timestamp(Val,0) ->
    {Megas,Secs,_} = string_to_etimestamp(Val),
    {Megas,Secs,0};
string_to_timestamp(Val,Prec) when Prec > 0 ->
    {Megas,Secs,Micros} = string_to_etimestamp(Val),
    {Megas,Secs,erlang:round(erlang:round(math:pow(10, Prec-6) * Micros) * erlang:round(math:pow(10,6-Prec)))};
string_to_timestamp(Val,Prec) when Prec < 0 ->
    {Megas,Secs,_} = string_to_etimestamp(Val),
    {Megas,erlang:round(erlang:round(math:pow(10, -Prec) * Secs) * erlang:round(math:pow(10,Prec))),0}.  

string_to_edatetime("today") ->
    {Date,_} = string_to_edatetime("localtime"),
    {Date,{0,0,0}}; 
string_to_edatetime("systime") ->
    string_to_edatetime("localtime"); 
string_to_edatetime("sysdate") ->
    string_to_edatetime("localtime"); 
string_to_edatetime("now") ->
    string_to_edatetime("localtime"); 
string_to_edatetime("localtime") ->
    erlang:localtime(); 
string_to_edatetime(Val) ->
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

string_to_year(Val) -> 
    try     
        parse_year(Val)
    catch
        _:_ ->  ?ClientError({"Data conversion format error",{year,Val}})
    end.    

string_to_etimestamp("sysdate") ->
    erlang:now();
string_to_etimestamp("now") ->
    erlang:now();
string_to_etimestamp(Val) ->
    try     
        case string:tokens(Val, ":") of
            [Megas,Secs,Micros] ->  {list_to_integer(Megas),list_to_integer(Secs),list_to_integer(Micros)};
            _ ->                    ?ClientError({})
        end
    catch
        _:_ ->  ?ClientError({"Data conversion format error",{eTimestamp,Val}})
    end.    

string_to_eipaddr(Val,Len) ->
    Result = try 
        [ip_item(Item) || Item <- string:tokens(Val, ".")]
    catch
        _:_ -> ?ClientError({"Data conversion format error",{eIpaddr,Len,Val}})
    end,
    RLen = length(lists:flatten(Result)),
    if 
        Len == 0 andalso RLen == 4 ->   list_to_tuple(Result);
        Len == 0 andalso RLen == 6 ->   list_to_tuple(Result);
        Len == 0 andalso RLen == 8 ->   list_to_tuple(Result);
        RLen /= Len ->                  ?ClientError({"Data conversion format error",{eIpaddr,Len,Val}});
        true ->                         list_to_tuple(Result)
    end.

ip_item(Val) ->
    case string_to_eterm(Val) of
        V when is_integer(V), V >= 0, V < 256  ->   V;
        _ ->                                        ?ClientError({})
    end.

string_to_number(Val,Len,Prec) ->
    string_to_decimal(Val,Len,Prec). 

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

string_to_elist(Val, 0) -> 
    case string_to_eterm(Val) of
        V when is_list(V) ->    V;
        _ ->                    ?ClientError({"Data conversion format error",{eList,0,Val}})
    end;
string_to_elist(Val,Len) -> 
    case string_to_eterm(Val) of
        V when is_list(V) ->
            if 
                length(V) == Len -> V;
                true ->             ?ClientError({"Data conversion format error",{eList,Len,Val}})
            end;
        _ ->
            ?ClientError({"Data conversion format error",{eTuple,Len,Val}})
    end.

string_to_etuple(Val,0) -> 
    case string_to_eterm(Val) of
        V when is_tuple(V) ->   V;
        _ ->                    ?ClientError({"Data conversion format error",{eTuple,0,Val}})
    end;
string_to_etuple(Val,Len) -> 
    case string_to_eterm(Val) of
        V when is_tuple(V) ->
            if 
                size(V) == Len ->   V;
                true ->             ?ClientError({"Data conversion format error",{eTuple,Len,Val}})
            end;
        _ ->
            ?ClientError({"Data conversion format error",{eTuple,Len,Val}})
    end.

string_to_eterm(Val) -> 
    try
        F = erl_value(Val),
        if 
            is_function(F,0) ->
                ?ClientError({"Data conversion format error",{eTerm,Val}}); 
            true ->                 
                F
        end
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

db_to_string(Type, Len, Prec, Val) ->
    if 
        (Type == bigint) ->     integer_to_list(Val);
%       (Type == blob) ->       string_to_ebinary(Val,Len);
        (Type == character) ->  Val;
        (Type == decimal) ->    decimal_to_string(Val,Prec);
        (Type == date) ->       date_to_string(Val);
        (Type == double) ->     string_to_double(Val,Prec);
        (Type == eatom) ->      list_to_atom(Val);
        (Type == ebinary) ->    string_to_ebinary(Val,Len);
        (Type == ebinstr) ->    list_to_binary(Val);
        (Type == edatetime) ->  string_to_edatetime(Val);
        (Type == efun) ->       string_to_fun(Val,Len);
        (Type == eipaddr) ->    string_to_eipaddr(Val,Len);
        (Type == elist) ->      string_to_elist(Val,Len);
        (Type == enum) ->       string_to_enum(Val,Len);
        (Type == epid) ->       list_to_pid(Val);
        (Type == estring) ->    string_to_list(Val,Len);
        (Type == etimestamp) -> string_to_etimestamp(Val); 
        (Type == etuple) ->     string_to_etuple(Val,Len);
        (Type == float) ->      string_to_float(Val,Prec);
        (Type == int) ->        string_to_integer(Val,Len,Prec);
        (Type == integer) ->    string_to_integer(Val,Len,Prec);
        (Type == longblob) ->   string_to_ebinary(Val,Len);
        (Type == longtext) ->   string_to_list(Val,map(Len,0,4294967295));
        (Type == mediumint) ->  string_to_integer(Val,Len,Prec);
        (Type == mediumtext) -> string_to_list(Val,map(Len,0,16777215));
        (Type == number) ->     string_to_number(Val,Len,Prec);
        (Type == numeric) ->    string_to_number(Val,Len,Prec);
        (Type == set) ->        string_to_set(Val,Len);
        (Type == smallint) ->   string_to_integer(Val,Len,Prec);
        (Type == text) ->       string_to_list(Val,map(Len,0,65535));
        (Type == time) ->       string_to_time(Val);
        (Type == timestamp) ->  string_to_timestamp(Val,Prec);
        (Type == tinyint) ->    string_to_integer(Val,Len,Prec);
        (Type == tinytext) ->   string_to_list(Val,map(Len,0,255));
        (Type == varchar) ->    string_to_list(Val,map(Len,0,255));
        (Type == varchar2) ->   string_to_list(Val,map(Len,0,4000));
        (Type == year) ->       string_to_year(Val);                    
        true ->                 string_to_eterm(Val)   
    end.

date_to_string(Datetime) ->
    date_to_string(Datetime, eu).

date_to_string({{Year,Month,Day},{Hour,Min,Sec}},eu) ->
    io_lib:format("~2.10.0B.~2.10.0B.~4.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Day, Month, Year, Hour, Min, Sec]);
date_to_string({{Year,Month,Day},{Hour,Min,Sec}},raw) ->
    io_lib:format("~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",
        [Year, Month, Day, Hour, Min, Sec]);
date_to_string({{Year,Month,Day},{Hour,Min,Sec}},iso) ->
    io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Year, Month, Day, Hour, Min, Sec]);
date_to_string({{Year,Month,Day},{Hour,Min,Sec}},us) ->
    io_lib:format("~2.10.0B/~2.10.0B/~4.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Month, Day, Year, Hour, Min, Sec]);
date_to_string(Datetime, Fmt) ->
    ?ClientError({"Data conversion format error",{eDatetime,Fmt,Datetime}}).


decimal_to_string(Val,0) ->
    integer_to_list(Val);
decimal_to_string(Val,Prec) when Val < 0 ->
    lists:concat(["-",decimal_to_string(-Val,Prec)]);
decimal_to_string(Val,Prec) when Prec > 0 ->
    Str = integer_to_list(Val),
    Len = length(Str),
    if 
        Prec-Len+1 > 0 -> 
            {Whole,Frac} = lists:split(1,lists:duplicate(Prec-Len+1,$0) ++ Str),
            lists:concat([Whole,".",Frac]);
        true ->
            {Whole,Frac} = lists:split(Len-Prec,Str),
            lists:concat([Whole,".",Frac])
    end;
decimal_to_string(Val,Prec) ->
    lists:concat([integer_to_list(Val),lists:duplicate(-Prec,$0)]).


ebinary_to_string(_Val) -> ?UnimplementedException({}).

edatetime_to_string(Val) ->
    date_to_string(Val).

edatetime_to_string(Val,Fmt) ->
    date_to_string(Val,Fmt).

eipaddr_to_string({A,B,C,D}) ->
    lists:concat([integer_to_list(A),".",integer_to_list(B),".",integer_to_list(C),".",integer_to_list(D)]);
eipaddr_to_string(_IpAddr) -> ?UnimplementedException({}).


etimestamp_to_string({Megas,Secs,Micros}) ->
    lists:concat([integer_to_list(Megas),":",integer_to_list(Secs),":",integer_to_list(Micros)]).    

number_to_string(Val,Prec)->
    decimal_to_string(Val,Prec).

time_to_string(_Val) -> ?UnimplementedException({}).

timestamp_to_string(Timestamp) ->
    etimestamp_to_string(Timestamp).

year_to_string(Year) ->
    integer_to_list(Year).

%% ----- Data Type Conversions ----------------------------------------

localTimeToSysDate(LTime) -> LTime. %% ToDo: convert to oracle 7 bit date

nowToSysTimeStamp(Now) -> Now.      %% ToDo: convert to oracle 20 bit timestamp


%% ----- Helper Functions ---------------------------------------------

map(Val,From,To) ->
    if 
        Val == From ->  To;
        true ->         Val
    end.

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

        ?assertEqual("123", decimal_to_string(123,0)),
        ?assertEqual("-123", decimal_to_string(-123,0)),
        ?assertEqual("12.3", decimal_to_string(123,1)),
        ?assertEqual("-12.3", decimal_to_string(-123,1)),
        ?assertEqual("0.123", decimal_to_string(123,3)),
        ?assertEqual("-0.123", decimal_to_string(-123,3)),
        ?assertEqual("0.00123", decimal_to_string(123,5)),
        ?assertEqual("-0.00123", decimal_to_string(-123,5)),

        ?assertEqual("0.0.0.0", eipaddr_to_string({0,0,0,0})),
        ?assertEqual("1.2.3.4", eipaddr_to_string({1,2,3,4})),

        ?assertEqual("1:2:3", etimestamp_to_string({1,2,3})),

        ?assertEqual("-0.00123", number_to_string(-123,5)),
        ?assertEqual("12300000", number_to_string(123,-5)),
        ?assertEqual("-12300000", number_to_string(-123,-5)),

        LocalTime = erlang:localtime(),
        {Date,Time} = LocalTime,
        ?assertEqual({{2004,3,1},{0,0,0}}, string_to_edatetime("1.3.2004")),
        ?assertEqual({{2004,3,1},{3,45,0}}, string_to_edatetime("1.3.2004 3:45")),
        ?assertEqual({Date,{0,0,0}}, string_to_edatetime("today")),
        ?assertEqual(LocalTime, string_to_edatetime("sysdate")),
        ?assertEqual(LocalTime, string_to_edatetime("systime")),
        ?assertEqual(LocalTime, string_to_edatetime("now")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_edatetime("18.8.1888 1:23:59")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_edatetime("1888-08-18 1:23:59")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_edatetime("8/18/1888 1:23:59")),
        ?assertEqual({{1888,8,18},{1,23,0}}, string_to_edatetime("8/18/1888 1:23")),
        ?assertEqual({{1888,8,18},{1,0,0}}, string_to_edatetime("8/18/1888 01")),
        ?assertException(throw,{ClEr,{"Data conversion format error",{eDatetime,"8/18/1888 1"}}}, string_to_edatetime("8/18/1888 1")),
        ?assertEqual({{1888,8,18},{0,0,0}}, string_to_edatetime("8/18/1888 ")),
        ?assertEqual({{1888,8,18},{0,0,0}}, string_to_edatetime("8/18/1888")),
        ?assertEqual({1,23,59}, parse_time("01:23:59")),        
        ?assertEqual({1,23,59}, parse_time("1:23:59")),        
        ?assertEqual({1,23,0}, parse_time("01:23")),        
        ?assertEqual({1,23,59}, parse_time("012359")),        
        ?assertEqual({1,23,0}, parse_time("0123")),        
        ?assertEqual({1,0,0}, parse_time("01")),        
        ?assertEqual({0,0,0}, parse_time("")),        
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_edatetime("18880818012359")),
        ?assertEqual(Time, string_to_time("systime")),
        ?assertEqual(Time, string_to_time("sysdate")),
        ?assertEqual(Time, string_to_time("now")),
        ?assertEqual(Time, string_to_time("localtime")),
        ?assertEqual({12,13,14}, string_to_time("12:13:14")),
        ?assertEqual({0,1,11}, string_to_time("0:1:11")),

        Item = 0,
        OldString = "OldString",
        StringType = estring,
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
        ?assertException(throw,{ClEr,{"Data conversion format error",{0,{string,3,"NewVal"}}}}, value_to_db(Item,OldString,StringType,Len,Prec,Def,RW,"NewVal")),
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

        ETermType = eterm,
        OldETerm = {-1.2,[a,b,c]},
        ?assertEqual(OldETerm, value_to_db(Item,OldETerm,ETermType,Len,Prec,Def,RW,OldETerm)),
        ?assertEqual(default, value_to_db(Item,OldETerm,ETermType,Len,Prec,Def,RW,"default")),
        ?assertEqual([a,b], value_to_db(Item,OldETerm,ETermType,Len,Prec,Def,RW,"[a,b]")),
        ?assertEqual(-1.1234567, value_to_db(Item,OldETerm,ETermType,Len,Prec,Def,RW,"-1.1234567")),
        ?assertEqual({[1,2,3]}, value_to_db(Item,OldETerm,ETermType,Len,Prec,Def,RW,"{[1,2,3]}")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eTerm,"[a|]"}}}}, value_to_db(Item,OldETerm,ETermType,Len,Prec,Def,RW,"[a|]")),

        ?assertEqual({1,2,3}, value_to_db(Item,OldETerm,eTuple,Len,Prec,Def,RW,"{1,2,3}")),
        ?assertEqual({}, value_to_db(Item,OldETerm,eTuple,0,Prec,Def,RW,"{}")),
        ?assertEqual({1}, value_to_db(Item,OldETerm,eTuple,0,Prec,Def,RW,"{1}")),
        ?assertEqual({1,2}, value_to_db(Item,OldETerm,eTuple,0,Prec,Def,RW,"{1,2}")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eTuple,Len,"[a]"}}}}, value_to_db(Item,OldETerm,etuple,Len,Prec,Def,RW,"[a]")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eTuple,Len,"{a}"}}}}, value_to_db(Item,OldETerm,etuple,Len,Prec,Def,RW,"{a}")),

        ?assertEqual([a,b,c], value_to_db(Item,OldETerm,elist,Len,Prec,Def,RW,"[a,b,c]")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eList,Len,"[a]"}}}}, value_to_db(Item,OldETerm,elist,Len,Prec,Def,RW,"[a]")),
        ?assertEqual([], value_to_db(Item,[],eList,Len,Prec,Def,RW,"[]")),
        ?assertEqual([], value_to_db(Item,OldETerm,eList,Len,Prec,[],RW,"[]")),
        ?assertEqual([], value_to_db(Item,OldETerm,eList,0,Prec,Def,RW,"[]")),
        ?assertEqual([a], value_to_db(Item,OldETerm,eList,0,Prec,Def,RW,"[a]")),
        ?assertEqual([a,b], value_to_db(Item,OldETerm,eList,0,Prec,Def,RW,"[a,b]")),

        ?assertEqual({10,132,7,92}, value_to_db(Item,OldETerm,eipaddr,0,Prec,Def,RW,"10.132.7.92")),
        ?assertEqual({0,0,0,0}, value_to_db(Item,OldETerm,eipaddr,4,Prec,Def,RW,"0.0.0.0")),
        ?assertEqual({255,255,255,255}, value_to_db(Item,OldETerm,eipaddr,4,Prec,Def,RW,"255.255.255.255")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eIpaddr,0,"1.2.3.4.5"}}}}, value_to_db(Item,OldETerm,eipaddr,0,0,Def,RW,"1.2.3.4.5")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eIpaddr,4,"1.2.-1.4"}}}}, value_to_db(Item,OldETerm,eipaddr,4,0,Def,RW,"1.2.-1.4")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eIpaddr,6,"1.256.1.4"}}}}, value_to_db(Item,OldETerm,eipaddr,6,0,Def,RW,"1.256.1.4")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eIpaddr,8,"1.2.1.4"}}}}, value_to_db(Item,OldETerm,eipaddr,8,0,Def,RW,"1.2.1.4")),

        Fun = fun(X) -> X*X end,
        io:format(user, "Fun ~p~n", [Fun]),
        Res = value_to_db(Item,OldETerm,efun,1,Prec,Def,RW,"fun(X) -> X*X end"),
        io:format(user, "Run ~p~n", [Res]),
        ?assertEqual(Fun(4), Res(4)),

        ?assertEqual(true, true)
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
