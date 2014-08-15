%% -*- coding: utf-8 -*-
-module(imem_index).

%% @doc == imem INDEX operations ==

-include("imem.hrl").
-include("imem_meta.hrl").


-export([binstr_to_lcase_ascii/1,

         binstr_accentfold/1,
         binstr_to_lower/1,
         binstr_to_upper/1,
         binstr_only_ascii/1, binstr_only_ascii/2,
         binstr_only_valid/1, binstr_only_valid/2,
         binstr_only_latin1/1, binstr_only_latin1/2,

         binstr_match_anywhere/2,
         binstr_match_sub/4,
         binstr_match_precompile/1
		]).

-export([remove/2       %% (IndexTable,Removes)
        ,insert/2       %% (IndexTable,Inserts)
        ]).

-export([iff_true/1
        ]).

-define(BIN_APP,binstr_append_placeholder).

remove(_IndexTable,[]) -> ok;
remove(IndexTable,[{ID,ivk,Key,Value}|Items]) ->
    imem_if:delete(IndexTable,{ID,Value,Key}),
    remove(IndexTable,Items);
    %ToDo: implement iv_kl, iv_h, ...
remove(IndexTable,[{ID,iv_k,_,Value}|Items]) ->
    imem_if:delete(IndexTable,{ID,Value}),
    remove(IndexTable,Items).

insert(_IndexTable,[]) -> ok;
insert(IndexTable,[{ID,ivk,Key,Value}|Items]) ->
    imem_if:write(IndexTable,#ddIndex{stu={ID,Value,Key}}),
    insert(IndexTable,Items);
    %ToDo: implement iv_kl, iv_h, ...
insert(IndexTable,[{ID,iv_k,Key,Value}|Items]) ->
    case imem_if:read(IndexTable,{ID,Value}) of
        [] ->                   imem_if:write(IndexTable,#ddIndex{stu={ID,Value},lnk=Key});
        [#ddIndex{lnk=K0}] ->   ?ClientError({"Unique index violation",{IndexTable,ID,Value,K0}})
    end,
    insert(IndexTable,Items).

binstr_to_lcase_ascii(<<"\"\"">>) -> <<>>; 
binstr_to_lcase_ascii(B) when is_binary(B) -> 
    %% unicode_string_only_ascii(string:to_lower(unicode:characters_to_list(B, utf8)));
    binstr_only_ascii(
        binstr_accentfold(
            binstr_to_lower(B)
            )
        );
binstr_to_lcase_ascii(Val) -> 
	% unicode_string_only_ascii(io_lib:format("~p",[Val])).
    BinStr = try io_lib:format("~s",[Val])
             catch error:badarg -> io_lib:format("~p",[Val]) end,
    binstr_only_ascii(
        binstr_accentfold(
            binstr_to_lower(
                unicode:characters_to_binary(BinStr)
                )
            )
        ).


%% Glossary:
%% ¯¯¯¯¯¯¯¯¯
%% IndexId: 
%%      ID of the index. (indexes share the same table, ID is used to
%%      differentiate indexes on different fields).
%% Search key: 
%%      Key on which the search gets done
%% Reference key: 
%%      Sometimes used key to store reference
%% Reference: 
%%      ID/Key of the object holding the value in the master table
%% FastLookupNumber:
%%      Plain integer or short hash of a value
%%
%%
%% Index Types:
%% ¯¯¯¯¯¯¯¯¯¯¯¯
%% ivk: default index type
%%          stu =  {IndexId,<<"Value">>,Reference}
%%          lnk =  0
%%
%% iv_k: unique key index
%%          stu =  {IndexId,<<"UniqueValue">>}
%%          lnk =  Reference
%%       observation: should crash/throw/error on duplicate value insertion
%%
%% iv_kl: high selectivity index (aka "almost unique")
%%          stu =  {IndexId,<<"AlmostUniqueValue"}
%%          lnk =  [Reference | ListOfReferences]
%%
%% iv_h: low selectivity hash map index 
%%          For the values:
%%              stu =  {IndexId,<<"CommonValue">>}
%%              lnk =  FastLookupNumber
%%          For the links to the references:
%%              stu =  {IndexId, {FastLookupNumber, Reference}}
%%              lnk =  0
%%
%% ivvk: combined index of 2 fields
%%          stu =  {IndexId,<<"ValueA">>,<<"ValueB">>,Reference}
%%          lnk =  0
%%
%% ivvvk: combined index of 3 fields
%%          stu =  {IndexId,<<"ValueA">>,<<"ValueB">>,<<"ValueB">>,Reference}
%%          lnk =  0
%%
%% How it should be used:
%% ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
%% Basically, it's an mnesia-managed orderes set ETS table, where one uses regexp or binary_match
%% operations to iterate on and find matching values and their link back to the objects
%% stored in the master table.
%%
%% It avoids the need to decode raw binary json documents stored in the master table, for
%% faster filtering/searching.
%%
%% It could also be used to provide search-term and/or auto-correction suggestions.
%%
%% Index SHOULD NOT normalize (accent fold, lowercase, ...). That should be left over 
%% to higher level processes (this precludes the use of binary:match/2 for any matching,
%% because case insensitivity can not be guaranteed. Twice as slow regexp will have to be
%% used instead).
%%
%% Suggested implementation:
%% ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
%% As a simple_one_for_one gen_server, so index queries can be non-blocking and resolved
%% in parallel, while still being supervised.
%%
%% Index queries could also use the module as a library, having access to all its functionality,
%% but in a sequential, single-threaded way.
%% 
%% Offered functions would abstract different modes of usage, through the use of an
%% environment setting, constant or even global variable.
%%
%%
%% Observations:
%% ¯¯¯¯¯¯¯¯¯¯¯¯¯
%% - imem_index should use imem_if primitives to access data
%%
%% Proposed functionality:
%% ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
%% case insensitive search: 
%%    - provide IndexId, input string, Limit
%%    - output format:	[ {headmatch, HeadMatchString, HeadMatchResults}
%%						, {anymatch, AnyMatchString, AnyMatchResults}
%%						, {regexpmatch, RegexpMatchString, RegexpMatchResults}
%%						]
%%
%% How it should work:
%%    If input string contains wildcards or regexp-like characters (*?%_)
%%		-> convert to regexp pattern, and perform only a regexp-match. Other result "sets" will be empty.
%% 	  Else
%%    	Should first execute headmatch.
%%		If enough results
%%		  ->	other result "sets" will be empty
%%		Else (not enough results)
%%        -> do anymatch (basic binary_match inside string)


%% @doc Supports accent folding for all alphabetical characters supported by ISO 8859-15
%% ISO 8859-15 supports the following languages: Albanian, Basque, Breton,
%% Catalan,  Danish,  Dutch,  English, Estonian, Faroese, Finnish, French,
%% Frisian,  Galician,  German,  Greenlandic,  Icelandic,  Irish   Gaelic,
%% Italian,  Latin,  Luxemburgish,  Norwegian, Portuguese, Rhaeto-Romanic,
%% Scottish Gaelic, Spanish, and Swedish.
-spec binstr_accentfold(binary()) -> binary().
binstr_accentfold(Str) when is_binary(Str) ->
   b_convert(Str,<<>>). 

    b_convert(<<>>,A) -> A;
    b_convert(<<195,C,R/binary>>,A) when C >= 128, C =<  133 -> % À Á Â Ã Ä Å
        b_convert(R,<<A/binary,$A>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 136, C =<  139 -> % È É Ê Ë
        b_convert(R,<<A/binary,$E>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 140, C =<  143 -> % Ì Í Î Ï 
        b_convert(R,<<A/binary,$I>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 146, C =<  150; C =:= 152 -> % Ò Ó Ô Õ Ö Ø
        b_convert(R,<<A/binary,$O>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 153, C =<  156 -> % Ù Ú Û Ü
        b_convert(R,<<A/binary,$U>>);   
    b_convert(<<195,C,R/binary>>,A) when C =:= 157 -> % Ý
        b_convert(R,<<A/binary,$Y>>);   
    b_convert(<<197,C,R/binary>>,A) when C =:= 184 -> % CAPITAL LETTER Y WITH DIAERESIS
        b_convert(R,<<A/binary,$Y>>);   
    b_convert(<<195,C,R/binary>>,A) when C =:= 135 -> % Ç
        b_convert(R,<<A/binary,$C>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 145 -> % Ñ
        b_convert(R,<<A/binary,$N>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 134 -> % Æ -> AE
        b_convert(R,<<A/binary,$A,$E>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 158 -> % Þ -> TH
        b_convert(R,<<A/binary,$T,$H>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 144 -> % Ð -> D
        b_convert(R,<<A/binary,$D>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 160 -> % S WITH CARON
        b_convert(R,<<A/binary,$S>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 189 -> % Z WITH CARON
        b_convert(R,<<A/binary,$Z>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 146 -> % OE LIGATURE -> OE
        b_convert(R,<<A/binary,$O,$E>>); 
    
    b_convert(<<195,C,R/binary>>,A) when C >= 160, C =<  165 -> % à á â ã ä å
        b_convert(R,<<A/binary,$a>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 168, C =<  171 -> % è é ê ë
        b_convert(R,<<A/binary,$e>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 172, C =<  175 -> % ì í î ï
        b_convert(R,<<A/binary,$i>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 178, C =<  182; C =:= 184 -> % ò ó ô õ ö ø
        b_convert(R,<<A/binary,$o>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 185, C =<  188 -> % ù ú û ü
        b_convert(R,<<A/binary,$u>>);   
    b_convert(<<195,C,R/binary>>,A) when C =:= 189; C =:=  191 -> % ý ÿ
        b_convert(R,<<A/binary,$y>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 167 -> % ç
        b_convert(R,<<A/binary,$c>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 166 -> % æ -> ae
        b_convert(R,<<A/binary,$a,$e>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 177 -> % ñ
        b_convert(R,<<A/binary,$n>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 161 -> % s WITH CARON
        b_convert(R,<<A/binary,$s>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 190 -> % z WITH CARON
        b_convert(R,<<A/binary,$z>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 159 -> % ß -> ss
        b_convert(R,<<A/binary,$s,$s>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 147 -> % oe LIGATURE -> oe
        b_convert(R,<<A/binary,$o,$e>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 190 -> % þ -> th
        b_convert(R,<<A/binary,$t,$h>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 176 -> % ð -> d
        b_convert(R,<<A/binary,$d>>); 
    
    b_convert(<<H,R/binary>>,A) -> 
        b_convert(R,<<A/binary,H>>).

%% @doc lowercases all alphabetical characters supported by ISO 8859-15
-spec binstr_to_lower(binary()) -> binary().
binstr_to_lower(Str) when is_binary(Str) ->
    b_lower(Str,<<>>).

    b_lower(<<>>,A) -> A;
    b_lower(<<195,C,R/binary>>,A) when C >= 128, C =<  150; C >= 152, C =< 158-> % Standard range
        NC = C + 32,
        b_lower(R,<<A/binary,195,NC>>);
    b_lower(<<197,C,R/binary>>,A) when C =:= 160; C =:= 146; C =:= 189-> % Non-standard range
        NC = C + 1,
        b_lower(R,<<A/binary,197,NC>>);
    b_lower(<<197,C,R/binary>>,A) when C =:= 184-> % Special case CAPITAL LETTER Y WITH DIAERESIS
        b_lower(R,<<A/binary,195,191>>);
    b_lower(<<C,R/binary>>,A) when C >=  65, C =< 90 -> 
        NC = C + 32,
        b_lower(R,<<A/binary,NC>>);
    b_lower(<<H,R/binary>>,A) -> 
        b_lower(R,<<A/binary,H>>).
 
%% @doc uppercases all alphabetical characters supported by ISO 8859-15
-spec binstr_to_upper(binary()) -> binary().
binstr_to_upper(Str) when is_binary(Str) ->
    b_upper(Str,<<>>).

    b_upper(<<>>,A) -> A;

    b_upper(<<195,C,R/binary>>,A) when C >= 160, C =<  182; C >= 184, C =< 190-> % Standard range
        NC = C - 32,
        b_upper(R,<<A/binary,195,NC>>);
    b_upper(<<197,C,R/binary>>,A) when C =:= 161; C =:= 147; C =:= 190-> % Non-standard range
        NC = C - 1,
        b_upper(R,<<A/binary,197,NC>>);
    b_upper(<<195,C,R/binary>>,A) when C =:= 191-> % Special case CAPITAL LETTER Y WITH DIAERESIS
        b_upper(R,<<A/binary,197,184>>);
    b_upper(<<C,R/binary>>,A) when C >=  97, C =< 122 -> 
        NC = C - 32,
        b_upper(R,<<A/binary,NC>>);
    b_upper(<<H,R/binary>>,A) -> 
        b_upper(R,<<A/binary,H>>).


%% @doc Walks binary string, keeps only valid ascii characters
-spec binstr_only_ascii(binary()) -> binary().
binstr_only_ascii(BinStr) when is_binary(BinStr) ->
    binstr_only_ascii(BinStr,"");
binstr_only_ascii(BinStr) when is_list(BinStr) ->
    binstr_only_ascii(list_to_binary(BinStr),"").
binstr_only_ascii(BinStr,PlaceHolder) ->
    b_afilter(BinStr,PlaceHolder,<<>>).

    b_afilter(<<>>,_,A) -> A;
    % Unprintable characters:0 - 9, 11, 12, 14 - 31, 127
    b_afilter(<<C,R/binary>>,PH,A) when C >= 0, C =<  9;
                                       C =:= 11; C =:= 12;
                                       C >= 14, C =< 31;
                                       C >= 127 ->

        b_vfilter(R,PH,?BIN_APP(A,PH));
    b_afilter(<<C,R/binary>>,PH,A) ->
        b_afilter(R,PH,<<A/binary,C>>).

%% @doc Walks binary string, keeps only valid and displayable) utf8 characters
-spec binstr_only_valid(binary()) -> binary().
binstr_only_valid(Binstr) ->
    binstr_only_valid(Binstr,"").
binstr_only_valid(BinStr, PH) when is_binary(BinStr) ->
    b_vfilter(BinStr,PH,<<>>).

    b_vfilter(<<>>,_,A) -> A;
    % Displayable One-byte UTF8 (== ASCII)
    b_vfilter(<<C,R/binary>>,PH,A) when C =:= 10; % \n
                                       C =:= 13;  % \r
                                       C >= 32, C =<  126 % character range
                                       ->
        b_vfilter(R,PH,<<A/binary,C>>);
    % Two-byte UTF8 192-223, 128-191
    b_vfilter(<<M,C,R/binary>>,PH,A) when M >= 195, M =< 223, C >= 128, C =<  191;
                                          M >= 194, C >= 160, C =< 191 ->
        b_vfilter(R,PH,<<A/binary,M,C>>);

    % Three-byte UTF8 224-239, 128-191, 128-191
    b_vfilter(<<M,C1,C2,R/binary>>,PH,A) when M >= 224, M =< 239,
                                              C1 >= 128, C1 =<  191,
                                              C2 >= 128, C2 =<  191 ->
        b_vfilter(R,PH,<<A/binary,M,C1,C2>>);
   
    % Four-byte UTF8 240-247, 128-191, 128-191, 128-191
    b_vfilter(<<M,C1,C2,C3,R/binary>>,PH,A) when M >= 240, M =< 247,
                                              C1 >= 128, C1 =<  191,
                                              C2 >= 128, C2 =<  191,
                                              C3 >= 128, C3 =<  191 ->
        b_vfilter(R,PH,<<A/binary,M,C1,C2,C3>>);
    % Five-byte UTF8 248-251, 128-191, 128-191, 128-191, 128-191
    b_vfilter(<<M,C1,C2,C3,C4,R/binary>>,PH,A) when M >= 248, M =< 251,
                                              C1 >= 128, C1 =<  191,
                                              C2 >= 128, C2 =<  191,
                                              C3 >= 128, C3 =<  191,
                                              C4 >= 128, C4 =<  191 ->
        b_vfilter(R,PH,<<A/binary,M,C1,C2,C3,C4>>);
    % Six-byte UTF8 252-253, 128-191, 128-191, 128-191, 128-191, 128-191, 
    b_vfilter(<<M,C1,C2,C3,C4,C5,R/binary>>,PH,A) when M >= 252, M =< 253,
                                              C1 >= 128, C1 =<  191,
                                              C2 >= 128, C2 =<  191,
                                              C3 >= 128, C3 =<  191,
                                              C4 >= 128, C4 =<  191,
                                              C5 >= 128, C5 =<  191 ->
        b_vfilter(R,PH,<<A/binary,M,C1,C2,C3,C4,C5>>);
        
    % Everything else (garbage)
    b_vfilter(<<_,R/binary>>,PH,A) ->
        b_vfilter(R,PH,?BIN_APP(A,PH)).

%% @doc Walks binary string, keeps only valid and displayable) utf8 characters
%% also present in the latin1 characterset
-spec binstr_only_latin1(binary()) -> binary().
binstr_only_latin1(Binstr) ->
    binstr_only_latin1(Binstr,"").

binstr_only_latin1(BinStr, PH) when is_binary(BinStr) ->
    b_lfilter(BinStr,PH,<<>>).

    b_lfilter(<<>>,_,A) -> A;
    % Displayable One-byte UTF8 (== ASCII)
    b_lfilter(<<C,R/binary>>,PH,A) when C =:= 10; % \n
                                       C =:= 13;  % \r
                                       C >= 32, C =<  126 % character range
                                       ->
        b_lfilter(R,PH,<<A/binary,C>>);
    % Two-byte UTF8 194-197, 128-191, basic latin1 extension
    b_lfilter(<<M,C,R/binary>>,PH,A) when M >= 195, M =< 197, C >= 128, C =<  191;
                                          M >= 194, C >= 160, C =< 191 ->
        b_lfilter(R,PH,<<A/binary,M,C>>);
       
    % Everything else (garbage)
    b_lfilter(<<_,R/binary>>,PH,A) ->
        b_lfilter(R,PH,?BIN_APP(A,PH)).

binstr_append_placeholder(Binstr,PH) ->
        case PH of
            "" -> <<Binstr/binary>>;
            <<>> -> <<Binstr/binary>>;
            _ -> <<Binstr/binary,PH/binary>>
        end.
        
    

binstr_match_anywhere(Subject,Pattern) when is_binary(Pattern); is_tuple(Pattern) ->
    case binary:match(Subject,Pattern) of
        nomatch -> false;
        {_,_}   -> true
    end;
binstr_match_anywhere(Subject,Pattern) when is_list(Pattern) ->
    binstr_match_anywhere(Subject,list_to_binary(Pattern)).

binstr_match_sub(Subject,Start,Length,Pattern) when is_binary(Pattern); is_tuple(Pattern) ->
    case binary:match(Subject,Pattern,[{scope, {Start,Length}}]) of
        nomatch -> false;
        {_,_}   -> true
    end;
binstr_match_sub(Subject,Start,Length,Pattern) when is_list(Pattern) ->
    binstr_match_sub(Subject,Start,Length,list_to_binary(Pattern)).

binstr_match_precompile(Pattern) ->
    binary:compile_pattern(Pattern).


iff_true({_Key,_Value}) -> true.

    
%% ===================================================================
%% TESTS 
%% ===================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

setup() -> ok.

teardown(_) -> ok.

string_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun string_operations/1
        ]}}.    

string_operations(_) ->
    ?Info("---TEST---~p:string_operations~n", [?MODULE]),
    ?assertEqual(<<"table">>, binstr_to_lcase_ascii(<<"täble"/utf8>>)),
    ?assertEqual(<<"tuble">>, binstr_to_lcase_ascii(<<"tüble"/utf8>>)).

binstr_accentfold_test_() ->
    %UpperCaseAcc = <<"À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ð Ñ Ò Ó Ô Õ Ö Ø Ù Ú Û Ü Ý Þ Ÿ Œ Š Ž"/utf8>>,
    UpperCaseRaw = <<195,128,32,195,129,32,195,130,32,195,131,32,195,132,32,195,133,32,195,134,32,
                     195,135,32,195,136,32,195,137,32,195,138,32,195,139,32,195,140,32,195,141,32,
                     195,142,32,195,143,32,195,144,32,195,145,32,195,146,32,195,147,32,195,148,32,
                     195,149,32,195,150,32,195,152,32,195,153,32,195,154,32,195,155,32,195,156,32,
                     195,157,32,195,158,32,197,184,32,197,146,32,197,160,32,197,189>>,
    UpperCaseUnn = <<"A A A A A A AE C E E E E I I I I D N O O O O O O U U U U Y TH Y OE S Z">>,
    %LowerCaseAcc = <<"à á â ã ä å æ ç è é ê ë ì í î ï ð ñ ò ó ô õ ö ø ù ú û ü ý þ ÿ œ š ß ž"/utf8>>,
    LowerCaseRaw = <<195,160,32,195,161,32,195,162,32,195,163,32,195,164,32,195,165,32,195,166,32,
                     195,167,32,195,168,32,195,169,32,195,170,32,195,171,32,195,172,32,195,173,32,
                     195,174,32,195,175,32,195,176,32,195,177,32,195,178,32,195,179,32,195,180,32,
                     195,181,32,195,182,32,195,184,32,195,185,32,195,186,32,195,187,32,195,188,32,
                     195,189,32,195,190,32,195,191,32,197,147,32,197,161,32,195,159,32,197,190>>,
    LowerCaseUnn = <<"a a a a a a ae c e e e e i i i i d n o o o o o o u u u u y th y oe s ss z">>,

    [?_assertEqual(UpperCaseUnn,binstr_accentfold(UpperCaseRaw)),
     ?_assertEqual(LowerCaseUnn,binstr_accentfold(LowerCaseRaw))
    ].

binstr_casemod_test_()->
    %UpperCaseAcc = <<"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789[]{}À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ð Ñ Ò Ó Ô Õ Ö Ø Ù Ú Û Ü Ý Þ Ÿ"/utf8>>,
    %LowerCaseAcc = <<"abcdefghijklmnopqrstuvwxyz0123456789[]{}à á â ã ä å æ ç è é ê ë ì í î ï ð ñ ò ó ô õ ö ø ù ú û ü ý þ ÿ"/utf8>>,
    UpperCaseRaw = <<65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,48,49,50,51,52,53,54,55,56,57,91,93,123,125,195,128,32,195,129,32,195,130,32,195,131,32,195,132,32,195,133,32,195,134,32,195,135,32,195,136,32,195,137,32,195,138,32,195,139,32,195,140,32,195,141,32,195,142,32,195,143,32,195,144,32,195,145,32,195,146,32,195,147,32,195,148,32,195,149,32,195,150,32,195,152,32,195,153,32,195,154,32,195,155,32,195,156,32,195,157,32,195,158,32,197,184>>,
    LowerCaseRaw = <<97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,48,49,50,51,52,53,54,55,56,57,91,93,123,125,195,160,32,195,161,32,195,162,32,195,163,32,195,164,32,195,165,32,195,166,32,195,167,32,195,168,32,195,169,32,195,170,32,195,171,32,195,172,32,195,173,32,195,174,32,195,175,32,195,176,32,195,177,32,195,178,32,195,179,32,195,180,32,195,181,32,195,182,32,195,184,32,195,185,32,195,186,32,195,187,32,195,188,32,195,189,32,195,190,32,195,191>>,
    [?_assertEqual(UpperCaseRaw,binstr_to_upper(LowerCaseRaw)),
     ?_assertEqual(LowerCaseRaw,binstr_to_lower(UpperCaseRaw))].

-define(TL,unicode:characters_to_list).
binstr_to_lcase_ascii_test_() ->
    [{"empty",?_assertEqual(<<>>,binstr_to_lcase_ascii(<<"">>))},
     {"from binary0",?_assertEqual(<<"table">>,binstr_to_lcase_ascii(<<"täble"/utf8>>))},
     {"from binary1",?_assertEqual(<<"tuble">>,binstr_to_lcase_ascii(<<"tüble"/utf8>>))},
     {"from binary2",?_assertEqual("aaaeeeuu",?TL(binstr_to_lcase_ascii(<<"AÀäëéÈüÜ"/utf8>>)))},
     {"from list",?_assertEqual("aaaeee",?TL(binstr_to_lcase_ascii("AÀäëéÈ")))},
     {"from atom",?_assertEqual("atom",?TL(binstr_to_lcase_ascii(aTom)))},
     {"from tuple",?_assertEqual("{\"aaaeee\"}",?TL(binstr_to_lcase_ascii({"AÀäëéÈ"})))},
     {"from integer",?_assertEqual("12798",?TL(binstr_to_lcase_ascii(12798)))},
     {"from random",?_assertEqual("g:xr*a\\6r",?TL(binstr_to_lcase_ascii(<<71,191,58,192,88,82,194,42,223,65,187,19,92,145,228,248, 26,54,196,114>>)))}
     ].

binstr_only_ascii_test_() ->
    [{"form àç90{}",?_assertEqual(<<" 90{}">>,binstr_only_ascii(<<"àç 90{}">>))},
     {"random",?_assertEqual(<<"G:XR*A\\6r">>,binstr_only_ascii(<<71,191,58,192,88,82,194,42,223,65,187,19,92,145,228,248, 26,54,196,114>>))}
    ].

binstr_only_valid_test_() ->
    Random = <<74,94,160,102,193,249,94,21,66,87,242,109,13,107,163,36,165,68,215,
               193,133,58,191,65,41,23,172,79,127,88,215,14,244,33,223,179,217,17,
               86,174,55,29,132,221,124,112,34,14,192,37,153,199,176,212,35,207,115,
               22,41,104,150,48,92,245>>,
    [{"form àç90{}",?_assertEqual(<<"àç 90{}"/utf8>>,binstr_only_valid(<<"àç 90{}"/utf8,138,255>>))},
     {"random",?_assertEqual(<<"J^f^BWm\rk$D:A)OX!ß³V7|p\"%Ç°#s)h0\\">>
                            ,binstr_only_valid(Random))}

    ].

binstr_only_latin1_test_() ->
    Random = <<74,94,160,102,193,249,94,21,66,87,242,109,13,107,163,36,165,68,215,
               193,133,58,191,65,41,23,172,79,127,88,215,14,244,33,223,179,217,17,
               86,174,55,29,132,221,124,112,34,14,192,37,153,199,176,212,35,207,115,
               22,41,104,150,48,92,245>>,
    [{"form àç90{}",?_assertEqual(<<"àç 90{}"/utf8>>,binstr_only_latin1(<<"àç 90{}"/utf8,138,255>>))},
     {"random",?_assertEqual(<<"J^f^BWm\rk$D:A)OX!ß³V7|p\"%Ç°#s)h0\\">>
                            ,binstr_only_latin1(Random))}

    ].
    
-endif.
