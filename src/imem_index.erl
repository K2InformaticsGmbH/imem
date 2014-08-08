-module(imem_index).

%% @doc == imem INDEX operations ==

-define(TEST,t).

-include("imem_meta.hrl").

-export([binstr_to_lcase_ascii/1,

         binstr_accentfold/1,
         binstr_to_lower/1,
         binstr_to_upper/1,

         binstr_match_anywhere/2,
         binstr_match_sub/4,
         binstr_precompile/1
		]).

binstr_to_lcase_ascii(<<"\"\"">>) -> <<>>; 
binstr_to_lcase_ascii(B) when is_binary(B) -> 
    unicode_string_to_ascii(string:to_lower(unicode:characters_to_list(B, utf8)));
binstr_to_lcase_ascii(Val) -> 
	unicode_string_to_ascii(io_lib:format("~p",[Val])).

unicode_string_to_ascii(U) -> 
	Ascii = U, 		%% ToDo: really do the accent folding here 
					%% and map all remaining codepoints > 254 to 254 (tilda)
	unicode:characters_to_binary(Ascii).


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
    b_convert(<<195,C,R/binary>>,A) when C >= 146, C =<  148; C =:= 152 -> % Ò Ó Ô Õ Ö Ø
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
    b_convert(<<195,C,R/binary>>,A) when C >= 178, C =<  180; C =:= 184 -> % ò ó ô õ ö ø
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

binstr_match_anywhere(Token,Target) when is_binary(Token); is_tuple(Token) ->
    case binary:match(Target,Token) of
        nomatch -> false;
        {_,_}   -> true
    end;
binstr_match_anywhere(Token,Target) when is_list(Token) ->
    binstr_match_anywhere(list_to_binary(Token),Target).

binstr_match_sub(Token,Start,Length,Target) when is_binary(Token); is_tuple(Token) ->
    case binary:match(Target,Token,[{scope, {Start,Length}}]) of
        nomatch -> false;
        {_,_}   -> true
    end;
binstr_match_sub(Token,Start,Length,Target) when is_list(Token) ->
    binstr_match_sub(list_to_binary(Token),Start,Length,Target).

binstr_precompile(Token) ->
    binary:compile_pattern(Token).

    
%% ===================================================================
%% TESTS 
%% ===================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

binstr_accentfold_test_() ->
    UpperCaseAcc = <<"À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ð Ñ Ò Ó Ô Õ Ö Ø Ù Ú Û Ü Ý Þ Ÿ Œ Š Ž"/utf8>>,
    UpperCaseUnn = <<"A A A A A A AE C E E E E I I I I D N O O O Õ Ö O U U U U Y TH Y OE S Z"/utf8>>,

    LowerCaseAcc = <<"à á â ã ä å æ ç è é ê ë ì í î ï ð ñ ò ó ô õ ö ø ù ú û ü ý þ ÿ œ š ß ž"/utf8>>,
    LowerCaseUnn = <<"a a a a a a ae c e e e e i i i i d n o o o õ ö o u u u u y th y oe s ss z"/utf8>>,

    [?_assertEqual(UpperCaseUnn,binstr_accentfold(UpperCaseAcc)),
     ?_assertEqual(LowerCaseUnn,binstr_accentfold(LowerCaseAcc))
    ].

binstr_casemod_test_()->
    UpperCaseAcc = <<"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789[]{}À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ð Ñ Ò Ó Ô Õ Ö Ø Ù Ú Û Ü Ý Þ Ÿ Œ Š Ž"/utf8>>,
    LowerCaseAcc = <<"abcdefghijklmnopqrstuvwxyz0123456789[]{}à á â ã ä å æ ç è é ê ë ì í î ï ð ñ ò ó ô õ ö ø ù ú û ü ý þ ÿ œ š ß ž"/utf8>>,
    LowerCaseRaw = <<97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,
                     116,117,118,119,120,121,122,48,49,50,51,52,53,54,55,56,57,91,93,123,125,
                     195,160,32,195,161,32,195,162,32,195,163,32,195,164,32,195,165,32,195,166,
                     32,195,167,32,195,168,32,195,169,32,195,170,32,195,171,32,195,172,32,195,
                     173,32,195,174,32,195,175,32,195,176,32,195,177,32,195,178,32,195,179,32,
                     195,180,32,195,181,32,195,182,32,195,184,32,195,185,32,195,186,32,195,187,
                     32,195,188,32,195,189,32,195,190,32,195,191,32,197,147,32,197,161,32,197,190>>,
    UpperCaseRaw = <<65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,
                     90,48,49,50,51,52,53,54,55,56,57,91,93,123,125,195,128,32,195,129,32,195,130,
                     32,195,131,32,195,132,32,195,133,32,195,134,32,195,135,32,195,136,32,195,137,
                     32,195,138,32,195,139,32,195,140,32,195,141,32,195,142,32,195,143,32,195,144,
                     32,195,145,32,195,146,32,195,147,32,195,148,32,195,149,32,195,150,32,195,152,
                     32,195,153,32,195,154,32,195,155,32,195,156,32,195,157,32,195,158,32,197,184,
                     32,197,146,32,197,160,32,195,159,32,197,189>>,
    [?_assertEqual(UpperCaseRaw,binstr_to_upper(LowerCaseAcc)),
     ?_assertEqual(LowerCaseRaw,binstr_to_lower(UpperCaseAcc))].

-endif.
