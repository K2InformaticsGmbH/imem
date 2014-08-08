-module(imem_index).

%% @doc == imem INDEX operations ==


-include("imem_meta.hrl").

-export([binstr_to_lcase_ascii/1,

         binstr_accentfold/1,
         binstr_to_lower/1,
         binstr_to_higher/1,

         binstr_match_anywhere/2,
         binstr_match_sub/4,
         binstr_match_precompile/1
		]).

binstr_to_lcase_ascii(<<"\"\"">>) -> <<>>; 
binstr_to_lcase_ascii(B) when is_binary(B) -> 
    %% unicode_string_to_ascii(string:to_lower(unicode:characters_to_list(B, utf8)));
    binstr_accentfold(binstr_to_lower(B));
binstr_to_lcase_ascii(Val) -> 
	% unicode_string_to_ascii(io_lib:format("~p",[Val])).
    binstr_accentfold(binstr_to_lower(unicode:characters_to_binary(io_lib:format("~p",[Val])))).


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
    b_convert(<<195,C,R/binary>>,A) when C >= 157, C =<  159 -> % Ý
        b_convert(R,<<A/binary,$Y>>);   
    b_convert(<<195,C,R/binary>>,A) when C =:= 135 -> % Ç
        b_convert(R,<<A/binary,$C>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 145 -> 
        b_convert(R,<<A/binary,$N>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 134 -> % Æ
        b_convert(R,<<A/binary,$A,$E>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 160 -> % LATIN CAPITAL LETTER S WITH CARON
        b_convert(R,<<A/binary,$S>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 158 -> % Þ -> TH
        b_convert(R,<<A/binary,$T,$H>>); 
    
    b_convert(<<195,C,R/binary>>,A) when C >= 160, C =<  165 -> 
        b_convert(R,<<A/binary,$a>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 168, C =<  171 -> 
        b_convert(R,<<A/binary,$e>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 172, C =<  175 -> 
        b_convert(R,<<A/binary,$i>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 178, C =<  180 -> 
        b_convert(R,<<A/binary,$o>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 185, C =<  188 -> 
        b_convert(R,<<A/binary,$u>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 189, C =<  191 -> 
        b_convert(R,<<A/binary,$y>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 167 -> 
        b_convert(R,<<A/binary,$c>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 166 -> 
        b_convert(R,<<A/binary,$a,$e>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 177 -> 
        b_convert(R,<<A/binary,$n>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 161 -> % LATIN SMALL LETTER S WITH CARON
        b_convert(R,<<A/binary,$s>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 159 -> % ß -> ss
        b_convert(R,<<A/binary,$s,$s>>); 
    
    b_convert(<<H,R/binary>>,A) -> 
        b_convert(R,<<A/binary,H>>).

%% @doc lowercases all alphabetical characters supported by ISO 8859-15
-spec binstr_to_lower(binary()) -> binary().
binstr_to_lower(Str) when is_binary(Str) ->
    b_lower(Str,<<>>).

    b_lower(<<>>,A) -> A;
    b_lower(<<195,C,R/binary>>,A) when C >= 128, C =<  158 -> 
        NC = C + 32,
        b_lower(R,<<A/binary,195,NC>>);
    b_lower(<<C,R/binary>>,A) when C >=  65, C =< 90 -> 
        NC = C + 32,
        b_lower(R,<<A/binary,NC>>);
    b_lower(<<H,R/binary>>,A) -> 
        b_lower(R,<<A/binary,H>>).
 
%% @doc uppercases all alphabetical characters supported by ISO 8859-15
-spec binstr_to_higher(binary()) -> binary().
binstr_to_higher(Str) when is_binary(Str) ->
    b_higher(Str,<<>>).

    b_higher(<<>>,A) -> A;
    b_higher(<<195,C,R/binary>>,A) when C >= 160, C =<  191 -> 
        NC = C - 32,
        b_higher(R,<<A/binary,195,NC>>);
    b_higher(<<C,R/binary>>,A) when C >=  97, C =< 122 -> 
        NC = C - 32,
        b_higher(R,<<A/binary,NC>>);
    b_higher(<<H,R/binary>>,A) -> 
        b_higher(R,<<A/binary,H>>).

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

