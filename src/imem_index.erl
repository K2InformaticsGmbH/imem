-module(imem_index).

%% @doc == imem INDEX operations ==

-define(DD_INDEX_OPTS,  [{record_name,ddIndex}
                        ,{type,ordered_set}
                        %% ,{purge_delay,430000}  %% inherit from parent table
                        ]).          

% Record field names are terse because of variable meaning according to index type.

-record(ddIndex,  %% record definition for index tables (one per indexed master data table)              
                  { stu  		:: tuple()	%% search tuple, cannot be empty
                  , lnk = 0     :: term()   %% Link to key of master data
                  							%% 0=unused when key is part of search tuple, used in ivk
                  							%% 0..n  hashed value for the hashmap index (iv_h)
                  							%% single key of any data type for unique index (iv_k)
                  							%% list of keys for almost unique index (iv_kl)
                  }     
       ). 
-define(ddIndex, [tuple,term]).


-record(ddIdxDef, %% record definition for index definition              
                  { id  		:: integer()		%% index id within the table
                  , name 		:: binary()			%% name of the index
                  , pos     	:: integer()		%% record field to be indexed 1 = key (used maybe on set tables)
                  , type  		:: ivk|iv_k|iv_kl|iv_h|ivvk|ivvvk	%% Type of index
                  , pl 			:: list(binary())	%% list of JSON path expressions as binstr (to be compiled)
                  , vnf = <<"imem_index:binstr_to_lcase_ascii/1">> :: binary() 		
                  				%% value_normalising_fun(Value)  
                  				%% applied to each value result of all path scans for given JSON document
                  				%% return ?nav = '$not_a_value' if indexing is not wanted, otherwise let iff() decide
                  , iff = <<"fun(_,_) -> true end">> :: binary() 		
                  				%% boolean index_filter_fun(Key,Value) for the inclusion/exclusion of indexes
                  				%% applied to each result of all path scans for given JSON document
                  }     
       ).


-export([ binstr_to_lcase_ascii/1
        , index_table_name/1        %% (TableName)                      derive index table name from data table name
        , create_index_table/3      %% (IndexTable,ParentOpts,Owner)    create index table, possibly inheriting some options from parent table
		]).


index_table_name(Table) when is_atom(Table) -> list_to_atom("idx_" ++ atom_to_list(Table)).


create_index_table(IndexTable,ParentOpts,Owner) ->
    IndexOpts = case lists:keysearch(purge_delay, 1, ParentOpts) of
                false ->        ?DD_INDEX_OPTS;
                {value,PD} ->   ?DD_INDEX_OPTS ++ [{purge_delay,PD}]
    end,
    imem_meta:init_create_table(IndexTable, {record_info(fields, ddIndex), ?ddIndex, #ddIndex{}}, IndexOpts, Owner). 


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



