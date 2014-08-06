-module(imem_index).

%% @doc == imem INDEX operations ==

% Record field names are terse because of variable meaning according to index type.
-record(ddindex,{sk,  % Search key
                 rk   % Reference key: empty, single reference or list of references
                }).

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
%% IVK: default index type
%%          sk = {IndexId,<<"Value">>,Reference}
%%          rk = undefined
%%
%% IV_K: unique key index
%%          sk = {IndexId,<<"UniqueValue">>}
%%          rk = Reference
%%       observation: should crash/throw/error on duplicate value insertion
%%
%% IV_KL: high selectivity index (aka "almost unique")
%%          sk = {IndexId,<<"AlmostUniqueValue"}
%%          rk = [Reference | ListOfReferences]
%%
%% IV_B: low selectivity index (aka "bitmap", although a true bitmap index)
%%          For the values:
%%              sk = {IndexId,<<"CommonValue">>}
%%              rk = FastLookupNumber
%%          For the links to the references:
%%              sk = {IndexId, {FastLookupNumber, Reference}}
%%              rk = undefined
%%
%% IVVK: combined index
%%          sk = {IndexId,<<"ValueA">>,<<"ValueB">>,Reference}
%%          rk = undefined
%%
%%
%% How it should be used:
%% ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
%% Basically, it's an mnesia-managed ETS table, where one uses regexp or binary_match
%% operations to iterate on and find matching values, and their link back to the objects
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
%%    - output format: [{headmatch, HeadMatchString, HeadMatchResults},
%%                    {anymatch, AnyMatchString, AnyMatchResults},
%%                    {regexpmatch, RegexpMatchString, RegexpMatchResults}]
%%
%%      How it should work:
%%       Should first execute headmatch. If enough results, other result "sets" will be empty.
%%       If not enough results, do anymatch (basic binary_match)
%%       If input string contains wildcards or regexp-like characters (?), convert to regexp pattern, and match. 
%%          Other result "sets" will be empty.


