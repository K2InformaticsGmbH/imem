-module(imem_dal_skvh).

-include("imem.hrl").
-include("imem_meta.hrl").

-define(TABLE(__Channel),<<"skvh",__Channel/binary>>).
-define(AUDIT(__Channel),<<"skvhAudit",__Channel/binary, "_86400@">>).

-define(TABLE_OPTS, [{record_name,skvhTable}
                    ,{type,ordered_set}
                    ]).          

-define(AUDIT_OPTS, [{record_name,skvhAudit}
                    ,{type,ordered_set}
                    ,{purge_delay,430000}        %% 430000 = 5 Days - 2000 sec
                    ]).          

-record(skvhTable,                            %% sorted key value hash table    
                    { ckey                    :: term()
                    , cvalue               	  :: binary()      
                    , chash                   :: term()
                    }
       ).
-define(skvhTable,  [term,binstr,term]).

-record(skvhAudit,                            %% sorted key value hash audit table    
                    { time                    :: ddTimestamp()	%% erlang:now()
                    , ckey                    :: term()
                    , cvalue               	  :: binary()      
                    }
       ).
-define(skvhAudit,  [timestamp,term,binstr]).


% -define(E100,{100,"Invalid key"}).
% -define(E101,{101,"Duplicate key"}).
-define(E102(__Term),{102,"Invalid key value pair",__Term}).
-define(E103,{103,<<"Invalid source peer key (short code)">>}).
% -define(E104,{104,"Unknown white list IP addresses"}).
-define(E105,{105,<<"Invalid white list IP address">>}).
-define(E106,{106,<<"Unknown source peer">>}).
% -define(E107,{107,"Duplicate SMS proxy service"}).
% -define(E108,{108,"Invalid service name"}).
-define(E109,{109,<<"Invalid destination channel (port)">>}).
% -define(E110,{110,"Duplicate white list IP address"}).
% -define(E111,{111,"Invalid WAP push preference value"}).
% -define(E112,{112,"Unknown error"}).
% -define(E113,{113,"Could not delete content provider with SMS short code <value> because SMS proxy service exists for it."}).
% -define(E114,{114,"Could not create SMS proxy service."}).
% -define(E115,{115,"Could not activate SMS proxy service."}).
-define(E116,{116,<<"Invalid option value">>}).


-export([ write/2 		%% (Channel, KVTable)    			resource may not exist, will be created, return list of hashes 
		, read/3		%% (Channel, KeyTable)   			return empty Arraylist if none of these resources exists
		, readGELT/4	%% (Channel, Item, CKey1, CKey2)	start with first key after CKey1, end with last key before CKey2
 		, readGT/4		%% (Channel, Item, CKey1, Limit)	start with first key after CKey1, return Limit results or less
 		, delete/2		%% (Channel, KeyTable)    			do not complain if keys do not exist
 		, deleteGELT/3	%% (Channel, CKey1, CKey2)			delete range of keys >= CKey1
		, deleteGTLT/3	%% (Channel, CKey1, CKey2)			delete range of keys > CKey1
		]).

%% @doc Checks existence of interface tables by checking existence of table name atoms in atom cache
%% creates these tables if not already existing 
%% return atom names of the tables (StateTable and AuditTable)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	Tuple of table names to be used for data transfer
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec create_check_channel(binary()) -> {atom(),atom()}.
create_check_channel(Channel) ->
	TBin = ?TABLE(Channel),
	% Token = list_to_binary([TBin,integer_to_list(erlang:phash2({TBin,node()}))]),
	try 
		% _ = ?binary_to_existing_atom(Token), 	%% probe atom cache for token
		T = ?binary_to_existing_atom(TBin),
		A = ?binary_to_existing_atom(?AUDIT(Channel)),
		imem_meta:check_table(T),
		{T,A}
	catch 
		_:_ -> 
			TN = ?binary_to_atom(TBin),
			imem_meta:create_check_table(TN, {record_info(fields, skvhTable),?skvhTable, #skvhTable{}}, ?TABLE_OPTS, system),    
			AN = ?binary_to_atom(?AUDIT(Channel)),
			imem_meta:create_check_table(AN, {record_info(fields, skvhAudit),?skvhAudit, #skvhAudit{}}, ?AUDIT_OPTS, system),
			% _ = ?binary_to_atom(Token), 		%% put token to atom cache
			{TN,AN}
	end.

% return(Cmd, Result) ->
% 	debug(Cmd, Result), 
% 	Result.

% return_error(Cmd, Tuple) -> 
% 	debug(Cmd, Tuple), 
% 	{error,Tuple}.

return_list(Cmd, List) -> 
	debug(Cmd, List), 
	{ok,List}.

% return_tuple(Cmd, Tuple) -> 
% 	debug(Cmd, Tuple), 
% 	{ok,Tuple}.

%% Converters for input parameters

io_clean_table(Bin) ->
	case binary:last(Bin) of
		10 ->	binary:replace(binary_part(Bin,0,size(Bin)-1), <<13>>, <<>>, [global]);
		_ ->	binary:replace(Bin, <<13>>, <<>>, [global])
	end.

io_key_to_term(Key) when is_binary(Key) ->
	imem_datatype:io_to_term(Key).

io_value_to_term(V) -> V.	%% ToDo: Maybe convert to map datatype when available

% io_hash_to_term(Hash) when is_binary(Hash) -> Hash.

io_kv_pair_to_tuple(KVPair) ->
	case binary:split(KVPair,<<9>>,[global]) of			%% split at tab position
		[K,V] -> 	{io_key_to_term(K),io_value_to_term(V)};
		Other ->	?ClientError(?E102(Other))
	end.

io_key_table_to_term_list(KeyTable) ->
	BList = binary:split(io_clean_table(KeyTable),<<10>>,[global]),	%% split at line feed positions
	[io_key_to_term(Key) || Key <- BList].

io_kv_table_to_tuple_list(KVTable) ->
	BList = binary:split(io_clean_table(KVTable),<<10>>,[global]),		%% split at line feed positions
	[io_kv_pair_to_tuple(KVbin) || KVbin <- BList].

%% Converters for output (results)

term_key_to_io(Key) ->
	imem_datatype:term_to_io(Key).

term_value_to_io(undefined) -> <<"undefined">>;	%% ToDo: Maybe convert from map datatype when available
term_value_to_io(V) -> V.   					%% ToDo: Maybe convert from map datatype when available

term_hash_to_io(undefined) -> <<"undefined">>; 
term_hash_to_io(Hash) when is_binary(Hash) -> Hash; 
term_hash_to_io(Hash) when is_integer(Hash) -> list_to_binary(integer_to_list(Hash)).

term_kv_tuple_to_io({K,V}) ->
	KeyBin = term_key_to_io(K),
	ValueBin = term_value_to_io(V), 
	<<KeyBin/binary,9,ValueBin/binary>>.

term_kh_tuple_to_io({K,H}) ->
	KeyBin = term_key_to_io(K),
	HashBin = term_hash_to_io(H), 
	<<KeyBin/binary,9,HashBin/binary>>.

%% Data access per key (read,write,delete)

read(Channel, Item, KeyTable) when is_binary(Channel), is_binary(Item), is_binary(KeyTable) ->
	Cmd = [read,Channel,Item,KeyTable],
	read(Cmd, create_check_channel(Channel), Item, io_key_table_to_term_list(KeyTable), []).

read(Cmd, _, Item, [], Acc)  -> project_result(Cmd, lists:reverse(Acc), Item);
read(Cmd, {TN,AN}, Item, [Key|Keys], Acc)  ->
	KVP = case imem_meta:read(TN,Key) of
		[] ->		#skvhTable{ckey=Key} ;
		[Rec] ->	Rec
	end,
	read(Cmd, {TN,AN}, Item, Keys, [KVP|Acc]).
	

write(Channel, KVTable) when is_binary(Channel), is_binary(KVTable) ->
	Cmd = [write,Channel,KVTable],
	write(Cmd, create_check_channel(Channel), io_kv_table_to_tuple_list(KVTable), []).

write(Cmd, _, [], Acc)  -> return_list(Cmd, lists:reverse(Acc));
write(Cmd, {TN,AN}, [{K,V}|KVPairs], Acc)  ->
	Hash = list_to_binary(integer_to_list(erlang:phash2({K,V}))),
	imem_meta:write(TN,#skvhTable{ckey=K,cvalue=V,chash=Hash}),					%% ToDo: wrap in transaction
	catch (imem_meta:write(AN,#skvhAudit{time=erlang:now(),ckey=K,cvalue=V})),	%% ToDo: wrap in transaction
	write(Cmd, {TN,AN}, KVPairs, [Hash|Acc]).

delete(Channel, KeyTable) when is_binary(Channel), is_binary(KeyTable) -> 
	Cmd = [delete,Channel,KeyTable],
	delete(Cmd, create_check_channel(Channel), io_key_table_to_term_list(KeyTable), []).

delete(Cmd, _, [], Acc)  -> return_list(Cmd, lists:reverse(Acc));
delete(Cmd, {TN,AN}, [Key|Keys], Acc)  ->
	Hash = case imem_meta:read(TN,Key) of
		[] ->						<<"undefined">>;
		[#skvhTable{chash=H}] ->	H
	end,
	imem_meta:delete(TN,Key),									%% ToDo: wrap in transaction
	imem_meta:write(AN,#skvhAudit{time=erlang:now(),ckey=Key}),	%% ToDo: wrap in transaction
	delete(Cmd, {TN,AN}, Keys, [Hash|Acc]).

%% Data Access per key range

readGELT(Channel, Item, CKey1, CKey2) when is_binary(Item), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [readGELT, Channel, Item, CKey1, CKey2],
	readGELT(Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_key_to_term(CKey2)).

readGELT(Cmd, {TN,_}, Item, Key1, Key2) ->
	MatchHead = {skvhTable, '$1', '$2', '$3'},
	MatchFunction = {MatchHead, [{'>=', '$1', Key1}, {'<', '$1', Key2}], ['$_']},
	{L,true} = imem_meta:select(TN, [MatchFunction]),
	project_result(Cmd, L, Item).

readGT(Channel, Item, CKey1, Limit)  when is_binary(Item), is_binary(CKey1), is_binary(Limit) ->
	Cmd = [readGT, Channel, Item, CKey1, Limit],
	readGT(Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_key_to_term(Limit)).

readGT(Cmd, {TN,_}, Item, Key1, Limit) ->
	MatchHead = {skvhTable, '$1', '$2', '$3'},
	MatchFunction = {MatchHead, [{'>', '$1', Key1}], ['$_']},
	{L,_} = imem_meta:select(TN, [MatchFunction], Limit),
	project_result(Cmd, L, Item).

deleteGELT(Channel, CKey1, CKey2) when is_binary(Channel), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [deleteGELT, Channel, CKey1, CKey2],
	deleteGELT(Cmd, create_check_channel(Channel), io_key_to_term(CKey1), io_key_to_term(CKey2)).

deleteGELT(Cmd, {TN,AN}, Key1, Key2) ->
	MatchHead = {skvhTable, '$1', '$2', '$3'},
	MatchFunction = {MatchHead, [{'>=', '$1', Key1}, {'<', '$1', Key2}], ['$1']},
	{L,true} = imem_meta:select(TN, [MatchFunction]),
	delete(Cmd, {TN,AN}, L, []).

deleteGTLT(Channel, CKey1, CKey2) when is_binary(Channel), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [deleteGTLT, Channel, CKey1, CKey2],
	deleteGTLT(Cmd, create_check_channel(Channel), io_key_to_term(CKey1), io_key_to_term(CKey2)).

deleteGTLT(Cmd, {TN,AN}, Key1, Key2) ->
	MatchHead = {skvhTable, '$1', '$2', '$3'},
	MatchFunction = {MatchHead, [{'>', '$1', Key1}, {'<', '$1', Key2}], ['$1']},
	{L,true} = imem_meta:select(TN, [MatchFunction]),
	delete(Cmd, {TN,AN}, L, []).

project_result(Cmd, L, <<"key">>) ->
	return_list(Cmd, [term_key_to_io(K) ||  #skvhTable{ckey=K} <- L]);
project_result(Cmd, L, <<"value">>) ->
	return_list(Cmd, [term_value_to_io(V) ||  #skvhTable{cvalue=V} <- L]);
project_result(Cmd, L, <<"hash">>) ->
	return_list(Cmd, [term_hash_to_io(H) ||  #skvhTable{chash=H} <- L]);
project_result(Cmd, L, <<"kvpair">>) ->
	return_list(Cmd, [term_kv_tuple_to_io({K,V}) ||  #skvhTable{ckey=K,cvalue=V} <- L]);
project_result(Cmd, L, <<"khpair">>) ->
	return_list(Cmd, [term_kh_tuple_to_io({K,H}) ||  #skvhTable{ckey=K,chash=H} <- L]).

debug(Cmd, Resp) ->
    lager:debug([ {cmd, hd(Cmd)}]
    			  , "PROVISIONING EVENT for ~p ~p returns ~p"
    			  , [hd(Cmd), tl(Cmd), Resp]
    			 ).


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ?imem_test_setup(),
    catch imem_meta:drop_table(skvhTEST),
    catch imem_meta:drop_table(skvhAuditTEST_86400@).

teardown(_) ->
    catch imem_meta:drop_table(skvhTEST),
    catch imem_meta:drop_table(skvhAuditTEST_86400@),
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun skvh_operations/1
        ]}}.    

skvh_operations(_) ->
    try 
        ClEr = 'ClientError',
        Channel = <<"TEST">>,

        ?Info("----TEST--~p:test_skvh~n", [?MODULE]),

        ?assertEqual(<<"skvhTEST">>, ?TABLE(Channel)),

        ?assertException(throw, {'ClientError',{"Table does not exist",_}}, imem_meta:check_table(skvhTEST)), 
        ?assertException(throw, {'ClientError',{"Table does not exist",_}}, imem_meta:check_table(skvhAuditTEST_86400@)), 

        KVa = <<"[1,a]",9,"123456">>,
        KVb = <<"[1,b]",9,"234567">>,
        KVc = <<"[1,c]",9,"345678">>,

        ?assertEqual({ok,[<<"[1,a]",9,"undefined">>]}, read(Channel, <<"kvpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"[1,a]",9,"undefined">>]}, read(Channel, <<"khpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"[1,a]">>]}, read(Channel, <<"key">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"undefined">>]}, read(Channel, <<"value">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"undefined">>]}, read(Channel, <<"hash">>, <<"[1,a]">>)),

        ?assertEqual(ok, imem_meta:check_table(skvhTEST)),        
        ?assertEqual(ok, imem_meta:check_table(skvhAuditTEST_86400@)),

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

        ?assertEqual({ok,[KVa]}, read(Channel, <<"kvpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[KVc]}, read(Channel, <<"kvpair">>, <<"[1,c]">>)),
        ?assertEqual({ok,[KVb]}, read(Channel, <<"kvpair">>, <<"[1,b]">>)),
        ?assertEqual({ok,[<<"[1,c]",9,"117221887">>]}, read(Channel, <<"khpair">>, <<"[1,c]">>)),
        ?assertEqual({ok,[<<"124793159">>]}, read(Channel, <<"hash">>, <<"[1,b]">>)),

        ?assertEqual({ok,[KVc,KVb,KVa]}, read(Channel, <<"kvpair">>, <<"[1,c]",13,10,"[1,b]",10,"[1,a]">>)),
        ?assertEqual({ok,[KVa,<<"[1,ab]",9,"undefined">>,KVb,KVc]}, read(Channel, <<"kvpair">>, <<"[1,a]",13,10,"[1,ab]",13,10,"[1,b]",10,"[1,c]">>)),

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, delete(Channel, <<"[1,a]",10,"[1,b]",13,10,"[1,c]",10>>)),

        Aud = imem_meta:read(skvhAuditTEST_86400@),
        ?Info("audit trail~n~p~n", [Aud]),
        ?assertEqual(6, length(Aud)),

        ?assertEqual({ok,[<<"[1,a]",9,"undefined">>]}, read(Channel, <<"kvpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"[1,b]",9,"undefined">>]}, read(Channel, <<"kvpair">>, <<"[1,b]">>)),
        ?assertEqual({ok,[<<"[1,c]",9,"undefined">>]}, read(Channel, <<"kvpair">>, <<"[1,c]">>)),

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

		?assertEqual({ok,[]},deleteGELT(Channel, <<"[]">>, <<"[]">>)),
		?assertEqual({ok,[]},deleteGELT(Channel, <<"[]">>, <<"[1,a]">>)),

		?assertEqual({ok,[<<"1296644">>]},deleteGELT(Channel, <<"[]">>, <<"[1,ab]">>)),
		?assertEqual({ok,[]},deleteGELT(Channel, <<"[]">>, <<"[1,ab]">>)),

		?assertEqual({ok,[<<"124793159">>,<<"117221887">>]},deleteGELT(Channel, <<"[1,ab]">>, <<"[1,d]">>)),		
		?assertEqual({ok,[]},deleteGELT(Channel, <<"[1,ab]">>, <<"[1,d]">>)),

        ?assertEqual({ok,[<<"undefined">>,<<"undefined">>,<<"undefined">>]}, delete(Channel, <<"[1,a]",10,"[1,b]",13,10,"[1,c]",10>>)),

        ?assertEqual({ok,[<<"73740797">>]}, write(Channel, <<"[90074,[],\"AaaEnabled\"]",9,"true">>)),
		?assertEqual({ok,[<<"81705319">>]}, write(Channel, <<"[90074,[],\"ContentSizeMax\"]",9,"297000">>)),
		?assertEqual({ok,[<<"99247098">>]}, write(Channel, <<"[90074,[],<<\"MmscId\">>]",9,"\"testMMSC\"">>)),
		?assertEqual({ok,[<<"55096411">>]}, write(Channel, <<"[90074,\"MMS-DEL-90074\",\"TpDeliverUrl\"]",9,"\"http:\/\/10.132.30.84:18888\/deliver\"">>)),

        ?Info("audit trail~n~p~n", [imem_meta:read(skvhAuditTEST_86400@)]),

		?assertEqual(ok,imem_meta:drop_table(skvhTEST)),

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[]">>, <<"[]">>)),
		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[]">>, <<"[1,a]">>)),

		?assertEqual({ok,[<<"1296644">>]},deleteGTLT(Channel, <<"[]">>, <<"[1,ab]">>)),
		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[]">>, <<"[1,ab]">>)),

		?assertEqual({ok,[<<"117221887">>]},deleteGTLT(Channel, <<"[1,b]">>, <<"[1,d]">>)),		
		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[1,b]">>, <<"[1,d]">>)),

		?assertEqual({ok,[<<"124793159">>]},deleteGTLT(Channel, <<"[1,a]">>, <<"[1,c]">>)),		
		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[1,a]">>, <<"[1,c]">>)),		

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, readGELT(Channel, <<"hash">>, <<"[]">>, <<"[1,d]">>)),
        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, readGELT(Channel, <<"hash">>, <<"[1,a]">>, <<"[1,d]">>)),
    	?assertEqual({ok,[<<"124793159">>,<<"117221887">>]}, readGELT(Channel, <<"hash">>, <<"[1,ab]">>, <<"[1,d]">>)),
    	?assertEqual({ok,[<<"[1,b]">>]}, readGELT(Channel, <<"key">>, <<"[1,ab]">>, <<"[1,c]">>)),
    	?assertEqual({ok,[<<"234567">>]}, readGELT(Channel, <<"value">>, <<"[1,ab]">>, <<"[1,c]">>)),
    	?assertEqual({ok,[<<"[1,b]",9,"234567">>]}, readGELT(Channel, <<"kvpair">>, <<"[1,ab]">>, <<"[1,c]">>)),
    	?assertEqual({ok,[<<"[1,b]",9,"124793159">>]}, readGELT(Channel, <<"khpair">>, <<"[1,ab]">>, <<"[1,c]">>)),
 
    	?assertEqual(ok, imem_meta:drop_table(skvhAuditTEST_86400@)),

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, readGT(Channel, <<"hash">>, <<"[]">>, <<"1000">>)),
        ?assertEqual({ok,[<<"124793159">>,<<"117221887">>]}, readGT(Channel, <<"hash">>, <<"[1,a]">>, <<"1000">>)),
    	?assertEqual({ok,[<<"124793159">>]}, readGT(Channel, <<"hash">>, <<"[1,a]">>, <<"1">>)),
    	?assertEqual({ok,[<<"[1,b]">>,<<"[1,c]">>]}, readGT(Channel, <<"key">>, <<"[1,a]">>, <<"2">>)),
    	?assertEqual({ok,[<<"234567">>]}, readGT(Channel, <<"value">>, <<"[1,ab]">>, <<"1">>)),
    	?assertEqual({ok,[<<"[1,b]",9,"234567">>]}, readGT(Channel, <<"kvpair">>, <<"[1,ab]">>, <<"1">>)),
    	?assertEqual({ok,[<<"[1,b]",9,"124793159">>]}, readGT(Channel, <<"khpair">>, <<"[1,ab]">>, <<"1">>)),

        ?assertEqual(ok, imem_meta:drop_table(skvhTEST)),
        ?assertEqual(ok, imem_meta:drop_table(skvhAuditTEST_86400@)),

        ?Info("success ~p~n", [drop_tables])
    catch
        Class:Reason ->     
            timer:sleep(1000),
            ?Info("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
            throw ({Class, Reason})
    end,
    ok.
    
-endif.
