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
-define(E102,{102,"Invalid key value pair"}).
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


-export([ write/2 					%% (Channel, KVTable)    			' resource may not exist
		, read/2					%% (Channel, KeyTable)   			' return empty Arraylist if none of these resources exists
		, readGELT/4				%% (Channel, Item, CKey1, CKey2)	  start with first key after CKey1, end with last key before CKey2
 		, readGT/4					%% (Channel, Item, CKey1, Limit)	' start with first key after CKey1, return Limit results or less
 		, delete/2					%% (Channel, KeyTable)    			' do not complain if keys do not exist
 		, deleteGELT/3				%% (Channel, CKey1, CKey2)			
		, deleteGTLT/3				%% (Channel, CKey1, CKey2)
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
	Token = list_to_binary([TBin,integer_to_list(erlang:phash2(TBin))]),
	try 
		_ = ?binary_to_existing_atom(Token), 	%% probe atom cache for token
		{?binary_to_existing_atom(TBin),?binary_to_existing_atom(?AUDIT(Channel))}
	catch 
		_:_ -> 
			TN = ?binary_to_atom(TBin),
			imem_meta:create_check_table(TN, {record_info(fields, skvhTable),?skvhTable, #skvhTable{}}, ?TABLE_OPTS, system),    
			AN = ?binary_to_atom(?AUDIT(Channel)),
			imem_meta:create_check_table(AN, {record_info(fields, skvhAudit),?skvhAudit, #skvhAudit{}}, ?AUDIT_OPTS, system),
			_ = ?binary_to_atom(Token), 		%% put token to atom cache
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

clean_table(Bin) ->
	case binary:last(Bin) of
		10 ->	binary:replace(binary_part(Bin,0,size(Bin)-1), <<13>>, <<>>, [global]);
		_ ->	binary:replace(Bin, <<13>>, <<>>, [global])
	end.

io_key_to_term(Key) when is_binary(Key) ->
	imem_datatype:io_to_term(Key).

term_key_to_io(Key) ->
	imem_datatype:term_to_io(Key).

% io_hash_to_term(Hash) when is_binary(Hash) -> Hash.

term_hash_to_io(Hash) when is_binary(Hash) -> 
	Hash; 
term_hash_to_io(Hash) when is_integer(Hash) -> 
	list_to_binary(integer_to_list(Hash)).

io_value_to_term(V) ->
	% imem_datatype:io_to_term(V).
	V.

term_value_to_io(V) ->
	% imem_datatype:term_to_io(V).
	V.

io_key_table_to_term_list(KeyTable) ->
	BList = binary:split(clean_table(KeyTable),<<10>>,[global]),	%% split at line feed positions
	[io_key_to_term(Key) || Key <- BList].

io_kv_pair_to_tuple(KVPair) ->
	case binary:split(KVPair,<<9>>,[global]) of			%% split at tab position
		[K,V] -> 	{io_key_to_term(K),io_value_to_term(V)};
		_ ->		?ClientError(?E102)
	end.

term_kv_tuple_to_io({K,V}) ->
	KeyBin = term_key_to_io(K),
	ValueBin = term_value_to_io(V), 
	<<KeyBin/binary,9,ValueBin/binary>>.

term_kh_tuple_to_io({K,H}) ->
	KeyBin = term_key_to_io(K),
	HashBin = term_hash_to_io(H), 
	<<KeyBin/binary,9,HashBin/binary>>.

io_kv_table_to_tuple_list(KVTable) ->
	BList = binary:split(clean_table(KVTable),<<10>>,[global]),		%% split at line feed positions
	[io_kv_pair_to_tuple(KVbin) || KVbin <- BList].

read(Channel, KeyTable) when is_binary(Channel), is_binary(KeyTable) ->
	Cmd = [read,Channel,KeyTable],
	read(Cmd, create_check_channel(Channel), io_key_table_to_term_list(KeyTable), []).

read(Cmd, _, [], Acc)  -> return_list(Cmd, lists:reverse(Acc));
read(Cmd, {TN,AN}, [Key|Keys], Acc)  ->
	KVP = case imem_meta:read(TN,Key) of
		[] ->								term_kv_tuple_to_io({Key,<<"undefined">>});
		[#skvhTable{ckey=K,cvalue=V}] ->	term_kv_tuple_to_io({K,V})
	end,
	read(Cmd, {TN,AN}, Keys, [KVP|Acc]).
	

write(Channel, KVTable) when is_binary(Channel), is_binary(KVTable) ->
	Cmd = [write,Channel,KVTable],
	write(Cmd, create_check_channel(Channel), io_kv_table_to_tuple_list(KVTable), []).

write(Cmd, _, [], Acc)  -> return_list(Cmd, lists:reverse(Acc));
write(Cmd, {TN,AN}, [{K,V}|KVPairs], Acc)  ->
	Hash = list_to_binary(integer_to_list(erlang:phash2({K,V}))),
	imem_meta:write(TN,#skvhTable{ckey=K,cvalue=V,chash=Hash}),			%% ToDo: wrap in transaction
	imem_meta:write(AN,#skvhAudit{time=erlang:now(),ckey=K,cvalue=V}),	%% ToDo: wrap in transaction
	write(Cmd, {TN,AN}, KVPairs, [Hash|Acc]).

readGELT(Channel, Item, CKey1, CKey2) when is_binary(Item), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [readGELT, Channel, Item, CKey1, CKey2],
	readGELT(Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_key_to_term(CKey2)).

readGELT(Cmd, {TN,_}, Item, Key1, Key2) ->
	MatchHead = {skvhTable, '$1', '$2', '$3'},
	MatchFunction = {MatchHead, [{'>=', '$1', Key1}, {'<', '$1', Key2}], ['$_']},
	{L,true} = imem_meta:select(TN, [MatchFunction]),
	project_result(Cmd, L, Item).

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

readGT(Channel, Item, CKey1, Limit)  when is_binary(Item), is_binary(CKey1), is_binary(Limit) ->
	Cmd = [readGT, Channel, Item, CKey1, Limit],
	readGT(Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_key_to_term(Limit)).

readGT(Cmd, {TN,_}, Item, Key1, Limit) ->
	MatchHead = {skvhTable, '$1', '$2', '$3'},
	MatchFunction = {MatchHead, [{'>', '$1', Key1}], ['$_']},
	{L,_} = imem_meta:select(TN, [MatchFunction], Limit),
	project_result(Cmd, L, Item).

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

        ?assertEqual({ok,[<<"[1,a]",9,"undefined">>]}, read(Channel, <<"[1,a]">>)),

        ?assertEqual(ok, imem_meta:check_table(skvhTEST)),        
        ?assertEqual(ok, imem_meta:check_table(skvhAuditTEST_86400@)),

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

        ?assertEqual({ok,[KVa]}, read(Channel, <<"[1,a]">>)),
        ?assertEqual({ok,[KVc]}, read(Channel, <<"[1,c]">>)),
        ?assertEqual({ok,[KVb]}, read(Channel, <<"[1,b]">>)),

        ?assertEqual({ok,[KVc,KVb,KVa]}, read(Channel, <<"[1,c]",13,10,"[1,b]",10,"[1,a]">>)),

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, delete(Channel, <<"[1,a]",10,"[1,b]",13,10,"[1,c]",10>>)),

        ?assertEqual({ok,[<<"[1,a]",9,"undefined">>]}, read(Channel, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"[1,b]",9,"undefined">>]}, read(Channel, <<"[1,b]">>)),
        ?assertEqual({ok,[<<"[1,c]",9,"undefined">>]}, read(Channel, <<"[1,c]">>)),

        ?assertEqual({ok,[<<"1296644">>,<<"124793159">>,<<"117221887">>]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

		?assertEqual({ok,[]},deleteGELT(Channel, <<"[]">>, <<"[]">>)),
		?assertEqual({ok,[]},deleteGELT(Channel, <<"[]">>, <<"[1,a]">>)),

		?assertEqual({ok,[<<"1296644">>]},deleteGELT(Channel, <<"[]">>, <<"[1,ab]">>)),
		?assertEqual({ok,[]},deleteGELT(Channel, <<"[]">>, <<"[1,ab]">>)),

		?assertEqual({ok,[<<"124793159">>,<<"117221887">>]},deleteGELT(Channel, <<"[1,ab]">>, <<"[1,d]">>)),		
		?assertEqual({ok,[]},deleteGELT(Channel, <<"[1,ab]">>, <<"[1,d]">>)),

        ?assertEqual({ok,[<<"undefined">>,<<"undefined">>,<<"undefined">>]}, delete(Channel, <<"[1,a]",10,"[1,b]",13,10,"[1,c]",10>>)),

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
