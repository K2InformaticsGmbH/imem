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
		, readKeyHeads/1 			%% (Channel)      		 			' return empty Arraylist if no resources exists
		, readGELT/3				%% (Channel, CKey1, CKey2)			' start with first key after CKey1, end with last key before CKey2
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

key_to_term(Key) ->
	imem_datatype:io_to_term(Key).

key_table_to_term_list(KeyTable) ->
	BList = binary:split(clean_table(KeyTable),<<10>>,[global]),	%% split at line feed positions
	[key_to_term(Key) || Key <- BList].

kv_pair_to_tuple(KVPair) ->
	case binary:split(KVPair,<<9>>,[global]) of						%% split at tab position
		[K,V] -> 	{imem_datatype:io_to_term(K),V};				%% leave V as binary for now
		_ ->		?ClientError(?E102)
	end.

kv_table_to_tuple_list(KVTable) ->
	BList = binary:split(clean_table(KVTable),<<10>>,[global]),		%% split at line feed positions
	[kv_pair_to_tuple(KVbin) || KVbin <- BList].

read(Channel, KeyTable) when is_binary(Channel), is_binary(KeyTable) ->
	Cmd = [read,Channel,KeyTable],
	read(Cmd, create_check_channel(Channel), key_table_to_term_list(KeyTable), []).

read(Cmd, _, [], Acc)  -> return_list(Cmd, lists:reverse(Acc));
read(Cmd, {TN,AN}, [Key|Keys], Acc)  ->
	KVP = case imem_meta:read(TN,Key) of
		[] ->								list_to_binary(io_lib:format("~p\tundefined",[Key]));
		[#skvhTable{ckey=K,cvalue=V}] ->	list_to_binary(io_lib:format("~p\t~s",[K,V]))
	end,
	read(Cmd, {TN,AN}, Keys, [KVP|Acc]).
	

write(Channel, KVTable) when is_binary(Channel), is_binary(KVTable) ->
	Cmd = [write,Channel,KVTable],
	write(Cmd, create_check_channel(Channel), kv_table_to_tuple_list(KVTable), []).

write(Cmd, _, [], Acc)  -> return_list(Cmd, lists:reverse(Acc));
write(Cmd, {TN,AN}, [{K,V}|KVPairs], Acc)  ->
	Hash = erlang:phash2({K,V}),
	imem_meta:write(TN,#skvhTable{ckey=K,cvalue=V,chash=Hash}),			%% ToDo: wrap in transaction
	imem_meta:write(AN,#skvhAudit{time=erlang:now(),ckey=K,cvalue=V}),	%% ToDo: wrap in transaction
	write(Cmd, {TN,AN}, KVPairs, [Hash|Acc]).

readKeyHeads(_Channel) ->
	ok.

readGELT(_Channel, _CKey1, _CKey2) ->
	ok.

readGT(_Channel, _Item, _CKey1, _Limit) ->
	ok.

delete(Channel, KeyTable) when is_binary(Channel), is_binary(KeyTable) -> 
	Cmd = [delete,Channel,KeyTable],
	delete(Cmd, create_check_channel(Channel), key_table_to_term_list(KeyTable), []).

delete(Cmd, _, [], Acc)  -> return_list(Cmd, lists:reverse(Acc));
delete(Cmd, {TN,AN}, [Key|Keys], Acc)  ->
	Hash = case imem_meta:read(TN,Key) of
		[] ->						undefined;
		[#skvhTable{chash=H}] ->	H
	end,
	imem_meta:delete(TN,Key),									%% ToDo: wrap in transaction
	imem_meta:write(AN,#skvhAudit{time=erlang:now(),ckey=Key}),	%% ToDo: wrap in transaction
	delete(Cmd, {TN,AN}, Keys, [Hash|Acc]).

deleteGELT(Channel, CKey1, CKey2) when is_binary(Channel), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [deleteGELT, Channel, CKey1, CKey2],
	deleteGELT(Cmd, create_check_channel(Channel), key_to_term(CKey1), key_to_term(CKey2)).

deleteGELT(Cmd, {TN,AN}, Key1, Key2) ->
	MatchHead = {skvhTable, '$1', '$2', '$3'},
	MatchFunction = {MatchHead, [{'>=', '$1', Key1}, {'<', '$1', Key2}], ['$1']},
	{L,true} = imem_dal:select(TN, [MatchFunction]),
	delete(Cmd, {TN,AN}, L, []).

deleteGTLT(Channel, CKey1, CKey2) when is_binary(Channel), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [deleteGTLT, Channel, CKey1, CKey2],
	deleteGTLT(Cmd, create_check_channel(Channel), key_to_term(CKey1), key_to_term(CKey2)).

deleteGTLT(Cmd, {TN,AN}, Key1, Key2) ->
	MatchHead = {skvhTable, '$1', '$2', '$3'},
	MatchFunction = {MatchHead, [{'>', '$1', Key1}, {'<', '$1', Key2}], ['$1']},
	{L,true} = imem_dal:select(TN, [MatchFunction]),
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

        ?assertEqual({ok,[1296644,124793159,117221887]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

        ?assertEqual({ok,[KVa]}, read(Channel, <<"[1,a]">>)),
        ?assertEqual({ok,[KVc]}, read(Channel, <<"[1,c]">>)),
        ?assertEqual({ok,[KVb]}, read(Channel, <<"[1,b]">>)),

        ?assertEqual({ok,[KVc,KVb,KVa]}, read(Channel, <<"[1,c]",13,10,"[1,b]",10,"[1,a]">>)),

        ?assertEqual({ok,[1296644,124793159,117221887]}, delete(Channel, <<"[1,a]",10,"[1,b]",13,10,"[1,c]",10>>)),

        ?assertEqual({ok,[<<"[1,a]",9,"undefined">>]}, read(Channel, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"[1,b]",9,"undefined">>]}, read(Channel, <<"[1,b]">>)),
        ?assertEqual({ok,[<<"[1,c]",9,"undefined">>]}, read(Channel, <<"[1,c]">>)),


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
