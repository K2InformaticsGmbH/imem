-module(imem_dal_skvh).

-include("imem.hrl").
-include("imem_meta.hrl").

-define(CHANNEL(__Table),list_to_binary(lists:nthtail(4,atom_to_list(__Table)))).
-define(TABLE(__Channel),<<"skvh",__Channel/binary>>).
-define(AUDIT(__Channel),<<"skvhAudit",__Channel/binary, "_86400@">>).
-define(MATCHHEAD,{skvhTable, '$1', '$2', '$3'}).
-define(AUDIT_MATCHHEAD,{skvhAudit, '$1', '$2', '$3', '$4'}).

-define(TABLE_OPTS, [{record_name,skvhTable}
                    ,{type,ordered_set}
                    ]).          

-define(AUDIT_OPTS, [{record_name,skvhAudit}
                    ,{type,ordered_set}
                    ,{purge_delay,430000}        %% 430000 = 5 Days - 2000 sec
                    ]).          

-record(skvhAudit,                            %% sorted key value hash audit table    
                    { time                    :: ddTimestamp()	%% erlang:now()
                    , ckey                    :: term()|undefined
                    , cvalue               	  :: binary()|undefined
                    , cuser=unknown			  :: ddEntityId()|unknown
                    }
       ).
-define(skvhAudit,  [timestamp,term,binstr,userid]).

-record(skvhTable,                            %% sorted key value hash table    
                    { ckey                    :: term()
                    , cvalue  				  :: binary()      
                    , chash	= <<"fun(_,__Rec) -> list_to_binary(io_lib:format(\"~.36B\",[erlang:phash2({element(2,__Rec),element(3,__Rec)})])) end.">>
                    						  :: binary()
                    }
       ).
-define(skvhTable,  [term,binstr,binstr]).
-define(skvhTableTrigger, <<"fun (__OR,__NR,__T,__U) ->
                    			imem_dal_skvh:write_audit(__OR,__NR,__T,__U)
                    		end.">>
	   ).

-define(typeStr,1).

% -define(E100,{100,"Invalid key"}).
% -define(E101,{101,"Duplicate key"}).
-define(E102(__Term),{102,"Invalid key value pair",__Term}).
% -define(E103,{103,<<"Invalid source peer key (short code)">>}).
% -define(E104,{104,"Unknown white list IP addresses"}).
% -define(E105,{105,<<"Invalid white list IP address">>}).
% -define(E106,{106,<<"Unknown source peer">>}).
% -define(E107,{107,"Duplicate SMS proxy service"}).
% -define(E108,{108,"Invalid service name"}).
% -define(E109,{109,<<"Invalid destination channel (port)">>}).
% -define(E110,{110,"Duplicate white list IP address"}).
% -define(E111,{111,"Invalid WAP push preference value"}).
% -define(E112,{112,"Unknown error"}).
% -define(E113,{113,"Could not delete content provider with SMS short code <value> because SMS proxy service exists for it."}).
% -define(E114,{114,"Could not create SMS proxy service."}).
% -define(E115,{115,"Could not activate SMS proxy service."}).
% -define(E116,{116,<<"Invalid option value">>}).
-define(E117(__Term),{117,"Too many values, Limit exceeded",__Term}).


-export([ write/2 			%% (Channel, KVTable)    			resource may not exist, will be created, return list of hashes 
		, read/3			%% (Channel, KeyTable)   			return empty Arraylist if none of these resources exists
		, readGELT/5		%% (Channel, Item, CKey1, CKey2, L)	start with first key after CKey1, end with last key before CKey2, fails if more than L rows
		, readGT/4			%% (Channel, Item, CKey1, Limit)	start with first key after CKey1, return Limit results or less
		, audit_readGT/4	%% (Channel, Item, TS1, Limit)		read audit info after Timestamp1, return Limit results or less
 		, readGE/4			%% (Channel, Item, CKey1, Limit)	start with first key at or after CKey1, return Limit results or less
 		, delete/2			%% (Channel, KeyTable)    			do not complain if keys do not exist
 		, deleteGELT/4		%% (Channel, CKey1, CKey2), L		delete range of keys >= CKey1 and < CKey2, fails if more than L rows
		, deleteGTLT/4		%% (Channel, CKey1, CKey2), L		delete range of keys > CKey1 and < CKey2, fails if more than L rows
		, write_audit/4 	%% (OldRec,NewRec,Table,User)		default trigger for writing audit trail
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
	try 
		T = ?binary_to_existing_atom(TBin),
		A = ?binary_to_existing_atom(?AUDIT(Channel)),
		imem_meta:check_table(T),
		{T,A}
	catch 
		_:_ -> 
			TN = ?binary_to_atom(TBin),
			case (catch imem_meta:create_check_table(TN, {record_info(fields, skvhTable),?skvhTable, #skvhTable{}}, ?TABLE_OPTS, system)) of
				ok ->				ok;
    			{error, Reason} -> 	imem_meta:log_to_db(warning,?MODULE,create_check_table,[{table,TN}],io_lib:format("~p",[Reason]));
    			Reason -> 			imem_meta:log_to_db(warning,?MODULE,create_check_table,[{table,TN}],io_lib:format("~p",[Reason]))
    		end,
			AN = ?binary_to_atom(?AUDIT(Channel)),
			catch imem_meta:create_check_table(AN, {record_info(fields, skvhAudit),?skvhAudit, #skvhAudit{}}, ?AUDIT_OPTS, system),
        	ok = imem_meta:create_trigger(TN, ?skvhTableTrigger),
			{TN,AN}
	end.

write_audit(OldRec,NewRec,Table,User) ->
	{K,V} = case {OldRec,NewRec} of
		{{},{}} ->	{undefined,undefined};					%% truncate table
		{_,{}} ->	{element(2,OldRec),undefined}; 			%% delete old rec
		{_, _} ->	{element(2,NewRec),element(3,NewRec)}	%% write new rec
	end,
	CH = ?CHANNEL(Table),
	A = ?binary_to_existing_atom(?AUDIT(CH)),
	catch (imem_meta:write(A,#skvhAudit{time=erlang:now(),ckey=K,cvalue=V,cuser=User})).

% return(Cmd, Result) ->
% 	debug(Cmd, Result), 
% 	Result.

% return_error(Cmd, Tuple) -> 
% 	debug(Cmd, Tuple), 
% 	{error,Tuple}.

return_stringlist(Cmd, List) -> 
	debug(Cmd, List), 
	{ok,[{?typeStr,E} || E <- List]}.

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

io_to_integer(I) when is_integer(I) -> 		I;
io_to_integer(Key) when is_binary(Key) ->	imem_datatype:io_to_integer(Key,0,0).

io_value_to_term(V) when is_binary(V) -> 		V.	%% ToDo: Maybe convert to map datatype when available

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

term_key_to_io(Key) ->	imem_datatype:term_to_io(Key).

term_time_to_io(T) ->	imem_datatype:timestamp_to_io(T).

term_value_to_io(undefined) -> <<"undefined">>;		%% ToDo: Maybe convert from map datatype when available
term_value_to_io(V) when is_binary(V) -> V.   		%% ToDo: Maybe convert from map datatype when available

term_hash_to_io(undefined) -> <<"undefined">>;
term_hash_to_io(F) when is_function(F) -> <<"undefined">>;
term_hash_to_io(H) when is_binary(H) -> H.

term_user_to_io(U) ->	imem_datatype:userid_to_io(U).

term_kv_tuple_to_io({K,V}) ->
	KeyBin = term_key_to_io(K),
	ValueBin = term_value_to_io(V), 
	<<KeyBin/binary,9,ValueBin/binary>>.

term_kh_tuple_to_io({K,H}) ->
	KeyBin = term_key_to_io(K),
	HashBin = term_hash_to_io(H), 
	<<KeyBin/binary,9,HashBin/binary>>.

term_kvh_triple_to_io({K,V,H}) ->
	KeyBin = term_key_to_io(K),
	ValueBin = term_value_to_io(V), 
	HashBin = term_hash_to_io(H), 
	<<KeyBin/binary,9,ValueBin/binary,9,HashBin/binary>>.

term_tkv_triple_to_io({T,K,V}) ->
	TimeBin = term_time_to_io(T),
	KeyBin = term_key_to_io(K),
	ValueBin = term_value_to_io(V), 
	<<TimeBin/binary,9,KeyBin/binary,9,ValueBin/binary>>.

term_tkvu_quadruple_to_io({T,K,V,U}) ->
	TimeBin = term_time_to_io(T),
	KeyBin = term_key_to_io(K),
	ValueBin = term_value_to_io(V), 
	UserBin = term_user_to_io(U),
	<<TimeBin/binary,9,KeyBin/binary,9,ValueBin/binary,9,UserBin/binary>>.

%% Data access per key (read,write,delete)

read(Channel, Item, KeyTable) when is_binary(Channel), is_binary(Item), is_binary(KeyTable) ->
	Cmd = [read,Channel,Item,KeyTable],
	read(Cmd, create_check_channel(Channel), Item, io_key_table_to_term_list(KeyTable), []).

read(Cmd, _, Item, [], Acc)  -> project_result(Cmd, lists:reverse(Acc), Item);
read(Cmd, {TN,AN}, Item, [Key|Keys], Acc)  ->
	KVP = case imem_meta:read(TN,Key) of
		[] ->		#skvhTable{ckey=Key,cvalue=undefined,chash=undefined} ;
		[Rec] ->	Rec
	end,
	read(Cmd, {TN,AN}, Item, Keys, [KVP|Acc]).
	

write(Channel, KVTable) when is_binary(Channel), is_binary(KVTable) ->
	Cmd = [write,Channel,KVTable],
	write(Cmd, create_check_channel(Channel), io_kv_table_to_tuple_list(KVTable), []).

write(Cmd, _, [], Acc)  -> return_stringlist(Cmd, lists:reverse(Acc));
write(Cmd, {TN,AN}, [{K,V}|KVPairs], Acc)  ->
	#skvhTable{chash=Hash} = imem_meta:merge(TN,#skvhTable{ckey=K,cvalue=V}),
	write(Cmd, {TN,AN}, KVPairs, [Hash|Acc]).

delete(Channel, KeyTable) when is_binary(Channel), is_binary(KeyTable) -> 
	Cmd = [delete,Channel,KeyTable],
	delete(Cmd, create_check_channel(Channel), io_key_table_to_term_list(KeyTable), []).

delete(Cmd, _, [], Acc)  -> return_stringlist(Cmd, lists:reverse(Acc));
delete(Cmd, {TN,AN}, [Key|Keys], Acc)  ->
	Hash = try
		#skvhTable{chash=H} = imem_meta:remove(TN,#skvhTable{ckey=Key}),
		H
	catch
		throw:{'ConcurrencyException',{"Remove failed, key does not exist", _}} -> undefined
	end,
	delete(Cmd, {TN,AN}, Keys, [term_hash_to_io(Hash)|Acc]).

%% Data Access per key range

match_val(T) when is_tuple(T) -> {const,T};
match_val(V) -> V.

readGT(Channel, Item, CKey1, Limit)  when is_binary(Item), is_binary(CKey1) ->
	Cmd = [readGT, Channel, Item, CKey1, Limit],
	readGT(Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_to_integer(Limit)).

readGT(Cmd, {TN,TA}, Item, Key1, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>', '$1', match_val(Key1)}], ['$_']},
	read_limited(Cmd, {TN,TA}, Item, MatchFunction, Limit).

audit_readGT(Channel, Item, TS1, Limit)  when is_binary(Item), is_binary(TS1) ->
	Cmd = [audit_readGT, Channel, Item, TS1, Limit],
	audit_readGT(Cmd, create_check_channel(Channel), Item, imem_datatype:io_to_timestamp(TS1), io_to_integer(Limit)).

audit_readGT(Cmd, {TN,TA}, Item, TS1, Limit) ->
	MatchFunction = {?AUDIT_MATCHHEAD, [{'>', '$1', match_val(TS1)}], ['$_']},
	audit_read_limited(Cmd, {TN,TA}, Item, MatchFunction, Limit).

audit_read_limited(Cmd, {_,TA}, Item, MatchFunction, Limit) ->
	{L,_} = imem_meta:select(TA, [MatchFunction], Limit),
	audit_project_result(Cmd, L, Item).

readGE(Channel, Item, CKey1, Limit)  when is_binary(Item), is_binary(CKey1) ->
	Cmd = [readGT, Channel, Item, CKey1, Limit],
	readGE(Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_to_integer(Limit)).

readGE(Cmd, {TN,TA}, Item, Key1, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}], ['$_']},
	read_limited(Cmd, {TN,TA}, Item, MatchFunction, Limit).

read_limited(Cmd, {TN,_}, Item, MatchFunction, Limit) ->
	{L,_} = imem_meta:select(TN, [MatchFunction], Limit),
	project_result(Cmd, L, Item).

readGELT(Channel, Item, CKey1, CKey2, Limit) when is_binary(Item), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [readGELT, Channel, Item, CKey1, CKey2, Limit],
	readGELT(Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_key_to_term(CKey2), io_to_integer(Limit)).

readGELT(Cmd, {TN,TA}, Item, Key1, Key2, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$_']},
	read_with_limit(Cmd, {TN,TA}, Item, MatchFunction, Limit).

read_with_limit(Cmd, {TN,_}, Item, MatchFunction, Limit) ->
	{L,_} = imem_meta:select(TN, [MatchFunction], Limit+1),
	if  
		length(L) > Limit ->	?ClientError(?E117(Limit));
		true ->					project_result(Cmd, L, Item)
	end.


deleteGELT(Channel, CKey1, CKey2, Limit) when is_binary(Channel), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [deleteGELT, Channel, CKey1, CKey2, Limit],
	deleteGELT(Cmd, create_check_channel(Channel), io_key_to_term(CKey1), io_key_to_term(CKey2), io_to_integer(Limit)).

deleteGELT(Cmd, {TN,AN}, Key1, Key2, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$1']},
	delete_with_limit(Cmd, {TN,AN}, MatchFunction, Limit).

deleteGTLT(Channel, CKey1, CKey2, Limit) when is_binary(Channel), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [deleteGTLT, Channel, CKey1, CKey2, Limit],
	deleteGTLT(Cmd, create_check_channel(Channel), io_key_to_term(CKey1), io_key_to_term(CKey2), io_to_integer(Limit)).

deleteGTLT(Cmd, {TN,AN}, Key1, Key2, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$1']},
	delete_with_limit(Cmd, {TN,AN}, MatchFunction, Limit).

delete_with_limit(Cmd, {TN,AN}, MatchFunction, Limit) ->
	{L,_} = imem_meta:select(TN, [MatchFunction], Limit+1),
	if  
		length(L) > Limit ->	?ClientError(?E117(Limit));
		true ->					delete(Cmd, {TN,AN}, L, [])
	end.

project_result(Cmd, L, <<"key">>) ->
	return_stringlist(Cmd, [term_key_to_io(K) ||  #skvhTable{ckey=K} <- L]);
project_result(Cmd, L, <<"value">>) ->
	return_stringlist(Cmd, [term_value_to_io(V) ||  #skvhTable{cvalue=V} <- L]);
project_result(Cmd, L, <<"hash">>) ->
	return_stringlist(Cmd, [term_hash_to_io(H) ||  #skvhTable{chash=H} <- L]);
project_result(Cmd, L, <<"kvpair">>) ->
	return_stringlist(Cmd, [term_kv_tuple_to_io({K,V}) ||  #skvhTable{ckey=K,cvalue=V} <- L]);
project_result(Cmd, L, <<"kvhtriple">>) ->
	return_stringlist(Cmd, [term_kvh_triple_to_io({K,V,H}) ||  #skvhTable{ckey=K,cvalue=V,chash=H} <- L]);
project_result(Cmd, L, <<"khpair">>) ->
	return_stringlist(Cmd, [term_kh_tuple_to_io({K,H}) ||  #skvhTable{ckey=K,chash=H} <- L]).


audit_project_result(Cmd, L, <<"time">>) ->
	return_stringlist(Cmd, [term_time_to_io(T) ||  #skvhAudit{time=T} <- L]);
audit_project_result(Cmd, L, <<"key">>) ->
	return_stringlist(Cmd, [term_key_to_io(K) ||  #skvhAudit{ckey=K} <- L]);
audit_project_result(Cmd, L, <<"value">>) ->
	return_stringlist(Cmd, [term_value_to_io(V) ||  #skvhAudit{cvalue=V} <- L]);
audit_project_result(Cmd, L, <<"user">>) ->
	return_stringlist(Cmd, [term_user_to_io(U) ||  #skvhAudit{cuser=U} <- L]);
audit_project_result(Cmd, L, <<"kvpair">>) ->
	return_stringlist(Cmd, [term_kv_tuple_to_io({K,V}) ||  #skvhAudit{ckey=K,cvalue=V} <- L]);
audit_project_result(Cmd, L, <<"tkvtriple">>) ->
	return_stringlist(Cmd, [term_tkv_triple_to_io({T,K,V}) ||  #skvhAudit{time=T,ckey=K,cvalue=V} <- L]);
audit_project_result(Cmd, L, <<"tkvuquadruple">>) ->
	return_stringlist(Cmd, [term_tkvu_quadruple_to_io({T,K,V,U}) ||  #skvhAudit{time=T,ckey=K,cvalue=V,cuser=U} <- L]).

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

        ?Info("---TEST---~p:test_skvh~n", [?MODULE]),

        ?assertEqual(<<"skvhTEST">>, ?TABLE(Channel)),

        ?assertException(throw, {ClEr,{"Table does not exist",_}}, imem_meta:check_table(skvhTEST)), 
        ?assertException(throw, {ClEr,{"Table does not exist",_}}, imem_meta:check_table(skvhAuditTEST_86400@)), 

        KVa = <<"[1,a]",9,"123456">>,
        KVb = <<"[1,b]",9,"234567">>,
        KVc = <<"[1,c]",9,"345678">>,

		K0 = <<"{<<\"0\">>,<<>>,<<>>}">>,

        ?assertEqual({ok,[{1,<<"{<<\"0\">>,<<>>,<<>>}\tundefined">>}]}, read(Channel, <<"kvpair">>, K0)),
		?assertEqual({ok,[]}, readGT(Channel, <<"khpair">>, <<"{<<\"0\">>,<<>>,<<>>}">>, <<"1000">>)),

        ?assertEqual({ok,[{1,<<"[1,a]\tundefined">>}]}, read(Channel, <<"kvpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[{1,<<"[1,a]\tundefined">>}]}, read(Channel, <<"khpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[{1,<<"[1,a]">>}]}, read(Channel, <<"key">>, <<"[1,a]">>)),
        ?assertEqual({ok,[{1,<<"undefined">>}]}, read(Channel, <<"value">>, <<"[1,a]">>)),
        ?assertEqual({ok,[{1,<<"undefined">>}]}, read(Channel, <<"hash">>, <<"[1,a]">>)),

        ?assertEqual(ok, imem_meta:check_table(skvhTEST)),        
        ?assertEqual(ok, imem_meta:check_table(skvhAuditTEST_86400@)),

        ?assertEqual({ok,[{1,<<"RSHW">>},{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

        ?assertEqual({ok,[{1,KVa}]}, read(Channel, <<"kvpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[{1,KVc}]}, read(Channel, <<"kvpair">>, <<"[1,c]">>)),
        ?assertEqual({ok,[{1,KVb}]}, read(Channel, <<"kvpair">>, <<"[1,b]">>)),
        ?assertEqual({ok,[{1,<<"[1,c]",9,"1XSGZJ">>}]}, read(Channel, <<"khpair">>, <<"[1,c]">>)),
        ?assertEqual({ok,[{1,<<"22AR0N">>}]}, read(Channel, <<"hash">>, <<"[1,b]">>)),

        ?assertEqual({ok,[{1,KVc},{1,KVb},{1,KVa}]}, read(Channel, <<"kvpair">>, <<"[1,c]",13,10,"[1,b]",10,"[1,a]">>)),
        ?assertEqual({ok,[{1,KVa},{1,<<"[1,ab]",9,"undefined">>},{1,KVb},{1,KVc}]}, read(Channel, <<"kvpair">>, <<"[1,a]",13,10,"[1,ab]",13,10,"[1,b]",10,"[1,c]">>)),

        Dat = imem_meta:read(skvhTEST),
        ?Info("TEST data ~n~p~n", [Dat]),
        ?assertEqual(3, length(Dat)),

        ?assertEqual({ok,[{1,<<"RSHW">>},{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, delete(Channel, <<"[1,a]",10,"[1,b]",13,10,"[1,c]",10>>)),

        Aud = imem_meta:read(skvhAuditTEST_86400@),
        ?Info("audit trail~n~p~n", [Aud]),
        ?assertEqual(6, length(Aud)),
        {ok,Aud1} = audit_readGT(Channel,<<"tkvuquadruple">>, <<"{0,0,0}">>, <<"100">>),
        ?Info("audit trail~n~p~n", [Aud1]),
        ?assertEqual(6, length(Aud1)),
        {ok,Aud2} = audit_readGT(Channel,<<"tkvtriple">>, <<"{0,0,0}">>, 4),
        ?assertEqual(4, length(Aud2)),
        {ok,Aud3} = audit_readGT(Channel,<<"kvpair">>, <<"now">>, 100),
        ?assertEqual(0, length(Aud3)),
        {ok,Aud4} = audit_readGT(Channel,<<"key">>, <<"2100-01-01">>, 100),
        ?assertEqual(0, length(Aud4)),
        Ex4 = {'ClientError',{"Data conversion format error",{timestamp,"1900-01-01",{"Cannot handle dates before 1970"}}}},
        ?assertException(throw,Ex4,audit_readGT(Channel,<<"tkvuquadruple">>, <<"1900-01-01">>, 100)),
        {ok,Aud5} = audit_readGT(Channel,<<"tkvuquadruple">>, <<"1970-01-01">>, 100),
        ?assertEqual(Aud1, Aud5),

        ?assertEqual({ok,[{1,<<"[1,a]",9,"undefined">>}]}, read(Channel, <<"kvpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[{1,<<"[1,b]",9,"undefined">>}]}, read(Channel, <<"kvpair">>, <<"[1,b]">>)),
        ?assertEqual({ok,[{1,<<"[1,c]",9,"undefined">>}]}, read(Channel, <<"kvpair">>, <<"[1,c]">>)),

        ?assertEqual({ok,[{1,<<"RSHW">>},{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

		?assertEqual({ok,[]},deleteGELT(Channel, <<"[]">>, <<"[]">>, <<"2">>)),
		?assertEqual({ok,[]},deleteGELT(Channel, <<"[]">>, <<"[1,a]">>, <<"2">>)),

		?assertEqual({ok,[{1,<<"RSHW">>}]},deleteGELT(Channel, <<"[]">>, <<"[1,ab]">>, <<"2">>)),
		?assertEqual({ok,[]},deleteGELT(Channel, <<"[]">>, <<"[1,ab]">>, <<"2">>)),

		?assertException(throw,{ClEr,{117,"Too many values, Limit exceeded",1}},deleteGELT(Channel, <<"[1,ab]">>, <<"[1,d]">>, <<"1">>)),		
		?assertEqual({ok,[{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]},deleteGELT(Channel, <<"[1,ab]">>, <<"[1,d]">>, <<"2">>)),		
		?assertEqual({ok,[]},deleteGELT(Channel, <<"[1,ab]">>, <<"[1,d]">>, <<"2">>)),

        ?assertEqual({ok,[{1,<<"undefined">>},{1,<<"undefined">>},{1,<<"undefined">>}]}, delete(Channel, <<"[1,a]",10,"[1,b]",13,10,"[1,c]",10>>)),

        ?assertEqual({ok,[{1,<<"17WIRH">>}]}, write(Channel, <<"[90074,[],\"AaaEnabled\"]",9,"true">>)),
		?assertEqual({ok,[{1,<<"1CN887">>}]}, write(Channel, <<"[90074,[],\"ContentSizeMax\"]",9,"297000">>)),
		?assertEqual({ok,[{1,<<"1N37JU">>}]}, write(Channel, <<"[90074,[],<<\"MmscId\">>]",9,"\"testMMSC\"">>)),
		?assertEqual({ok,[{1,<<"WSWNV">>}]}, write(Channel, <<"[90074,\"MMS-DEL-90074\",\"TpDeliverUrl\"]",9,"\"http:\/\/10.132.30.84:18888\/deliver\"">>)),

		?assertEqual(ok,imem_meta:truncate_table(skvhTEST)),
				
        ?Info("audit trail~n~p~n", [imem_meta:read(skvhAuditTEST_86400@)]),

		?assertEqual(ok,imem_meta:drop_table(skvhTEST)),

        ?assertEqual({ok,[{1,<<"RSHW">>},{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[]">>, <<"[]">>, <<"1">>)),
		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[]">>, <<"[1,a]">>, <<"1">>)),

		?assertEqual({ok,[{1,<<"RSHW">>}]},deleteGTLT(Channel, <<"[]">>, <<"[1,ab]">>, <<"1">>)),
		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[]">>, <<"[1,ab]">>, <<"1">>)),

		?assertEqual({ok,[{1,<<"1XSGZJ">>}]},deleteGTLT(Channel, <<"[1,b]">>, <<"[1,d]">>, <<"1">>)),		
		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[1,b]">>, <<"[1,d]">>, <<"1">>)),

		?assertEqual({ok,[{1,<<"22AR0N">>}]},deleteGTLT(Channel, <<"[1,a]">>, <<"[1,c]">>, <<"1">>)),		
		?assertEqual({ok,[]},deleteGTLT(Channel, <<"[1,a]">>, <<"[1,c]">>, <<"1">>)),		

        ?assertEqual({ok,[{1,<<"RSHW">>},{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, write(Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

        ?assertException(throw,{ClEr,{117,"Too many values, Limit exceeded",1}}, readGELT(Channel, <<"hash">>, <<"[]">>, <<"[1,d]">>, <<"1">>)),
        ?assertEqual({ok,[{1,<<"RSHW">>},{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, readGELT(Channel, <<"hash">>, <<"[]">>, <<"[1,d]">>, <<"3">>)),

        ?assertEqual({ok,[{1,<<"RSHW">>},{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, readGELT(Channel, <<"hash">>, <<"[1,a]">>, <<"[1,d]">>, <<"5">>)),
    	?assertEqual({ok,[{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, readGELT(Channel, <<"hash">>, <<"[1,ab]">>, <<"[1,d]">>, <<"2">>)),
    	?assertEqual({ok,[{1,<<"[1,b]">>}]}, readGELT(Channel, <<"key">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    	?assertEqual({ok,[{1,<<"234567">>}]}, readGELT(Channel, <<"value">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    	?assertEqual({ok,[{1,<<"[1,b]",9,"234567">>}]}, readGELT(Channel, <<"kvpair">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    	?assertEqual({ok,[{1,<<"[1,b]",9,"22AR0N">>}]}, readGELT(Channel, <<"khpair">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    	?assertEqual({ok,[{1,<<"[1,b]",9,"234567",9,"22AR0N">>}]}, readGELT(Channel, <<"kvhtriple">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
 
    	?assertEqual(ok, imem_meta:drop_table(skvhAuditTEST_86400@)),

        ?assertEqual({ok,[{1,<<"RSHW">>},{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, readGT(Channel, <<"hash">>, <<"[]">>, <<"1000">>)),
        ?assertEqual({ok,[{1,<<"22AR0N">>},{1,<<"1XSGZJ">>}]}, readGT(Channel, <<"hash">>, <<"[1,a]">>, <<"1000">>)),
    	?assertEqual({ok,[{1,<<"22AR0N">>}]}, readGT(Channel, <<"hash">>, <<"[1,a]">>, <<"1">>)),
    	?assertEqual({ok,[{1,<<"[1,b]">>},{1,<<"[1,c]">>}]}, readGT(Channel, <<"key">>, <<"[1,a]">>, <<"2">>)),
    	?assertEqual({ok,[{1,<<"234567">>},{1,<<"345678">>}]}, readGT(Channel, <<"value">>, <<"[1,ab]">>, <<"2">>)),
    	?assertEqual({ok,[{1,<<"[1,b]",9,"234567">>},{1,<<"[1,c]",9,"345678">>}]}, readGT(Channel, <<"kvpair">>, <<"[1,ab]">>, <<"2">>)),
    	?assertEqual({ok,[{1,<<"[1,b]",9,"22AR0N">>},{1,<<"[1,c]",9,"1XSGZJ">>}]}, readGT(Channel, <<"khpair">>, <<"[1,ab]">>, <<"2">>)),

		?assertEqual(ok,imem_meta:truncate_table(skvhTEST)),
    	KVtab = <<"{<<\"52015\">>,<<>>,<<\"AaaEnabled\">>}	false
{<<\"52015\">>,<<\"SMS-SUB-52015\">>,<<\"AaaEnabled\">>}	false">>,
		TabRes1 = write(Channel, KVtab),
        ?assertEqual({ok,[{1,<<"1ES4AG">>},{1,<<"DXS1E">>}]}, TabRes1),
        KVLong = 
<<"{<<\"52015\">>,<<>>,<<\"AllowedContentTypes\">>}	\"audio/amr;audio/mp3;audio/x-rmf;audio/x-beatnic-rmf;audio/sp-midi;audio/imelody;audio/smaf;audio/rmf;text/x-imelody;text/x-vcalendar;text/x-vcard;text/xml;text/html;text/plain;text/x-melody;image/png;image/vnd.wap.wbmp;image/bmp;image/gif;image/ief;image/jpeg;image/jpg;image/tiff;image/x-xwindowdump;image/vnd.nokwallpaper;application/smil;application/postscript;application/rtf;application/x-tex;application/x-texinfo;application/x-troff;audio/basic;audio/midi;audio/x-aifc;audio/x-aiff;audio/x-mpeg;audio/x-wav;video/3gpp;video/mpeg;video/quicktime;video/x-msvideo;video/x-rn-mp4;video/x-pn-realvideo;video/mpeg4;multipart/related;multipart/mixed;multipart/alternative;message/rfc822;application/vnd.oma.drm.message;application/vnd.oma.dm.message;application/vnd.sem.mms.protected;application/vnd.sonyericsson.mms-template;application/vnd.smaf;application/xml;video/mp4;\"">>,
        ?assertEqual({ok,[{1,<<"1KTZC8">>}]}, write(Channel, KVLong)),

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
