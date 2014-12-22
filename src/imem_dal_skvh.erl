-module(imem_dal_skvh).

-include("imem.hrl").
-include("imem_meta.hrl").

-define(AUDIT_SUFFIX,"Audit_86400@_").
-define(AUDIT(__Channel), binary_to_list(__Channel) ++ ?AUDIT_SUFFIX).
-define(HIST_SUFFIX,"Hist").
-define(HIST(__Channel), binary_to_list(__Channel) ++ ?HIST_SUFFIX).
-define(HIST_FROM_STR(__Channel),list_to_existing_atom(__Channel ++ ?HIST_SUFFIX)).
-define(MATCHHEAD,{skvhTable, '$1', '$2', '$3'}).
-define(AUDIT_MATCHHEAD,{skvhAudit, '$1', '$2', '$3', '$4', '$5'}).

-define(TABLE_OPTS, [{record_name,skvhTable}
                    ,{type,ordered_set}
                    ]).          

-define(AUDIT_OPTS, [{record_name,skvhAudit}
                    ,{type,ordered_set}
                    ,{purge_delay,430000}     %% 430000 = 5 Days - 2000 sec
                    ]).          

-define(HIST_OPTS,  [{record_name,skvhHist}
                    ,{type,ordered_set}
                    ]).          

-record(skvhCtx,                           	  %% table provisioning context    
                    { mainAlias               :: atom()					%% main table alias
                    , auditAlias              :: atom()					%% audit table alias
                    , histAlias               :: atom()					%% history table alias
                    }
       ).

-record(skvhCL,                           	  %% value change log    
                    { time                    :: ddTimestamp()			%% erlang:now()
                    , ovalue               	  :: binary()|undefined		%% old value
                    , nvalue               	  :: binary()|undefined		%% new value
                    , cuser=unknown 		  :: ddEntityId()|unknown
                    }
       ).

-record(skvhHist,                             %% sorted key value hash table    
                    { ckey = ?nav             :: binary()|?nav
                    , cvhist  				  :: list(#skvhCL{})      
                    }
       ).
-define(skvhHist,  [binterm,list]).

-record(skvhAudit,                            %% sorted key value hash audit table    
                    { time                    :: ddTimestamp()			%% erlang:now()
                    , ckey = ?nav             :: binary()|?nav			
                    , ovalue               	  :: binary()|undefined		%% old value
                    , nvalue               	  :: binary()|undefined		%% new value
                    , cuser=unknown 		  :: ddEntityId()|unknown
                    }
       ).
-define(skvhAudit,  [timestamp,binterm,binstr,binstr,userid]).

-record(skvhTable,                            %% sorted key value hash table    
                    { ckey = ?nav             :: binary()|?nav
                    , cvalue  				  :: binary()      
                    , chash	= <<"fun(_,__Rec) -> list_to_binary(io_lib:format(\"~.36B\",[erlang:phash2({element(2,__Rec),element(3,__Rec)})])) end.">>
                    						  :: binary()
                    }
       ).
-define(skvhTable,  [binterm,binstr,binstr]).
-define(skvhTableTrigger, <<"fun(__OR,__NR,__T,__U) -> imem_dal_skvh:write_audit(__OR,__NR,__T,__U) end.">>
	   ).

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


-export([ table_name/1              %% (Channel)                    return table name as binstr
        , atom_table_name/1         %% (Channel)                    return table name as atom
        , audit_alias/1             %% (Channel)                    return audit alias as bisntr
        , create_check_channel/1    %% (Channel)                    create empty table and audit table if necessary
        , write/3           %% (User, Channel, KVTable)             resource may not exist, will be created, return list of hashes
        , insert/3          %% (User, Channel, MapList)             resources should not exist will be created, retrun list of maps with inserted rows
        , insert/4          %% (User, Channel, Key, Value)          resource should not exist will be created, return map with inserted row
        , update/3          %% (user, Channel, ChangeList)          update a list of resources, it will fail if the old value was modified by someone else
        , update/4          %% (User, Channel, OldRow, NewRow)      update a resource, it will fail if the old value was modified, rows should be in map format
        , read/3            %% (User, Channel, KeyList)             return empty Arraylist if none of these resources exists, the list is returning as a map
        , read/4            %% (User, Channel, Item, KeyTable)      return empty Arraylist if none of these resources exists
        , readGELT/5        %% (User, Channel, CKey1, CKey2, L)     from key at or after CKey1 to last key before CKey2, result as map, fails if more than L rows
        , readGELT/6        %% (User, Channel, Item, CKey1, CKey2, L)   from key at or after CKey1 to last key before CKey2, fails if more than L rows
        , readGT/4          %% (User, Channel, CKey1, L)            start with first key after CKey1, return result as list of maps of lenght L or less
        , readGT/5          %% (User, Channel, Item, CKey1, Limit)  start with first key after CKey1, return Limit results or less
        , readGE/4          %% (User, Channel, CKey1, Limit)        start with first key at or after CKey1, return Limit results or less as maps
        , readGE/5          %% (User, Channel, Item, CKey1, Limit)  start with first key at or after CKey1, return Limit results or less
        , audit_readGT/4    %% (User, Channel, TS1, Limit)          read audit info after Timestamp1, resturn a list of maps with Limit results or less
        , hist_read/3       %% (User, Channel, KeyList)             return history list as maps for given keys
        , audit_readGT/5    %% (User, Channel, Item, TS1, Limit)    read audit info after Timestamp1, return Limit results or less
        , remove/3          %% (User, Channel, RowList)             delete a resource will fail if it was modified, rows should be in map format
        , delete/3          %% (User, Channel, KeyTable)            do not complain if keys do not exist
        , deleteGELT/5      %% (User, Channel, CKey1, CKey2, L)     delete range of keys >= CKey1 and < CKey2, fails if more than L rows
        , deleteGTLT/5      %% (User, Channel, CKey1, CKey2, L)     delete range of keys > CKey1 and < CKey2, fails if more than L rows
        , write_audit/4     %% (OldRec,NewRec,Table,User)           default trigger for writing audit trail
        ]).

-export([expand_inline_key/3, expand_inline/3]).

-spec expand_inline_key(User :: any(), Channel :: binary(), Key :: any()) ->
    Json :: binary().
expand_inline_key(User, Channel, Key) ->
    case imem_dal_skvh:read(User, Channel, [Key]) of
        [] -> [];
        [#{cvalue := Json}] -> expand_inline(User, Channel, Json)
    end.

-spec expand_inline(User :: any(), Channel :: binary(), Json :: binary()) ->
    ExpandedJson :: binary().
expand_inline(User, Channel, Json) when is_binary(Json) ->
    Binds =
    maps:fold(
      fun(K,V,Acc) ->
              case re:run(K, "^inline_*") of
                  nomatch -> Acc;
                  _ ->
                      case imem_dal_skvh:read(
                             User, Channel,
                             [imem_json_data:to_strbinterm(V)]) of
                          [] -> Acc;
                          [#{cvalue := BVal}] -> [{V, BVal} | Acc]
                      end
              end
      end,
      [], imem_json:decode(Json, [return_maps])),
    imem_json:expand_inline(Json, Binds).

%% @doc Returns table name from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably lower case or camel case)
%% returns:	main table name as binstr
-spec table_name(binary()) -> binary().
table_name(Channel) -> Channel.

%% @doc Returns table name as atom from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably lower case or camel case)
%% returns:	main table name as an atom
-spec atom_table_name(binary()) -> atom().
atom_table_name(Channel) -> ?binary_to_existing_atom(table_name(Channel)).

%% @doc Returns history alias name from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	history table alias as binstr
-spec history_alias(binary()) -> binary().
history_alias(Channel) -> list_to_binary(?HIST(Channel)).

%% @doc Returns history alias name from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	history table alias as atom
-spec atom_history_alias(binary()) -> atom().
atom_history_alias(Channel) -> ?binary_to_existing_atom(history_alias(Channel)).

%% @doc Returns audit alias name from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	audit table alias as binstr
-spec audit_alias(binary()) -> binary().
audit_alias(Channel) -> list_to_binary(?AUDIT(Channel)).

%% @doc Returns audit alias name from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	audit table alias as atom
-spec atom_audit_alias(binary()) -> atom().
atom_audit_alias(Channel) -> ?binary_to_existing_atom(audit_alias(Channel)).

%% @doc Returns audit table name from channel name 
%% Channel: String (not binstr) of channel name (preferrably upper case or camel case)
%% Key: 	Timestamp of change
%% returns:	audit table name as atom 
-spec audit_table_name(list(),term()) -> atom().
audit_table_name(Channel,Key) when is_list(Channel) -> 
	imem_meta:partitioned_table_name(Channel ++ ?AUDIT_SUFFIX,Key).

audit_table_time(TransTime,CH) ->
	ATName = audit_table_name(CH,TransTime),
	case imem_meta:last(ATName) of 
		'$end_of_table' ->	 
			{ATName,TransTime};
		LastLog when (LastLog >= TransTime) andalso (element(3,LastLog) < 999999) ->
			{ATName,{element(1,LastLog),element(2,LastLog),element(3,LastLog)+1}};
		LastLog when (LastLog >= TransTime) andalso (element(2,LastLog) < 999999) ->
			audit_table_time({element(1,LastLog),element(2,LastLog)+1,0},CH);
		LastLog when (LastLog >= TransTime) ->
			audit_table_time({element(1,LastLog)+1,0,0},CH);
		_ -> 
		 	{ATName,TransTime}
	end.

audit_table_next(ATName,TransTime,CH) -> 
	case TransTime of 
		{M,S,Mic} when Mic < 999999 ->
			{ATName,{M,S,Mic+1}};
		{M,S,999999} when S < 999999 ->
			audit_table_time({M,S+1,0},CH);
		{M,999999,999999} ->
			audit_table_time({M+1,0,0},CH)
	end.


%% @doc Checks existence of interface tables by checking existence of table name atoms in atom cache
%% creates these tables if not already existing 
%% return atom names of the tables (StateTable and AuditTable)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	provisioning record with table aliases to be used for data queries
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec create_check_channel(binary()) -> #skvhCtx{}.
create_check_channel(Channel) ->
	Main = table_name(Channel),
	try 
		M = ?binary_to_existing_atom(Main),
		H = list_to_existing_atom(?HIST(Channel)),
		A = list_to_existing_atom(?AUDIT(Channel)),
		imem_meta:check_local_table_copy(M),							%% throws if master table is not locally resident
		imem_meta:check_local_table_copy(H),							%% throws if history table is not locally resident
		AP = imem_meta:partitioned_table_name_str(A,?TRANS_TIME), 	
		imem_meta:check_local_table_copy(list_to_existing_atom(AP)),	%% throws if table is not locally resident
		#skvhCtx{mainAlias=M, auditAlias=A, histAlias=H}
	catch 
		_:_ -> 
			MC = ?binary_to_atom(Main),
			case (catch imem_meta:create_check_table(MC, {record_info(fields, skvhTable),?skvhTable, #skvhTable{}}, ?TABLE_OPTS, system)) of
				ok ->				ok;
    			{error, Reason} -> 	imem_meta:log_to_db(warning,?MODULE,create_check_table,[{table,MC}],io_lib:format("~p",[Reason]));
    			Reason -> 			imem_meta:log_to_db(warning,?MODULE,create_check_table,[{table,MC}],io_lib:format("~p",[Reason]))
    		end,
			AC = list_to_atom(?AUDIT(Channel)),
			catch imem_meta:create_check_table(AC, {record_info(fields, skvhAudit),?skvhAudit, #skvhAudit{}}, ?AUDIT_OPTS, system),
        	catch imem_meta:create_trigger(MC, ?skvhTableTrigger),
        	HC = list_to_atom(?HIST(Channel)),
			catch imem_meta:create_check_table(HC, {record_info(fields, skvhHist),?skvhHist, #skvhHist{}}, ?HIST_OPTS, system),
			#skvhCtx{mainAlias=MC, auditAlias=AC, histAlias=HC}
	end.

write_audit(OldRec,NewRec,Table,User) ->
	["","",CH,"","",""] = imem_meta:parse_table_name(Table),
	{AT1,TT1} = audit_table_time(?TRANS_TIME_GET,CH),
	?TRANS_TIME_PUT(TT1),
	HT = ?HIST_FROM_STR(CH),
	CL = case {OldRec,NewRec} of
		{{},{}} ->	% truncate table
			ok = imem_meta:truncate_table(HT),
			[{AT1,#skvhAudit{time=TT1,ckey=sext:encode(undefined),ovalue=undefined,nvalue=undefined,cuser=User}}];
		{_,{}} ->	% delete old rec
			[{AT1,#skvhAudit{time=TT1,ckey=element(2,OldRec),ovalue=element(3,OldRec),nvalue=undefined,cuser=User}}];
		{{},_} ->	% insert new rec
			[{AT1,#skvhAudit{time=TT1,ckey=element(2,NewRec),ovalue=undefined,nvalue=element(3,NewRec),cuser=User}}];		
		{_, _} ->	
			OldKey = element(2,OldRec),
			case element(2,NewRec) of
				OldKey ->	% update value
					[{AT1,#skvhAudit{time=TT1,ckey=OldKey,ovalue=element(3,OldRec),nvalue=element(3,NewRec),cuser=User}}];	
				NewKey ->	% delete and insert
					{AT2,TT2} = audit_table_next(AT1,TT1,CH),
					?TRANS_TIME_PUT(TT1),				
					[{AT1,#skvhAudit{time=TT1,ckey=OldKey,ovalue=element(3,OldRec),nvalue=undefined,cuser=User}}
					,{AT2,#skvhAudit{time=TT2,ckey=NewKey,ovalue=undefined,nvalue=element(3,NewRec),cuser=User}}
					]
			end
	end,
	[ write_audit_and_hist(AT,HT,C) || {AT,C} <- CL].

write_audit_and_hist(AT, HT, #skvhAudit{time=T,ckey=K,ovalue=O,nvalue=N,cuser=U}=C) ->
	ok = imem_meta:write(AT,C),
	case imem_meta:read(HT,K) of
		[#skvhHist{ckey=K,cvhist=CH}=H] ->	
			ok = imem_meta:write(HT, H#skvhHist{ckey=K,cvhist=[#skvhCL{time=T,ovalue=O,nvalue=N,cuser=U}|CH]});
		[] ->	
			ok = imem_meta:write(HT,#skvhHist{ckey=K,cvhist=[#skvhCL{time=T,ovalue=O,nvalue=N,cuser=U}]})
	end.


% return(Cmd, Result) ->
% 	debug(Cmd, Result), 
% 	Result.

% return_error(Cmd, Tuple) -> 
% 	debug(Cmd, Tuple), 
% 	{error,Tuple}.

return_stringlist(Cmd, List) -> 
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
	imem_datatype:io_to_binterm(Key).

io_to_integer(I) when is_integer(I) -> 		I;
io_to_integer(Key) when is_binary(Key) ->	imem_datatype:io_to_integer(Key,0,0).

io_value_to_term(V) when is_binary(V) -> 		V.	%% ToDo: Maybe convert to map datatype when available

term_key_to_binterm(MaybeEncodedKey) when is_binary(MaybeEncodedKey) ->
    try
        imem_datatype:binterm_to_term(MaybeEncodedKey),
        MaybeEncodedKey
    catch
        _:_ ->
            imem_datatype:term_to_binterm(MaybeEncodedKey)
    end;
term_key_to_binterm(TermKey) ->
    imem_datatype:term_to_binterm(TermKey).

map_to_skvh_rec(#{ckey := DecodedKey, cvalue := CValue, chash := CHash}) ->
    CKey = imem_datatype:term_to_binterm(DecodedKey),
    #skvhTable{ckey = CKey, cvalue = CValue, chash = CHash};
map_to_skvh_rec(#{ckey := DecodedKey, cvalue := CValue}) ->
    CKey = imem_datatype:term_to_binterm(DecodedKey),
    #skvhTable{ckey = CKey, cvalue = CValue}.

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

term_key_to_io(Key) ->	imem_datatype:binterm_to_io(Key).

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

term_kv_triple_to_io({K,O,N}) ->
	KeyBin = term_key_to_io(K),
	OldValueBin = term_value_to_io(O), 
	NewValueBin = term_value_to_io(N), 
	<<KeyBin/binary,9,OldValueBin/binary,9,NewValueBin/binary>>.

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

term_tkvu_quintuple_to_io({T,K,O,N,U}) ->
	TimeBin = term_time_to_io(T),
	KeyBin = term_key_to_io(K),
	OldValueBin = term_value_to_io(O), 
	NewValueBin = term_value_to_io(N), 
	UserBin = term_user_to_io(U),
	<<TimeBin/binary,9,KeyBin/binary,9,OldValueBin/binary,9,NewValueBin/binary,9,UserBin/binary>>.

skvh_rec_to_map(#skvhTable{ckey=EncodedKey, cvalue=CValue, chash=CHash}) ->
    CKey = imem_datatype:binterm_to_term(EncodedKey),
    #{ckey => CKey, cvalue => CValue, chash => CHash}.

skvh_audit_to_map(#skvhAudit{time = Time, ckey = EncodedKey, ovalue = OldValue, nvalue = NewValue, cuser = User}) ->
    CKey = imem_datatype:binterm_to_term(EncodedKey),
    #{time => Time, ckey => CKey, ovalue => OldValue, nvalue => NewValue, cuser => User}.

skvh_hist_to_map(#skvhHist{ckey = EncodedKey, cvhist = ChangeList}) ->
    CKey = imem_datatype:binterm_to_term(EncodedKey),
    MapChangeList = [skvh_cl_to_map(C) || C <- ChangeList],
    #{ckey => CKey, cvhist => MapChangeList}.

skvh_cl_to_map(#skvhCL{time = Time, ovalue = OldValue, nvalue = NewValue, cuser = User}) ->
    #{time => Time, ovalue => OldValue, nvalue => NewValue, cuser => User}.

%% Data access per key (read,write,delete)

read(User, Channel, Item, KeyTable) when is_binary(Channel), is_binary(Item), is_binary(KeyTable) ->
	Cmd = [read,User,Channel,Item,KeyTable],
	read(Cmd, create_check_channel(Channel), Item, io_key_table_to_term_list(KeyTable), []).

read(Cmd, _, Item, [], Acc)  -> project_result(Cmd, lists:reverse(Acc), Item);
read(Cmd, SkvhCtx, Item, [Key|Keys], Acc)  ->
	KVP = case imem_meta:dirty_read(SkvhCtx#skvhCtx.mainAlias,Key) of
		[] ->		#skvhTable{ckey=Key,cvalue=undefined,chash=undefined} ;
		[Rec] ->	Rec
	end,
	read(Cmd, SkvhCtx, Item, Keys, [KVP|Acc]).
	

write(User,Channel, KVTable) when is_binary(Channel), is_binary(KVTable) ->
	Cmd = [write,User,Channel,KVTable],
	write(User, Cmd, create_check_channel(Channel), io_kv_table_to_tuple_list(KVTable), []).

write(_User, Cmd, _, [], Acc)  -> return_stringlist(Cmd, lists:reverse(Acc));
write(User, Cmd, SkvhCtx, [{K,V}|KVPairs], Acc)  ->
	#skvhTable{chash=Hash} = imem_meta:merge(SkvhCtx#skvhCtx.mainAlias,#skvhTable{ckey=K,cvalue=V},User),
	write(User,Cmd, SkvhCtx, KVPairs, [Hash|Acc]).

delete(User, Channel, KeyTable) when is_binary(Channel), is_binary(KeyTable) -> 
	Cmd = [delete,User,Channel,KeyTable],
	delete(User, Cmd, create_check_channel(Channel), io_key_table_to_term_list(KeyTable), []).

delete(_User, Cmd, _, [], Acc)  -> return_stringlist(Cmd, lists:reverse(Acc));
delete(User, Cmd, SkvhCtx, [Key|Keys], Acc)  ->
	Hash = case imem_meta:read(SkvhCtx#skvhCtx.mainAlias,Key) of
		[] ->
			undefined;
		[Rec] ->	
			try
				#skvhTable{chash=H} = imem_meta:remove(SkvhCtx#skvhCtx.mainAlias,Rec,User),
				H
			catch
				throw:{'ConcurrencyException',{"Remove failed, key does not exist", _}} -> undefined
			end
	end,
	delete(User, Cmd, SkvhCtx, Keys, [term_hash_to_io(Hash)|Acc]).

%% Raw data access per key (read, insert, remove)
read(_User, _Channel, []) -> [];
read(User, Channel, [DecodedKey | Keys]) ->
    Key = term_key_to_binterm(DecodedKey),
    TableName = atom_table_name(Channel),
    case imem_meta:read(TableName, Key) of
        [ReadResult] ->
            [skvh_rec_to_map(ReadResult) |
             read(User, Channel, Keys)];
        [] ->
            read(User, Channel, Keys)
    end.

insert(User, Channel, DecodedKey, Value) ->
    Key = term_key_to_binterm(DecodedKey),
    TableName = atom_table_name(Channel),
    InsertResult = imem_meta:insert(TableName, #skvhTable{ckey=Key, cvalue=Value}, User),
    skvh_rec_to_map(InsertResult).

insert(User, Channel, MapList) ->
    InsertFun = fun() -> insert_priv(User, Channel, MapList) end,
    imem_if:return_atomic_list(imem_meta:transaction(InsertFun)).

insert_priv(_User, _Channel, []) -> [];
insert_priv(User, Channel, [#{ckey := DecodedKey, cvalue := Value} | MapList]) ->
    [insert(User, Channel, DecodedKey, Value) | insert_priv(User, Channel, MapList)].

update(User, Channel, #{} = OldMapRow, #{} = NewMapRow) ->
    OldRow = map_to_skvh_rec(OldMapRow),
    NewRow = map_to_skvh_rec(NewMapRow),
    TableName = atom_table_name(Channel),
    UpdateResult = imem_meta:update(TableName, {OldRow, NewRow}, User),
    skvh_rec_to_map(UpdateResult).

update(User, Channel, ChangeList) ->
    UpdateFun = fun() -> update_priv(User, Channel, ChangeList) end,
    imem_if:return_atomic_list(imem_meta:transaction(UpdateFun)).

update_priv(_User, _Channel, []) -> [];
update_priv(User, Channel, [{#{} = OldMapRow, #{} = NewMapRow} | ChangeList]) ->
    [update(User, Channel, OldMapRow, NewMapRow) | update_priv(User, Channel, ChangeList)].

remove(User, Channel, Rows) when is_list(Rows) ->
    RemoveFun = fun() -> remove_list(User, Channel, Rows) end,
    imem_if:return_atomic_list(imem_meta:transaction(RemoveFun));
remove(User, Channel, Row) ->
    remove_single(User, Channel, Row).

remove_single(User, Channel, #{ckey := DecodedKey, cvalue := Value, chash := Hash}) ->
    Key = imem_datatype:term_to_binterm(DecodedKey),
    TableName = atom_table_name(Channel),
    RemoveResult = imem_meta:remove(TableName, #skvhTable{ckey=Key, cvalue=Value, chash=Hash}, User),
    #skvhTable{ckey=DeletedKey, cvalue=CValue, chash=CHash} = RemoveResult,
    CKey = imem_datatype:binterm_to_term(DeletedKey),
    #{ckey => CKey, cvalue => CValue, chash => CHash}.

remove_list(_User, _Channel, []) -> [];
remove_list(User, Channel, [#{} = Row | Rows]) ->
    [remove_single(User, Channel, Row) | remove_list(User, Channel, Rows)].

hist_read(_User, _Channel, []) -> [];
hist_read(User, Channel, [DecodedKey | DecodedKeys]) ->
    HistTableName = atom_history_alias(Channel),
    Key = term_key_to_binterm(DecodedKey),
    case imem_meta:read(HistTableName, Key) of
        [HistResult] ->
            [skvh_hist_to_map(HistResult) | hist_read(User, Channel, DecodedKeys)];
        [] ->
            hist_read(User, Channel, DecodedKeys)
    end.

%% Data Access per key range

match_val(T) when is_tuple(T) -> {const,T};
match_val(V) -> V.

readGT(User, Channel, Item, CKey1, Limit)  when is_binary(Item), is_binary(CKey1) ->
	Cmd = [readGT, User, Channel, Item, CKey1, Limit],
	readGT(User, Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_to_integer(Limit)).

readGT(_User, Cmd, SkvhCtx, Item, Key1, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>', '$1', match_val(Key1)}], ['$_']},
	read_limited(Cmd, SkvhCtx, Item, MatchFunction, Limit).

audit_readGT(User, Channel, Item, TS1, Limit)  when is_binary(Item), is_binary(TS1) ->
	Cmd = [audit_readGT, User, Channel, Item, TS1, Limit],
	audit_readGT(User, Cmd, create_check_channel(Channel), Item, imem_datatype:io_to_timestamp(TS1), io_to_integer(Limit)).

audit_readGT(_User, Cmd, SkvhCtx, Item, TS1, Limit) ->
	MatchFunction = {?AUDIT_MATCHHEAD, [{'>', '$1', match_val(TS1)}], ['$_']},
	audit_read_limited(Cmd, audit_partitions(SkvhCtx#skvhCtx.auditAlias,TS1), Item, MatchFunction, Limit, []).

audit_partitions(Alias, TS1) ->
	FirstPartitionName = imem_meta:partitioned_table_name(Alias,TS1),
	Pred = fun(N) -> (N >= FirstPartitionName) end,
	lists:filter(Pred, imem_meta:physical_table_names(Alias)).

audit_read_limited(Cmd, _, Item, _MatchFunction, 0, Acc) ->
	audit_project_result(Cmd, Acc, Item);							%% enough audits collected
audit_read_limited(Cmd, [], Item, _MatchFunction, _Limit, Acc) ->
	audit_project_result(Cmd, Acc, Item);							%% no more partitions
audit_read_limited(Cmd, [TP|Partitions], Item, MatchFunction, Limit, Acc) ->
	{L,_} = imem_meta:select(TP, [MatchFunction], Limit),
	audit_read_limited(Cmd, Partitions, Item, MatchFunction, Limit-length(L), L++ Acc).

readGE(User, Channel, Item, CKey1, Limit)  when is_binary(Item), is_binary(CKey1) ->
	Cmd = [readGT, User, Channel, Item, CKey1, Limit],
	readGE(User, Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_to_integer(Limit)).

readGE(_User, Cmd, SkvhCtx, Item, Key1, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}], ['$_']},
	read_limited(Cmd, SkvhCtx, Item, MatchFunction, Limit).

read_limited(Cmd, SkvhCtx, Item, MatchFunction, Limit) ->
	{L,_} = imem_meta:select(SkvhCtx#skvhCtx.mainAlias, [MatchFunction], Limit),
	project_result(Cmd, L, Item).

readGELT(User, Channel, Item, CKey1, CKey2, Limit) when is_binary(Item), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [readGELT, User, Channel, Item, CKey1, CKey2, Limit],
	readGELT(User, Cmd, create_check_channel(Channel), Item, io_key_to_term(CKey1), io_key_to_term(CKey2), io_to_integer(Limit)).

readGELT(_User, Cmd, SkvhCtx, Item, Key1, Key2, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$_']},
	read_with_limit(Cmd, SkvhCtx, Item, MatchFunction, Limit).

read_with_limit(Cmd, SkvhCtx, Item, MatchFunction, Limit) ->
	{L,_} = imem_meta:select(SkvhCtx#skvhCtx.mainAlias, [MatchFunction], Limit+1),
	if  
		length(L) > Limit ->	?ClientError(?E117(Limit));
		true ->					project_result(Cmd, L, Item)
	end.


deleteGELT(User, Channel, CKey1, CKey2, Limit) when is_binary(Channel), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [deleteGELT, User, Channel, CKey1, CKey2, Limit],
	deleteGELT(User, Cmd, create_check_channel(Channel), io_key_to_term(CKey1), io_key_to_term(CKey2), io_to_integer(Limit)).

deleteGELT(User, Cmd, SkvhCtx, Key1, Key2, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$1']},
	delete_with_limit(User, Cmd, SkvhCtx, MatchFunction, Limit).

deleteGTLT(User, Channel, CKey1, CKey2, Limit) when is_binary(Channel), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [deleteGTLT, User, Channel, CKey1, CKey2, Limit],
	deleteGTLT(User, Cmd, create_check_channel(Channel), io_key_to_term(CKey1), io_key_to_term(CKey2), io_to_integer(Limit)).

deleteGTLT(User, Cmd, SkvhCtx, Key1, Key2, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$1']},
	delete_with_limit(User, Cmd, SkvhCtx, MatchFunction, Limit).

delete_with_limit(User, Cmd, SkvhCtx, MatchFunction, Limit) ->
	{L,_} = imem_meta:select(SkvhCtx#skvhCtx.mainAlias, [MatchFunction], Limit+1),	
	if
		length(L) > Limit ->	?ClientError(?E117(Limit));
		true ->					delete(User, Cmd, SkvhCtx, L, [])
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
	return_stringlist(Cmd, [term_value_to_io(V) ||  #skvhAudit{nvalue=V} <- L]);
audit_project_result(Cmd, L, <<"new">>) ->
	return_stringlist(Cmd, [term_value_to_io(V) ||  #skvhAudit{nvalue=V} <- L]);
audit_project_result(Cmd, L, <<"old">>) ->
	return_stringlist(Cmd, [term_value_to_io(V) ||  #skvhAudit{ovalue=V} <- L]);
audit_project_result(Cmd, L, <<"user">>) ->
	return_stringlist(Cmd, [term_user_to_io(U) ||  #skvhAudit{cuser=U} <- L]);
audit_project_result(Cmd, L, <<"kvpair">>) ->
	return_stringlist(Cmd, [term_kv_tuple_to_io({K,V}) ||  #skvhAudit{ckey=K,nvalue=V} <- L]);
audit_project_result(Cmd, L, <<"kvtriple">>) ->
	return_stringlist(Cmd, [term_kv_triple_to_io({K,O,N}) ||  #skvhAudit{ckey=K,ovalue=O,nvalue=N} <- L]);
audit_project_result(Cmd, L, <<"tkvtriple">>) ->
	return_stringlist(Cmd, [term_tkv_triple_to_io({T,K,V}) ||  #skvhAudit{time=T,ckey=K,nvalue=V} <- L]);
audit_project_result(Cmd, L, <<"tkvquadruple">>) ->
	return_stringlist(Cmd, [term_tkv_triple_to_io({T,K,O,N}) ||  #skvhAudit{time=T,ckey=K,ovalue=O,nvalue=N} <- L]);
audit_project_result(Cmd, L, <<"tkvuquadruple">>) ->
	return_stringlist(Cmd, [term_tkvu_quadruple_to_io({T,K,V,U}) ||  #skvhAudit{time=T,ckey=K,nvalue=V,cuser=U} <- L]);
audit_project_result(Cmd, L, <<"tkvuquintuple">>) ->
	return_stringlist(Cmd, [term_tkvu_quintuple_to_io({T,K,O,N,U}) ||  #skvhAudit{time=T,ckey=K,ovalue=O,nvalue=N,cuser=U} <- L]).

%% raw data access for key range

readGT(_User, Channel, DecodedKey, Limit) ->
    TableName = atom_table_name(Channel),
    Key = term_key_to_binterm(DecodedKey),
    MatchFunction = {?MATCHHEAD, [{'>', '$1', match_val(Key)}], ['$_']},
	{L,_} = imem_meta:select(TableName, [MatchFunction], Limit),
    [skvh_rec_to_map(R) || R <- L ].

readGE(_User, Channel, DecodedKey, Limit) ->
    TableName = atom_table_name(Channel),
    Key = term_key_to_binterm(DecodedKey),
    MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key)}], ['$_']},
	{L,_} = imem_meta:select(TableName, [MatchFunction], Limit),
    [skvh_rec_to_map(R) || R <- L ].

readGELT(_User, Channel, DecodedKey1, DecodedKey2, Limit) ->
    TableName = atom_table_name(Channel),
    Key1 = term_key_to_binterm(DecodedKey1),
    Key2 = term_key_to_binterm(DecodedKey2),
	MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$_']},
    {L,_} = imem_meta:select(TableName, [MatchFunction], Limit+1),
    if
        length(L) > Limit -> ?ClientError(?E117(Limit));
        true -> [skvh_rec_to_map(R) || R <- L ]
    end.
audit_readGT(User, Channel, TS1, Limit) when is_binary(TS1) ->
    audit_readGT(User, Channel, imem_datatype:io_to_timestamp(TS1), Limit);
audit_readGT(_User, Channel, TS1, Limit) ->
    TableName = atom_audit_alias(Channel),
    Partitions = audit_partitions(TableName, TS1),
    MatchFunction = {?AUDIT_MATCHHEAD, [{'>', '$1', match_val(TS1)}], ['$_']},
    audit_part_readGT(Partitions, MatchFunction, Limit).

audit_part_readGT([], _MatchFunction, _Limit) -> [];
audit_part_readGT(_Partitions, _MatchFunction, 0) -> [];
audit_part_readGT([Partition | Rest], MatchFunction, Limit) ->
    {L,_} = imem_meta:select(Partition, [MatchFunction], Limit),
    Res = [skvh_audit_to_map(R) || R <- L],
    Res ++ audit_part_readGT(Rest, MatchFunction, Limit-length(Res)).

debug(Cmd, Resp) ->
    lager:debug([ {cmd, hd(Cmd)}]
    			  , "PROVISIONING EVENT for ~p ~p returns ~p"
    			  , [hd(Cmd), tl(Cmd), Resp]
    			 ).


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ?imem_test_setup,
    catch imem_meta:drop_table(skvhTest),
    catch imem_meta:drop_table(skvhTestAudit_86400@_),
    catch imem_meta:drop_table(skvhTestHist).

teardown(_) ->
    catch imem_meta:drop_table(skvhTest),
    catch imem_meta:drop_table(skvhTestAudit_86400@_),
    catch imem_meta:drop_table(skvhTestHist),
    ?imem_test_teardown.

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun skvh_operations/1
        ]}}.    

hist_reset_time([]) -> [];
hist_reset_time([#{cvhist := CList} = Hist | Rest]) ->
    [Hist#{cvhist := [C#{time := {0,0,0}} || C <- CList]} | hist_reset_time(Rest)].

skvh_operations(_) ->
    try 
        ClEr = 'ClientError',
        Channel = <<"skvhTest">>,

        ?LogDebug("---TEST---~p:test_skvh~n", [?MODULE]),

        ?assertEqual(<<"skvhTest">>, table_name(Channel)),

        ?assertException(throw, {ClEr,{"Table does not exist",_}}, imem_meta:check_table(skvhTest)), 
        ?assertException(throw, {ClEr,{"Table does not exist",_}}, imem_meta:check_table(skvhTestAudit_86400@_)), 
        ?assertException(throw, {ClEr,{"Table does not exist",_}}, imem_meta:check_table(skvhTestHist)), 

        KVa = <<"[1,a]",9,"123456">>,
        KVb = <<"[1,b]",9,"234567">>,
        KVc = <<"[1,c]",9,"345678">>,

		K0 = <<"{<<\"0\">>,<<>>,<<>>}">>,

        ?assertEqual({ok,[<<"{<<\"0\">>,<<>>,<<>>}\tundefined">>]}, read(system, Channel, <<"kvpair">>, K0)),
		?assertEqual({ok,[]}, readGT(system, Channel, <<"khpair">>, <<"{<<\"0\">>,<<>>,<<>>}">>, <<"1000">>)),

        ?assertEqual({ok,[<<"[1,a]\tundefined">>]}, read(system, Channel, <<"kvpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"[1,a]\tundefined">>]}, read(system, Channel, <<"khpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"[1,a]">>]}, read(system, Channel, <<"key">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"undefined">>]}, read(system, Channel, <<"value">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"undefined">>]}, read(system, Channel, <<"hash">>, <<"[1,a]">>)),

        ?assertEqual(ok, imem_meta:check_table(skvhTest)),        
        ?assertEqual(ok, imem_meta:check_table(skvhTestAudit_86400@_)),
        ?assertEqual(ok, imem_meta:check_table(skvhTestHist)),

        ?assertEqual({ok,[<<"1EXV0I">>,<<"BFFHP">>,<<"ZCZ28">>]}, write(system, Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

        ?assertEqual({ok,[KVa]}, read(system, Channel, <<"kvpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[KVc]}, read(system, Channel, <<"kvpair">>, <<"[1,c]">>)),
        ?assertEqual({ok,[KVb]}, read(system, Channel, <<"kvpair">>, <<"[1,b]">>)),
        ?assertEqual({ok,[<<"[1,c]",9,"ZCZ28">>]}, read(system, Channel, <<"khpair">>, <<"[1,c]">>)),
        ?assertEqual({ok,[<<"BFFHP">>]}, read(system, Channel, <<"hash">>, <<"[1,b]">>)),

        ?assertEqual({ok,[KVc,KVb,KVa]}, read(system, Channel, <<"kvpair">>, <<"[1,c]",13,10,"[1,b]",10,"[1,a]">>)),
        ?assertEqual({ok,[KVa,<<"[1,ab]",9,"undefined">>,KVb,KVc]}, read(system, Channel, <<"kvpair">>, <<"[1,a]",13,10,"[1,ab]",13,10,"[1,b]",10,"[1,c]">>)),

        Dat = imem_meta:read(skvhTest),
        ?LogDebug("TEST data ~n~p~n", [Dat]),
        ?assertEqual(3, length(Dat)),

        ?assertEqual({ok,[<<"1EXV0I">>,<<"BFFHP">>,<<"ZCZ28">>]}, delete(system, Channel, <<"[1,a]",10,"[1,b]",13,10,"[1,c]",10>>)),

        Aud = imem_meta:read(skvhTestAudit_86400@_),
        ?LogDebug("audit trail~n~p~n", [Aud]),
        ?assertEqual(6, length(Aud)),
        {ok,Aud1} = audit_readGT(system, Channel,<<"tkvuquadruple">>, <<"{0,0,0}">>, <<"100">>),
        ?LogDebug("audit trail~n~p~n", [Aud1]),
        ?assertEqual(6, length(Aud1)),
        {ok,Aud2} = audit_readGT(system, Channel,<<"tkvtriple">>, <<"{0,0,0}">>, 4),
        ?assertEqual(4, length(Aud2)),
        {ok,Aud3} = audit_readGT(system, Channel,<<"kvpair">>, <<"now">>, 100),
        ?assertEqual(0, length(Aud3)),
        {ok,Aud4} = audit_readGT(system, Channel,<<"key">>, <<"2100-01-01">>, 100),
        ?assertEqual(0, length(Aud4)),
        Ex4 = {'ClientError',{"Data conversion format error",{timestamp,"1900-01-01",{"Cannot handle dates before 1970"}}}},
        ?assertException(throw,Ex4,audit_readGT(system, Channel,<<"tkvuquadruple">>, <<"1900-01-01">>, 100)),
        {ok,Aud5} = audit_readGT(system, Channel,<<"tkvuquadruple">>, <<"1970-01-01">>, 100),
        ?assertEqual(Aud1, Aud5),

        Hist = imem_meta:read(skvhTestHist),
        ?LogDebug("audit trail~n~p~n", [Hist]),
        ?assertEqual(3, length(Hist)),

        ?assertEqual({ok,[<<"[1,a]",9,"undefined">>]}, read(system, Channel, <<"kvpair">>, <<"[1,a]">>)),
        ?assertEqual({ok,[<<"[1,b]",9,"undefined">>]}, read(system, Channel, <<"kvpair">>, <<"[1,b]">>)),
        ?assertEqual({ok,[<<"[1,c]",9,"undefined">>]}, read(system, Channel, <<"kvpair">>, <<"[1,c]">>)),

        ?assertEqual({ok,[<<"1EXV0I">>,<<"BFFHP">>,<<"ZCZ28">>]}, write(system, Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

		?assertEqual({ok,[]},deleteGELT(system, Channel, <<"[]">>, <<"[]">>, <<"2">>)),
		?assertEqual({ok,[]},deleteGELT(system, Channel, <<"[]">>, <<"[1,a]">>, <<"2">>)),

		?assertEqual({ok,[<<"1EXV0I">>]},deleteGELT(system, Channel, <<"[]">>, <<"[1,ab]">>, <<"2">>)),
		?assertEqual({ok,[]},deleteGELT(system, Channel, <<"[]">>, <<"[1,ab]">>, <<"2">>)),

		?assertException(throw,{ClEr,{117,"Too many values, Limit exceeded",1}},deleteGELT(system, Channel, <<"[1,ab]">>, <<"[1,d]">>, <<"1">>)),		
		?assertEqual({ok,[<<"BFFHP">>,<<"ZCZ28">>]},deleteGELT(system, Channel, <<"[1,ab]">>, <<"[1,d]">>, <<"2">>)),		
		?assertEqual({ok,[]},deleteGELT(system, Channel, <<"[1,ab]">>, <<"[1,d]">>, <<"2">>)),

        ?assertEqual({ok,[<<"undefined">>,<<"undefined">>,<<"undefined">>]}, delete(system, Channel, <<"[1,a]",10,"[1,b]",13,10,"[1,c]",10>>)),

        ?assertEqual({ok,[<<"IEXQW">>]}, write(system, Channel, <<"[90074,[],\"AaaEnabled\"]",9,"true">>)),
		?assertEqual({ok,[<<"24OMVH">>]}, write(system, Channel, <<"[90074,[],\"ContentSizeMax\"]",9,"297000">>)),
		?assertEqual({ok,[<<"1W8TVA">>]}, write(system, Channel, <<"[90074,[],<<\"MmscId\">>]",9,"\"testMMSC\"">>)),
		?assertEqual({ok,[<<"22D5ZL">>]}, write(system, Channel, <<"[90074,\"MMS-DEL-90074\",\"TpDeliverUrl\"]",9,"\"http:\/\/10.132.30.84:18888\/deliver\"">>)),

		?assertEqual(ok,imem_meta:truncate_table(skvhTest)),
		?assertEqual(1,length(imem_meta:read(skvhTestHist))),
				
        ?LogDebug("audit trail~n~p~n", [imem_meta:read(skvhTestAudit_86400@_)]),

		?assertEqual(ok,imem_meta:drop_table(skvhTest)),

        ?assertEqual({ok,[<<"1EXV0I">>,<<"BFFHP">>,<<"ZCZ28">>]}, write(system, Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

		?assertEqual({ok,[]},deleteGTLT(system, Channel, <<"[]">>, <<"[]">>, <<"1">>)),
		?assertEqual({ok,[]},deleteGTLT(system, Channel, <<"[]">>, <<"[1,a]">>, <<"1">>)),

		?assertEqual({ok,[<<"1EXV0I">>]},deleteGTLT(system, Channel, <<"[]">>, <<"[1,ab]">>, <<"1">>)),
		?assertEqual({ok,[]},deleteGTLT(system, Channel, <<"[]">>, <<"[1,ab]">>, <<"1">>)),

		?assertEqual({ok,[<<"ZCZ28">>]},deleteGTLT(system, Channel, <<"[1,b]">>, <<"[1,d]">>, <<"1">>)),		
		?assertEqual({ok,[]},deleteGTLT(system, Channel, <<"[1,b]">>, <<"[1,d]">>, <<"1">>)),

		?assertEqual({ok,[<<"BFFHP">>]},deleteGTLT(system, Channel, <<"[1,a]">>, <<"[1,c]">>, <<"1">>)),		
		?assertEqual({ok,[]},deleteGTLT(system, Channel, <<"[1,a]">>, <<"[1,c]">>, <<"1">>)),		

        ?assertEqual({ok,[<<"1EXV0I">>,<<"BFFHP">>,<<"ZCZ28">>]}, write(system, Channel, <<"[1,a]",9,"123456",10,"[1,b]",9,"234567",13,10,"[1,c]",9,"345678">>)),

        ?assertException(throw,{ClEr,{117,"Too many values, Limit exceeded",1}}, readGELT(system, Channel, <<"hash">>, <<"[]">>, <<"[1,d]">>, <<"1">>)),
        ?assertEqual({ok,[<<"1EXV0I">>,<<"BFFHP">>,<<"ZCZ28">>]}, readGELT(system, Channel, <<"hash">>, <<"[]">>, <<"[1,d]">>, <<"3">>)),

        ?assertEqual({ok,[<<"1EXV0I">>,<<"BFFHP">>,<<"ZCZ28">>]}, readGELT(system, Channel, <<"hash">>, <<"[1,a]">>, <<"[1,d]">>, <<"5">>)),
    	?assertEqual({ok,[<<"BFFHP">>,<<"ZCZ28">>]}, readGELT(system, Channel, <<"hash">>, <<"[1,ab]">>, <<"[1,d]">>, <<"2">>)),
    	?assertEqual({ok,[<<"[1,b]">>]}, readGELT(system, Channel, <<"key">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    	?assertEqual({ok,[<<"234567">>]}, readGELT(system, Channel, <<"value">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    	?assertEqual({ok,[<<"[1,b]",9,"234567">>]}, readGELT(system, Channel, <<"kvpair">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    	?assertEqual({ok,[<<"[1,b]",9,"BFFHP">>]}, readGELT(system, Channel, <<"khpair">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    	?assertEqual({ok,[<<"[1,b]",9,"234567",9,"BFFHP">>]}, readGELT(system, Channel, <<"kvhtriple">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
 
    	?assertEqual(ok, imem_meta:drop_table(skvhTestAudit_86400@_)),

        ?assertEqual({ok,[<<"1EXV0I">>,<<"BFFHP">>,<<"ZCZ28">>]}, readGT(system, Channel, <<"hash">>, <<"[]">>, <<"1000">>)),
        ?assertEqual({ok,[<<"BFFHP">>,<<"ZCZ28">>]}, readGT(system, Channel, <<"hash">>, <<"[1,a]">>, <<"1000">>)),
    	?assertEqual({ok,[<<"BFFHP">>]}, readGT(system, Channel, <<"hash">>, <<"[1,a]">>, <<"1">>)),
    	?assertEqual({ok,[<<"[1,b]">>,<<"[1,c]">>]}, readGT(system, Channel, <<"key">>, <<"[1,a]">>, <<"2">>)),
    	?assertEqual({ok,[<<"234567">>,<<"345678">>]}, readGT(system, Channel, <<"value">>, <<"[1,ab]">>, <<"2">>)),
    	?assertEqual({ok,[<<"[1,b]",9,"234567">>,<<"[1,c]",9,"345678">>]}, readGT(system, Channel, <<"kvpair">>, <<"[1,ab]">>, <<"2">>)),
    	?assertEqual({ok,[<<"[1,b]",9,"BFFHP">>,<<"[1,c]",9,"ZCZ28">>]}, readGT(system, Channel, <<"khpair">>, <<"[1,ab]">>, <<"2">>)),

		?assertEqual(ok,imem_meta:truncate_table(skvhTest)),
    	KVtab = <<"{<<\"52015\">>,<<>>,<<\"AaaEnabled\">>}	false
{<<\"52015\">>,<<\"SMS-SUB-52015\">>,<<\"AaaEnabled\">>}	false">>,
		TabRes1 = write(system, Channel, KVtab),
        ?assertEqual({ok,[<<"2FAJ6">>,<<"G8J8Y">>]}, TabRes1),
        KVLong = 
<<"{<<\"52015\">>,<<>>,<<\"AllowedContentTypes\">>}	\"audio/amr;audio/mp3;audio/x-rmf;audio/x-beatnic-rmf;audio/sp-midi;audio/imelody;audio/smaf;audio/rmf;text/x-imelody;text/x-vcalendar;text/x-vcard;text/xml;text/html;text/plain;text/x-melody;image/png;image/vnd.wap.wbmp;image/bmp;image/gif;image/ief;image/jpeg;image/jpg;image/tiff;image/x-xwindowdump;image/vnd.nokwallpaper;application/smil;application/postscript;application/rtf;application/x-tex;application/x-texinfo;application/x-troff;audio/basic;audio/midi;audio/x-aifc;audio/x-aiff;audio/x-mpeg;audio/x-wav;video/3gpp;video/mpeg;video/quicktime;video/x-msvideo;video/x-rn-mp4;video/x-pn-realvideo;video/mpeg4;multipart/related;multipart/mixed;multipart/alternative;message/rfc822;application/vnd.oma.drm.message;application/vnd.oma.dm.message;application/vnd.sem.mms.protected;application/vnd.sonyericsson.mms-template;application/vnd.smaf;application/xml;video/mp4;\"">>,
        ?assertEqual({ok,[<<"206MFE">>]}, write(system, Channel, KVLong)),

        ?assertEqual(ok, imem_meta:drop_table(skvhTest)),
        ?assertEqual(ok, imem_meta:drop_table(skvhTestAudit_86400@_)),

        %% Test the raw access interface. %%

        ?assertException(throw, {ClEr,{"Table does not exist",_}}, imem_meta:check_table(skvhTest)),
        ?assertException(throw, {ClEr,{"Table does not exist",_}}, imem_meta:check_table(skvhTestAudit_86400@_)),
        ?assertException(throw, {ClEr,{"Table does not exist",_}}, read(system, Channel, ["1"])),

        %% Base maps
        Map1 = #{ckey => ["1"], cvalue => <<"{\"testKey\": \"testValue\"}">>, chash => <<"1HU42V">>},
        Map2 = #{ckey => ["1", "a"], cvalue => <<"{\"testKey\": \"a\", \"testNumber\": 2}">>, chash => <<"1Y22WI">>},
        Map3 = #{ckey => ["1", "b"], cvalue => <<"{\"testKey\": \"b\", \"testNumber\": 100}">>, chash => <<"3MBW5">>},
        Map4 = #{ckey => ["1", "c"], cvalue => <<"{\"testKey\": \"c\", \"testNumber\": 250}">>, chash => <<"1RZ299">>},
        Map5 = #{ckey => ["1", "d"], cvalue => <<"{\"testKey\": \"d\", \"testNumber\": 300}">>, chash => <<"1DKGDA">>},

        %% Keys not in the table.
        FirstKey = [""],
        MidleKey = ["1", "b", "1"],
        LastKey = ["1", "e"],

        %% Read some data text interface to create the tables
        ?assertEqual({ok,[<<"{<<\"0\">>,<<>>,<<>>}\tundefined">>]}, read(system, Channel, <<"kvpair">>, K0)),
        ?assertEqual(ok, imem_meta:check_table(skvhTest)),
        ?assertEqual(ok, imem_meta:check_table(skvhTestAudit_86400@_)),

        ?assertEqual([], read(system, Channel, [["1"]])),

        BeforeInsert = erlang:now(),

        ?assertEqual(Map1, insert(system, Channel, maps:get(ckey, Map1), maps:get(cvalue, Map1))),
        ?assertEqual(Map2, insert(system, Channel, maps:get(ckey, Map2), maps:get(cvalue, Map2))),

        %% Test insert using encoded key
        ?assertEqual(Map3, insert(system, Channel, imem_datatype:term_to_binterm(maps:get(ckey, Map3)), maps:get(cvalue, Map3))),

        %% Test insert using maps
        ?assertEqual([Map4, Map5], insert(system, Channel, [Map4#{chash := <<>>}, Map5#{chash := <<>>}])),

        %% Fail to insert concurrency exception
        CoEx = 'ConcurrencyException',

        MapNotInserted = #{ckey => ["1", "1"], cvalue => <<"{\"testKey\": \"roll\", \"testNumber\": 100}">>},
        ?assertException(throw, {CoEx, {"Insert failed, key already exists in", _}}, insert(system, Channel, maps:get(ckey, Map1), maps:get(cvalue, Map1))),
        ?assertException(throw, {CoEx, {"Insert failed, key already exists in", _}}, insert(system, Channel, [MapNotInserted, Map4#{chash := <<>>}])),
        ?assertEqual([], read(system, Channel, [maps:get(ckey, MapNotInserted)])),

        %% Read tests
        ?assertEqual([Map1], read(system, Channel, [maps:get(ckey, Map1)])),
        ?assertEqual([Map1, Map2, Map3], read(system, Channel, [maps:get(ckey, Map1), maps:get(ckey, Map2), maps:get(ckey, Map3)])),

        %% Test read using encoded key
        ?assertEqual([Map1], read(system, Channel, [imem_datatype:term_to_binterm(maps:get(ckey, Map1))])),

        %% Updated maps
        Map1Upd = #{ckey => ["1"], cvalue => <<"{\"testKey\": \"newValue\"}">>, chash => <<"1H51UT">>},
        Map2Upd = #{ckey => ["1", "a"], cvalue => <<"{\"testKey\": \"a\", \"newNumber\": 10}">>, chash => <<"16GAFP">>},
        Map3Upd = #{ckey => ["1", "b"], cvalue => <<"{\"testKey\": \"b\", \"newNumber\": 150}">>, chash => <<"H3LYB">>},

        BeforeUpdate = erlang:now(),

        %% Update using single maps
        ?assertEqual(Map1Upd, update(system, Channel, Map1, Map1Upd)),

        %% Update multiple objects
        ?assertEqual([Map2Upd, Map3Upd], update(system, Channel, [{Map2, Map2Upd#{chash := <<>>}}, {Map3, Map3Upd#{chash := <<>>}}])),

        %% Concurrency exception
        ?assertException(throw, {CoEx, {"Data is modified by someone else", _}}, update(system, Channel, Map1, Map1Upd)),
        ?assertException(throw, {CoEx, {"Data is modified by someone else", _}}, update(system, Channel, [{Map2, Map2Upd#{chash := <<>>}}, {Map3, Map3Upd#{chash := <<>>}}])),

        %% Read tests
        ?assertEqual([Map4, Map5], readGT(system, Channel, maps:get(ckey, Map3), 10)),
        ?assertEqual([Map3Upd, Map4], readGT(system, Channel, maps:get(ckey, Map2), 2)),
        ?assertEqual([Map4, Map5], readGT(system, Channel, MidleKey, 10)),
        ?assertEqual([], readGT(system, Channel, maps:get(ckey, Map5), 2)),

        ?assertEqual([Map3Upd, Map4, Map5], readGE(system, Channel, maps:get(ckey, Map3), 10)),
        ?assertEqual([Map3Upd, Map4], readGE(system, Channel, maps:get(ckey, Map3), 2)),
        ?assertEqual([Map4, Map5], readGE(system, Channel, MidleKey, 10)),
        ?assertEqual([Map5], readGE(system, Channel, maps:get(ckey, Map5), 2)),
        ?assertEqual([], readGE(system, Channel, LastKey, 2)),

        ?assertException(throw,{ClEr,{117,"Too many values, Limit exceeded",1}}, readGELT(system, Channel, FirstKey, LastKey, 1)),
        ?assertEqual([Map1Upd, Map2Upd, Map3Upd, Map4, Map5], readGELT(system, Channel, FirstKey, LastKey, 10)),

        ?assertEqual([Map3Upd, Map4, Map5], readGELT(system, Channel, maps:get(ckey, Map3), LastKey, 10)),
        ?assertEqual([Map3Upd, Map4], readGELT(system, Channel, maps:get(ckey, Map3), maps:get(ckey, Map5), 10)),
        ?assertEqual([Map4, Map5], readGELT(system, Channel, MidleKey, LastKey, 10)),
        ?assertEqual([], readGELT(system, Channel, LastKey, [LastKey | "1"], 10)),

        BeforeRemove = erlang:now(),

        %% Tests removing rows
        ?assertEqual(Map1Upd, remove(system, Channel, Map1Upd)),

        %% Concurrency exception
        ?assertException(throw, {CoEx, {"Remove failed, key does not exist", _}}, remove(system, Channel, Map1)),
        ?assertException(throw, {CoEx, {"Data is modified by someone else", _}}, remove(system, Channel, [Map2Upd, Map3])),

        %% Remove in bulk
        ?assertEqual([Map2Upd, Map3Upd], remove(system, Channel, [Map2Upd, Map3Upd])),

        %% Test final number of rows
        ?assertEqual(2, length(imem_meta:read(skvhTest))),

        %% Audit Tests maps interface.
        %% TODO: How to test reads with multiple partitions.
        ?assertEqual(11, length(audit_readGT(system, Channel, {0, 0, 0}, 100))),

        %% Set time to 0 since depends on the execution of the test.
        AuditInsert1 = #{time => {0,0,0}, ckey => maps:get(ckey, Map1), ovalue => undefined, nvalue => maps:get(cvalue, Map1), cuser => system},
        AuditInsert2 = #{time => {0,0,0}, ckey => maps:get(ckey, Map2), ovalue => undefined, nvalue => maps:get(cvalue, Map2), cuser => system},
        AuditInsert3 = #{time => {0,0,0}, ckey => maps:get(ckey, Map3), ovalue => undefined, nvalue => maps:get(cvalue, Map3), cuser => system},
        ResultAuditInserts = [AuditRow#{time := {0,0,0}} || AuditRow  <- audit_readGT(system, Channel, BeforeInsert, 3)],
        ?assertEqual([AuditInsert1, AuditInsert2, AuditInsert3], ResultAuditInserts),

        AuditUpdate1 = #{time => {0,0,0}, ckey => maps:get(ckey, Map1), ovalue => maps:get(cvalue, Map1), nvalue => maps:get(cvalue, Map1Upd), cuser => system},
        AuditUpdate2 = #{time => {0,0,0}, ckey => maps:get(ckey, Map2), ovalue => maps:get(cvalue, Map2), nvalue => maps:get(cvalue, Map2Upd), cuser => system},
        AuditUpdate3 = #{time => {0,0,0}, ckey => maps:get(ckey, Map3), ovalue => maps:get(cvalue, Map3), nvalue => maps:get(cvalue, Map3Upd), cuser => system},
        ResultAuditUpdates = [AuditRow#{time := {0,0,0}} || AuditRow  <- audit_readGT(system, Channel, BeforeUpdate, 3)],
        ?assertEqual([AuditUpdate1, AuditUpdate2, AuditUpdate3], ResultAuditUpdates),

        AuditRemove1 = #{time => {0,0,0}, ckey => maps:get(ckey, Map1), ovalue => maps:get(cvalue, Map1Upd), nvalue => undefined, cuser => system},
        AuditRemove2 = #{time => {0,0,0}, ckey => maps:get(ckey, Map2), ovalue => maps:get(cvalue, Map2Upd), nvalue => undefined, cuser => system},
        AuditRemove3 = #{time => {0,0,0}, ckey => maps:get(ckey, Map3), ovalue => maps:get(cvalue, Map3Upd), nvalue => undefined, cuser => system},
        ResultAuditRemoves = [AuditRow#{time := {0,0,0}} || AuditRow  <- audit_readGT(system, Channel, BeforeRemove, 3)],
        ?assertEqual([AuditRemove1, AuditRemove2, AuditRemove3], ResultAuditRemoves),

        ?assertEqual([], audit_readGT(system, Channel, <<"now">>, 100)),
        ?assertEqual([], audit_readGT(system, Channel, <<"2100-01-01">>, 100)),
        ?assertEqual(11, length(audit_readGT(system, Channel, <<"1970-01-01">>, 100))),

        Ex4 = {'ClientError',{"Data conversion format error",{timestamp,"1900-01-01",{"Cannot handle dates before 1970"}}}},
        ?assertException(throw, Ex4, audit_readGT(system, Channel, <<"1900-01-01">>, 100)),

        %% Read History tests, time is reset to 0 for comparison but should be ordered from new to old.
        CL1 = [#{time => {0,0,0}, ovalue => maps:get(cvalue, Map1Upd), nvalue => undefined, cuser => system}
              ,#{time => {0,0,0}, ovalue => maps:get(cvalue, Map1), nvalue => maps:get(cvalue, Map1Upd), cuser => system}
              ,#{time => {0,0,0}, ovalue => undefined, nvalue => maps:get(cvalue, Map1), cuser => system}],
        History1 = #{ckey => maps:get(ckey, Map1), cvhist => CL1},
        CL2 = [#{time => {0,0,0}, ovalue => maps:get(cvalue, Map2Upd), nvalue => undefined, cuser => system}
              ,#{time => {0,0,0}, ovalue => maps:get(cvalue, Map2), nvalue => maps:get(cvalue, Map2Upd), cuser => system}
              ,#{time => {0,0,0}, ovalue => undefined, nvalue => maps:get(cvalue, Map2), cuser => system}],
        History2 = #{ckey => maps:get(ckey, Map2), cvhist => CL2},
        CL3 = [#{time => {0,0,0}, ovalue => maps:get(cvalue, Map3Upd), nvalue => undefined, cuser => system}
              ,#{time => {0,0,0}, ovalue => maps:get(cvalue, Map3), nvalue => maps:get(cvalue, Map3Upd), cuser => system}
              ,#{time => {0,0,0}, ovalue => undefined, nvalue => maps:get(cvalue, Map3), cuser => system}],
        History3 = #{ckey => maps:get(ckey, Map3), cvhist => CL3},

        %% Read using a list of term keys.
        HistResult = hist_reset_time(hist_read(system, Channel, [maps:get(ckey, Map1), maps:get(ckey, Map2), maps:get(ckey, Map3)])),
        ?assertEqual(3, length(HistResult)),
        ?assertEqual([History1, History2, History3], HistResult),

        %% Using sext encoded Keys
        EncodedKeys = [imem_datatype:term_to_binterm(maps:get(ckey, Map1))
                      ,imem_datatype:term_to_binterm(maps:get(ckey, Map2))
                      ,imem_datatype:term_to_binterm(maps:get(ckey, Map3))],

        HistResultEnc = hist_reset_time(hist_read(system, Channel, EncodedKeys)),

        ?assertEqual([History1, History2, History3], HistResultEnc),

        ?assertEqual(ok, imem_meta:drop_table(skvhTest)),
        ?assertEqual(ok, imem_meta:drop_table(skvhTestAudit_86400@_)),
        ?assertEqual(ok, imem_meta:drop_table(skvhTestHist)),

        ?LogDebug("success ~p~n", [drop_tables])
    catch
        Class:Reason ->     
            timer:sleep(1000),
            ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
            throw ({Class, Reason})
    end,
    ok.
    
-endif.
