-module(imem_dal_skvh).

-include("imem_meta.hrl").

-define(AUDIT_SUFFIX,"Audit_86400@_").
-define(AUDIT(__Channel), binary_to_list(__Channel) ++ ?AUDIT_SUFFIX).
-define(HIST_SUFFIX,"Hist").
-define(EXTEND_CODE,"\n    %extend_code_start\n    %extend_code_end").
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
                    { time                    :: ddTimeUID()			%% ?TIME_UID
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
                    { time = ?ERL_MIN_TERM    :: ddTimeUID()                %% ?TIME_UID
                    , ckey = ?nav             :: binary()|?nav			
                    , ovalue               	  :: binary()|undefined		    %% old value
                    , nvalue               	  :: binary()|undefined		    %% new value
                    , cuser=unknown 		  :: ddEntityId()|unknown
                    }
       ).
-define(skvhAudit,  [timestamp,binterm,binstr,binstr,userid]).

-record(skvhTable,                            %% sorted key value hash table    
                    { ckey = ?nav             :: binary()|?nav
                    , cvalue                  :: binary() | list() | map()
                    , chash	= <<"fun(_,__Rec) -> list_to_binary(io_lib:format(\"~.36B\",[erlang:phash2({element(2,__Rec),element(3,__Rec)})])) end.">>
                    						  :: binary()
                    }
       ).
-define(skvhTable,  [binterm,binstr,binstr]).

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
        , atom_history_alias/1      %% (Channel)                    return history alias as atom
        , create_table/4            %% (Name,[],Opts,Owner)         create empty table / audit table / history table (Name as binary or atom)
        , drop_table/1              %% (Name)
        , channel_ctx/1             %% (Name)                       return #skvhCtx{mainAlias=Tab, auditAlias=Audit, histAlias=Hist}
        , create_check_channel/1    %% (Channel)                    create empty table / audit table / history table if necessary (Name as binary or atom)
        , create_check_channel/2    %% (Channel,Options)            create empty table / audit table / history table if necessary (Name as binary or atom)
        , create_check_skvh/2       %% (User, Channel)              create empty table / audit table / history table if necessary (Name as binary or atom)
        , create_check_skvh/3       %% (Channel,Options)            create empty table / audit table / history table if necessary (Name as binary or atom)
        , write/3           %% (User, Channel, KVTable)             resource may not exist, will be created, return list of hashes
        , write/4           %% (User, Channel, Key, Value)          resource may not exist, will be created, map of inserted row
        , insert/3          %% (User, Channel, MapList)             resources should not exist will be created, retrun list of maps with inserted rows
        , insert/4          %% (User, Channel, Key, Value)          resource should not exist will be created, return map with inserted row
        , update/3          %% (user, Channel, ChangeList)          update a list of resources, it will fail if the old value was modified by someone else
        , read/3            %% (User, Channel, KeyList)             return empty Arraylist if none of these resources exists, the list is returning as a map
        , read_siblings/3   %% (User, Channel, KeyList)             return list of maps
        , read_shallow/3    %% (User, Channel, KeyList)             return list of maps
        , read_deep/3       %% (User, Channel, KeyList)             return empty Arraylist if none of these resources exists, the list is returning as a map
        , read/4            %% (User, Channel, Item, KeyTable)      return empty Arraylist if none of these resources exists
        , readGELT/5        %% (User, Channel, CKey1, CKey2, L)     from key at or after CKey1 to last key before CKey2, result as map, fails if more than L rows
        , readGELT/6        %% (User, Channel, Item, CKey1, CKey2, L)   from key at or after CKey1 to last key before CKey2, fails if more than L rows
        , readGELTKeys/5    %% (User, Channel, CKey1, CKey2, L)     from key at or after CKey1 to last key before CKey2, list of keys
        , readGELTHashes/5  %% (User, Channel, CKey1, CKey2, L)     from key at or after CKey1 to last key before CKey2, list of {key, hash}
        , readGELTMap/5     %% (User, Channel, CKey1, CKey2, L)     from key at or after CKey1 to last key before CKey2, list of skvhmaps
        , readGT/4          %% (User, Channel, CKey1, L)            start with first key after CKey1, return result as list of maps of lenght L or less
        , readGT/5          %% (User, Channel, Item, CKey1, Limit)  start with first key after CKey1, return Limit results or less
        , readGE/4          %% (User, Channel, CKey1, Limit)        start with first key at or after CKey1, return Limit results or less as maps
        , readGE/5          %% (User, Channel, Item, CKey1, Limit)  start with first key at or after CKey1, return Limit results or less
        , audit_readGT/4    %% (User, Channel, TS1, Limit)          read audit info after Timestamp1, resturn a list of maps with Limit results or less
        , audit_readGT/5    %% (User, Channel, Item, TS1, Limit)    read audit info after Timestamp1, return Limit results or less
        , hist_read/3       %% (User, Channel, KeyList)             return history list as maps for given keys
        , hist_read_deleted/3 %% (User, Channel, Key)               return the oldvalue of the deleted object
        , prune_history/2   %% (User, Channel)                      keeps the last non noop history states and cverifies history state exists for all the keys
        , remove/3          %% (User, Channel, RowList)             delete a resource will fail if it was modified, rows should be in map format
        , remove/4          %% (User, Channel, RowList, Opts)       delete a resource will fail if it was modified, rows should be in map format, with trigger options
        , delete/3          %% (User, Channel, KeyTable)            do not complain if keys do not exist
        , deleteGELT/5      %% (User, Channel, CKey1, CKey2, L)     delete range of keys >= CKey1 and < CKey2, fails if more than L rows
        , deleteGTLT/5      %% (User, Channel, CKey1, CKey2, L)     delete range of keys > CKey1 and < CKey2, fails if more than L rows
        , get_longest_prefix/4
        , check_age_audit_entry/4 %% (User, Channel, Key, TS1)      returns the records if there is any for the key after the timestamp TS1
        , audit_write_noop/3 %% (User, Channel, Key)                creates an entry in audit table for channel wehere nvalue and ovalue are the same
        ]).

-export([build_aux_table_info/1,
         audit_info/6,
         write_audit/1,
         audit_recs_time/1,
         add_channel_trigger_hook/3,
         get_channel_trigger_hook/2,
         del_channel_trigger_hook/2,
         write_history/2]).

-export([expand_inline_key/3, expand_inline_key_map/3, expand_inline/3]).

-export([skvh_rec_to_map/1, map_to_skvh_rec/1]).

-export([foldl/4]).

-export([is_row_type/2]).

-safe([is_row_type,skvh_rec_to_map,map_to_skvh_rec,expand_inline_key,
       expand_inline_key_map,expand_inline,audit_recs_time,write_history,
       write_audit,audit_info,build_aux_table_info]).

-safe([table_name/1, atom_table_name/1, audit_alias/1, atom_history_alias/1,
       channel_ctx/1, read/3, read_siblings/3, read_shallow/3, read_deep/3,
       read/4, readGELT/5, readGELT/6, readGELTKeys/5, readGELTHashes/5,
       readGELTMap/5, readGT/4, readGT/5, readGE/4, readGE/5, audit_readGT/4,
       audit_readGT/5, hist_read/3, hist_read_deleted/3, get_longest_prefix/4,
       check_age_audit_entry/4]).

-spec foldl(User :: any(), FoldFun :: function(), InputAcc :: any(),
            Channel :: binary()) -> OutPutAcc :: any().
foldl(_User, FoldFun, InputAcc, Channel) when is_function(FoldFun, 2) ->
    Tab = atom_table_name(Channel),
    imem_meta:foldl(
        fun (Rec, Acc) when is_record(Rec,skvhTable) ->
            FoldFun(skvh_rec_to_map(Rec), Acc);
            (_Rec, Acc) -> Acc
        end, InputAcc, Tab).

get_channel_trigger_hook(Mod, Channel) when is_atom(Mod) ->
    Tab = atom_table_name(Channel),
    case imem_meta:get_trigger(Tab) of
        TrgrFun when is_binary(TrgrFun) ->
            HookTok = atom_to_list(Mod),
            HookStartTok = "\n%"++HookTok++"_hook_start",
            HookEndTok = "\n%"++HookTok++"_hook_end",
            ModHookCodeRe = lists:flatten([HookStartTok,"(.*)",HookEndTok]),
            case re:run(TrgrFun, ModHookCodeRe, [{capture, [1], binary},dotall]) of
                {match, [Hook|_]} -> Hook;
                _ -> {error, nohook}
            end;
        _ ->
            {error, nohook}
    end.

add_channel_trigger_hook(Mod, Channel, Hook) when is_atom(Mod) ->
    Tab = atom_table_name(Channel),
    case imem_meta:get_trigger(Tab) of
        TrgrFun when is_binary(TrgrFun) ->
            HookTok = atom_to_list(Mod),
            HookStartTok = "\n%"++HookTok++"_hook_start",
            HookEndTok = "\n%"++HookTok++"_hook_end",
            PreparedHook = lists:flatten([HookStartTok,"\n,",Hook,HookEndTok]),
            ModHookCodeRe = lists:flatten([HookStartTok,".*",HookEndTok]),
            case re:run(TrgrFun, ModHookCodeRe, [dotall, {capture, all, list}]) of
                % hook already exist (in all restarts)
                {match, [PreparedHook]} -> no_op;
                % replace with new hook code (hook upgrade)
                {match, _} ->                    
                    imem_meta:create_or_replace_trigger(
                        Tab, 
                        re:replace(
                            TrgrFun, ModHookCodeRe, PreparedHook,
                            [{return, binary},dotall]
                        )
                    );
                % no previous hook (cold start / empty DB / repair)
                nomatch ->
                    imem_meta:create_or_replace_trigger(
                        Tab, 
                        re:replace(
                            TrgrFun,
                            "\n    %extend_code_end",
                            PreparedHook++"\n    %extend_code_end",
                            [{return, binary}]
                        )
                    )
            end,
            ok;
        _ ->
            {error, notrigger}
    end.

del_channel_trigger_hook(Mod, Channel) when is_atom(Mod) ->
    Tab = atom_table_name(Channel),
    case imem_meta:get_trigger(Tab) of
        TrgrFun when is_binary(TrgrFun) ->
            HookTok = atom_to_list(Mod),
            HookStartTok = "\n%"++HookTok++"_hook_start",
            HookEndTok = "\n%"++HookTok++"_hook_end",
            ModHookCodeRe = lists:flatten([HookStartTok,".*",HookEndTok]),
            case re:replace(TrgrFun, ModHookCodeRe, "", [{return, binary},dotall]) of
                TrgrFun -> % No previous hook
                    ok;
                NewTrgrFun ->
                    imem_meta:create_or_replace_trigger(Tab, NewTrgrFun)
            end;
        _ ->
            {error, notrigger}
    end.

-spec expand_inline_key(User :: any(), Channel :: binary(), Key :: any()) ->
    Json :: binary().
expand_inline_key(User, Channel, Key) ->
    case read(User, Channel, [Key]) of
        [] -> [];
        [#{cvalue := Json}] -> expand_inline(User, Channel, Json)
    end.

-spec expand_inline_key_map(User :: any(), Channel :: binary(), Key :: any()) ->
    Json :: map().
expand_inline_key_map(User, Channel, Key) ->
    case read(User, Channel, [Key]) of
        [] -> [];
        [#{cvalue := Json}] -> expand_inline(User, Channel, imem_json:decode(Json, [return_maps]))
    end.

-spec expand_inline(User :: any(), Channel :: binary(), Json :: binary()) ->
    ExpandedJson :: binary().
expand_inline(User, Channel, Json) when is_binary(Json) ->
    imem_json:encode(expand_inline(User, Channel,
                                   imem_json:decode(Json, [return_maps])));
expand_inline(User, Channel, Json) when is_map(Json) ->
    Binds = maps:fold(
        fun(K,V,Acc) ->
            case re:run(K, "^inline_*") of
                nomatch -> Acc;
                _ ->
                    case read(User, Channel,[imem_json_data:to_strbinterm(V)]) of
                        [] -> Acc;
                        [#{cvalue := BVal}] -> [{V, BVal} | Acc]
                    end
            end
        end, [], Json),
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
atom_table_name(Channel) -> binary_to_existing_atom(table_name(Channel),utf8).

%% @doc Returns history alias name from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	history table alias as binstr
-spec history_alias(binary()) -> binary().
history_alias(Channel) -> list_to_binary(?HIST(Channel)).

%% @doc Returns history alias name from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	history table alias as atom
-spec atom_history_alias(binary()) -> atom().
atom_history_alias(Channel) -> binary_to_existing_atom(history_alias(Channel),utf8).

%% @doc Returns audit alias name from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	audit table alias as binstr
-spec audit_alias(binary()) -> binary().
audit_alias(Channel) -> list_to_binary(?AUDIT(Channel)).

%% @doc Returns audit alias name from channel name (may or may not be a valid channel name)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns:	audit table alias as atom
-spec atom_audit_alias(binary()) -> atom().
atom_audit_alias(Channel) -> binary_to_existing_atom(audit_alias(Channel),utf8).

%% @doc Returns audit table name from channel name 
%% Channel: String (not binstr) of channel name (preferrably upper case or camel case)
%% Key: 	Timestamp of change
%% returns:	audit table name as atom 
-spec audit_table_name(list(),term()) -> atom().
audit_table_name(Channel, Key) when is_list(Channel) -> 
	imem_meta:partitioned_table_name(Channel ++ ?AUDIT_SUFFIX, Key).

audit_table_time(Channel) ->
    	{AuditTab, TransTime} = audit_table_time(?TRANS_TIME_GET, Channel),
        ?TRANS_TIME_PUT(TransTime),
        {AuditTab, TransTime}.

audit_table_time({Sec, Mic, Node, TCnt}=TransTime, CH) ->
	ATName = audit_table_name(CH, TransTime),
	case imem_meta:last(ATName) of 
		'$end_of_table' ->	 
			{ATName, TransTime}; % audit is empty, any TransTime accepted 
        {_,_,_} ->   
            {ATName, TransTime}; % audit is of old format, TransTime accepted 
        TransTime ->
            {ATName, {Sec,Mic,Node,?INTEGER_UID}}; % I see my own audit write. May not occur
		LastLog when (LastLog >= TransTime) andalso (element(2,LastLog) < 999999) ->
			{ATName, {element(1,LastLog), element(2,LastLog)+1, Node, TCnt}}; % count usec up
		LastLog when (LastLog >= TransTime) ->
			audit_table_time({element(1,LastLog)+1, 0, Node, TCnt}, CH); % partition might change
		_ -> 
		 	{ATName, TransTime} % last audit key is smaller than TransTime, accepted
	end.

audit_table_next(ATName, {Sec, Mic, Node, _Cnt} , _CH) -> 
    {ATName, {Sec, Mic, Node, ?INTEGER_UID}}. % next bigger key for double transaction

is_row_type(map, R) when is_map(R) -> R;
is_row_type(map, R) when is_list(R) -> 
    [if not is_map(H) -> 
        ?ClientError({"Bad datatype, expected map", R});
        true -> H
     end || H <- R];
is_row_type(map, R) -> ?ClientError({"Bad datatype, expected map", R});
is_row_type(list, R) when is_list(R) -> R;
is_row_type(list, R) -> ?ClientError({"Bad datatype, expected list", R});
is_row_type(binary, R) when is_binary(R) -> R;
is_row_type(binary, R) -> ?ClientError({"Bad datatype, expected binary", R}).


%% @doc Checks existence of interface tables for Channel 
%% return atom names of the tables (MasterTable, AuditTable and HistoryTable if present)
%% Channel: Binary string of channel name
%% returns: provisioning record with table aliases to be used for data queries
%% throws   ?ClientError
-spec channel_ctx(binary()|atom()) -> #skvhCtx{}.
channel_ctx(Channel) when is_binary(Channel); is_atom(Channel) ->
    Main = table_name(Channel),
    Tab =
    try
        T = binary_to_existing_atom(Main,utf8),
        imem_meta:check_local_table_copy(T),           %% throws if master table is not locally resident
        T
    catch
        throw:{'ClientError',{"Table does not exist",_}} ->
            ?ClientError({"Channel does not exist", Channel});
        throw:Throw -> throw(Throw);
        Class:Error ->
            ?SystemException(
               {"Error creating channel context", {Class,Error}})
    end,
    Audit = try
        A = list_to_existing_atom(?AUDIT(Channel)),
        AP = imem_meta:partitioned_table_name_str(A,?TIME_UID),
        imem_meta:check_local_table_copy(list_to_existing_atom(AP)),     %% throws if table is not locally resident
        A
    catch _:_ ->  ignored
    end,    
    Hist = try
        H = list_to_existing_atom(?HIST(Channel)),
        imem_meta:check_local_table_copy(H),             %% throws if history table is not locally resident
        H
    catch _:_ -> ignored
    end,
    #skvhCtx{mainAlias=Tab, auditAlias=Audit, histAlias=Hist}.

%% @doc Checks existence of interface tables by checking existence of table name atoms in atom cache
%% creates these tables if not already existing 
%% return atom names of the tables (StateTable and AuditTable)
%% Channel: Binary string of channel name (preferrably upper case or camel case)
%% returns: provisioning record with table aliases to be used for data queries
%% throws   ?ClientError, ?UnimplementedException, ?SystemException
-spec create_check_skvh(ddEntityId(),binary()|atom()) -> ok.
create_check_skvh(UserId, Channel) ->
    %% TODO : Possible future validation, Ignored for now
    create_check_skvh(UserId, Channel, [audit,history]).

-spec create_check_skvh(ddEntityId(),binary()|atom(), list()) -> ok.
create_check_skvh(_UserId, Channel, Options) ->
    create_check_channel(Channel, Options).

-spec create_check_channel(binary()|atom()) ->  ok.
create_check_channel(Channel) ->
    create_check_channel(Channel, [audit,history]).

-spec create_check_channel(binary()|atom(), [atom()|{atom(),any()}]) -> ok.
create_check_channel(Channel, Options) ->
	Main = table_name(Channel),
    CreateAudit = proplists:get_value(audit, Options, false),
    CreateHistory = proplists:get_value(history, Options, false),
	Tab = try 
		T = binary_to_existing_atom(Main,utf8),
		imem_meta:check_local_table_copy(T),           %% throws if master table is not locally resident
        T
	catch
        Class1:_ when Class1 =:= error; Class1 =:= throw ->
            TC = binary_to_atom(Main,utf8),
            {NewCValue, NewTypes} = case proplists:get_value(type, Options, binary) of
                binary ->
                    {<<"fun(R) -> imem_dal_skvh:is_row_type(binary,R) end.">>,?skvhTable};
                list ->
                    {<<"fun(R) -> imem_dal_skvh:is_row_type(list,R) end.">>,
                        [lists:nth(1,?skvhTable), list | lists:nthtail(2, ?skvhTable)]};
                map ->
                    {<<"fun(R) -> imem_dal_skvh:is_row_type(map,R) end.">>,
                        [lists:nth(1,?skvhTable), map | lists:nthtail(2, ?skvhTable)]}
                end,
            %% TODO: Check if we need to only catch in special cases
            catch imem_meta:create_check_table(
                TC, {record_info(fields,skvhTable),NewTypes,#skvhTable{cvalue=NewCValue}},
                ?TABLE_OPTS, system),
            TC
    end,
    if 
        CreateAudit ->
            try
                A = list_to_existing_atom(?AUDIT(Channel)),
                AP = imem_meta:partitioned_table_name_str(A,?TIME_UID),
                imem_meta:check_local_table_copy(list_to_existing_atom(AP)),     %% throws if table is not locally resident
                A
            catch 
                Class2:_ when Class2 =:= error; Class2 =:= throw ->
                    AC = list_to_atom(?AUDIT(Channel)),
                    catch imem_meta:create_check_table(AC, {record_info(fields,skvhAudit),?skvhAudit,#skvhAudit{}},
                                                          ?AUDIT_OPTS, system),
                    AC
            end;
        true -> undefined
    end,    
    if 
        CreateAudit andalso CreateHistory ->
            try
                H = list_to_existing_atom(?HIST(Channel)),
                imem_meta:check_local_table_copy(H),             %% throws if history table is not locally resident
                H
            catch 
                Class:_Reason when Class =:= error; Class =:= throw ->
                    HC = list_to_atom(?HIST(Channel)),
                    catch imem_meta:create_check_table(HC, {record_info(fields, skvhHist),?skvhHist, #skvhHist{}},
                                                          ?HIST_OPTS, system),
                    HC
            end;
        true -> undefined
    end,
    %% TODO: This will replace the trigger each time maybe versioning will be better
    %% In cases when the table already exists the trigger was getting overwritten
    %% This is attempt to check if the trigger is default one or not with following steps
    %% check if trigger exists. If the trigger exists check if the extened_code part
    %% has been changed or not. If changed then do nothing, as we should not overwrite
    %% the trigger. If not changed then the trigger can be overwritten to have the
    %% new audit and history trigger functions.
    TiggerFun = skvh_trigger_fun_str(Options, ""),
    case catch imem_meta:get_trigger(Tab) of
        undefined -> 
            imem_meta:create_or_replace_trigger(Tab, TiggerFun);
        Trigger when is_binary(Trigger) ->
            case re:run(Trigger, ?EXTEND_CODE) of
                nomatch -> no_op;
                _ ->
                    imem_meta:create_or_replace_trigger(Tab, TiggerFun)
            end;
        _ -> no_op
    end,
    ok.

-spec create_table(ddSimpleTable(), ddTableMeta(), ddOptions(), ddEntityId()) -> {ok, ddString()}.
create_table(Name, [], _TOpts, Owner) when is_atom(Name) ->
    create_table(list_to_binary(atom_to_list(Name)), [], _TOpts, Owner);
create_table(Channel, [], TOpts, Owner) when is_list(Channel) ->
    create_table(list_to_binary(Channel), [], TOpts, Owner);
create_table(Channel, [], _TOpts, Owner) when is_binary(Channel) ->
    Tab = binary_to_atom(table_name(Channel),utf8),
    {ok, QTN} = imem_meta:create_table(Tab, {record_info(fields, skvhTable),?skvhTable, #skvhTable{}}, ?TABLE_OPTS, Owner),
    AC = list_to_atom(?AUDIT(Channel)),
    {ok, _} = imem_meta:create_table(AC, {record_info(fields, skvhAudit),?skvhAudit, #skvhAudit{}}, ?AUDIT_OPTS, Owner),
    ok = imem_meta:create_or_replace_trigger(binary_to_atom(Channel,utf8), skvh_trigger_fun_str([audit,history], "")),
    HC = list_to_atom(?HIST(Channel)),
    {ok, _} = imem_meta:create_table(HC, {record_info(fields, skvhHist),?skvhHist, #skvhHist{}}, ?HIST_OPTS, Owner),
    {ok, QTN}.

add_if(F, Opts, Code) ->
    case lists:member(F,Opts) of
        true -> Code;
        false -> ""
    end.
skvh_trigger_fun_str(Opts, ExtraFun) ->
    list_to_binary(
      ["fun(OldRec,NewRec,Table,User,TrOpts) ->\n"
       "    start_trigger",
       add_if(audit, Opts,
       ",\n    {AuditTable,HistoryTable,TransTime,Channel}\n"
       "        = imem_dal_skvh:build_aux_table_info(Table),\n"
       "    AuditInfoList = imem_dal_skvh:audit_info(User,Channel,AuditTable,TransTime,OldRec,NewRec),\n"
       "    case lists:member(no_audit,TrOpts) of \n"
       "        true ->  ok;\n"
       "        false -> ok = imem_dal_skvh:write_audit(AuditInfoList)\n"
       "    end"),
       add_if(history, Opts,
       ",\n    case lists:member(no_history,TrOpts) of \n"
       "        true ->  ok;\n"
       "        false -> ok = imem_dal_skvh:write_history(HistoryTable,AuditInfoList)\n"
       "    end"),
       if length(ExtraFun) == 0 -> "";
          true -> ",\n    "++ExtraFun end,
        ?EXTEND_CODE
       "\nend."]).

-spec drop_table(binary()|atom()) -> ok.
drop_table(Name) when is_atom(Name) ->
    drop_table(list_to_binary(atom_to_list(Name)));
drop_table(Channel) when is_binary(Channel) ->
    Tab = binary_to_atom(table_name(Channel),utf8),
    catch imem_meta:drop_table(Tab),
    AC = list_to_existing_atom(?AUDIT(Channel)),
    catch imem_meta:drop_table(AC),
    HC = list_to_existing_atom(?HIST(Channel)),
    catch imem_meta:drop_table(HC).

build_aux_table_info(Table) ->
	["","",Channel,"","","",""] = imem_meta:parse_table_name(Table),
    HistoryTable = try ?HIST_FROM_STR(Channel) of H -> H
                   catch _:_ -> '$no_history'
                   end,
	{AuditTable,TransTime} = audit_table_time(Channel),
    {AuditTable,HistoryTable,TransTime,Channel}.

audit_info(User,_Channel,AuditTable,TransTime,{},{}) ->
    [{AuditTable, #skvhAudit{time=TransTime,
                             ckey=sext:encode(undefined),
                             ovalue=undefined,
                             nvalue=undefined,
                             cuser=User}}
    ];  % truncate table
audit_info(User,_Channel,AuditTable,TransTime,OldRec,{}) ->
    [{AuditTable, #skvhAudit{time=TransTime,
                             ckey=element(2,OldRec),
                             ovalue=element(3,OldRec),
                             nvalue=undefined,
                             cuser=User}}
    ];  % delete old rec
audit_info(User,_Channel,AuditTable,TransTime,{},NewRec) ->
    [{AuditTable, #skvhAudit{time=TransTime,
                             ckey=element(2,NewRec),
                             ovalue=undefined,
                             nvalue=element(3,NewRec),
                             cuser=User}}
    ];  % insert new rec
audit_info(User, _Channel, AuditTable, TransTime, OldRec, NewRec)
  when element(2,OldRec) == element(2,NewRec) ->
    OldKey = element(2,OldRec),
    [{AuditTable, #skvhAudit{time=TransTime,
                             ckey=OldKey,
                             ovalue=element(3,OldRec),
                             nvalue=element(3,NewRec),
                             cuser=User}}
    ];  % update value
audit_info(User, Channel, AuditTable, TransTime, OldRec, NewRec) ->
    {AuditTable1, TransTime1} = audit_table_next(AuditTable, TransTime, Channel),
    OldKey = element(2,OldRec),
    NewKey = element(2,NewRec),
    [{AuditTable,  #skvhAudit{time=TransTime, 
                              ckey=OldKey,
                              ovalue=element(3,OldRec),
                              nvalue=undefined,
                              cuser=User}},
     {AuditTable1, #skvhAudit{time=TransTime1, 
                              ckey=NewKey,
                              ovalue=undefined,
                              nvalue=element(3,NewRec),
                              cuser=User}}
    ].  % delete and insert

audit_recs_time(A) when is_record(A, skvhAudit) -> A#skvhAudit.time;
audit_recs_time({_,A}) when is_record(A, skvhAudit) -> A#skvhAudit.time;
audit_recs_time([A|Rest]) -> [audit_recs_time(A)|audit_recs_time(Rest)];
audit_recs_time([]) -> [].

audit_write_noop(User, Channel, Key) ->
    case read(User, Channel, [Key]) of
        [] -> no_op;
        [#{cvalue := Value}] ->
            AuditNoop = fun() ->
                {AuditTable, TransTime} = audit_table_time(binary_to_list(Channel)),
                SkvhRec = #skvhTable{ckey=imem_datatype:term_to_binterm(Key), cvalue=Value},
                AuditInfo = audit_info(User, Channel, AuditTable, TransTime, SkvhRec,SkvhRec),
                write_audit(AuditInfo)
            end,
            imem_meta:return_atomic(imem_meta:transaction(AuditNoop))
    end.

write_audit([]) -> ok;
write_audit([{AuditTable, #skvhAudit{} = Rec}|Rest]) ->
    ok = imem_meta:write(AuditTable, Rec),
    write_audit(Rest).

write_history(_HistoryTable, []) -> ok;
write_history(HistoryTable, [{ _AuditTable
                             , #skvhAudit{time=T, ckey=K, ovalue=O, nvalue=N, cuser=U}
                             } | Rest
                            ]) ->
    if O == N andalso N == undefined ->
        ok = imem_meta:truncate_table(HistoryTable);
        true -> ok
    end,
    ok = imem_meta:write(
           HistoryTable,
           case imem_meta:read(HistoryTable,K) of
               [#skvhHist{ckey=K,cvhist=CH}=H] ->
                   H#skvhHist{ckey=K,cvhist=[#skvhCL{time=T,ovalue=O,nvalue=N,cuser=U}|CH]};
               [] ->
                   #skvhHist{ckey=K,cvhist=[#skvhCL{time=T,ovalue=O,nvalue=N,cuser=U}]}
           end),
    write_history(HistoryTable, Rest).

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

binterm_to_term_key(MaybeDecodedKey) when is_binary(MaybeDecodedKey) ->
    imem_datatype:binterm_to_term(MaybeDecodedKey);
binterm_to_term_key(MaybeDecodedKey) when is_list(MaybeDecodedKey) ->
    MaybeDecodedKey.

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
    #skvhTable{ckey = CKey, cvalue = CValue};
map_to_skvh_rec(#{ckey := DecodedKey, cvhist := CVHist}) ->
    CKey = imem_datatype:term_to_binterm(DecodedKey),
    #skvhHist{ckey = CKey, cvhist = [map_to_skvh_cl(C) || C <- CVHist]}.

map_to_skvh_cl(#{time := Time, ovalue := OldValue, nvalue := NewValue, cuser := User}) ->
    #skvhCL{time = Time, ovalue = OldValue, nvalue = NewValue, cuser = User}.

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
    #{ckey => CKey, cvalue => CValue, chash => CHash};
skvh_rec_to_map(#skvhHist{ckey = _, cvhist = _} = HistRec) ->
    skvh_hist_to_map(HistRec).

skvh_audit_to_map(#skvhAudit{time = Time, ckey = EncodedKey, ovalue = OldValue,
                             nvalue = NewValue, cuser = User}) ->
    CKey = imem_datatype:binterm_to_term(EncodedKey),
    #{time => Time, ckey => CKey, cuser => User, ovalue => OldValue, nvalue => NewValue}.

skvh_hist_to_map(#skvhHist{ckey = EncodedKey, cvhist = ChangeList}) ->
    CKey = imem_datatype:binterm_to_term(EncodedKey),
    MapChangeList = [skvh_cl_to_map(C) || C <- ChangeList],
    #{ckey => CKey, cvhist => MapChangeList}.

skvh_cl_to_map(#skvhCL{time = Time, ovalue = OldValue, nvalue = NewValue, cuser = User}) ->
    #{time => Time, ovalue => OldValue, nvalue => NewValue, cuser => User}.

%% Data access per key (read,write,delete)

read(User, Channel, Item, KeyTable) when is_binary(Channel), is_binary(Item), is_binary(KeyTable) ->
	Cmd = [read,User,Channel,Item,KeyTable],
	read(Cmd, channel_ctx(Channel), Item, io_key_table_to_term_list(KeyTable), []).

read(Cmd, _, Item, [], Acc)  -> project_result(Cmd, lists:reverse(Acc), Item);
read(Cmd, SkvhCtx, Item, [Key|Keys], Acc)  ->
	KVP = case imem_meta:dirty_read(SkvhCtx#skvhCtx.mainAlias,Key) of
		[] ->		#skvhTable{ckey=Key,cvalue=undefined,chash=undefined} ;
		[Rec] ->	Rec
	end,
	read(Cmd, SkvhCtx, Item, Keys, [KVP|Acc]).
	

write(User,Channel,DecodedKey,Value) when is_binary(Channel) ->
    Key = term_key_to_binterm(DecodedKey),
    TableName = atom_table_name(Channel),
    MergeResult = imem_meta:merge(TableName,#skvhTable{ckey=Key,cvalue=Value},User),
    skvh_rec_to_map(MergeResult).

write(User,Channel, KVTable) when is_binary(Channel), is_binary(KVTable) ->
	Cmd = [write,User,Channel,KVTable],
	write(User, Cmd, channel_ctx(Channel), io_kv_table_to_tuple_list(KVTable), []);
write(User,Channel, KVTuples) when is_binary(Channel), is_list(KVTuples) ->
	Cmd = [write,User,Channel,KVTuples],
	write(User, Cmd, channel_ctx(Channel), KVTuples, []).

write(_User, Cmd, _, [], Acc)  -> return_stringlist(Cmd, lists:reverse(Acc));
write(User, Cmd, SkvhCtx, [{K,V}|KVPairs], Acc)  ->
	#skvhTable{chash=Hash} = imem_meta:merge(SkvhCtx#skvhCtx.mainAlias,#skvhTable{ckey=K,cvalue=V},User),
	write(User,Cmd, SkvhCtx, KVPairs, [Hash|Acc]).

%% delete for list of term keys
delete(User, Channel, [_ | _] = Keys) when is_binary(Channel) ->
    EKeys = [imem_datatype:term_to_binterm(K) || K <- Keys],
    Cmd = [delete,User,Channel,Keys],
    delete(User, Cmd, channel_ctx(Channel), EKeys, []);
%% delete for io keys seperated by new line
delete(User, Channel, KeyTable) when is_binary(Channel), is_binary(KeyTable) -> 
	Cmd = [delete,User,Channel,KeyTable],
	delete(User, Cmd, channel_ctx(Channel), io_key_table_to_term_list(KeyTable), []).

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

read_siblings(_User, _Channel, []) -> [];
read_siblings(User, Channel, [Key | Keys]) ->
    [_|ParentKeyRev] = lists:reverse(binterm_to_term_key(Key)),
    read_shallow(User, Channel, [lists:reverse(ParentKeyRev)])
    ++ read_siblings(User, Channel, Keys).

read_shallow(User, Channel, Keys) ->
    ReadShallowFun = fun() ->
        read_shallow_internal(User, Channel, Keys)
    end,
    imem_meta:return_atomic(imem_meta:transaction(ReadShallowFun)).

read_shallow_internal(_User, _Channel, []) -> [];
read_shallow_internal(User, Channel, [Key | Keys]) ->
    KeyLen = length(binterm_to_term_key(Key)) + 1,
    StartKey = term_key_to_binterm(Key),
    EndKey = term_key_to_binterm(binterm_to_term_key(Key) ++ <<255>>),
    TableName = atom_table_name(Channel),
    read_shallow_single(Channel, TableName, StartKey, EndKey, KeyLen) ++ 
        read_shallow_internal(User, Channel, Keys).

read_shallow_single(Channel, TableName, CurrentKey, EndKey, KeyLen) ->
    case imem_meta:next(TableName, CurrentKey) of
        '$end_of_table' -> [];
        NextKey when NextKey >= EndKey -> [];
        NextKey ->
            case length(imem_datatype:binterm_to_term(NextKey)) =:= KeyLen of
                false -> read_shallow_single(Channel, TableName, NextKey, EndKey, KeyLen);
                true ->
                    [Row] = imem_meta:read(TableName, NextKey),
                    [skvh_rec_to_map(Row) | 
                         read_shallow_single(Channel, TableName, NextKey, EndKey, KeyLen)]
            end
    end.

read_deep(User, Channel, Keys) ->
    ReadDeepFun = fun() ->
        read_deep_internal(User, Channel, Keys)
    end,
    imem_meta:return_atomic(imem_meta:transaction(ReadDeepFun)).

read_deep_internal(_User, _Channel, []) -> [];
read_deep_internal(User, Channel, [Key | Keys]) ->
    StartKey = term_key_to_binterm(Key),
    EndKey = term_key_to_binterm(binterm_to_term_key(Key) ++ <<255>>), % improper list [...|<<255>>]
    TableName = atom_table_name(Channel),
    read_deep_single(Channel, TableName, StartKey, EndKey) ++
        read_deep_internal(User, Channel, Keys).

read_deep_single(Channel, TableName, CurrentKey, EndKey) ->
    case imem_meta:next(TableName, CurrentKey) of
        '$end_of_table' -> [];
        NextKey when NextKey >= EndKey -> [];
        NextKey ->
            [Row] = imem_meta:read(TableName, NextKey),
            [skvh_rec_to_map(Row) | read_deep_single(Channel, TableName, NextKey, EndKey)]
    end.

% @doc Returnes the longest prefix >= startKey and =< EndKey
-spec get_longest_prefix(User :: any(), Channel :: binary(),
                         StartKey :: [list()], EndKey :: [list()]) ->
    LongestPrefixMatchedKey :: [list()].
get_longest_prefix(User, Channel, StartKey, EndKey) when is_binary(Channel) ->
    StartKeyEnc = sext:encode(StartKey),
    EndKeyEnc = sext:encode(EndKey),
    case imem_meta:transaction(
           fun() ->
                   get_longest_prefix(User, {table, atom_table_name(Channel)},
                                      StartKeyEnc, EndKeyEnc)
           end) of
        {atomic, KeyEnc} -> sext:decode(KeyEnc);
        Error -> {error, Error}
    end;
get_longest_prefix(User, {table, Table}, StartKeyEnc, EndKeyEnc) ->
    case imem_meta:next(Table, StartKeyEnc) of
        '$end_of_table' -> StartKeyEnc;
        NextKeyEnc ->
            if NextKeyEnc == EndKeyEnc -> NextKeyEnc;
               NextKeyEnc < EndKeyEnc ->
                   get_longest_prefix(User, {table, Table},
                                      NextKeyEnc, EndKeyEnc);
               true -> StartKeyEnc
            end
    end.

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
    imem_if_mnesia:return_atomic_list(imem_meta:transaction(InsertFun)).

insert_priv(_User, _Channel, []) -> [];
insert_priv(User, Channel, [#{ckey := DecodedKey, cvalue := Value} | MapList]) ->
    [insert(User, Channel, DecodedKey, Value) | insert_priv(User, Channel, MapList)].

update(User, Channel, RowMap) when is_map(RowMap) ->
    NewRow = map_to_skvh_rec(RowMap),
    OldRow = NewRow#skvhTable{cvalue = '_'},
    TableName = atom_table_name(Channel),
    UpdateResult = imem_meta:update(TableName, {OldRow, NewRow}, User),
    skvh_rec_to_map(UpdateResult);
update(User, Channel, ChangeList) ->
    UpdateFun = fun() -> update_priv(User, Channel, ChangeList) end,
    imem_if_mnesia:return_atomic_list(imem_meta:transaction(UpdateFun)).

update_priv(_User, _Channel, []) -> [];
update_priv(User, Channel, [NewRow | ChangeList]) when is_map(NewRow) ->
    [update(User, Channel, NewRow) | update_priv(User, Channel, ChangeList)].

remove(User, Channel, Rows) -> remove(User, Channel, Rows, []).
remove(User, Channel, Rows, Opts) when is_list(Rows) ->
    RemoveFun = fun() -> remove_list(User, Channel, Rows, Opts) end,
    imem_if_mnesia:return_atomic_list(imem_meta:transaction(RemoveFun));
remove(User, Channel, Row, Opts) ->
    remove_single(User, Channel, Row, Opts).

remove_single(User, Channel, #{ckey := DecodedKey, cvalue := Value, chash := Hash}, Opts) ->
    Key = imem_datatype:term_to_binterm(DecodedKey),
    TableName = atom_table_name(Channel),
    RemoveResult = imem_meta:remove(TableName, #skvhTable{ckey=Key, cvalue=Value, chash=Hash}, User, Opts),
    #skvhTable{ckey=DeletedKey, cvalue=CValue, chash=CHash} = RemoveResult,
    CKey = imem_datatype:binterm_to_term(DeletedKey),
    #{ckey => CKey, cvalue => CValue, chash => CHash}.

remove_list(_User, _Channel, [], _Opts) -> [];
remove_list(User, Channel, [#{} = Row | Rows], Opts) ->
    [remove_single(User, Channel, Row, Opts) | remove_list(User, Channel, Rows, Opts)].

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

-spec hist_read_deleted(ddEntityId(), binary(), term()) -> binary().
hist_read_deleted(User, Channel, DecodedKey) ->
    [#{cvhist := [#{ovalue := Value} |_]}] = hist_read(User, Channel, [DecodedKey]),
    Value.

-spec prune_history(ddEntityId(), binary()) -> list().
prune_history(User, Channel) ->
    Time = ?TIME_UID,
    HistoryTable = ?HIST_FROM_STR(binary_to_list(Channel)),
    PruneFun = fun(#{ckey := Key, cvalue := Value}, _) ->
        EKey = imem_datatype:term_to_binterm(Key),
        {HistTime, OValue, NValue, CUser} = case 
            hist_read(User, Channel, [Key]) of
            [] ->                   {Time, undefined, Value, User};
            [#{cvhist := Hist}] ->  prune_cvhist(Hist, Value, Time, User)
        end,
        imem_meta:write(HistoryTable, 
            #skvhHist{ckey=EKey, cvhist=[#skvhCL{time=HistTime, ovalue=OValue, nvalue=NValue, cuser=CUser}]})
    end,
    foldl(User, PruneFun, [], Channel).

-spec prune_cvhist(list(), binary(), tuple(), ddEntityId()) -> tuple().
prune_cvhist([], Value, Time, User) -> {Time, undefined, Value, User};
prune_cvhist([#{nvalue := V, ovalue := V}|Rest], Value, Time, User) ->
    prune_cvhist(Rest, Value, Time, User);
prune_cvhist([#{nvalue := NVal, ovalue := OVal, time := HistTime, cuser := CUser} | _], NVal, _, _) ->
    {HistTime, OVal, NVal, CUser};
prune_cvhist([#{nvalue := NVal}| _], Value, Time, User) ->
    {Time, NVal, Value, User}.

%% Data Access per key range

match_val(T) when is_tuple(T) -> {const,T};
match_val(V) -> V.

readGT(User, Channel, Item, CKey1, Limit)  when is_binary(Item), is_binary(CKey1) ->
	Cmd = [readGT, User, Channel, Item, CKey1, Limit],
	readGT(User, Cmd, channel_ctx(Channel), Item, io_key_to_term(CKey1), io_to_integer(Limit)).

readGT(_User, Cmd, SkvhCtx, Item, Key1, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>', '$1', match_val(Key1)}], ['$_']},
	read_limited(Cmd, SkvhCtx, Item, MatchFunction, Limit).

audit_readGT(User, Channel, TimeStamp, Limit) when is_binary(Channel) ->
    audit_readGT(User, atom_audit_alias(Channel), TimeStamp, Limit);
audit_readGT(User, AuditAliasAtom, TimeStamp, Limit)
  when is_binary(TimeStamp) ->
    audit_readGT(User, AuditAliasAtom,
                 imem_datatype:io_to_timestamp(TimeStamp), Limit);
audit_readGT(User, AuditAliasAtom, TimeStamp, Limit)
  when is_atom(AuditAliasAtom), is_tuple(TimeStamp) ->
    audit_readGT(User, audit_partitions(AuditAliasAtom, TimeStamp), TimeStamp, Limit);
audit_readGT(_User, AuditPartitions, TimeStamp, Limit)
  when is_list(AuditPartitions), is_tuple(TimeStamp) ->
    audit_part_readGT(
      AuditPartitions,
      {#skvhAudit{time = '$1', _ = '_'},
       [{'>', '$1', match_val(TimeStamp)}],
       ['$_']},
      Limit).

audit_readGT(User, Channel, Item, TS1, Limit)  when is_binary(Item), is_binary(TS1) ->
	Cmd = [audit_readGT, User, Channel, Item, TS1, Limit],
	audit_readGT(User, Cmd, channel_ctx(Channel), Item, imem_datatype:io_to_timestamp(TS1), io_to_integer(Limit)).

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
	readGE(User, Cmd, channel_ctx(Channel), Item, io_key_to_term(CKey1), io_to_integer(Limit)).

readGE(_User, Cmd, SkvhCtx, Item, Key1, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}], ['$_']},
	read_limited(Cmd, SkvhCtx, Item, MatchFunction, Limit).

read_limited(Cmd, SkvhCtx, Item, MatchFunction, Limit) ->
	{L,_} = imem_meta:select(SkvhCtx#skvhCtx.mainAlias, [MatchFunction], Limit),
	project_result(Cmd, L, Item).

readGELT(User, Channel, Item, CKey1, CKey2, Limit) when is_binary(Item), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [readGELT, User, Channel, Item, CKey1, CKey2, Limit],
	readGELT(User, Cmd, channel_ctx(Channel), Item, io_key_to_term(CKey1), io_key_to_term(CKey2), io_to_integer(Limit)).

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
	deleteGELT(User, Cmd, channel_ctx(Channel), io_key_to_term(CKey1), io_key_to_term(CKey2), io_to_integer(Limit)).

deleteGELT(User, Cmd, SkvhCtx, Key1, Key2, Limit) ->
	MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$1']},
	delete_with_limit(User, Cmd, SkvhCtx, MatchFunction, Limit).

deleteGTLT(User, Channel, CKey1, CKey2, Limit) when is_binary(Channel), is_binary(CKey1), is_binary(CKey2) ->
	Cmd = [deleteGTLT, User, Channel, CKey1, CKey2, Limit],
	deleteGTLT(User, Cmd, channel_ctx(Channel), io_key_to_term(CKey1), io_key_to_term(CKey2), io_to_integer(Limit)).

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

readGELTMap(_User, Channel, DecodedKey1, DecodedKey2, Limit) ->
    TableName = atom_table_name(Channel),
    Key1 = term_key_to_binterm(DecodedKey1),
    Key2 = term_key_to_binterm(DecodedKey2),
    MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$_']},
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

readGELTKeys(_User, Channel, DecodedKey1, DecodedKey2, Limit) ->
    TableName = atom_table_name(Channel),
    Key1 = term_key_to_binterm(DecodedKey1),
    Key2 = term_key_to_binterm(DecodedKey2),
    MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], ['$1']},
    {L,_} = imem_meta:select(TableName, [MatchFunction], Limit),
    [binterm_to_term_key(R) || R <- L].

readGELTHashes(_User, Channel, DecodedKey1, DecodedKey2, Limit) ->
    TableName = atom_table_name(Channel),
    Key1 = term_key_to_binterm(DecodedKey1),
    Key2 = term_key_to_binterm(DecodedKey2),
    MatchFunction = {?MATCHHEAD, [{'>=', '$1', match_val(Key1)}, {'<', '$1', match_val(Key2)}], [{{'$1','$3'}}]},
    {L,_} = imem_meta:select(TableName, [MatchFunction], Limit),
    [{binterm_to_term_key(Key), Hash} || {Key, Hash} <- L].

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

% age function
check_age_audit_entry(_User, Channel, Key, TS1) ->
    MatchFunction = {?AUDIT_MATCHHEAD, [{'andalso',{'>=', '$1', match_val(TS1)}, {'==', '$2', Key}}], ['$_']},
    AuditTable = list_to_atom(binary_to_list(audit_alias(Channel))),
    imem_meta:select(AuditTable, [MatchFunction]).
