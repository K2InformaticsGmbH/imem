-module(imem_cache).

-include("imem_meta.hrl").

-export([ read/1
        , write/2
        , clear/1
        , clear_local/1
        ]).


read(Key) -> 
    case imem_meta:read(?CACHE_TABLE,Key) of
        [] ->   [];
        [#ddCache{cvalue=Value}] -> [Value]
    end.

write(Key,Value) -> 
    imem_meta:write(?CACHE_TABLE,#ddCache{ckey=Key,cvalue=Value}).

clear_local(Key) ->
    try
        imem_meta:delete(?CACHE_TABLE,Key)
    catch
        throw:{'ClientError',_} -> ok
    end.

-spec clear(any()) -> ok | {error, [{node(),any()}]}.
clear(Key) ->
    Res = [clear_local(Key) | [rpc:call(N, imem_cache, clear_local, [Key]) || {_,N} <- imem_meta:data_nodes(), N/=node()]],
    case lists:usort(Res) of
        [ok] -> ok;
        _ ->    Res
    end.
