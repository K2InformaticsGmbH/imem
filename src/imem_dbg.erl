-module(imem_dbg).
-include("imem.hrl").

-export([start/0]).

start() ->
    dbg:start(),
    dbg:tracer(process, {fun tracer/2, #{count => 0}}),
    Match_spec = [{'_', [], [{message,{process_dump}}, {exception_trace}]}],
    Trace_spec = {imem_meta, data_nodes, 0},
    dbg:tpl(Trace_spec, Match_spec),
    dbg:p(all, [c]).

tracer(Trace, #{count := Count}) ->
    ?Info("Trace ~p, Count ~p", [Trace, Count]),
    #{count => Count + 1}.