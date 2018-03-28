-ifndef(IMEM_TRACER_HRL).
-define(IMEM_TRACER_HRL, true).

-record(ddTrace,
    { caller    :: pid()
    , type      :: atom()
    , mod       :: atom()
    , func      :: atom()
    , args      :: term()
    , extra     :: term()
    }).
-define(ddTrace, [pid,atom,atom,atom,term,term]).

-endif.