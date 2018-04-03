-ifndef(IMEM_TRACER_HRL).
-define(IMEM_TRACER_HRL, true).
-include("imem.hrl").
-include("imem_if.hrl").
-include("imem_config.hrl").

-record(ddTrace, {
    trace_key               :: binary(),
    event_type  = register  :: atom(),
    enable      = false     :: boolean(),
    process                 :: term(),
    mod         = '_'       :: atom(),
    func        = '_'       :: atom(),
    args        = '_'       :: term(),
    match_spec  = "default" :: list(),
    rate        = 0         :: integer(),
    extra                   :: term(),
    overflow    = false     :: boolean()
}).
-define(ddTrace, [
    binstr,     % trace_key
    atom,       % event_type
    boolean,    % enable
    term,       % process
    atom,       % mod
    atom,       % func
    term,       % args
    list,       % match_spec
    integer,    % rate
    term,       % extra
    boolean     % overflow
]).

-endif.