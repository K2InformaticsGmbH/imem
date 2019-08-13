-ifndef(IMEM_TRACER_HRL).
-define(IMEM_TRACER_HRL, true).
-include("imem_if.hrl").
-include("imem_config.hrl").

-record(ddTrace, {
    trace_key               :: integer(),
    event_type  = register  :: atom(),
    enable      = false     :: boolean(),
    process                 :: pid(),
    trace_node              :: atom(),
    mod         = '_'       :: atom(),
    func        = '_'       :: atom(),
    args        = '_'       :: term(),
    match_spec  = "default" :: list(),
    rate        = 0         :: integer(),
    extra                   :: term(),
    overflow    = false     :: boolean()
}).
-define(ddTrace, [
    integer,    % trace_key
    atom,       % event_type
    boolean,    % enable
    pid,        % process
    atom,       % trace_node
    atom,       % mod
    atom,       % func
    term,       % args
    list,       % match_spec
    integer,    % rate
    term,       % extra
    boolean     % overflow
]).

-endif.