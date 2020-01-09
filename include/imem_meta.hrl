-ifndef(IMEM_META_HRL).

-define(IMEM_META_HRL, true).

-compile({parse_transform, imem_rec_pretty_pt}).

-include("imem_exception.hrl").
-include("imem_if.hrl").
-include("imem_if_csv.hrl").
-include("imem_config.hrl").

-define(LOG_TABLE, ddLog_86400@).

%% 86400 = 1 Day

-define(MONITOR_TABLE, ddMonitor_86400@).

%% 86400 = 1 Day

-define(CACHE_TABLE, ddCache@).
-define(META_ROWNUM, <<"rownum">>).
-define(META_NODE, <<"node">>).

-record(
    %% column definition
    ddColumn,
    {
        name :: atom(),
        type = term :: ddType(),
        len = undefined :: integer(),
        prec = undefined :: integer(),
        default = undefined :: any(),
        opts = [] :: list()
    }
).

-type ddEntityId() :: integer() | atom().
-type ddType() :: atom() | tuple() | list().

% term | list | tuple | integer | float | binary | string | ref | pid | ipaddr
-type ddSchema() :: atom() | binary().
-type ddSimpleTable() :: ddMnesiaTable() | ddBinStr() | ddString().

% does not include a schema name
-type ddQualifiedTable() :: {ddSchema(), ddSimpleTable()}.

% does include a schema name
-type ddTable() :: ddSimpleTable() | ddQualifiedTable().

% may or may not include a schema name
-type ddIndex() :: binary() | integer() | ddMnesiaIndex().

% binstr name or integer id or mnesia column (atom)
-type ddColumnType() :: atom().
-type ddColumnDefault() :: any().
-type ddTypeList() :: [ddColumnType()].
-type ddDefaultList() :: [ddColumnDefault()].
-type ddTableMeta() :: [#ddColumn{}] | {ddColumnList(), ddTypeList(), ddDefaultList()}.

-record(
    %% record definition for index definition
    ddIdxDef,
    {
        %% index id within the table
        id :: integer(),
        %% name of the index
        name :: binary(),
        %% Type of index
        type :: ivk | iv_k | iv_kl | iv_h | ivvk | ivvvk,
        %% list of JSON path expressions as binstr/compiled to term
        pl = [] :: list(),
        %% value_normalising_fun(Value)
        %% applied to each value result of all path scans for given JSON document
        %% return ?nav = '$not_a_value' if indexing is not wanted, otherwise let iff() decide
        vnf = <<"fun imem_index:vnf_lcase_ascii_ne/1">> :: binary(),
        iff = <<"fun imem_index:iff_true/1">> :: binary()
        %% boolean index_filter_fun({Key,Value}) for the inclusion/exclusion of indexes
        %% applied to each result of all path scans for given JSON document
    }
).
-record(
    %% record definition plan, combining all index definitions for a table
    ddIdxPlan,
    {
        %% index definitions
        def = [] :: list(#ddIdxDef{}),
        %% record positions needing JSON decoding
        jpos = [] :: list(integer())
    }
).
-record(
    %% record definition for index tables (one per indexed master data table)
    ddIndex,
    {
        %% search tuple, cannot be empty
        stu :: tuple(),
        %% Link to key of master data
        lnk = 0 :: term()
        %% 0=unused when key is part of search tuple, used in ivk
        %% 0..n  hashed value for the hashmap index (iv_h)
        %% single key of any data type for unique index (iv_k)
        %% list of keys for almost unique index (iv_kl)
    }
).

-define(ddIndex, [tuple, term]).

-record(
    %% local kv cache, created empty
    ddCache,
    {
        %% key
        ckey :: term(),
        %% value
        cvalue :: term(),
        %% options
        opts = [] :: list()
    }
).

-define(ddCache, [term, term, list]).

-record(
    %% table alias for partitioned tables
    ddAlias,
    {
        %% {Schema,TableAlias}
        qname :: {atom(), atom()},
        columns :: list(#ddColumn{}),
        opts = [] :: list(),
        %% AccountId of creator / owner
        owner = system :: ddEntityId(),
        readonly = false :: true | false
    }
).

-define(ddAlias, [tuple, list, list, userid, boolean]).

-record(
    %% table
    ddTable,
    {
        %% {Schema,Table}
        qname :: {atom(), atom()},
        columns :: list(#ddColumn{}),
        opts = [] :: list(),
        %% AccountId of creator / owner
        owner = system :: ddEntityId(),
        readonly = false :: true | false
    }
).

-define(ddTable, [tuple, list, list, userid, boolean]).

-record(
    %% log table
    ddLog,
    {
        % ?TIME_UID
        logTime :: ddTimeUID(),
        logLevel :: atom(),
        pid :: pid(),
        module :: atom(),
        function :: atom(),
        line = 0 :: integer(),
        node :: atom(),
        fields = [] :: list(),
        message = <<"">> :: binary(),
        stacktrace = [] :: list()
    }
).

-define(ddLog, [timestamp, atom, pid, atom, atom, integer, atom, list, binstr, list]).

-record(
    %% node
    ddNode,
    {
        %% erlang node name
        name :: atom(),
        %% erlang:statistics(wall_clock)
        wall_clock :: integer(),
        %% ?TIMESTAMP
        time :: ddTimestamp(),
        extra :: list()
    }
).

-define(ddNode, [atom, integer, timestamp, list]).

-record(
    %% snapshot
    ddSnap,
    {
        %% snapshot file name
        file :: binary(),
        %% snapshot file type
        type :: bkp | zip,
        %% snapshot file size in bytes
        size :: integer(),
        %% ?TIMESTAMP
        lastModified :: ddTimestamp()
    }
).

-define(ddSnap, [binstr, atom, integer, datetime]).

-record(
    %% schema node
    ddSchema,
    {
        %% {schema,node}
        schemaNode :: tuple(),
        extra :: list()
    }
).

-define(ddSchema, [tuple, list]).

-record(
    %% monitor
    ddMonitor,
    {
        %% ?TIME_UID
        time :: ddTimeUID(),
        %% erlang node name
        node :: atom(),
        %% erlang:memory(total)
        memory = 0 :: integer(),
        %% erlang:system_info(process_count)
        process_count = 0 :: integer(),
        %% erlang:system_info(port_count)
        port_count = 0 :: integer(),
        %% erlang:statistics(run_queue)
        run_queue = 0 :: integer(),
        %% erlang:statistics(wall_clock)
        wall_clock = 0 :: integer(),
        %% erlang:statistics(reductions)
        reductions = 0 :: integer(),
        %% erlang:statistics(Item :: io)
        input_io = 0 :: integer(),
        %% erlang:statistics(Item :: io)
        output_io = 0 :: integer(),
        %% application dependent proplist
        extra = [] :: list()
    }
).

-define(
    ddMonitor,
    [timestamp, atom, integer, integer, integer, integer, integer, integer, integer, integer, list]
).
-define(nav, '$not_a_value').

%% used as default value which must not be used (not null columns)

-define(navio, <<"'$not_a_value'">>).

%% used as default value which must not be used (not null columns)

-define(nac, '$not_a_column').

%% used as value column name for key only tables

-record(
    %% table
    dual,
    {
        % fixed string "X"
        dummy = "X" :: list(),
        % not a column
        nac = ?nav :: atom()
    }
).

-define(dual, [string, atom]).

-record(
    %% table size
    ddSize,
    {
        name :: atom(),
        size :: integer(),
        memory :: integer(),
        %% expiry time (first ts of next partition)
        expiry :: ddTimestamp(),
        %% time until expiry (sec)
        tte :: integer()
    }
).

-define(ddSize, [atom, integer, integer, timestamp, integer]).

-record(
    %% term diff
    ddTermDiff,
    {
        %% record id
        id :: number(),
        %% left item (text line)
        left = ?nav :: binary(),
        %% comparison token
        cmp = <<>> :: binary(),
        %% right item (text line)
        right = ?nav :: binary()
    }
).

-define(ddTermDiff, [number, binstr, binstr, binstr]).

-record(
    %% code version
    ddVersion,
    {
        %% application
        app :: binary(),
        %% application version
        appVsn :: binary(),
        %% module / file name
        file :: binary(),
        %% module / file version
        fileVsn :: binary(),
        %% module / file binary path
        filePath = <<>> :: binary(),
        %% module / file origin path (e.g. git commit url)
        fileOrigin = <<>> :: binary()
    }
).

-define(ddVersion, [binstr, binstr, binstr, binstr, binstr, binstr]).
-define(OneWeek, 7.0).

%% span of  datetime or timestamp (fraction of 1 day)

-define(OneDay, 1.0).

%% span of  datetime or timestamp (fraction of 1 day)

-define(OneHour, 0.041666666666666664).

%% span of  datetime or timestamp (1.0/24.0 of 1 day)

-define(OneMinute, 6.944444444444444e-4).

%% span of  datetime or timestamp (1.0/24.0 of 1 day)

-define(OneSecond, 1.1574074074074073e-5).

%% span of  datetime or timestamp (fraction of 1 day)

-define(
    DataTypes,
    [
        'fun',
        atom,
        binary,
        binstr,
        boolean,
        datetime,
        decimal,
        float,
        integer,
        number,
        ipaddr,
        list,
        map,
        pid,
        ref,
        string,
        term,
        binterm,
        timestamp,
        tuple,
        userid,
        json
    ]
).
-define(NumberTypes, [decimal, float, integer, number]).
-define(VirtualTables, [ddSize | ?DataTypes]).
-endif.
