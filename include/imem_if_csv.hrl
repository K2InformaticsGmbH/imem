-ifndef(IMEM_IF_CSV_HRL).
-define(IMEM_IF_CSV_HRL, true).

-define(CSV_RECORD_NAME,csv_rec).
-define(CSV_SCHEMA, <<"csv">>).
-define(CSV_SCHEMA_DEFAULT, <<"csv$">>).
-define(CSV_SCHEMA_PATTERN,<<"csv$",_CsvRest/binary>>).
-define(CSV_DEFAULT_COLS, [<<"file">>,<<"offset">>,<<"bytes">>]).
-define(CSV_DEFAULT_INFO, [ {<<"file">>,binstr,<<>>}
						  , {<<"offset">>,integer,0}
						  , {<<"bytes">>,integer,0}
						  ]).
-endif.
