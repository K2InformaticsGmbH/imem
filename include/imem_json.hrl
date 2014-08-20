%% -*- coding: utf-8 -*-

% Decode raw json to a map (when supported)
-define(DECODE_TO_MAPS,true).

%% Basic Configuration
%% ===================================================================

-define(DEFAULT_TYPE, proplist). % proplist | map | json

-define(JSON_PATH_SEPARATOR,":").

-define(JSON_POWER_MODE,false). % Use binary parsing, when implemented, to handle raw binary data

-define(NO_JSON_LIB,throw(no_json_library_found)).

-define(NOT_SUPPORTED,throw(not_yet_supported)).

-define(BAD_VERSION,throw(json_imem_disabled)).


%% Technical macros
%% ===================================================================
-define(TO_JSON, encode).
-define(FROM_JSON, decode).
-define(CL,clean_null_values).

%% Type definitions
%% ===================================================================
-ifdef(MAPS).
-type data_object() :: list() | map() | binary().
-type diff_object() :: list() | map() | binary().
-else.
-type data_object() :: list() | binary().
-type diff_object() :: list() | binary().
-endif.

-type value() :: term().
-type key() :: binary().
