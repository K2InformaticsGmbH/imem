-ifndef(IMEM_JSON_HRL).

-define(IMEM_JSON_HRL, true).
-define(decode_maps(__J), decode(__J, [return_maps])).

-type data_object() :: jsx:json_term().

%% TODO dead code review and cleanup
%% - -define(JSON_PATH_SEPARATOR,":").
%% - -type diff_object() :: list() | map() | binary().

-type value() :: term().
-type key() :: binary().

-endif.
