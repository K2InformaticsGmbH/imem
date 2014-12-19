%% -*- coding: utf-8 -*-
-define(JSON_PATH_SEPARATOR,":").

% Use binary parsing, when implemented, to handle raw binary data
-define(JSON_POWER_MODE,false).

-define(decode_maps(__J), decode(__J, [return_maps])).

-type data_object() :: list() | map() | binary().
-type diff_object() :: list() | map() | binary().

-type value() :: term().
-type key() :: binary().
