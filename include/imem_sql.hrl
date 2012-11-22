
-include("imem_seco.hrl").

-record(statement, {
        table = undefined                   ::atom()
        , block_size = 0                    ::integer()
        , key = '$start_of_table'           ::any()
        , stmt_str = ""                     ::string()
        , stmt_parse = undefined            ::any()
        , cols = []                         ::list()
    }).
