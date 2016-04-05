-module(select_SUITE).

-include_lib("common_test/include/ct.hrl").

-include("../../include/imem_sql.hrl").

-export([all/0]).
-export([end_per_suite/1,init_per_suite/1]).
-export([db1/0]).

all() ->
    [db1].

init_per_suite(_Config) ->
    ?imem_test_setup.

end_per_suite(_Config) ->
    ?imem_test_teardown.

db1() ->
    ok.