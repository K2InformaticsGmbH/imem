-module(imem_mnesia_tests).

%% Application callbacks
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(ROWCOUNT, 100).
-define(THREAD_A_DELAY, 2000).
-define(THREAD_B_DELAY, 2000).
-define(THREAD_A_CHUNK, 5).
-define(THREAD_B_CHUNK, 10).
-define(TEST_TIMEOUT, 1000000).

% EUnit tests --
setup() ->
    mnesia:start(),
    {atomic, ok} = mnesia:create_table(table, [{attributes, [col1, col2, col3]}]),
    ok.

teardown(_) ->
    mnesia:delete_table(table),
    mnesia:stop().

imem_mnesia_test_() ->
    {timeout, ?TEST_TIMEOUT, {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
                fun run_test_eunit/1
        ]}
        }
    }.

run_test_eunit(_) ->
    io:format(user, "EUNIT ---~n", []),
    run_test_core().

run_test_core() ->
    async_insert(),
    timer:sleep(?THREAD_A_DELAY div 2),
    recv_async("_A_", ?THREAD_A_CHUNK, ?THREAD_A_DELAY),
    recv_async("_B_", ?THREAD_B_CHUNK, ?THREAD_B_DELAY),
    TotalDelay = round(?ROWCOUNT / ?THREAD_A_CHUNK * ?THREAD_A_DELAY + ?ROWCOUNT / ?THREAD_B_CHUNK * ?THREAD_B_DELAY),
    io:format("waiting... ~p~n", [TotalDelay]),
    timer:sleep(TotalDelay).

run_console() ->
    io:format(user, "CONSOLE ---~n", []),
    mnesia:start(),
    {atomic, ok} = mnesia:create_table(table, [{attributes, [col1, col2, col3]}]),
    run_test_core(),
    mnesia:delete_table(table),
    mnesia:stop().

async_insert() ->
    F = fun
            (_, 0) -> ok;
            (F, R) ->
                timer:sleep(?THREAD_B_DELAY div 20),
                mnesia:transaction(fun() ->
                    io:format(user, "insert ~p~n", [R]),
                    mnesia:write({table, R, R+1, R+2})
                end),
                F(F,R-1)
    end,
    spawn(fun() -> F(F, ?ROWCOUNT div 3) end).

recv_async(Title, Limit, Delay) ->
    F0 = fun() ->
        Pid = start_trans(self(), Limit),
        F = fun(F) ->
            Pid ! next,
            receive
                eot ->
                    io:format("[~p] finished~n", [Title]);
                {row, Row} ->
                    io:format("[~p] got rows ~p~n", [Title, length(Row)]),
                    timer:sleep(Delay),
                    F(F)
            end
        end,
        F(F)
    end,
    spawn(F0).

start_trans(Pid, Limit) ->
    F =
    fun(F,Contd0) ->
        receive
            abort ->
                io:format("Abort~n", []);
            next ->
                case (case Contd0 of
                      undefined -> mnesia:select(table, [{'$1', [], ['$_']}], Limit, read);
                      Contd0 -> mnesia:select(Contd0)
                      end) of
                {Rows, Contd1} ->
                    Pid ! {row, Rows},
                    F(F,Contd1);
                '$end_of_table' -> Pid ! eot
                end
        end
    end,
    spawn(mnesia, async_dirty, [F, [F,undefined]]).
