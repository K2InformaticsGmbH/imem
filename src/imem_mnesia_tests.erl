-module(imem_mnesia_tests).

-include("imem_if.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ROWCOUNT, 1000).
-define(THREAD_A_DELAY, 200).
-define(THREAD_B_DELAY, 200).
-define(THREAD_A_CHUNK, 10).
-define(THREAD_B_CHUNK, 20).

-define(TEST_TIMEOUT, 1000000).

-define(TABLE, imem_table_123).

% EUnit tests --
setup() ->
    ?imem_test_setup(),
    {atomic, ok} = mnesia:create_table(?TABLE, [{attributes, [col1, col2, col3]}]).

teardown(_) ->
    catch imem_if:drop_table(?TABLE),
    application:stop(imem).

imem_mnesia_test_() ->
    {timeout, ?TEST_TIMEOUT, {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
                fun concurent_test/1
        ]}
        }
    }.

concurent_test(_) ->
    async_insert(),
    timer:sleep(?THREAD_A_DELAY),
    recv_async("_A_", ?THREAD_A_CHUNK, ?THREAD_A_DELAY),
    TotalDelay = round(?ROWCOUNT / ?THREAD_A_CHUNK * ?THREAD_A_DELAY + ?ROWCOUNT / ?THREAD_B_CHUNK * ?THREAD_B_DELAY + ?THREAD_A_DELAY),
%    TotalDelay = ?TEST_TIMEOUT div 400,
    io:format(user, "waiting... ~p~n", [TotalDelay]),
    timer:sleep(TotalDelay).

async_insert() ->
    F = fun
            (_, 0) -> ok;
            (F, R) ->
                mnesia:transaction(fun() ->
                    io:format(user, "insert ~p~n", [R]),
                    mnesia:write({?TABLE, R, R+1, R+2})
                end),
%                timer:sleep(?THREAD_B_DELAY div 200),
                F(F,R-1)
    end,
    spawn(fun() -> F(F, ?ROWCOUNT) end).

recv_async(Title, Limit, Delay) ->
    F0 = fun() ->
        Pid = start_trans(self(), Limit),
        F = fun(F) ->
            Pid ! next,
            receive
                eot ->
                    io:format(user, "[~p] finished~n", [Title]);
                {row, Row} ->
                    io:format(user, "[~p] got rows ~p~n", [Title, length(Row)]),
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
                io:format(user, "Abort~n", []);
            next ->
                case (case Contd0 of
                      undefined -> mnesia:select(?TABLE, [{'$1', [], ['$_']}], Limit, read);
                      Contd0 -> mnesia:select(Contd0)
                      end) of
                {Rows, Contd1} ->
                    Pid ! {row, Rows},
                    F(F,Contd1);
                '$end_of_table' -> Pid ! eot
                end
        end
    end,
    spawn(mnesia, transaction, [F, [F,undefined]]).

