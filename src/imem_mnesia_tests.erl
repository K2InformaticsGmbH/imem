-module(imem_mnesia_tests).

-ifdef(TEST).

%% Application callbacks
-compile(export_all).


-include("imem.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ROWCOUNT, 50).
-define(THREAD_A_DELAY, 200).
-define(THREAD_B_DELAY, 200).
-define(THREAD_A_CHUNK, 5).
-define(THREAD_B_CHUNK, 10).
-define(TEST_TIMEOUT, 1000000).

% EUnit tests --
setup() ->
    case erlang:whereis(imem_sup) of
        undefined -> mnesia:start();
        _ -> ok
    end,
    {atomic, ok} = mnesia:create_table(table, [{attributes, [col1, col2, col3]}]),
    ok.

teardown(_) ->
    mnesia:delete_table(table),
    case erlang:whereis(imem_sup) of
        undefined -> mnesia:stop();
        _ -> imem:stop()
    end.


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
    %?LogDebug("---TEST---~n", []),
    run_test_core().

run_test_core() ->
    async_insert(),
    recv_async("_A_", ?THREAD_A_CHUNK, ?THREAD_A_DELAY),
    recv_async("_B_", ?THREAD_B_CHUNK, ?THREAD_B_DELAY),
    TotalDelay = round(?ROWCOUNT / ?THREAD_A_CHUNK * ?THREAD_A_DELAY + ?ROWCOUNT / ?THREAD_B_CHUNK * ?THREAD_B_DELAY),
    % ?LogDebug("waiting... ~p~n", [TotalDelay]),
    timer:sleep(TotalDelay).

async_insert() ->
    F = fun
            (_, 0) -> ok;
            (F, R) ->
                mnesia:transaction(fun() ->
                    % ?LogDebug("insert ~p~n", [R]),
                    mnesia:write({table, R, R+1, R+2})
                end),
                timer:sleep(?THREAD_B_DELAY div 20),
                F(F,R-1)
    end,
    spawn(fun() -> F(F, ?ROWCOUNT) end).

recv_async(Title, Limit, Delay) ->
    F0 = fun() ->
        Pid = start_trans(self(), Title, Limit),
        F = fun(F) ->
            timer:sleep(Delay),
            Pid ! next,
            receive
                eot ->
                    % ?LogDebug("[~p] finished~n", [Title]),
                    ok;
                {row, _Row} ->
                    % ?LogDebug("[~p] got rows ~p~n", [Title, length(_Row)]),
                    F(F)
            end
        end,
        F(F)
    end,
    spawn(F0).

start_trans(Pid, _Title, Limit) ->
    F =
    fun(F,Contd0) ->
        receive
            abort ->
                % ?LogDebug("[~p] {T} Abort~n", [_Title]),
                ok;
            next ->
                case (case Contd0 of
                      undefined -> mnesia:select(table, [{'$1', [], ['$_']}], Limit, read);
                      Contd0 -> mnesia:select(Contd0)
                      end) of
                {Rows, Contd1} ->
                    % ?LogDebug("[~p] {T} -> ~p~n", [_Title, length(Rows)]),
                    Pid ! {row, Rows},
                    F(F,Contd1);
                '$end_of_table' -> Pid ! eot
                end
        end
    end,
    spawn(mnesia, async_dirty, [F, [F,undefined]]).

-endif. 
