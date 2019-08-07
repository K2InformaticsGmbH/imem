-module(imem_win32).
-on_load(init/0).

-export([getLocalTime/0, getSystemTimePreciseAsFileTime/0,
         queryPerformanceCounter/0, queryPerformanceFrequency/0]).

init() ->
    case os:type() of
        {win32, nt} ->
            erlang:load_nif(imem:priv_dir() ++ "/imem_win32", 0);
        _ -> ok
    end.

getLocalTime() ->
    exit(win32_nif_library_not_loaded).

getSystemTimePreciseAsFileTime() ->
    exit(win32_nif_library_not_loaded).

queryPerformanceCounter() ->
    exit(win32_nif_library_not_loaded).

queryPerformanceFrequency() ->
    exit(win32_nif_library_not_loaded).

-ifdef(WIN32).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    {inparallel, [
        {"getLocalTime",
            ?_assertMatch(
                #{
                    day := _, dayOfWeek := _, hour := _, milliseconds := _,
                    minute := _,month := _, second := _, year := _
                },
                getLocalTime()
            )},
        {"getSystemTimePreciseAsFileTime",
            ?_assertEqual(true, is_integer(getSystemTimePreciseAsFileTime()))},
        {"queryPerformanceCounter",
            ?_assertEqual(true, is_integer(queryPerformanceCounter()))},
        {"queryPerformanceFrequency",
            ?_assertEqual(true, is_integer(queryPerformanceFrequency()))},
        {"hires_timer", fun hires_timer/0},
        {"missing_ms", {timeout, 60, [fun missing_time/0]}}
    ]}.

hires_timer() ->
    OsTimes = (
        fun OST(0, Ts) -> Ts;
                OST(Count, Ts) ->
                OST(
                    Count - 1,
                    [imem_datatype:timestamp_to_io(os:timestamp()) | Ts]
                )
        end
    )(100, []),
    ?assertEqual(100, length(OsTimes)),
    % demonstrating that duplicate timestamps are generated in tight loops
    ?assertNotEqual(100, length(lists:usort(OsTimes))),

    FrqCountsPerSec = queryPerformanceFrequency(),
    OsTimesV2 = (
        fun OST1(0, Ts) -> Ts;
            OST1(Count, Ts) ->
                MiliSec = queryPerformanceCounter() * 1000 / FrqCountsPerSec,
                {match, [MsStr]} = re:run(
                    float_to_list(MiliSec, [{decimals, 3}]),
                    "^[0-9]+\\.([0-9]{3,3})$",
                    [{capture, [1], list}]
                ),
                TS = imem_datatype:timestamp_to_io(os:timestamp()),
                TS1 = re:replace(
                    TS, "(.*)000$", "\\g{1}"++MsStr, [{return, list}]
                ),
                OST1(
                    Count - 1,
                    [TS1 | Ts]
                )
        end
    )(100, []),
    ?assertEqual(100, length(OsTimesV2)),
    ?assertEqual(100, length(lists:usort(OsTimesV2))).

missing_time() ->
    missing_time(undefined, os:timestamp()).
missing_time(OldTime, NewTime) when OldTime == NewTime; OldTime == undefined ->
    missing_time(NewTime, os:timestamp());
missing_time(OldTime, NewTime) when OldTime /= NewTime ->
    Start = queryPerformanceCounter(),
    timer:sleep(10000),
    End = queryPerformanceCounter(),
    DiffMs = (End - Start) * 1000 div queryPerformanceFrequency(),
    SleepError = abs(10000 - DiffMs),
    % timer:sleep/1 is +/-20ms (approx) in windows
    ?assert(20 >= SleepError).

-endif. % TEST
-endif. % WIN32
