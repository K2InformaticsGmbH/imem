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
    Counters = lists:seq(1, 100),
    Count = length(Counters),
    OsTimes = [imem_datatype:timestamp_to_io(os:timestamp()) || _ <- Counters],
    ?assertEqual(Count, length(OsTimes)),
    % demonstrating that duplicate timestamps are generated in tight loops
    ?assertNotEqual(Count, length(lists:usort(OsTimes))),

    OsTimesV2 = [
        begin
            timer:sleep(1),
            queryPerformanceCounter()
        end
    || _ <- Counters],
    ?assertEqual(Count, length(OsTimesV2)),
    ?assertEqual(Count, length(lists:usort(OsTimesV2))).

missing_time() ->
    missing_time(undefined, os:timestamp()).
missing_time(OldTime, NewTime) when OldTime == NewTime; OldTime == undefined ->
    missing_time(NewTime, os:timestamp());
missing_time(OldTime, NewTime) when OldTime /= NewTime ->
    Start = queryPerformanceCounter(),
    StartErl = erlang:system_time(millisecond),
    timer:sleep(10000),
    EndErl = erlang:system_time(millisecond),
    End = queryPerformanceCounter(),
    DiffMs = (End - Start) * 1000 div queryPerformanceFrequency(),
    DiffErl = EndErl - StartErl,
    Q = DiffErl/DiffMs,
    ?assert(Q > 0.997 andalso Q < 1.003).

-endif. % TEST
-endif. % WIN32
