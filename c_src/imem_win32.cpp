#include <string.h>
#include <stdio.h>

#include "erl_nif.h"

#include "Windows.h"

static ERL_NIF_TERM getLocalTime(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    argc = argc; // for unused variable warning

    SYSTEMTIME st;
    GetLocalTime(&st);

    ERL_NIF_TERM map = enif_make_new_map(env);
    if (!enif_make_map_put(
            env, map,
            enif_make_atom(env, "year"),
            enif_make_uint(env, (unsigned int)st.wYear),
            &map))
        return enif_make_badarg(env);
    if (!enif_make_map_put(
            env, map,
            enif_make_atom(env, "month"),
            enif_make_uint(env, (unsigned int)st.wMonth),
            &map))
        return enif_make_badarg(env);
    if (!enif_make_map_put(
            env, map,
            enif_make_atom(env, "dayOfWeek"),
            enif_make_uint(env, (unsigned int)st.wDayOfWeek),
            &map))
        return enif_make_badarg(env);
    if (!enif_make_map_put(
            env, map,
            enif_make_atom(env, "day"),
            enif_make_uint(env, (unsigned int)st.wDay),
            &map))
        return enif_make_badarg(env);
    if (!enif_make_map_put(
            env, map,
            enif_make_atom(env, "hour"),
            enif_make_uint(env, (unsigned int)st.wHour),
            &map))
        return enif_make_badarg(env);
    if (!enif_make_map_put(
            env, map,
            enif_make_atom(env, "minute"),
            enif_make_uint(env, (unsigned int)st.wMinute),
            &map))
        return enif_make_badarg(env);
    if (!enif_make_map_put(
            env, map,
            enif_make_atom(env, "second"),
            enif_make_uint(env, (unsigned int)st.wSecond),
            &map))
        return enif_make_badarg(env);
    if (!enif_make_map_put(
            env, map,
            enif_make_atom(env, "milliseconds"),
            enif_make_uint(env, (unsigned int)st.wMilliseconds),
            &map))
        return enif_make_badarg(env);

    return map;
}

static ERL_NIF_TERM queryPerformanceCounter(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    argc = argc; // for unused variable warning

    LARGE_INTEGER lpPerformanceCount;
    QueryPerformanceCounter(&lpPerformanceCount);

    return enif_make_uint64(
        env, (ErlNifUInt64)lpPerformanceCount.QuadPart);
}

static ERL_NIF_TERM queryPerformanceFrequency(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    argc = argc; // for unused variable warning

    LARGE_INTEGER lpFrequency;
    QueryPerformanceFrequency(&lpFrequency);

    return enif_make_uint64(
        env, (ErlNifUInt64)lpFrequency.QuadPart);
}

#if defined(_WIN32_WINNT_WIN8) && WINVER > _WIN32_WINNT_WIN8
typedef void(__cdecl *WINAPIPTR)(_Out_ LPFILETIME);
static ERL_NIF_TERM getSystemTimePreciseAsFileTime(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    argc = argc; // for unused variable warning

    WINAPIPTR getSystemTimePreciseAsFileTime;
    if (NULL == (getSystemTimePreciseAsFileTime =
                     (WINAPIPTR)GetProcAddress(
                         GetModuleHandle(TEXT("kernel32.dll")),
                         "GetSystemTimePreciseAsFileTime")))
    {
        return enif_raise_exception(
            env,
            enif_make_string(
                env,
                "GetSystemTimePreciseAsFileTime unavaiable",
                ERL_NIF_LATIN1));
    }
    else
    {
        FILETIME ft;
        (getSystemTimePreciseAsFileTime)(&ft);

        ErlNifUInt64 SystemTimeAsFileTime = ft.dwHighDateTime;
        SystemTimeAsFileTime <<= 32;
        SystemTimeAsFileTime |= ft.dwLowDateTime;

        return enif_make_uint64(env, SystemTimeAsFileTime);
    }
}
#endif

int upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data,
            ERL_NIF_TERM load_info)
{
    return 0;
}

static ErlNifFunc nif_funcs[] = {
    {"getLocalTime", 0, getLocalTime},
#if defined(_WIN32_WINNT_WIN8) && WINVER > _WIN32_WINNT_WIN8
    {"getSystemTimePreciseAsFileTime", 0, getSystemTimePreciseAsFileTime},
#endif
    {"queryPerformanceCounter", 0, queryPerformanceCounter},
    {"queryPerformanceFrequency", 0, queryPerformanceFrequency}};

ERL_NIF_INIT(imem_win32, nif_funcs, NULL, NULL, upgrade, NULL)
