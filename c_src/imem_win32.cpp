#include <string.h>
#include <stdio.h>

#include "erl_nif.h"

#include "Windows.h"

static ERL_NIF_TERM queryPerformanceCounter(
    ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]
)
{
    argc = argc; // for unused variable warning

    LARGE_INTEGER lpPerformanceCount;
    QueryPerformanceCounter(&lpPerformanceCount);

    return enif_make_uint64(
        env, (ErlNifUInt64)lpPerformanceCount.QuadPart
    );
}

static ERL_NIF_TERM getSystemTimePreciseAsFileTime(
    ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]
)
{
    argc = argc; // for unused variable warning

    FILETIME ft;
    GetSystemTimePreciseAsFileTime(&ft);

    ErlNifUInt64 SystemTimeAsFileTime = ft.dwHighDateTime;
	SystemTimeAsFileTime <<= 32;
	SystemTimeAsFileTime |= ft.dwLowDateTime;

    return enif_make_uint64(env, SystemTimeAsFileTime);
}

int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data,
            ERL_NIF_TERM load_info)
{
    return 0;
}

static ErlNifFunc nif_funcs[] = {
    {"queryPerformanceCounter",        0, queryPerformanceCounter},
    {"getSystemTimePreciseAsFileTime", 0, getSystemTimePreciseAsFileTime}
};

ERL_NIF_INIT(imem_win32, nif_funcs, NULL, NULL, upgrade, NULL)
