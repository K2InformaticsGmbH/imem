#include <string.h>
#include <stdio.h>

#include "erl_nif.h"

#include "Windows.h"

#define ERTS_MONOTONIC_TIME_KILO \
    ((unsigned long long) 1000)
#define ERTS_MONOTONIC_TIME_MEGA \
    (ERTS_MONOTONIC_TIME_KILO*ERTS_MONOTONIC_TIME_KILO)
#define ERTS_MONOTONIC_TIME_GIGA \
    (ERTS_MONOTONIC_TIME_MEGA*ERTS_MONOTONIC_TIME_KILO)
#define ERTS_MONOTONIC_TIME_TERA \
    (ERTS_MONOTONIC_TIME_GIGA*ERTS_MONOTONIC_TIME_KILO)

const __int64 DELTA_EPOCH_IN_MICROSECS = 11644473600000000ULL;

// 11644473600 000000

static ERL_NIF_TERM now(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    argc = argc; // for unused variable warning

    FILETIME ft;
    GetSystemTimePreciseAsFileTime(&ft);

    unsigned long long now = ft.dwHighDateTime;
	now <<= 32;
	now |= ft.dwLowDateTime;
    now /= 1000; // microseconds
	now -= DELTA_EPOCH_IN_MICROSECS;

    /*
     * Implementation taken from 'get_now'
     * https://github.com/erlang/otp/blob/master/erts/emulator/beam/erl_time_sup.c#L1717-L1745
     */
    unsigned long long now_megasec = now / ERTS_MONOTONIC_TIME_TERA;
    unsigned long long now_sec = now / ERTS_MONOTONIC_TIME_MEGA;

    unsigned long long megasec = now_megasec;
    unsigned long long sec = now_sec - now_megasec * ERTS_MONOTONIC_TIME_MEGA;
    unsigned long long microsec = now - now_sec * ERTS_MONOTONIC_TIME_MEGA;


    return enif_make_tuple3(
            env,
            enif_make_uint64(env, megasec),
            enif_make_uint64(env, sec),
            enif_make_uint64(env, microsec));
}

int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

static ErlNifFunc nif_funcs[] = {
    {"now", 0, now}
};

ERL_NIF_INIT(imem_nif, nif_funcs, NULL, NULL, upgrade, NULL)
