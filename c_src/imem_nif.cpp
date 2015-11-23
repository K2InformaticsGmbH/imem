#include <string.h>
#include <stdio.h>

#include "erl_nif.h"

static ERL_NIF_TERM now(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    argc = argc; // for unused variable warning

    printf("Print from NIF\n");
    return enif_make_atom(env, "ok");
}

int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

static ErlNifFunc nif_funcs[] = {
    {"now", 0, now}
};

ERL_NIF_INIT(imem_nif, nif_funcs, NULL, NULL, upgrade, NULL)
