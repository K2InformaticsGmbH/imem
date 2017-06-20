-ifndef(IMEM_REC_TEST).
-define(IMEM_REC_TEST, true).

-record(rcrd2, {a,b}).

-define(PP(X), rcrd2_pretty(X)).

-endif.
