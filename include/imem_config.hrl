-ifndef(IMEM_CONFIG_HRL).

-define(IMEM_CONFIG_HRL, true).
-define(CONFIG_TABLE, ddConfig).

-record(
    ddConfig,
    {
        % hierarchical key list [item,context1,context2,...]
        hkl :: list(),
        val = <<"fun(_,__Rec,__Table) -> imem_config:val(__Table,__Rec) end.">> :: any(),
        % the module who owns this config
        owner :: atom(),
        % create comments
        remark = <<"">> :: binary()
    }
).

-define(ddConfig, [list, term, atom, binstr]).
-define(
    GET_CONFIG(__PName, __Context, __Default, __Documentation),
    imem_config:get_config_hlk(
        ?CONFIG_TABLE,
        {element(2, application:get_application(?MODULE)), ?MODULE, __PName},
        ?MODULE,
        __Context ++ [node()],
        __Default,
        __Documentation
    )
).
-define(
    PUT_CONFIG(__PName, __Context, __Default, __Remark),
    imem_config:put_config_hlk(
        ?CONFIG_TABLE,
        {element(2, application:get_application(?MODULE)), ?MODULE, __PName},
        ?MODULE,
        __Context,
        __Default,
        __Remark
    )
).
-define(
    PUT_CONFIG(__PName, __Context, __Default, __Remark, __Documentation),
    imem_config:put_config_hlk(
        ?CONFIG_TABLE,
        {element(2, application:get_application(?MODULE)), ?MODULE, __PName},
        ?MODULE,
        __Context,
        __Default,
        __Remark,
        __Documentation
    )
).
-define(
    LOOKUP_CONFIG(__PName, __Context),
    imem_config:lookup(
        ?CONFIG_TABLE,
        {element(2, application:get_application(?MODULE)), ?MODULE, __PName},
        __Context ++ [node()]
    )
).
-endif.

% IMEM_CONFIG_HRL
