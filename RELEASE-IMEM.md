Application imem
================

Release history with new or improved features and bugfixes

Version 3.8.0 (Release Date 10.02.2020)
=======================================
* 009158 Bad param for parameterized SQL crashes FSM (#315)
* implement safe subnet_filter(IPv4/6 address, subnet-length) -> mask (#309)

Version 3.7.2 (Release Date 27.12.2019)
=======================================
* Frameworks and APIs to filter cluster snapshot (#299)
* Activated auto truncate on high memory (#306)
* Float support of diff 

Version 3.7.1 (Release Date 22.11.2019)
=======================================
* Return_atomic wrap on transaction calls #302

Version 3.7.0 (Release Date 15.11.2019)
=======================================
* imem_dal_skvh dirty_next api implemented
* imem_snap initial zip taken synchronously

Version 3.6.0 (Release Date 13.09.2019)
=======================================
* Cluster Query support
* On demand cluster snapshotting
* Purge History tables (older than configured days)
* Partition size metric
* Virtual memory metric for windows

Version 3.5.0 (Release Date 19.06.2019)
=======================================
* Moved lager handlers into config 
* Added support of put and delete to imem_client

Vesrion 3.4.1 (Release Date 30.04.2019)
=======================================
* Improved performance of imem_snap candidate check

Vesrion 3.4.0 (Release Date 18.04.2019)
=======================================
* http interface in imem_client
* git diff functionality with srource code
* sql parse 4.6.1

Vesrion 3.3.2 (Release Date 20.03.2019)
=======================================
* ddVersion: git command optimized
* ddVesrion: unnecessary folder switch removed
* ddVersion: path blacklist

Vesrion 3.3.1 (Release Date 15.03.2019)
=======================================
* Implemented ddVersion
* ddVersion : check beam for git URL info
* Sqlparse upgraded to version 4.5.0

Vesrion 3.3.0 (Release Date 04.03.2019)
=======================================
* Implemented text comparison and diff functionality
* Sqlparse upgraded to version 4.4.0

Version 3.2.0 (Release Date 18.01.2019)
=======================================
* process_statistics metrics added #227
* imem_compiler hot code loading issue fixed #229

Version 3.1.0 (Release Date 21.11.2018)
=======================================
* Added specs to imem_snap module
* cluster snapshot reading tables to snapshot at time of snapshotting #221
* skvh - check trigger before overwritting #223

Version 3.0.1 (Release Date 04.10.2018)
=======================================
* Windows NIFs for hi-resolution time

Version 3.0.0 (Release Date 31.08.2018)
=======================================
* Upgraded sqlparser to version 4.3.1
* DB restore from cluster snapshot on cold start (since 2.1.0)
* `to_boolean` SQL function
* move windows native time API to branch
* manual 4-byte header in imem_server is replaced with `{packet, 4}` socket option
* log table creation is deferred till imem start in `imem_lager_backend`

Version 2.4.0 (Release Date 14.06.2018)
=======================================
* Added epmd API to allow registration of node in case of node hidden from cluster
* Upgraded ranch to version 1.5.0
* `imem_metrics:get_metric/1`, `erlang:node/0`, `imem_meta:schema/0` and `imem_meta:node_shard/0` added to safe list of function.
* Upgraded port_compiler to 1.9.1
* Upgraded erlscrypt to 1.2.2

Version 2.3.0 (Release Date 03.05.2018)
=======================================
* Upgraded sqlparse to version 4.2.0

Version 2.2.0 (Release Date 25.04.2018)
=======================================
* Upgraded sqlparse to version 4.1.0
* live debug tracing feature support
* windows native timestamp APIs as NIF

Version 2.1.0 (Release Date 05.04.2018)
=======================================
* Upgraded sqlparse to version 4.0.0
* Improved type inference in JSON projection updates

Version 2.0.0 (Release Date 19.01.2018)
=======================================
* Common tests added [#183](https://github.com/K2InformaticsGmbH/imem/pulls/183)
* Removed `filename.erl` as fixed in OTP 20.2 [#185](https://github.com/K2InformaticsGmbH/imem/issues/185)

Version 1.9.1 (Release Date 23.11.2017)
=======================================
* APIs `ets`, `select_count` [#184](https://github.com/K2InformaticsGmbH/imem/issues/184)

Version 1.9.0 (Release Date 20.11.2017)
=======================================
* Fixed decimals when using datatype to convert from float to io
* Skvh delete using keys as terms
* Implemented circular reference for imem_config
* sqlparse to version 3.0.0

Version 1.8.2 (Release Date 10.11.2017)
=======================================
* sext-1.4.1
* fixes for node sharded tables [#175](https://github.com/K2InformaticsGmbH/imem/issues/175)

Version 1.8.1 (Release Date 16.10.2017)
=======================================
* OTP-20.1
* Erlscrypt to version
* Exoprted decode/2 function to imem_json to allow options.

Version 1.8.0 (Release Date 11.09.2017)
=======================================
* Migration to rebar3.
* Improved application start / stop.
* purge/proll logic re-worked

Version 1.7.1 (Release Date 27.07.2017)
=======================================
* OTP 20
* Backward compatibility added for partition name calculation based on key.
* Added list_to_binstr sql function
* Export start, suspend and restart in imem_snap
* Using new imem_meta monotinic time to check if snapshot is required and reduced the check frequency to 60 seconds.

Version 1.7.0 (Release Date 03.05.2017)
=======================================
* Added new time methods to replace deprecated erlang:now 

Version 1.6.3 (Release Date 28.04.2017)
=======================================
* OTP 19 support
* [lager-3.4.2](https://github.com/K2InformaticsGmbH/lager/tree/3.4.2)
* [Clustering between OTP18 and OTP19 fixed](https://github.com/K2InformaticsGmbH/imem/commit/b1f873fc763649a4d0d22a8e3750a18bb92f03fa)

Version 1.6.2 (Release Date 12.04.2017)
=======================================
* Added node information to the metric results
* metric modification support for noreply path
* Implemented safe callback function and suspend agent execution of metric for at least 2 seconds after a crash
* fixed return value of request_metric on behaviour as the caller expect it to be ok, and the metric result should be sent as message
* Changed request_metric from imem_sec to use imem_gen_metrics and pass the callback module as a parameter
* Required nodes reutrned with elrang and data nodes metrics
* [`sqlparse-2.0.0`](https://github.com/K2InformaticsGmbH/sqlparse/releases/tag/2.0.0)
* [`jpparse-1.0.0`](https://github.com/K2InformaticsGmbH/jpparse/releases/tag/1.0.0)
* [Bug Fixes](https://github.com/K2InformaticsGmbH/imem/issues?q=is%3Aclosed+milestone%3A1.6.2)

Version 1.6.1 (Release Date 09.03.2017)
=======================================
* Added support for async request metrics from remote connection
* Implemented driven sync reply
* Ranch dependency upgraded to 1.3.2
* 127.0.0.1 set as default listener address 
* Implemented snapshot exclusion pattern as function inside imem_snap
* Implemented sql function round
* replaced safe sql funciton for typed version safe_<datatype> i.e.: (safe_atom, safe_binstr, ...) 
* Implmeented projection update/insert for map_get
* Added maps, binary and strings modules to the list of safe sql functions.

Version 1.6.0 (Release Date 18.01.2017)
=======================================
* Improved support for escaped text in CSV files.
* Improved speed of ``imem_datatype:io_to_integer()``
* Add sql preview function (index scan)
* Bind table names from parameters in sql_select

Version 1.5.9 (Release Date 09.12.2016)
=======================================
* Enum metadata property support
* imem_gen_metrics behaviour support asynchronous requests
* get_config_hlk support for configuration without comment removed
* Double sms issue fixed with smsott authentication after saml
* Fixed code inject vulnerabilities by only allowing safe functions

Version 1.5.8 (Release Date 16.11.2016)
=======================================
* Pretty print records using parse transform
* Using ``erlang:now()`` instead of ``os:timestamp()`` in datatype
* Implemented imem_metrics system information

Version 1.5.7 (Release Date 01.11.2016)
=======================================
* Binding Strategy for from_binterm
* Auto purge only local shards on low memory
* Fixed like operation not working on strings containing ~n

Version 1.5.6 (Release Date 19.10.2016)
=======================================
* Cluster snapshot fixes #101 and #102

Version 1.5.5 (Release Date 17.10.2016)
=======================================
* Added behaviour imem_gen_metrics defining the api for metrics
* Support for json path edits, empty binary instead of '$not_a_value'
* Updated sqlparse to version 1.5.4

Version 1.5.4 (Release Date 16.09.2016)
=======================================
* Added complexity check to change password
* Tail mode propagate deleted rows using detailed MNESIA events
* Suspend snapshot during ``imem_snap:restore_as``
* Added saml auth support

Version 1.5.3 (Release Date 16.06.2016)
=======================================
* [snapshot restore error message improvements](https://github.com/K2InformaticsGmbH/imem/pull/81)

Version 1.5.2 (Release Date 10.06.2016)
=======================================
* [imem_server (TCP/SSL) certificate management](https://github.com/K2InformaticsGmbH/imem/issues?q=milestone%3A1.5.2+is%3Aclosed)

Version 1.5.1 (Release Date 29.05.2016)
=======================================
* [imem_server (TCP/SSL) restart APIs](https://github.com/K2InformaticsGmbH/imem/issues/88)
* dirty_select support
* ddConfig dirty access
* tial bug fix

Version 1.5.0 (Release Date 17.05.2016)
=======================================
* Added map functions
* Implement slice sql function

Version 1.4.15 (Release Date 26.04.2016)
=======================================
* Set sext dependency to a particular commit (map support)
* Improve generation for binstr and binary

Version 1.4.14 (Release Date 06.04.2016)
=======================================
* Fixed sql generation issues for filter
* Added support for drop imem_dal_skvh tables
* Added edit on simple json path expression projections
* Allow update and insert for vector query
* Enabled generators for in()
* Support arity 2 vnf(Key, Value)
* Updated sqlparse to version 1.5.3

Version 1.4.13 (Release Date 21.03.2016)
=======================================
* Fixed preview not returning the complete list of results
* Fixed data dictionary trigger
* Added imem config documentation
* drop with table opts support and ignore
* Fixed sql generation from filters for json

Version 1.4.12 (Release Date 14.03.2016)
=======================================
* Prune history implemented
* Audit write no op implemented
* Fixed cluster snapshots
* Improve speed of skvh read shallow and deep.

Version 1.4.11 (Release Date 05.03.2016)
=======================================
* Fixed some security vulnerabilities
* Moved JSON Path documentation to Wiki
* Added map interface to skvh

Version 1.4.10 (Release Date 01.03.2016)
=======================================
* json_diff bug fixes

Version 1.4.9 (Release Date 26.02.2016)
=======================================
* Added json_diff to the sql layer


Version 1.4.8 (Release Date 18.02.2016)
=======================================
* Removed search deleted from imem_dal_skvh
* Fixed bug in imem_snap
* Added imem_domain gen_server for translation functions


Version 1.4.7 (Release Date 12.02.2016)
=======================================
* Fix imem_if_csv for large lines
* Fixed bug in authentication failure handling


Version 1.4.6 (Release Date 05.02.2016)
=======================================
* Improved snapshot restore efficiency
* Added to_json as sql function
* Fixed bug in imem_doc_config


Version 1.4.5 (Release Date 29.01.2016)
=======================================
* Fixed bug searching delete objects


Version 1.4.4 (Release Date 26.01.2016)
=======================================

* Added is_val support for sql
* Corrected to_map function


Version 1.4.3 (Release Date 19.01.2016)
=======================================

* Added support for functions (mfa) using sql
* Improved generators for primitive data types
* Added complexity check for "alter user password" sql


Version 1.4.2 (Release Date 12.12.2015)
=======================================

* adding temporary lock to authentication
* fix modulo calculation in failure tuple


Version 1.4.1 (Release Date 10.12.2015)
=======================================

* Added support for continuation to imem_index preview


Version 1.4.0 (Release Date 07.12.2015)
=======================================

* CSV file read
* Major backward incompatible DB schema changes
* Lot of bug fixes


Version 1.2.3 (Release Date 04.09.2013)
=======================================

* spawn zipping of files on startup into separate process, avoid zip startup delays
* unzip goes into default snapshot directory, avoiding problems with temp directory
* delete orphan ddTable entry for drop table when table is dropped on MNESIA level 
* remove logging of drop / truncate in imem_meta, reduce risk of call recursion
* use separate processes for purge and monitor, will allow logging in these functions
* improve variable binding in joins and support tuple values better

* fix permission problem for drop_table / truncate_table, used in dderl

* added virtual table ddSchema

Version 1.2.2 (Release Date 29.08.2013)
=======================================

* add ddTable record to user_properties (included in shnapshot)
* extend timeout for snapshots
* create table in restore using user_propertties if it does not exist
* implement nodef() formatter function for suppressing default values in SQL select columns
* add pseudocolumn expiry = time_of_partition_expiry()  to ddSize virtual table
* add pseudocolumn tte = time_to_partition_expiry() to ddSize virtual table
* add value owner to config entries and prevent different key owner from overwriting


* bad_rpc issue fixed in ddNodes evaluation
* ddSize typo fixed

* use os_mon / lager for system monitoring
* change logging from info to debug for drop_table, subscribem unsubscribe

* snapshot issues fixed
* snap for only local readable tables
* snap restore and backup one file at a time

Version 1.2.1 (Release Date 13.08.2013)
=======================================

* change dependencies to forked own versions (for reproducability)
* no functional changes

Version 1.2.0 (Release Date 08.08.2013)
=======================================

* configurable snapshot fun
* chunked streaming format for snapshot and restore
* configurable purge fun which can act based on available os memory limits
* memory usage column added to virtual table ddSize

* fixes for binary table names in security layer imem_sec
* clean and central parsing for time partitioned tables
* teardown of imem_if in case of MNESIA down signal
* improved zip-file handling for snapshots
* fix connection probe issue with accept_once

Version 1.1.2 (Release Date 23.07.2013)
=======================================

* using lager logging for low level imem functions
* configurable monitor extensions
* configurable monitor dump to file system

* fix lock situation in log partition rollover

Version 1.1.1 (Release Date 05.07.2013)
=======================================

* extended snapshotting functions and escript access
* ddSize virtual table with row count column for joins with table names

* fix for sorting issue

Version 1.1.0 (Release Date 02.07.2013)
=======================================
