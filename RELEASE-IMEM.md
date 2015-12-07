Application imem
================

Release history with new or improved features and bugfixes

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
