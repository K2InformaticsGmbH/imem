imem [![Build Status][travis badge]][travis] [![Coverage Status][coveralls badge]][coveralls] [![Erlang Versions][erlang version badge]][travis]
====

Clustered in-memory database based on MNESIA with simple SQL layer.

* Support for application configuration management.
* Support for application control by use of MNESIA data change notifications.
* Logging mechanism for time partitioned tables with automatic data ageing.
* Snapshot/restore functions giving limited persistance gurantees, useful for consistent cold starts.

[travis]: https://travis-ci.org/K2InformaticsGmbH/imem
[travis badge]: https://img.shields.io/travis/K2InformaticsGmbH/imem/master.svg?style=flat-square
[coveralls]: https://coveralls.io/github/K2InformaticsGmbH/imem
[coveralls badge]: https://img.shields.io/coveralls/K2InformaticsGmbH/imem/master.svg?style=flat-square
[erlang version badge]: https://img.shields.io/badge/erlang-20.0%20to%2020.1-blue.svg?style=flat-square
