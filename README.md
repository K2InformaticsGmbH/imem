# imem 

![Travis (.org)](https://img.shields.io/travis/K2InformaticsGmbH/imem.svg)
![Coveralls github](https://img.shields.io/coveralls/github/K2InformaticsGmbH/imem.svg)
![GitHub](https://img.shields.io/github/license/K2InformaticsGmbH/imem.svg)
![GitHub release](https://img.shields.io/github/release/K2InformaticsGmbH/imem.svg)
![GitHub Release Date](https://img.shields.io/github/release-date/K2InformaticsGmbH/imem.svg)
![GitHub commits since latest release](https://img.shields.io/github/commits-since/K2InformaticsGmbH/imem/3.3.1.svg)

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

## Build
`rebar3 compile`
**please note that IMEM uses NIF (win32 timers) which needs platform specific C/C++ build tool chains (cl.exe and link.exe) available and correctly set up for port_compiler to compile**
