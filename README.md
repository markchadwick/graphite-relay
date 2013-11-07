Graphite Relay
==============
A fast(er) [Graphite](http://graphite.wikidot.com/) relay written with
[Netty](http://www.jboss.org/netty). This will read updates in the line-oriented
format (`metric value timestamp`) and write Pickle format to the backends.

Running the Relay
------------------
    java -jar graphite-relay-0.1.jar -c /path/to/relay.properites


Backend Strategies
------------------
A backend strategy will pick which Graphite backends to send a given metric to.
The include ones are:

* `Broadcast` - Broadcast each metric to all backends
* `ConsistentHash` - Use a simple consistent hash to send an update to a backend
  with a minimal change in (metric -> backend) mapping on backend addition or
  deletion.
* `RoundRobin` - Cycle through the backends sending a batch of metrics to one at a
  time (Not reccomended as this will not play nicely with other Graphite Tools).

Overflow Handlers
-----------------
When a backend goes down, this relay will buffer up messages to a point. After
that point, it will dump updates to an `OverflowHandler` to ensure we don't blow
the heap. The `OverflowHandler` should do something with an update toher than
keep it in memory. The included implementations are:

* `BitchingOverflowHandler` - Simply log ever so often to let someone know that
  not all metrics are making it to backends.
* `LoggingOverflowHandler` - Stream the metrics to a rolling, gzipped set of
  files on disk which can be replayed with
  [netcat](http://netcat.sourceforge.net/) (or similar) at a later point.

Config Properties
-----------------
There are a number of variables you can set in the config file. Popular ones are
set below. If a required parameter is missing it will manifest itself (for the
moment) with [Guice](http://code.google.com/p/google-guice/) vomiting a stack
trace on you while trying to start the program.

### `relay.backends`
Graphite backends which will recieve messages in the normal Graphite Pickle
format. This should be a line-delimited list of `host:port` pairs. For example:

    relay.backends: \
        127.0.0.1:1234:a \
        127.0.0.1:1235:b \
        127.0.0.1:1236:c \
        127.0.0.1:1237:d

(You can omit instance part(:a,:b...), but for ConsistentHash routing its mandatory
Also please use same hostname as for CARBONLINK_HOSTS in graphite-web, i.e. not mixed localhost
and 127.0.0.1, fqdn with hostname etc. Instance names and hostname are using in hashing algorithm
and any inconsistency will break caching, which is mandatory if you have significant load)

### `relay.hostbuffer`
Number of updates to buffer for each host in the event that it goes down

### `relay.port`
Default port to listen on. This port will expect line-oriented Graphite update
metrics.

### `relay.reconnect`
Number of seconds to sleep before trying to reconnect to a disconnected backend.

### `relay.backendstrategy`
Backend Strategy to use for finding a backend for each metric. Default available
values are:
- `graphite.relay.backend.strategy.Broadcast`
- `graphite.relay.backend.strategy.ConsistentHash`
- `graphite.relay.backend.strategy.RoundRobin`
You may also set to the FQCN of any other `BackendStrategy` in the `CLASSPATH`.
If using the `ConsistentHash` strategy, you will also have to set
`hash.replicas` in the config. A reasonable default value is `100`.
(NB: Please do not change it, unless Graphite-web use 100 as hardcoded default)

### `relay.overflowhandler`
Handler which will recieve updates that no backend is available to handle. This
is generally because it is unavailable or overwhelmed. Default available values
are:
- `graphite.relay.overflow.BitchingOverflowHandler`
- `graphite.relay.overflow.LoggingOverflowHandler`
Like `relay.backendstrategy`, the FQCN of any other `OverflowHandler` in the
`CLASSPATH` is just as valid. If using the `LoggingOverflowHandler`, you _must_
set `overflow.directory` in the configuration to a directory that the current
user has permission to create.

A complete config file might be as follows:

    relay.hostbuffer:   1000
    relay.port:         2002
    relay.reconnect:    2
    
    relay.backendstrategy: graphite.relay.backend.strategy.ConsistentHash
    hash.replicas: 20

    relay.overflowhandler: graphite.relay.overflow.LoggingOverflowHandler
    overflow.directory:    /mnt/overflow/
    
    relay.backends: \
        localhost:1234:a \
        localhost:1235:b \
        localhost:1236:c \
        localhost:1237:d


Pickle Format
-------------
The included Pickle formatting is _very_ rudimentary and only groks the data
type that the Graphite backends expect. Because of its simple format, this will
actually use a very early Pickle encoding, which cPickle and friends can grok,
but may not be most effecient.
