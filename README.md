# Elasticsearch graphite plugin

This plugin creates a little push service, which regularly updates a graphite host with indices stats and nodes stats. In case you are running a cluster, these datas are always only pushed from the master node.

The data sent to the graphite server tries to be roughly equivalent to [Indices Stats API](http://www.elasticsearch.org/guide/reference/api/admin-indices-stats.html) and [Nodes Stats Api](http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-stats.html)


## Installation

As plugins (except site plugins) cannot be automatically installed from github currently you need to build the plugin yourself (takes half a minute including an integrations test).

```
git clone https://github.com/spinscale/elasticsearch-graphite-plugin
cd elasticsearch-graphite-plugin
mvn package
/path/to/elasticsearch/bin/plugin -install graphite -url file:///absolute/path/to/current/dir/target/releases/elasticsearch-plugin-graphite-6.2.1-0-RELEASE.zip
```

NOTES:  Integration test has been move to IT and surefire will only test on deploy task.

Also, if trying to use with 6.x.y.  Make sure to update es version in plugin descriptor file.


## Configuration

Configuration is possible via three parameters:

* `metrics.graphite.host`: The graphite host to connect to (default: none)
* `metrics.graphite.port`: The port to connect to (default: 2003)
* `metrics.graphite.every`: The interval to push data (default: 1m).  Dynamically updatable.
* `metrics.graphite.prefix`: The metric prefix that's sent with metric names (default: elasticsearch.your_cluster_name)
* `metrics.graphite.exclude`: A regular expression allowing you to exclude certain metrics (note that the node does not start if the regex does not compile)
* `metrics.graphite.include`: A regular expression to explicitely include certain metrics even though they matched the exclude (note that the node does not start if the regex does not compile)
* `metrics.graphite.perIndex`: A boolean.  This defaults to false.  This prevents collecting from ES and sending to graphite of per-index and shard stats.  Dynamically updatable.
* `metrics.graphite.include.indices`: A list of strings.  defaults to _all (All indices).  This will only send per-index/shard stats on specific indices.  Dyanmically updatable.


Check your elasticsearch log file for a line like this after adding the configuration parameters below to the configuration file

```
[2013-02-08 16:01:49,153][INFO ][service.graphite         ] [Sea Urchin] Graphite reporting triggered every [1m] to host [graphite.example.com:2003]
```


## Bugs/TODO

* Plugin is install on each node. Works with master only, data only, no data/master (query or coordinating node).
* Our version 1.x has been used in production for several years on several clusters.  Largest over 50 nodes.  6.x will be deployed in 2018.  Extensively tested at least in 1.7.5
* In case of a master node failover, counts are starting from 0 again (in case you are wondering about spikes).
* Add grafana dashboards

## Credits

Heavily inspired by the excellent [metrics library](http://metrics.codahale.com) by Coda Hale and its [GraphiteReporter add-on](http://metrics.codahale.com/manual/graphite/).


## License

See LICENSE


