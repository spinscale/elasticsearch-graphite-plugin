# Elasticsearch graphite plugin

This plugin creates a little push service, which regularly updates a graphite host with indices stats and nodes stats. In case you are running a cluster, these datas are always only pushed from the master node.

The data sent to the graphite server tries to be roughly equivalent to [Indices Stats API](http://www.elasticsearch.org/guide/reference/api/admin-indices-stats.html) and [Nodes Stats Api](http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-stats.html)


## Installation

As plugins (except site plugins) cannot be automatically installed from github currently you need to build the plugin yourself (takes half a minute including an integrations test).

```
git clone https://github.com/spinscale/elasticsearch-graphite-plugin
cd elasticsearch-graphite-plugin
mvn package
/path/to/elasticsearch/bin/plugin -install graphite -url file:///absolute/path/to/current/dir/target/releases/elasticsearch-plugin-graphite-0.2-SNAPSHOT.zip
```


## Configuration

Configuration is possible via three parameters:

* `metrics.graphite.host`: The graphite host to connect to (default: none)
* `metrics.graphite.port`: The port to connect to (default: 2003)
* `metrics.graphite.every`: The interval to push data (default: 1m)
* `metrics.graphite.prefix`: The metric prefix that's sent with metric names (default: elasticsearch.your_cluster_name)
* `metrics.graphite.exclude`: A regular expression allowing you to exclude certain metrics (note that the node does not start if the regex does not compile)
* `metrics.graphite.include`: A regular expression to explicitely include certain metrics even though they matched the exclude (note that the node does not start if the regex does not compile)

Check your elasticsearch log file for a line like this after adding the configuration parameters below to the configuration file

```
[2013-02-08 16:01:49,153][INFO ][service.graphite         ] [Sea Urchin] Graphite reporting triggered every [1m] to host [graphite.example.com:2003]
```


## Bugs/TODO

* No really nice cluster support yet (needed it for a single instance system)
* Not extensively tested
* In case of a master node failover, counts are starting from 0 again (in case you are wondering about spikes)


## Credits

Heavily inspired by the excellent [metrics library](http://metrics.codahale.com) by Coda Hale and its [GraphiteReporter add-on](http://metrics.codahale.com/manual/graphite/).


## License

See LICENSE

