hbase-table-stat
================

hbase-table-stat is a command-line tool for monitoring HBase tables.

![](../../../../raw/master/resource/hbase-table-stat.gif)

Usage
-----
1. Download `hbase-table-stat-[hbase_version]-[letest_version].jar` from [releases][rel] page
    - Or build it by using [package.sh](../../package.sh)
1. `java -jar hbase-table-stat-[hbase_version]-[letest_version].jar`
```
Show some important metrics periodically.
usage: hbase-table-stat (<zookeeper quorum>|<args file>) [table] [options]
  options:
    --interval=<secs> : Iteration interval in seconds. Default 10 secs.
    --region: Stats on region level.
    --rs=<rs name regex> : Show stats of specific region server at region level.
    --output : File name. Save stats in csv format.
    --port=<http port> : Http server port. Default 0.
  dynamic options:
    h - show this help message
    q - quit this app
    p - pause iteration. toggle
    d - show differences from the start. toggle
    R - reset diff start point to now
    c - show changed records only. toggle
    r - show change rate instead of diff. toggle
    [shift]0-9 - sort by selected column value or diff (with shift). in ascending order
    S - save current load data to a csv file
    L - load a saved csv file and set it as diff start point
    C - show connection information
  args file:
    Plain text file that contains args and options.
  common options:
    --force-proceed: Do not ask whether proceed or not.
    --test: Set test mode.
    --debug: Print debug log.
    --verbose: Print some more messages.
    --keytab=<keytab file>: Kerberos keytab file. Use absolute path.
    --principal=<principal>: Kerberos principal.
    --realm=<realm>: Kerberos realm to use. Set this arg if it is not the default realm.
    --krbconf=<kerberos config file>: Kerberos config file. Use absolute path.
```

[rel]: ../../../../releases
