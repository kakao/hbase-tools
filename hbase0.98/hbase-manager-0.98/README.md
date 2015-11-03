hbase-manager
===================

hbase-manager is a command-line tool for managing HBase tables.

Usage
=====
1. Download `hbase-manager-[hbase_version]-[letest_version].jar` from [releases][rel] page
    - Or build it by using [package.sh](../../package.sh)
1. `java -jar hbase-manager-[hbase_version]-[letest_version].jar`
```
Usage: Manager <command> (<zookeeper quorum>|<args file>) [args...]
  commands:
    assign
    balance
    exportkeys
    merge
    split
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
