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
    mc
    merge
    split
  args file:
    Plain text file that contains args and options.
  common options:
    --force-proceed
        Do not ask whether to proceed.
    --debug
        Print debug log.
    --verbose
        Print some more messages.
    -c<key=value>, --conf=<key=value>
        Set a configuration for HBase. Can be used many times for several configurations.
    --after-failure=<script>
        The script to run when this running is failed.
        The first argument of the script should be a message string.
    --after-success=<script>
        The script to run when this running is successfully finished.
        The first argument of the script should be a message string.
    --after-finish=<script>
        The script to run when this running is successfully finished or failed.
        The first argument of the script should be a message string.
    -k<keytab file>, --keytab=<keytab file>
        Kerberos keytab file. Use absolute path.
    -p<principal>, --principal=<principal>
        Kerberos principal.
    --realm=<realm>
        Kerberos realm to use. Set this arg if it is not the default realm.
    --krbconf=<kerberos config file>
        Kerberos config file. Use absolute path.
```

[rel]: ../../../../releases
