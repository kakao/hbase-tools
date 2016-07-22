hbase-snapshot
==============

hbase-snapshot is a command-line tool for managing snapshots of HBase tables.

Usage
=====
1. Download `hbase-snapshot-[hbase_version]-[letest_version].jar` from [releases][rel] page
    - Or build it by using [package.sh](../../package.sh)
1. `java -jar hbase-snapshot-[hbase_version]-[letest_version].jar`
```
Create snapshots of HBase tables.
usage: snapshot (<zookeeper quorum>|<args file>) <tables expression>
  options:
    --keep=<num of snapshots> : The number of snapshots to keep. Default 0(unlimited).
    --skip-flush=<tables expression> : Tables to skip flush. Default false.
    --override=<tables in list type> : List to override.
    --exclude=<tables expression> : Tables to be excluded.
    --clear-watch-leak : Clear watch leaks. Workaround for HBASE-13885. This is not necessary as of HBase 0.98.14.
    --clear-watch-leak-only : Clear watch leaks only. It does not create any snapshot. Workaround for HBASE-13885. This is not necessary as of HBase 0.98.14.
    --delete-snapshot-for-not-existing-table : Delete the snapshots for not existing tables.
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
  tables expression:
    regexp or comma separated list
  list entity format:
    <table name>[/<keep count>/<skip flush>]
    regexp
```

[rel]: ../../../../releases
