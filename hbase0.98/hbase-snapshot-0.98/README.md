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
    --alert-script=<alert script> : The script to run when an error occurs.
    --clear-watch-leak : Clear watch leaks. Workaround for HBASE-13885. This is not necessary as of HBase 0.98.14.
    --clear-watch-leak-only : Clear watch leaks only. It does not create any snapshot. Workaround for HBASE-13885. This is not necessary as of HBase 0.98.14.
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
  tables expression:
    regexp or comma separated list
  list entity format:
    <table name>[/<keep count>/<skip flush>]
    regexp
```

[rel]: ../../../../releases
