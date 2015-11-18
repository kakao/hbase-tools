hbase-tools
===

Collection of command-line tools for HBase. This is developed and tested with CDH.

Usage
===
1. Download jars from [releases](../../../releases) page
    - Or build it by using [package.sh](../package.sh) on your PC
1. `java -jar [module-name]-[hbase_version]-[latest_version].jar`

Common Options for All Modules
===
```
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

Args Example
===
* You can simply store these args into a plain text file and reuse it. It is called "args file".

1. Non secured cluster
    
    ```
    node1.a.b:2181,node2.a.b:2181,node3.a.b:2181
    ```
1. Secured cluster
    
    ```
    node1.a.b:2181,node2.a.b:2181,node3.a.b:2181
    --krbconf=/Example/hbase-krb5.conf
    --principal=hbase
    --keytab=/Example/hbase-tools/hbase.keytab
    ```
    
License
===
[Apache License, Version 2.0](../LICENSE.txt)
