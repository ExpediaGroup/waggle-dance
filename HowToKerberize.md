![Bee waggle-dancing on a hive.](logo.png "Federating Hive Meta Stores.")

# Additional instructions to use Waggle Dance in a Kerberized environment


### Process

In a Kerberos environment a client make a request to Waggle Dance which in turn requests the proxy user's token from the metastore and then uses this token to communicate with the metastore.

This is necessary in certain scenarios that need authentication - for example the `create_table` API that requires the proxy user to create HDFS directories.

![image](https://user-images.githubusercontent.com/13965087/229339323-260f3c17-46c0-4471-81d2-cdbcfa0fe3ce.png)

In addition, because Kerberos authentication requires a delegation-token to proxy as other users, the proxy user of the session is shared globally. This means we need to make all Hive Metastores share a set of delegation-token storage so that a single delegation-token can be authenticated by multiple Metastores.

**One solution is to use Zookeeper to store tokens for all Hive Metastores**

### Prerequisites

* Kerberized cluster:
  active KDC,
  some required properties in configuration files of Hadoop services
* User account with privileges in ipa
* Zookeeper to store delegation-token (Recommended)

### Configuration

Waggle Dance does not read Hadoop's `core-site.xml` so a general property providing Kerberos auth should be added to
the Hive configuration file `hive-site.xml`:

```
<property>
  <name>hadoop.security.authentication</name>
  <value>KERBEROS</value>
</property>
```


Waggle Dance also needs a keytab file to communicate with the Metastore so the following properties should be present:
```
<property>
  <name>hive.metastore.sasl.enabled</name>
  <value>true</value>
</property>
<property>
  <name>hive.metastore.kerberos.principal</name>
  <value>hive/_HOST@DEV.DF.SBRF.RU</value>
</property>
<property>
  <name>hive.metastore.kerberos.keytab.file</name>
  <value>/etc/hive.keytab</value>
</property>
```

In addition, all metastores need to use the Zookeeper shared token:
```
  <property>
    <name>hive.cluster.delegation.token.store.class</name>
    <value>org.apache.hadoop.hive.thrift.ZooKeeperTokenStore</value>
  </property>
  <property>
    <name>hive.cluster.delegation.token.store.zookeeper.connectString</name>
    <value>zk1:2181,zk2:2181,zk3:2181</value>
  </property>
  <property>
    <name>hive.cluster.delegation.token.store.zookeeper.znode</name>
    <value>/hive/token</value>
  </property>
```

If you are intending to use a Beeline client, the following properties may be valuable:
```
<property>
  <name>hive.server2.transport.mode</name>
  <value>http</value>
</property>
<property>
  <name>hive.server2.authentication</name>
  <value>KERBEROS</value>
</property>
<property>
  <name>hive.server2.authentication.kerberos.principal</name>
  <value>hive/_HOST@DEV.DF.SBRF.RU</value>
</property>
<property>
  <name>hive.server2.authentication.kerberos.keytab</name>
  <value>/etc/hive.keytab</value>
</property>
<property>
  <name>hive.server2.enable.doAs</name>
  <value>false</value>
</property>
```


### Running

Waggle Dance should be started by a privileged user with a fresh keytab.

If Waggle Dance throws a GSS exception, you have problem with the keytab file.
Try to perform `kdestroy` and `kinit` operations and check the keytab file ownership flags.

If the Metastore throws an exception with code -127, Waggle Dance is probably using the wrong authentication policy.
Check the values in `hive-conf.xml` and make sure that HIVE_HOME and HIVE_CONF_DIR are defined.

Don't forget to restart hive services!
