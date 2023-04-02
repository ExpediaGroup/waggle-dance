![Bee waggle-dancing on a hive.](logo.png "Federating Hive Meta Stores.")

# Additional instruction to use Waggle Dance in kerberized environment


### Process

In a kerberos environment, after a client requests waggle-dance, waggle-dance requests the proxy user's token from the metastore and then uses that token to communicate with the metastore.

This is necessary in some scenarios that require permission authentication. Such as the `create_table` API that requires the proxy user to create hdfs directories.

![image](https://user-images.githubusercontent.com/13965087/229339323-260f3c17-46c0-4471-81d2-cdbcfa0fe3ce.png)

In addition, because kerberos authentication requires a delegation-token to proxy as other users. The proxy user of the session is shared globally, which means we need to make all Hive Metastores share a set of delegation-token storage so that a single delegation-token can be authenticated by multiple Metastores.

**One solution is to use zookeeper to store tokens for all Hive Metastores, which is essential.**

### Prerequisites

* Kerberized claster:
  active KDC,
  some required properties in configuration files of hadoop services
* User account with privileges in ipa
* Zookeeper to store delegation-token (Recommend)

### Configuration

Waggle Dance does not read hadoop's *core-site.xml* so a general property providing kerberos auth should be added to
the Hive configuration file *hive-site.xml*:

```
<property>
  <name>hadoop.security.authentication</name>
  <value>KERBEROS</value>
</property>
```


Besides Waggle Dance needs a keytab file to communicate with the Metastore so following properties should be present:
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

In addition, all metastores need to use the zk shared token:
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

If you are intending to use a beeline client, following properties may be valuable:
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

The Waggle Dance should be starting as privileged user with a fresh keytab.

If Waggle Dance throws a GSS exception, you have problem with the keytab file.
Try to perform `kdestroy` and `kinit` operations and check for a keytab file ownership.

If the Metastore throws an exception with code -127, Waggle Dance uses wrong authentication policy.
Check hive-conf.xml for a correct configuration and make sure that HIVE_HOME and HIVE_CONF_DIR are defined.

Don't forget to restart hive services!
