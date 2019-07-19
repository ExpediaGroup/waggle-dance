![Bee waggle-dancing on a hive.](logo.png "Federating Hive Meta Stores.")

# Additional instruction to use Waggle Dance in kerberized environment
 
 
### Prerequisites

* Kerberized claster: 
    active KDC, 
    some required properties in configuration files of hadoop services
* User account with privileges in ipa 


### Configuration

Waggle Dance does not read hadoop's *core-site.xml* so a general property providing kerberos auth should be added to 
the Hive configuration file *hive-site.xml*:

```
<property>
    <name>hadoop.security.authentication</name>
    <value>KERBEROS</value>
</property>
```
 
Besides Waggle Dance needs a keytab file to communicate with the Metastore so next properties should be present:
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

If you are intending to use a beeline client, next properties may be valuable:
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