![Bee waggle-dancing on a hive.](logo.png "Federating Hive Meta Stores.")

# Instructions to ranger plugin in Waggle Dance


### Process

Apache Ranger is a framework to enable, monitor and manage comprehensive data security across the Hadoop platform. [Apache ranger](https://ranger.apache.org/).

Waggle-dance can implement permission verification based on ranger hive plugin on the metadata side. Permission control is implemented by intercepting the API request to waggle-dance, obtaining the db name and table name, and then sending an authentication request to the ranger. Waggle-Dance ranger-based permission control has the following constraints.

1. The waggle-dance ranger authentication scheme only achieves table-level granularity and cannot achieve column-level granularity.
2. The waggle-dance ranger authentication scheme does not support ranger’s advanced features, such as `Row level filtering`, `Data masking`, etc.
3. When a table has `alter`, `update` or `drop` permissions, the table must have `select` permissions
4. Currently, only the acquisition of `user` and `group` in the Kerberos environment is implemented (other methods need to be expanded in `RangerWrappingHMSHandler`)



### Configuration

Waggle Dance does not read Hadoop's core-site.xml so the property of ranger plugin should be added to the Hive configuration file `hive-site.xml`:

```
<property>
  <name>ranger.plugin.waggle-dance.policy.rest.url</name>
  <value>http://ranger.url:6080</value>
</property>
<property>
  <name>ranger.plugin.waggle-dance.policy.cache.dir</name>
  <value>/home/hadoop/cache/path</value>
</property>
<property>
  <name>ranger.plugin.waggle-dance.service.name</name>
  <value>ranger_service_name</value>
</property>
<property>
  <name>ranger.plugin.waggle-dance.policy.pollIntervalMs</name>
  <value>180000</value>
</property>

```

In addition, if use LDAP for account management, the follow property should be add in the `hive-site.xml`.

```
  <property>
    <name>hadoop.security.group.mapping</name>
    <value>org.apache.hadoop.security.LdapGroupsMapping</value>
  </property>
  <property>
    <name>hadoop.security.group.mapping.ldap.url</name>
    <value>ldap://ldap.url.com:389</value>
  </property>
  <property>
    <name>hadoop.security.group.mapping.ldap.bind.user</name>
    <value>cn=readonlyuser,dc=x,dc=xxx,dc=xx</value>
  </property>
  <property>
    <name>hadoop.security.group.mapping.ldap.bind.password.file</name>
    <value>/home/hadoop/path/of/ldap.password</value>
  </property>
  <property>
    <name>hadoop.security.group.mapping.ldap.base</name>
    <value>dc=xx,dc=xx,dc=x,dc=xx,dc=xx</value>
  </property>
  <property>
    <name>hadoop.security.group.mapping.ldap.search.filter.user</name>
    <value>(search.filter.user)</value>
  </property>
  <property>
    <name>hadoop.security.group.mapping.ldap.search.filter.group</name>
    <value>(search.filter.user))</value>
  </property>
  <property>
    <name>hadoop.security.group.mapping.ldap.search.attr.member</name>
    <value>xxx</value>
  </property>
  <property>
    <name>hadoop.security.group.mapping.ldap.search.attr.group.name</name>
    <value>xx</value>
  </property>
```

### Running

After running, Waggle-dance will pull a ranger policy cache in json format to the `ranger.plugin.waggle-dance.policy.cache.dir` configuration path. And the cache is not updated within the `ranger.plugin.waggle-dance.policy.pollIntervalMs` configured time interval.
