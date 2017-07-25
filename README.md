![Bee waggle-dancing on a hive.](logo.png "Federating Hive Meta Stores.")

## Overview
Waggle Dance is a request routing Hive metastore proxy that allows tables to be concurrently accessed across multiple Hive deployments. It was created to tackle the appearance of the dataset silos that arose as our large organization gradually migrated from monolithic on-premises clusters, to cloud based platforms.

In short, Waggle Dance allows you to describe, query, and join tables that may exist in multiple distinct Hive deployments. Such deployments may exist in disparate regions, accounts, or clouds (security and network permitting). Dataset access is not limited to the Hive query engine, and should work with any Hive metastore enabled platform. We've been successfully using it with Spark for example.

We also use Waggle Dance to apply a simple security layer to cloud based platforms such as Qubole, DataBricks, and EMR. These currently provide no means to construct cross platform authentication and authorization strategies. Therefore we use a combination of Waggle Dance and network configuration to restrict writes and destructive Hive operations to specific user groups and applications.

## Start using

You can obtain Waggle Dance from Maven Central:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/waggle-dance/badge.svg?subject=com.hotels:waggle-dance)](https://maven-badges.herokuapp.com/maven-central/com.hotels/waggle-dance) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/waggle-dance.svg)


## Premise

We maintain a mapping of virtual database names to federated metastore instances. These virtual names are resolved by Waggle Dance during execution and requests are forwarded to the mapped metastore instance.

| Virtual database name | Mapped database name | Mapped metastore URIs   |
| --------------------- | -------------------- | ------------------------|
| mydb                  | mydb                 | thrift://host:port/     |

So when we do the following in a Hive CLI client connected to a Waggle Dance instance:

    select *
    from mydb.table;

We are actually performing the query against the `thrift://host:port/` metastore. All metastore calls will be forwarded and data will be fetched and processed locally.

This makes is possible to read and join data from different Hive clusters via a single Hive CLI.

## System architecture

![Waggle Dance system diagram.](system-diagram.png "Routing of requests via virtual databases.")

## Building

    mvn clean package

NOTE: by default the waggle-dance-rpm module will build an RPM artifact for installation. This is done via the [maven rpm plugin](http://www.mojohaus.org/rpm-maven-plugin/) which requires the 'rpm' command to be available on the command line. So please make sure that this is installed (for example on mac-OSX do `brew install rpm` to install the package).

## Installing

Waggle Dance is available as a RPM or TGZ package. It is intended to be installed as a service available on a machine that is accessible from wherever you want to query it from (and with access to the Hive metastore service(s) that it is federating). If you are using AWS this could be on an existing EMR instance (for example the EMR cluster of your primary Hive metastore) or on a separate EC2 instance. It would be deployed in a similar fashion for other cloud or internal platforms.

### TGZ package

You can uncompress the file by executing:

    tar -xzf waggle-dance-<version>-bin.tgz

Although it's not necessary, we recommend exporting the environment variable _WAGGLE_DANCE_HOME_ by setting its value to wherever you extracted it to:

    export WAGGLE_DANCE_HOME=/<foo>/<var>/waggle-dance

Then _cd_ into the uncompressed directory _waggle-dance_ or `$WAGGLE_DANCE_HOME` and type the following commands:

    cp $WAGGLE_DANCE_HOME/conf/waggle-dance-server.yml.template $WAGGLE_DANCE_HOME/conf/waggle-dance-server.yml
    cp $WAGGLE_DANCE_HOME/conf/waggle-dance-federation.yml.template $WAGGLE_DANCE_HOME/conf/waggle-dance-federation.yml

Edit the property `remote-meta-store-uris` in _./conf/waggle-dance-federation.yml_ and modify this to contain the URL(s) of the metastore(s) you want to federate.

Refer to the [configuration](#configuration) section for further details about the available configuration settings.

### Running on the command line

To run Waggle Dance just execute:

    $WAGGLE_DANCE_HOME/bin/waggle-dance.sh --server-config=$WAGGLE_DANCE_HOME/conf/waggle-dance-server.yml --federation-config=$WAGGLE_DANCE_HOME/conf/waggle-dance-federation.yml

Log messages will be output to the standard output.

### RPM package

It is intended that Waggle Dance is run as a service serving as a proxy to different metastores. The primary configured metastore is the only one to which you may also write data via Hive.

    sudo yum install waggle-dance-rpm

### Running as a service

Installing the RPM will register waggle-dance as an init.d service.
Configuration is in _/opt/waggle-dance/conf/_
After the first installation configuration needs to be manually changed and then service needs to be started:

    sudo service waggle-dance start

Currently any changes to the configuration require restarting the service in order for the changes to come into effect:

    sudo service waggle-dance restart

Log messages can be found in _/var/log/waggle-dance/waggle-dance.log_


## Configuration

#### Server

Server config is by default located in _/opt/waggle-dance/conf/waggle-dance-server.yml_

The table below describes all the available configuration values for Waggle Dance server:

| Property                          | Required   | Description |
|:----|:----:|:----|
| `port`                            | No         | Port on which the waggle-dance listens. Default is `0xBEE5` (`48869`) |
| `verbose`                         | No         | Log detailed trace. Default is `false` |
| `disconnect-connection-delay`     | No         | Idle metastore connection timeout. Default is `5` |
| `disconnect-time-unit`            | No         | Idle metastore connection timeout units. Default is `MINUTES` |
| `database-resolution`             | No         | Controls what type of database resolution to use. See the [Database Resolution](#database-resolution) section. Default is `MANUAL`. |

#### Federation

Server config is by default located in: _/opt/waggle-dance/conf/waggle-dance-federation.yml_

Example:

    primary-meta-store:                                     # Primary metastore
      access-control-type: READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST
      name: primary                                         # unique name to identify this metastore
      remote-meta-store-uris: thrift://127.0.0.1:9083
      writable-database-white-list:
      - my_writable_db1
      - my_writable_db2
      - ...
    federated-meta-stores:                                  # List of read only metastores to federate
    - remote-meta-store-uris: thrift://10.0.0.1:9083
      name: secondary
      metastore-tunnel:
        route: ec2-user@bastion-host -> hadoop@emr-master
        private-keys: /home/user/.ssh/bastion-key-pair.pem,/home/user/.ssh/emr-key-pair.pem
        known-hosts: /home/user/.ssh/known_hosts
      mapped-databases:
      - prod_db1
      - prod_db2
    - ...

The table below describes all the available configuration values for Waggle Dance federations:

| Property                                              | Required | Description |
|:----|:----:|:----|
| `primary-meta-store`                                  | No       | Primary MetaStore config. Can be empty but it is advised to configure it. |
| `primary-meta-store.remote-meta-store-uris`           | Yes      | Thrift URIs of the federated read-only metastore. |
| `primary-meta-store.name`                             | Yes      | Database name that uniquely identifies this metastore used internally. Cannot be empty. |
| `primary-meta-store.database-prefix`                  | No       | This will be ignored for the primary metastore and an empty string will always be used instead. |
| `primary-meta-store.access-control-type`              | No       | Sets how the client access controls should be handled. Default is `READ_ONLY` Other options `READ_AND_WRITE_AND_CREATE`, `READ_AND_WRITE_ON_DATABASE_WHITELIST` and `READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST` see Access Control section below. |
| `primary-meta-store.writable-database-white-list`     | No       | White-list of databases used to verify write access used in conjunction with `primary-meta-store.access-control-type`. The list of databases should be listed without any `primary-meta-store.database-prefix`. |
| `primary-meta-store.metastore-tunnel`                 | No       | The address on which to bind the local end of the tunnel. Default is '`localhost`'. |
| `primary-meta-store.metastore-tunnel.port`            | No       | See metastore tunnel configuration values below. |
| `federated-meta-stores`                               | No       | Possible empty list of read only federated metastores. |
| `federated-meta-stores[n].remote-meta-store-uris`     | Yes      | Thrift URIs of the federated read-only metastore. |
| `federated-meta-stores[n].name`                       | Yes      | Name that uniquely identifies this metastore, used internally. Cannot be empty. |
| `federated-meta-stores[n].database-prefix`            | No       | Prefix used to access this particular metastore and differentiate databases in it from databases in another metastore. Typically used if databases have the same name across metastores but federated access to them is still needed. Default prefix is {federated-meta-stores[n].name} lowercased and postfixed with an underscore. For example if the metastore name was configured as "waggle" and no database prefix was provided but `PREFIXED` database resolution was used then the value of `database-prefix` would be "waggle_". |
| `federated-meta-stores[n].metastore-tunnel`           | No       | See metastore tunnel configuration values below. |
| `federated-meta-stores[n].mapped-databases`           | No       | List of databases to federate from this federated metastore, all other databases will be ignored. Used in conjunction with _waggle-dance-server.yml_:`database-resolution:MANUAL`. |

The table below describes the metastore tunnel configuration values:

| Property                                                | Required | Description |
|:----|:----:|:----|
| `*.metastore-tunnel.localhost`                          | No       | The address on which to bind the local end of the tunnel. Default is '`localhost`'. |
| `*.metastore-tunnel.port`                               | No       | The port on which SSH runs on the remote node. Default is `22`. |
| `*.metastore-tunnel.route`                              | No       | A SSH tunnel can be used to connect to federated metastores. The tunnel may consist of one or more hops which must be declared in this property. See [Configuring a SSH tunnel](#configuring-a-ssh-tunnel) for details. |
| `*.metastore-tunnel.known-hosts`                        | No       | Path to a known hosts file. |
| `*.metastore-tunnel.private-keys`                       | No       | A comma-separated list of paths to any SSH keys required in order to set up the SSH tunnel. |

###### Access Control

The primary metastore is the only proxied metastore that can be configured to have write access.
This is controlled by the property: `primary-meta-store.access-control-type` It can have the following values:

| Property                                              | Description |
|:----|:----|
| `READ_ONLY`                                           | Read only access, creation of databases and and update/alters or other data manipulation requests to the metastore are not allowed. |
| `READ_AND_WRITE_AND_CREATE`                           | Reads are allowed, writes are allowed on all databases, creating new databases is allowed. |
| `READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST`     | Reads are allowed, writes are allowed on database names listed in the `primary-meta-store.writable-database-white-list` property, creating new databases is allowed and they are added to the white-list automatically. |
| `READ_AND_WRITE_ON_DATABASE_WHITELIST`                | Reads are allowed, writes are allowed on database names listed in the `primary-meta-store.writable-database-white-list` property, creating new databases is not allowed. |


#### Configuring a SSH tunnel

Each federation in Waggle Dance can be configured to use a SSH tunnel to access a remote Hive metastore in cases where certain network restrictions prevent a direct connection from the machine running Waggle Dance to the machine running the Thrift Hive metastore service. A SSH tunnel consists of one or more hops or jump-boxes. The connection between each pair of nodes requires a user - which if not specified defaults to the current user - and a private key to establish the SSH connection.

As outlined above the `metastore-tunnel` property is used to configure Waggle Dance to use a tunnel. The tunnel `route` expression is described with the following <a href="https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_Form">EBNF</a>:

    path = path part, {"->", path part}
    path part = {user, "@"}, hostname
    user = ? user name ?
    hostname = ? hostname ?

For example, if the Hive metastore runs on the host _hive-server-box_ which can only be reached first via _bastion-host_ and then _jump-box_ then the SSH tunnel route expression will be `bastion-host -> jump-box -> hive-server-box`. If _bastion-host_ is only accessible by user _ec2-user_, _jump-box_ by user _user-a_ and _hive-server-box_ by user _hadoop_ then the expression above becomes `ec2-user@bastion-host -> user-a@jump-box -> hadoop@hive-server-box`.

Once the tunnel is established Waggle Dance will set up port forwarding from the local machine specified in `metastore-tunnel.localhost` to the remote machine specified in `remote-meta-store-uris`. The last node in the tunnel expression doesn't need to be the Thrift server, the only requirement is that the this last node must be able to communicate with the Thrift service. Sometimes this is not possible due to firewall restrictions so in these cases they must be the same.

Note that all the machines in the tunnel expression must be included in the *known_hosts* file and the keys required to access each box must be set in `metastore-tunnel.private-keys`. For example, if _bastion-host_ is authenticated with _bastion.pem_ and both _jump-box_ and _hive-server-box_ are authenticated with _emr.pem_ then the property must be set as`metastore-tunnel.private-keys=<path-to-ssh-keys>/bastion.pem, <path-to-ssh-keys>/emr.pem`.

The following configuration snippets show a few examples of valid tunnel expressions.

##### Simple tunnel to metastore server

        remote-meta-store-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: user@metastore.domain
          private-keys: /home/user/.ssh/user-key-pair.pem
          known-hosts: /home/user/.ssh/known_hosts

##### Simple tunnel to cluster node with current user

        remote-meta-store-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: cluster-node.domain
          private-keys: /home/run-as-user/.ssh/key-pair.pem
          known-hosts: /home/run-as-user/.ssh/known_hosts

##### Bastion host to cluster node with different users and key-pairs

        remote-meta-store-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: bastionuser@bastion-host.domain -> user@cluster-node.domain
          private-keys: /home/run-as-user/.ssh/bastionuser-key-pair.pem, /home/run-as-user/.ssh/user-key-pair.pem
          known-hosts: /home/run-as-user/.ssh/known_hosts

##### Bastion host to cluster node with same user

        remote-meta-store-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: user@bastion-host.domain -> user@cluster-node.domain
          private-keys: /home/user/.ssh/user-key-pair.pem
          known-hosts: /home/user/.ssh/known_hosts

##### Bastion host to cluster node with current user

        remote-meta-store-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: bastion-host.domain -> cluster-node.domain
          private-keys: /home/run-as-user/.ssh/run-as-user-key-pair.pem
          known-hosts: /home/run-as-user/.ssh/known_hosts

##### Bastion host to metastore via jump-box with different users and key-pairs

        remote-meta-store-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: bastionuser@bastion-host.domain -> user@jump-box.domain -> hive@metastore.domain
          private-keys: /home/run-as-user/.ssh/bastionuser-key-pair.pem, /home/run-as-user/.ssh/user-key-pair.pem, /home/run-as-user/.ssh/hive-key-pair.pem
          known-hosts: /home/run-as-user/.ssh/known_hosts


#### Metrics

Waggle Dance exposes a set of metrics that can be accessed on the `/metrics` end-point. These metrics include a few standard JVM and Spring plus per-federation metrics which include per-metastore number of calls and invocation duration. If a Graphite server is provided in the server configuration then all the metrics will be exposed in the endpoint and Graphite.

The following snippet shows a typical Graphite configuration:

    graphite:
      port: 2003
      host: graphite.domain
      prefix: aws.myservice.myapplication
      poll-interval: 1000
      poll-interval-time-unit: MILLISECONDS

| Property                                              | Required | Description |
|:----|:----:|:----|
| `graphite.port`                                       | No       | Port where Graphite listens for metrics. Defaults to `2003`. |
| `graphite.host`                                       | No       | Hostname of the Graphite server. If not specified then no metrics will be sent to Graphite. |
| `graphite.prefix`                                     | No       | Graphite path prefix. |
| `graphite.poll-time`                                  | No       | Amount of time between Graphite polls. Defaults to `5000`. |
| `graphite.poll-time-unit`                             | No       | Time unit of `graphite.poll-time` - this is [the list of allowed values](https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/TimeUnit.html). Defaults to `MILLISECONDS`. |

#### Database Resolution

Waggle Dance presents a view over multiple (federated) Hive metastores and therefore could potentially encounter the same database in different metastores. Waggle Dance has two ways of resolving this situation, the choice of which can be configured in _waggle-dance-server.yml_ via the property `database-resolution`. This property can have two possible values `MANUAL` and `PREFIXED`. These are explained below in more detail.

##### Database resolution: `MANUAL`

Waggle Dance can be configured to use a static list of databases in the configuration _waggle-dance-federations.yml_:`federated-meta-stores[n].mapped-databases`. It is up to the user to make sure there are no conflicting database names with the primary-metastore or other federated metastores. If Waggle Dance encounters a duplicate database it will throw an error and won't start. Example configuration:

_waggle-dance-server.yml_:

    database-resolution: MANUAL

_waggle-dance-federation.yml_:

    primary-meta-store:
      name: primary
      remote-meta-store-uris: thrift://primaryLocalMetastore:9083
    federated-meta-stores:
      - name: waggle_prod
        remote-meta-store-uris: thrift://federatedProdMetastore:9083
        mapped-databases:
        - etldata
          mydata

Using this example Waggle Dance can be used to access all databases in the primary metastore and `etldata`/`mydata` from the federated metastore. The databases listed must not be present in the primary metastore otherwise Waggle Dance will throw an error on start up. If you have multiple federated metastores listed a database can only be uniquely configured for one metastore. Following the example configuration a query `select * from etldata` will be resolved to the federated metastore. Any database that is not mapped in the config is assumed to be in the primary metastore.

All non-mapped databases of a federated metastore are ignored and are not accessible.

Adding a mapped database in the configuration requires a restart of the Waggle Dance service in order to detect the new database name and to ensure that there are no clashes.

NOTE: in the case of manual database resolution the configuration still requires a unique prefix per metastore, this is used internally.

##### Database resolution: `PREFIXED`

Waggle Dance can be configured to use a prefix when resolving the names of databases in its primary or federated metastores. All queries issued to Waggle Dance need to use fully qualified database names and the database names need to use the same prefixes configured here.

_waggle-dance-server.yml_:

    database-resolution: PREFIXED

_waggle-dance-federation.yml_:

    primary-meta-store:
      name: primary
      remote-meta-store-uris: thrift://primaryLocalMetastore:9083
    federated-meta-stores:
      - name: waggle_prod
        remote-meta-store-uris: thrift://federatedProdMetastore:9083

Using this example Waggle Dance will prefix all databases and will require the prefix to be present in queries in order to map to correct metastores.
The query: `select * from waggle_prod_etldata` will effectively be this query: `select * from etldata` on the federated metastore. If a database is encountered that is not prefixed the primary metastore is used to resolve the database name. Any duplicate database name is made unique by prefixing it.

Newly created databases are immediately accessible - no service restart is necessary.


## Sample run through

Assumes database resolution is done by adding prefixes. If database resolution is done manually via the a list of configured databases the prefixes in this example can be ommitted.

##### Connect to Waggle Dance:

    hive --hiveconf hive.metastore.uris=thrift://localhost:48869

##### Show databases in all your metastores:

    hive> show databases;
    OK
    default
    somedata
    waggle_aws_dw_default
    waggle_aws_dw_mydata
    waggle_aws_dw_moredata
    waggle_aws_dw_extredata
    Time taken: 0.827 seconds, Fetched: 6 row(s)

##### Join two tables in different metastores:

    select h.data_id, h.entity_id, p.entity_id, p.hotel_brand_name
      from waggle_aws_dw_mydata.some_data h
      join somedata.other_table p
        on h.entity_id = p.entity_id
     where h.date = '2016-05-13'
       and h.hour = 1
    ;


## Notes

 * Only the metadata communications are rerouted.
 * Access to underlying table data is still directly to the locations encoded in the metadata.
 * Users of Waggle Dance must still have the relevant authority to access the underlying table data.
 * All data processing occurs in the client cluster, not the external clusters. Data is simply pulled into the client cluster that connect to Waggle Dance.
 * Metadata read operations are routed only. Write and destructive operations can be performed on the local metastore only.
 * When using Spark to read tables with a big number of partitions it may be necessary to set `spark.sql.hive.metastorePartitionPruning=true` to enable partition pruning. If this property is `false` Spark will try to fetch all the partitions of the tables in the query which may result on a `OutOfMemoryError` in Waggle Dance.


# Credits

Created by [Elliot West](https://github.com/teabot), [Patrick Duin](https://github.com/patduin) & [Daniel del Castillo](https://github.com/ddcprg) with thanks to: [Adrian Woodhead](https://github.com/massdosage), [Dave Maughan](https://github.com/nahguam) and [James Grant](https://github.com/Noddy76).


# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016-2017 Expedia Inc.

