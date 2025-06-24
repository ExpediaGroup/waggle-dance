package com.hotels.bdp.waggledance.client.compatibility;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.thrift.TException;


/**
 * This interface contains methods that are missing from Hive 2.x.x but added in Hive 3.x.x
 * https://github.com/apache/hive/blob/rel/release-3.1.3/standalone-metastore/src/main/java/org/apache/hadoop/hive/metastore/IMetaStoreClient.java
 */
public interface HiveThriftMetaStoreIfaceCompatibility2x {

  void create_table_with_constraints(
      Table tbl,
      List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException;

  void truncate_table(String dbName, String tableName, List<String> partNames) throws MetaException, TException;

  void add_unique_constraint(AddUniqueConstraintRequest addUniqueConstraintRequest)
    throws NoSuchObjectException, MetaException, TException;

  void add_not_null_constraint(AddNotNullConstraintRequest addNotNullConstraintRequest)
    throws NoSuchObjectException, MetaException, TException;

  void add_default_constraint(AddDefaultConstraintRequest addDefaultConstraintRequest)
    throws NoSuchObjectException, MetaException, TException;

  void add_check_constraint(AddCheckConstraintRequest addCheckConstraintRequest)
    throws NoSuchObjectException, MetaException, TException;

  UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest uniqueConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException;

  NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest notNullConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException;

  DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest defaultConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException;

  CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest checkConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException;

}
