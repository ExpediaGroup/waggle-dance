/**
 * Copyright (C) 2016-2018 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.waggledance.aws.glue.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

final class AwsGlueHive2Shims implements AwsGlueHiveShims {
  private static final String HIVE_2_VERSION = "2.";

  AwsGlueHive2Shims() {}

  static boolean supportsVersion(String version) {
    return version.startsWith("2.");
  }

  public ExprNodeGenericFuncDesc getDeserializeExpression(byte[] exprBytes) {
    return org.apache.hadoop.hive.ql.exec.SerializationUtilities.deserializeExpressionFromKryo(exprBytes);
  }

  public byte[] getSerializeExpression(ExprNodeGenericFuncDesc expr) {
    return org.apache.hadoop.hive.ql.exec.SerializationUtilities.serializeExpressionToKryo(expr);
  }

  public org.apache.hadoop.fs.Path getDefaultTablePath(Database db, String tableName, Warehouse warehouse)
    throws org.apache.hadoop.hive.metastore.api.MetaException {
    return warehouse.getDefaultTablePath(db, tableName);
  }

  public boolean validateTableName(String name, Configuration conf) {
    return MetaStoreUtils.validateName(name, conf);
  }

  public boolean requireCalStats(
      Configuration conf,
      Partition oldPart,
      Partition newPart,
      Table tbl,
      EnvironmentContext environmentContext) {
    return MetaStoreUtils.requireCalStats(conf, oldPart, newPart, tbl, environmentContext);
  }

  public boolean updateTableStatsFast(
      Database db,
      Table tbl,
      Warehouse wh,
      boolean madeDir,
      boolean forceRecompute,
      EnvironmentContext environmentContext)
    throws org.apache.hadoop.hive.metastore.api.MetaException {
    return MetaStoreUtils.updateTableStatsFast(db, tbl, wh, madeDir, forceRecompute, environmentContext);
  }
}
