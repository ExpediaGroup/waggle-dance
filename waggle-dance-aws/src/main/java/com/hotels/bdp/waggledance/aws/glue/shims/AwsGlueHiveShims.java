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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

public abstract interface AwsGlueHiveShims
{
  public abstract ExprNodeGenericFuncDesc getDeserializeExpression(byte[] paramArrayOfByte);

  public abstract byte[] getSerializeExpression(ExprNodeGenericFuncDesc paramExprNodeGenericFuncDesc);

  public abstract Path getDefaultTablePath(Database paramDatabase, String paramString, Warehouse paramWarehouse)
      throws MetaException;

  public abstract boolean validateTableName(String paramString, Configuration paramConfiguration);

  public abstract boolean requireCalStats(Configuration paramConfiguration, Partition paramPartition1, Partition paramPartition2, Table paramTable, EnvironmentContext paramEnvironmentContext);

  public abstract boolean updateTableStatsFast(Database paramDatabase, Table paramTable, Warehouse paramWarehouse, boolean paramBoolean1, boolean paramBoolean2, EnvironmentContext paramEnvironmentContext)
      throws MetaException;
}
