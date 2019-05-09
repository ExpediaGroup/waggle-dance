/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.waggledance.stubs;

import java.util.ArrayList;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;

import com.google.common.collect.Lists;

public final class HiveStubs {

  private HiveStubs() {}

  public static Function newFunction(String databaseName, String functionName) {
    ArrayList<ResourceUri> resourceUris = Lists
        .newArrayList(new ResourceUri(ResourceType.JAR, "hdfs://path/to/my/jar/my.jar"));
    Function function = new Function(functionName, databaseName, "com.hotels.hive.FN", "hadoop", PrincipalType.USER, 0,
        FunctionType.JAVA, resourceUris);
    return function;
  }

}
