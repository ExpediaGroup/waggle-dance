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
package com.hotels.bdp.waggledance.server.glue.converter.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import com.hotels.bdp.waggledance.server.glue.shims.ShimsLoader;

public final class ExpressionHelper {
  private static final String HIVE_STRING_TYPE_NAME = "string";
  private static final String HIVE_IN_OPERATOR = "IN";
  private static final String HIVE_NOT_IN_OPERATOR = "NOT IN";
  private static final String HIVE_NOT_OPERATOR = "not";
  private static final Logger logger = Logger.getLogger(ExpressionHelper.class);

  private static final List<String> QUOTED_TYPES = ImmutableList.of("string", "char", "varchar", "date", "datetime",
      "timestamp");
  private static final Joiner JOINER = Joiner.on(" AND ");

  public ExpressionHelper() {}

  public static String convertHiveExpressionToCatalogExpression(byte[] exprBytes) throws MetaException {
    ExprNodeGenericFuncDesc exprTree = deserializeExpr(exprBytes);
    Set<String> columnNamesInNotInExpression = Sets.newHashSet();
    fieldEscaper(exprTree.getChildren(), exprTree, columnNamesInNotInExpression);
    String expression = rewriteExpressionForNotIn(exprTree.getExprString(), columnNamesInNotInExpression);
    return expression;
  }

  private static ExprNodeGenericFuncDesc deserializeExpr(byte[] exprBytes) throws MetaException {
    ExprNodeGenericFuncDesc expr = null;
    try {
      expr = ShimsLoader.getHiveShims().getDeserializeExpression(exprBytes);
    } catch (Exception ex) {
      logger.error("Failed to deserialize the expression", ex);
      throw new MetaException(ex.getMessage());
    }
    if (expr == null) {
      throw new MetaException("Failed to deserialize expression - ExprNodeDesc not present");
    }
    return expr;
  }

  private static void fieldEscaper(
      List<ExprNodeDesc> exprNodes,
      ExprNodeDesc parent,
      Set<String> columnNamesInNotInExpression) {
    if ((exprNodes == null) || (exprNodes.isEmpty())) {
      return;
    }
    for (ExprNodeDesc nodeDesc : exprNodes) {
      String nodeType = nodeDesc.getTypeString().toLowerCase();
      if (QUOTED_TYPES.contains(nodeType)) {
        PrimitiveTypeInfo tInfo = new PrimitiveTypeInfo();
        tInfo.setTypeName("string");
        nodeDesc.setTypeInfo(tInfo);
      }
      addColumnNamesOfNotInExpressionToSet(nodeDesc, parent, columnNamesInNotInExpression);
      fieldEscaper(nodeDesc.getChildren(), nodeDesc, columnNamesInNotInExpression);
    }
  }

  private static void addColumnNamesOfNotInExpressionToSet(
      ExprNodeDesc childNode,
      ExprNodeDesc parentNode,
      Set<String> columnsInNotInExpression) {
    if ((parentNode != null)
        && (childNode != null)
        && ((parentNode instanceof ExprNodeGenericFuncDesc))
        && ((childNode instanceof ExprNodeGenericFuncDesc))) {
      ExprNodeGenericFuncDesc parentFuncNode = (ExprNodeGenericFuncDesc) parentNode;
      ExprNodeGenericFuncDesc childFuncNode = (ExprNodeGenericFuncDesc) childNode;
      if (((parentFuncNode.getGenericUDF() instanceof GenericUDFOPNot))
          && ((childFuncNode.getGenericUDF() instanceof GenericUDFIn))) {
        columnsInNotInExpression.addAll(childFuncNode.getCols());
      }
    }
  }

  private static String rewriteExpressionForNotIn(String expression, Set<String> columnsInNotInExpression) {
    for (String columnName : columnsInNotInExpression) {
      if (columnName != null) {
        String hiveExpression = getHiveCompatibleNotInExpression(columnName);
        hiveExpression = escapeParentheses(hiveExpression);
        String catalogExpression = getCatalogCompatibleNotInExpression(columnName);
        catalogExpression = escapeParentheses(catalogExpression);
        expression = expression.replaceAll(hiveExpression, catalogExpression);
      }
    }
    return expression;
  }

  private static String getHiveCompatibleNotInExpression(String columnName) {
    return String.format("%s (%s) %s (", new Object[] { "not", columnName, "IN" });
  }

  private static String getCatalogCompatibleNotInExpression(String columnName) {
    return String.format("(%s) %s (", new Object[] { columnName, "NOT IN" });
  }

  private static String escapeParentheses(String expression) {
    expression = expression.replaceAll("\\(", "\\\\\\(");
    expression = expression.replaceAll("\\)", "\\\\\\)");
    return expression;
  }

  public static String buildExpressionFromPartialSpecification(Table table, List<String> partitionValues)
    throws MetaException {
    List<FieldSchema> partitionKeys = table.getPartitionKeys();

    if ((partitionValues == null) || (partitionValues.isEmpty())) {
      return null;
    }

    if ((partitionKeys == null) || (partitionValues.size() > partitionKeys.size())) {
      throw new MetaException("Incorrect number of partition values: " + partitionValues);
    }

    partitionKeys = partitionKeys.subList(0, partitionValues.size());
    List<String> predicates = new LinkedList();
    for (int i = 0; i < partitionValues.size(); i++) {
      if (!Strings.isNullOrEmpty((String) partitionValues.get(i))) {
        predicates.add(buildPredicate((FieldSchema) partitionKeys.get(i), (String) partitionValues.get(i)));
      }
    }

    return JOINER.join(predicates);
  }

  private static String buildPredicate(FieldSchema schema, String value) {
    if (isQuotedType(schema.getType())) {
      return String.format("(%s='%s')", new Object[] { schema.getName(), escapeSingleQuotes(value) });
    }
    return String.format("(%s=%s)", new Object[] { schema.getName(), value });
  }

  private static String escapeSingleQuotes(String s) {
    return s.replaceAll("'", "\\\\'");
  }

  private static boolean isQuotedType(String type) {
    return QUOTED_TYPES.contains(type);
  }

  public static String replaceDoubleQuoteWithSingleQuotes(String s) {
    return s.replaceAll("\"", "'");
  }
}
