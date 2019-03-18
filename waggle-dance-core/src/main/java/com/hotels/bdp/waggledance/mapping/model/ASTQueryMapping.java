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
package com.hotels.bdp.waggledance.mapping.model;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.unescapeIdentifier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public enum ASTQueryMapping implements QueryMapping {

  INSTANCE;

  private final static Comparator<CommonToken> ON_START_INDEX = new Comparator<CommonToken>() {

    @Override
    public int compare(CommonToken o1, CommonToken o2) {
      return Integer.compare(o1.getStartIndex(), o2.getStartIndex());
    }

  };

  @Override
  public String transformOutboundDatabaseName(MetaStoreMapping metaStoreMapping, String query) {
    ASTNode root;
    try {
      root = ParseUtils.parse(query);
    } catch (ParseException e) {
      throw new WaggleDanceException("Can't parse query: '" + query + "'", e);
    }

    SortedSet<CommonToken> dbNameTokens = extractDbNameTokens(root);

    StringBuilder result = new StringBuilder();
    int startIndex = 0;
    for (CommonToken dbNameNode : dbNameTokens) {
      final String dbName = dbNameNode.getText();
      final boolean escaped = dbName.startsWith("`") && dbName.endsWith("`");
      String transformedDbName = metaStoreMapping.transformOutboundDatabaseName(unescapeIdentifier(dbName));
      if (escaped) {
        transformedDbName = "`" + transformedDbName + "`";
      }
      result.append(query.substring(startIndex, dbNameNode.getStartIndex()));
      result.append(transformedDbName);
      startIndex = dbNameNode.getStopIndex() + 1;
    }
    result.append(query.substring(startIndex));
    return result.toString();
  }

  private SortedSet<CommonToken> extractDbNameTokens(ASTNode root) {
    SortedSet<CommonToken> dbNameTokens = new TreeSet<>(ON_START_INDEX);
    Stack<ASTNode> stack = new Stack<>();
    stack.push(root);

    while (!stack.isEmpty()) {
      ASTNode current = stack.pop();
      for (ASTNode child : getChildren(current)) {
        stack.push(child);
      }
      if (current.getType() == HiveParser.TOK_TABNAME) {
        if (current.getChildCount() == 2 && childrenAreIdentifiers(current)) {
          // First child of TOK_TABNAME node is the database name node
          CommonToken dbNameNode = (CommonToken) ((ASTNode) current.getChild(0)).getToken();
          dbNameTokens.add(dbNameNode);
        }
        // Otherwise TOK_TABNAME node only has one child which contains just the table name
      }
    }
    return dbNameTokens;
  }

  private boolean childrenAreIdentifiers(ASTNode current) {
    for (ASTNode child : getChildren(current)) {
      if (child.getType() != HiveParser.Identifier) {
        return false;
      }
    }
    return true;
  }

  private static List<ASTNode> getChildren(ASTNode pt) {
    List<ASTNode> rt = new ArrayList<>();
    List<Node> children = pt.getChildren();
    if (children != null) {
      for (Node child : pt.getChildren()) {
        rt.add((ASTNode) child);
      }
    }
    return rt;
  }

}
