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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public enum ASTQueryMapping implements QueryMapping {

  INSTANCE;

  private static final String PRESTO_VIEW_MARKER = "/* Presto View";
  private final static String RE_WORD_BOUNDARY = "\\b";
  private final static Comparator<CommonToken> ON_START_INDEX = Comparator.comparingInt(CommonToken::getStartIndex);

  @Override
  public String transformOutboundDatabaseName(MetaStoreMapping metaStoreMapping, String query) {
    if (hasMarker(query)) {
      // skipping view that are not "Hive" views queries. We can't parse those at this moment.
      return query;
    }
    ASTNode root;
    try {
      root = ParseUtils.parse(query);
    } catch (ParseException e) {
      throw new WaggleDanceException("Can't parse query: '" + query + "'", e);
    }

    StringBuilder result = transformDatabaseTableTokens(metaStoreMapping, root, query);
    transformFunctionTokens(metaStoreMapping, root, result);
    return result.toString();
  }

  boolean hasMarker(String query) {
    if (query != null && query.trim().startsWith(PRESTO_VIEW_MARKER)) {
      return true;
    }
    return false;
  }

  private StringBuilder transformDatabaseTableTokens(MetaStoreMapping metaStoreMapping, ASTNode root, String query) {
    StringBuilder result = new StringBuilder();
    SortedSet<CommonToken> dbNameTokens = extractDbNameTokens(root);
    int startIndex = 0;
    for (CommonToken dbNameNode : dbNameTokens) {
      final String dbName = dbNameNode.getText();
      final boolean escaped = dbName.startsWith("`") && dbName.endsWith("`");
      String transformedDbName = metaStoreMapping.transformOutboundDatabaseName(unescapeIdentifier(dbName));
      if (escaped) {
        transformedDbName = "`" + transformedDbName + "`";
      }
      result.append(query, startIndex, dbNameNode.getStartIndex());
      result.append(transformedDbName);
      startIndex = dbNameNode.getStopIndex() + 1;
    }
    result.append(query.substring(startIndex));
    return result;
  }

  private void transformFunctionTokens(MetaStoreMapping metaStoreMapping, ASTNode root, StringBuilder result) {
    // Done differently from the extractDbNameTokens as the Function tokens do not contain a correct start index. We'll
    // have to fall back to search and replace.
    List<CommonToken> functionTokens = extractFunctionTokens(root);
    for (CommonToken functionNode : functionTokens) {
      final String functionName = functionNode.getText();
      Pattern pattern = Pattern.compile(RE_WORD_BOUNDARY + functionName + RE_WORD_BOUNDARY);
      Matcher matcher = pattern.matcher(result);
      if (matcher.find()) {
        int index = matcher.start();
        String prefix = metaStoreMapping.getDatabasePrefix();
        result.replace(index, index, prefix);
      }
    }
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

  private List<CommonToken> extractFunctionTokens(ASTNode root) {
    List<CommonToken> tokens = new ArrayList<>();
    Stack<ASTNode> stack = new Stack<>();
    stack.push(root);

    while (!stack.isEmpty()) {
      ASTNode current = stack.pop();
      for (ASTNode child : getChildren(current)) {
        stack.push(child);
      }
      if (current.getType() == HiveParser.TOK_FUNCTION) {
        if (current.getChildCount() == 1 && childrenAreIdentifiers(current)) {
          // TOK_FUNCTION has one child, <dbName.functionName>.
          CommonToken dbNameDotFunctionNameNode = (CommonToken) ((ASTNode) current.getChild(0)).getToken();
          tokens.add(dbNameDotFunctionNameNode);
        }
      }
    }
    return tokens;
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
