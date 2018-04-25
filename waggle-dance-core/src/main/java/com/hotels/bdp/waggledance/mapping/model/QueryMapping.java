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
package com.hotels.bdp.waggledance.mapping.model;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.unescapeIdentifier;

import static com.hotels.bdp.waggledance.parse.ASTNodeUtils.getChildren;
import static com.hotels.bdp.waggledance.parse.ASTNodeUtils.getRoot;
import static com.hotels.bdp.waggledance.parse.ASTNodeUtils.replaceNode;

import java.util.Stack;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Token;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.parse.ASTConverter;

public enum  QueryMapping {

  INSTANCE;

  public String transformOutboundDatabaseName(MetaStoreMapping metaStoreMapping, String query) {
    ASTNode root;
    try {
      root = ParseUtils.parse(query);
    } catch (ParseException e) {
      throw new WaggleDanceException(e.getMessage());
    }

    Stack<ASTNode> stack = new Stack<>();
    stack.push(root);
    while (!stack.isEmpty()) {
      ASTNode current = stack.pop();
      for (ASTNode child : getChildren(current)) {
        stack.push(child);
      }

      if (current.getType() == HiveParser.TOK_TABNAME) {
        if (current.getChildCount() == 2) {
          // First child of TOK_TABNAME node is the database name node
          ASTNode dbNameNode = (ASTNode) current.getChild(0);
          final String dbName = dbNameNode.getText();
          final boolean escaped = dbName.startsWith("`") && dbName.endsWith("`");
          String transformedDbName = metaStoreMapping.transformOutboundDatabaseName(unescapeIdentifier(dbName));
          if (escaped) {
            transformedDbName = "`" + transformedDbName + "`";
          }

          Token token = new CommonToken(dbNameNode.getType(), transformedDbName);
          ASTNode newNode = new ASTNode(token);
          replaceNode(getRoot(dbNameNode), dbNameNode, newNode);
        }
        // Otherwise TOK_TABNAME node only has one child which contains just the table name
      }
    }

    ASTConverter converter = new ASTConverter(false);
    query = converter.treeToQuery(root);
    return query;
  }
}
