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
package com.hotels.bdp.waggledance.parse;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;

public class ASTNodeUtils {

  public static List<ASTNode> getChildren(ASTNode pt) {
    List<ASTNode> rt = new ArrayList<>();
    List<Node> children = pt.getChildren();
    if (children != null) {
      for (Node child : pt.getChildren()) {
        rt.add((ASTNode) child);
      }
    }
    return rt;
  }

  public static List<ASTNode> getChildrenWithTypes(ASTNode pt, int type) {
    List<ASTNode> children = new ArrayList<>();
    for (ASTNode node : getChildren(pt)) {
      if (node.getType() == type) {
        children.add(node);
      }
    }
    return children;
  }

  public static ASTNode replaceNode(ASTNode currentNode, ASTNode nodeToReplace, ASTNode newNode) {
    Token treeToken = currentNode.getToken();
    Token replaceNodeToken = nodeToReplace.getToken();

    if ((treeToken.getType() == replaceNodeToken.getType())
        && treeToken.getTokenIndex() == replaceNodeToken.getTokenIndex()) {

      if (currentNode.getParent() != null) { // if its not the root node
        Tree parentNode = currentNode.getParent();
        int nodeIndex = currentNode.getChildIndex();
        newNode.setParent(parentNode);
        parentNode.replaceChildren(nodeIndex, nodeIndex, newNode);
      }
      List<Node> childNodes = currentNode.getChildren();
      if (childNodes != null) {
        newNode.addChildren(childNodes);
        for (Node child : childNodes) {
          ((ASTNode) child).setParent(newNode);
        }
      }
    } else {
      List<Node> childNodes = currentNode.getChildren();
      if (childNodes != null) {
        for (Node child : childNodes) {
          replaceNode((ASTNode) child, nodeToReplace, newNode);
        }
      }
    }
    return currentNode;
  }

  public static ASTNode getRoot(ASTNode ast) {
    List<? extends Tree> ancestors = ast.getAncestors();
    if (ancestors != null) {
      ASTNode node = (ASTNode) ancestors.get(0);
      if (node.getToken() == null && node.getChildCount() > 0) {
        ast = (ASTNode) node.getChild(0);
      }
    }
    return ast;
  }
}
