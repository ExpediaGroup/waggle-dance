/**
 * Copyright (C) 2016-2018 Expedia Inc.
 *
 * Based on {@code com.flaminem.flamy} from Flamy 0.7.0
 *
 * https://github.com/flaminem/flamy
 *
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

import static com.hotels.bdp.waggledance.parse.ASTNodeUtils.getChildren;
import static com.hotels.bdp.waggledance.parse.ASTNodeUtils.getChildrenWithTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.http.annotation.Experimental;

import com.sun.tools.javac.util.Pair;

import com.hotels.bdp.waggledance.parse.Rule.RuleSet;
import com.hotels.bdp.waggledance.parse.Rule.SingleRule;

/**
 * There will probably be some statements on which conversions using this class fails.
 **/
@Experimental
public class ASTConverter {

  private String indent = " ";
  private boolean splitMultiInsert;

  public ASTConverter(boolean splitMultiInsert) {
    this.splitMultiInsert = splitMultiInsert;
  }

  List<String> recurseChildren(ASTNode pt, String prefix) {
    return recTransform(getChildren(pt), prefix);
  }

  List<String> recurseChildren(ASTNode pt) {
    String prefix = "";
    return recTransform(getChildren(pt), prefix);
  }

  private String formatCaseWhen(List<ASTNode> children, String prefix) {
    int i = 0;
    String elseBlock = "";
    if (children.size() % 2 != 0) {
      elseBlock = "\n" + prefix + indent + "ELSE " + recTransform(children.get(children.size() - 1));
      children = dropRight(children, 1); // removes the else block from the list
    }
    List<String> newChildren = new ArrayList<>();
    for (ASTNode child : children) { // go through all the children
      if (i % 2 == 0) { // if i is an even number
        newChildren.add("\n" + prefix + indent + "WHEN " + recTransform(child));
      } else {
        newChildren.add(prefix + " THEN " + recTransform(child));
      }
      i++;
    }
    newChildren.add(elseBlock);
    return mkString(newChildren, "", "", "\n" + prefix + "END\n");
  }

  /**
   * Formats {@codeb [NOT] BETWEEN a AND c} (probably!)
   *
   * <pre>
   * {@code
   *  TOK_FUNCTION
   *  ├──between
   *  ├──[ KW_FALSE | KW_TRUE ]
   *  ├──TOK_TABLE_OR_COL
   *  │  └──b
   *  ├──TOK_TABLE_OR_COL
   *  │  └──a
   *  └──TOK_TABLE_OR_COL
   *     └──c
   *  if KW_TRUE, then this means NOT BETWEEN
   *  }
   * </pre>
   *
   * Original scala
   *
   * <pre>
   * children.drop(3).map {recTransform(_)}.mkString(recTransform(children.get(2)) + not + " BETWEEN ", " AND ", "")
   *
   * <pre>
   */
  private String formatBetween(String prefix, List<ASTNode> children) {
    String not = "";
    if (children.get(1).getType() == HiveParser.KW_TRUE) {
      not = " NOT";
    }
    return mkString(recTransform(dropLeft(children, 3)), recTransform(children.get(2)) + not + " BETWEEN ", " AND ",
        "");
  }

  private String formatFunction(String prefix, boolean distinct, ASTNode pt) {
    List<ASTNode> children = getChildren(pt);

    switch (children.get(0).getType()) {
    case HiveParser.TOK_ISNULL:
      return recTransform(children.get(1)) + " IS NULL";
    case HiveParser.TOK_ISNOTNULL:
      return recTransform(children.get(1)) + " IS NOT NULL";
    case HiveParser.KW_CASE:
      return "CASE " + recTransform(children.get(1)) + formatCaseWhen(dropLeft(children, 2), prefix);
    case HiveParser.KW_WHEN:
      return "\nCASE " + formatCaseWhen(dropLeft(children, 1), prefix);

    case HiveParser.KW_IN:
      return inFunctionHandler(pt, prefix);
    case HiveParser.Identifier:
      String nodeText = children.get(0).getText();
      if (nodeText.equalsIgnoreCase("between")) {
        return formatBetween(prefix, children);
      }
      if (nodeText.equalsIgnoreCase("in")) {
        return inFunctionHandler(pt, prefix); // catcher for when 'in' isnt recognised as KW_IN
      }
      return defaultFormatFunction(pt, prefix, children);
    case HiveParser.TOK_TINYINT:
      return "CAST(" + recTransform(children.get(1)) + " AS TINYINT)";
    case HiveParser.TOK_SMALLINT:
      return "CAST(" + recTransform(children.get(1)) + " AS SMALLINT)";
    case HiveParser.TOK_INT:
      return "CAST(" + recTransform(children.get(1)) + " AS INT)";
    case HiveParser.TOK_BIGINT:
      return "CAST(" + recTransform(children.get(1)) + " AS BIGINT)";
    case HiveParser.TOK_FLOAT:
      return "CAST(" + recTransform(children.get(1)) + " AS FLOAT)";
    case HiveParser.TOK_DOUBLE:
      return "CAST(" + recTransform(children.get(1)) + " AS DOUBLE)";
    case HiveParser.TOK_DECIMAL:
      return "CAST(" + recTransform(children.get(1)) + " AS DECIMAL)";
    case HiveParser.TOK_TIMESTAMP:
      return "CAST(" + recTransform(children.get(1)) + " AS TIMESTAMP)";
    case HiveParser.TOK_DATE:
      return "CAST(" + recTransform(children.get(1)) + " AS DATE)";
    case HiveParser.TOK_STRING:
      return "CAST(" + recTransform(children.get(1)) + " AS STRING)";
    case HiveParser.TOK_BOOLEAN:
      return "CAST(" + recTransform(children.get(1)) + " AS BOOLEAN)";
    case HiveParser.TOK_BINARY:
      return "CAST(" + recTransform(children.get(1)) + " AS BINARY)";

    default:
      String distinctString = "";
      if (distinct) {
        distinctString = "DISTINCT ";
      }
      String startString = children.get(0) + "(" + distinctString;
      return defaultFormatFunction(pt, startString, children);
    }
  }

  private String defaultFormatFunction(ASTNode pt, String startString, List<ASTNode> children) {
    String function = children.get(0).getText();
    return mkString(recTransform(dropLeft(children, 1)), function + "(", ", ", ")");
  }

  /**
   * handles the functionality of 'in'
   *
   * <pre>
   * ... where a in (1, 2)
   * ├──  └──
   * {@code
   * TOK_WHERE
   * [└── not]
   *    └──TOK_FUNCTION
   *        ├── in
   *        ├── TOK_TABLE_OR_COL
   *        |       └── a
   *        ├── 1
   *        └── 2
  </pre>
   */
  private String inFunctionHandler(ASTNode pt, String prefix) {
    List<ASTNode> children = getChildren(pt);
    if (pt.getParent().getType() == HiveParser.KW_NOT) {
      return mkString(recTransform(dropLeft(children, 2)), recTransform(children.get(1)) + " NOT IN (", ", ", ")");
    }
    return mkString(recTransform(dropLeft(children, 2)), recTransform(children.get(1)) + " IN (", ", ", ")");
  }

  /**
   * <pre>
   * {@code
   * TOK_LATERAL_VIEW
   * ├──TOK_SELECT
   * │  └──TOK_SELEXPR
   * │     ├──TOK_FUNCTION
   * │     │  ├──explode
   * │     │  └──TOK_TABLE_OR_COL
   * │     │     └──mymap
   * │     ├──k1
   * │     ├──v1
   * │     └──TOK_TABALIAS
   * │        └──LV1
   * └──TOK_TABREF
   *     ├──TOK_TABNAME
   *     |  ├──db2
   *     |  └──source
   *     └──T1
   *
  </pre>
   */
  private String formatLateralView(String prefix, ASTNode pt, String name) {
    ASTNode table = (ASTNode) pt.getChild(1);
    List<ASTNode> children = getChildren((ASTNode) pt.getChild(0).getChild(0));

    return recTransform(table)
        + prefix
        + name
        + " "
        + recTransform(children.get(0))
        + " "
        + recTransform(children.get(children.size() - 1))
        + mkString(recTransform(dropRight(dropLeft(children, 1), 1)), " as ", ", ", "");
  }

  private String formatJoin(String prefix, ASTNode pt, String join) {
    List<ASTNode> children = getChildren(pt);
    String on = "";
    if (children.size() > 2) {
      on = prefix + "ON " + recTransform(children.get(2), ""); // uncomment for no new line
    }
    if (children.get(1).getType() == HiveParser.TOK_SUBQUERY) {
      join += " (";
    }
    return mkString(recTransform(take(children, 2), prefix), "", prefix + join + " ", " ") + on; // uncomment for no
  }

  private String formatUnion(String prefix, ASTNode pt, String union) {
    List<ASTNode> children = getChildren(pt);
    return mkString(recTransform(take(children, 2), prefix), "", prefix + union, ""); // uncomment for no new line
  }

  private Pair<String, String> formatSubQuery(ASTNode pt, String prefix) {
    if (pt.getType() == HiveParser.TOK_SUBQUERY) {
      String alias = recTransform((ASTNode) pt.getChild(1));
      String firstPair = recTransform((ASTNode) pt.getChild(0), prefix + indent) + prefix + ")"; // uncomment for no
      return new Pair<>(firstPair, alias);
    }
    return new Pair<>("", "");
  }

  Rule defaultRule = new Rule() {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt, prefix), " ");
    }
  };

  /**
   * create [temporary] [external] table [if not exists] a as.. .. (name string, age int) OR as select a from b
   *
   * <pre>
   * {@code
   * TOK_CREATETABLE
   * ├── TOK_TABNAME
   * │      └── a
   * ├── [temporary]
   * ├── [external]
   * ├── [TOK_IFNOTEXISTS]
   * ├── [TOK_LIKETABLE]
   * └── ...
  </pre>
   */
  SingleRule tok_CREATETABLE = new SingleRule(HiveParser.TOK_CREATETABLE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      List<ASTNode> children = getChildren(pt);

      List<ASTNode> tok_tableName = getChildrenWithTypes(pt, HiveParser.TOK_TABNAME);

      List<ASTNode> tok_temporary = getChildrenWithTypes(pt, HiveParser.KW_TEMPORARY);

      List<ASTNode> tok_external = getChildrenWithTypes(pt, HiveParser.KW_EXTERNAL);

      List<ASTNode> tok_ifNotExists = getChildrenWithTypes(pt, HiveParser.TOK_IFNOTEXISTS);

      List<ASTNode> tok_likeTable = getChildrenWithTypes(pt, HiveParser.TOK_LIKETABLE);

      List<ASTNode> tok_tabColList = getChildrenWithTypes(pt, HiveParser.TOK_TABCOLLIST);

      List<ASTNode> tok_partitions = getChildrenWithTypes(pt, HiveParser.TOK_TABLEPARTCOLS);

      List<ASTNode> tok_tableSerializer = getChildrenWithTypes(pt, HiveParser.TOK_TABLESERIALIZER);

      List<ASTNode> tok_tableRowFormat = getChildrenWithTypes(pt, HiveParser.TOK_TABLEROWFORMAT);

      List<ASTNode> tok_tableFileFormat = getChildrenWithTypes(pt, HiveParser.TOK_TABLEFILEFORMAT);

      List<ASTNode> tok_fileFormatGeneric = getChildrenWithTypes(pt, HiveParser.TOK_FILEFORMAT_GENERIC);

      List<ASTNode> tok_location = getChildrenWithTypes(pt, HiveParser.TOK_TABLELOCATION);

      List<ASTNode> tok_tableProperties = getChildrenWithTypes(pt, HiveParser.TOK_TABLEPROPERTIES);

      List<ASTNode> tok_query = getChildrenWithTypes(pt, HiveParser.TOK_QUERY);

      List<ASTNode> nodes = new ArrayList<>();
      String keyword = "CREATE";

      if (tok_temporary.size() == 1 || children.get(1).getText().equalsIgnoreCase("temporary")) {
        keyword += " TEMPORARY";
      }
      if (tok_external.size() == 1
          || children.get(1).getText().equalsIgnoreCase("external")
          || children.get(2).getText().equalsIgnoreCase("external")) {
        keyword += " EXTERNAL";
      }
      keyword += " TABLE";
      if (tok_ifNotExists.size() == 1) {
        nodes.add(tok_ifNotExists.get(0));
      }

      nodes.addAll(tok_tableName);

      if (!tok_tabColList.isEmpty()) {
        nodes.addAll(tok_tabColList);
      }
      if (!tok_partitions.isEmpty()) {
        nodes.add(tok_partitions.get(0));
      }
      if (!tok_tableRowFormat.isEmpty()) {
        nodes.add(tok_tableRowFormat.get(0));
      } else if (!tok_tableSerializer.isEmpty()) {
        nodes.add(tok_tableSerializer.get(0));
      }

      if (!tok_fileFormatGeneric.isEmpty()) {
        nodes.add(tok_fileFormatGeneric.get(0));
      } else {
        if (!tok_tableFileFormat.isEmpty()) {
          nodes.add(tok_tableFileFormat.get(0));
        }
      }

      if (!tok_location.isEmpty()) {
        nodes.add(tok_location.get(0));
      }

      if (!tok_tableProperties.isEmpty()) {
        nodes.addAll(tok_tableProperties);
      }

      if (tok_likeTable.size() == 1 && !tok_query.isEmpty()) {
        nodes.add(tok_likeTable.get(0));
        nodes.add(tok_query.get(0));
      }

      return mkString(recTransform(nodes, prefix), keyword + indent, " ", "");
    }
  };

  SingleRule tok_LIKETABLE = new SingleRule(HiveParser.TOK_LIKETABLE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "AS";
    }
  };

  // ****
  SingleRule tok_TABLEPARTCOLS = new SingleRule(HiveParser.TOK_TABLEPARTCOLS) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "PARTITIONED BY ", " ", "");
    }
  };

  SingleRule tok_IFNOTEXISTS = new SingleRule(HiveParser.TOK_IFNOTEXISTS) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "IF NOT EXISTS";
    }
  };

  SingleRule tok_INSERT = new SingleRule(HiveParser.TOK_INSERT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      if (pt.getChild(0).getType() == HiveParser.TOK_INSERT_INTO) {
        return mkString(recurseChildren(pt), "", " ", "");
      } else {
        return mkString(recurseChildren(pt), "INSERT OVERWRITE ", " ", "");
      }
    }
  };

  /**
   * <pre>
   * {@code
   * TOK_TABCOLLIST
   * ├──TOK_TABCOL
   * │  └──a
   * │     └──TOK_STRING
   * └──TOK_TABCOL
   *    └──b
   *       └──TOK_INT
   *
  </pre>
   */
  SingleRule tok_TABCOLLIST = new SingleRule(HiveParser.TOK_TABCOLLIST) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "(", ", ", ")");

    }
  };

  SingleRule tok_TABCOL = new SingleRule(HiveParser.TOK_TABCOL) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "", " ", "");
    }
  };

  SingleRule tok_FILEFORMAT_GENERIC = new SingleRule(HiveParser.TOK_FILEFORMAT_GENERIC) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "STORED AS ", "", "");
    }
  };

  SingleRule tok_TABLEFILEFORMAT = new SingleRule(HiveParser.TOK_TABLEFILEFORMAT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "STORED AS INPUTFORMAT ", " OUTPUTFORMAT ", "");
    }
  };

  SingleRule tok_TABLELOCATION = new SingleRule(HiveParser.TOK_TABLELOCATION) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "LOCATION ", "", "");
    }
  };

  SingleRule tok_TABLESERIALIZER = new SingleRule(HiveParser.TOK_TABLESERIALIZER) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "ROW FORMAT SERDE ", " ", "");
    }
  };

  SingleRule tok_TABLEROWFORMAT = new SingleRule(HiveParser.TOK_TABLEROWFORMAT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "ROW FORMAT DELIMITED ", " ", "");
    }
  };

  SingleRule tok_TABLEROWFORMATFIELD = new SingleRule(HiveParser.TOK_TABLEROWFORMATFIELD) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "FIELDS TERMINATED BY ", " ", "\n");
    }
  };

  SingleRule tok_TABLEPROPERTIES = new SingleRule(HiveParser.TOK_TABLEPROPERTIES) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "TBLPROPERTIES (", " ", ")");
    }
  };

  SingleRule tok_TABLEPROPERTY = new SingleRule(HiveParser.TOK_TABLEPROPERTY) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " ", " = ", " ");
    }
  };

  SingleRule tok_INT = new SingleRule(HiveParser.TOK_INT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "INT";
    }
  };

  SingleRule tok_STRING = new SingleRule(HiveParser.TOK_STRING) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "STRING";
    }
  };

  SingleRule tok_TINYINT = new SingleRule(HiveParser.TOK_TINYINT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "TINYINT";
    }
  };

  SingleRule tok_SMALLINT = new SingleRule(HiveParser.TOK_SMALLINT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "SMALLINT";
    }
  };

  SingleRule tok_BIGINT = new SingleRule(HiveParser.TOK_BIGINT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "BIGINT";
    }
  };

  SingleRule tok_FLOAT = new SingleRule(HiveParser.TOK_FLOAT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "FLOAT";
    }
  };

  SingleRule tok_DOUBLE = new SingleRule(HiveParser.TOK_DOUBLE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "DOUBLE";
    }
  };

  SingleRule tok_DECIMAL = new SingleRule(HiveParser.TOK_DECIMAL) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "DECIMAL";
    }
  };

  SingleRule tok_TIMESTAMP = new SingleRule(HiveParser.TOK_TIMESTAMP) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "TIMESTAMP";
    }
  };

  SingleRule tok_DATE = new SingleRule(HiveParser.TOK_DATE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "DATE";
    }
  };

  SingleRule tok_VARCHAR = new SingleRule(HiveParser.TOK_VARCHAR) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "VARCHAR";
    }
  };

  SingleRule tok_CHAR = new SingleRule(HiveParser.TOK_CHAR) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "CHAR";
    }
  };

  SingleRule tok_BOOLEAN = new SingleRule(HiveParser.TOK_BOOLEAN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "BOOLEAN";
    }
  };

  SingleRule tok_INSERT_INTO = new SingleRule(HiveParser.TOK_INSERT_INTO) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "INSERT INTO ", " ", "");
    }
  };

  SingleRule tok_DESTINATION = new SingleRule(HiveParser.TOK_DESTINATION) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      List<ASTNode> children = getChildren(pt);
      if (children.get(0).getType() == HiveParser.TOK_DIR) {
        if (children.get(0).getChild(0).getType() != HiveParser.TOK_TMP_FILE) {
          return mkString(recurseChildren(pt), "INSERT OVERWRITE DIRECTORY ", " ", "");
        }
        return "";
      } else {
        return mkString(recurseChildren(pt), "INSERT OVERWRITE ", " ", "");
      }
    }
  };

  /**
   * <pre>
   * {@code
   * TOK_QUERY
   * ├──TOK_FROM
   * │    └── ...
   * └──TOK_INSERT
   *      └── ...
   *
  </pre>
   */
  SingleRule tok_QUERY = new SingleRule(HiveParser.TOK_QUERY) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      List<ASTNode> tok_from = getChildrenWithTypes(pt, HiveParser.TOK_FROM);
      List<ASTNode> tok_ctes = getChildrenWithTypes(pt, HiveParser.TOK_CTE);
      List<ASTNode> tok_inserts = getChildrenWithTypes(pt, HiveParser.TOK_INSERT);

      if (tok_inserts.size() == 1) {
        ASTNode insert = tok_inserts.get(0);
        List<ASTNode> children = getChildren(insert);
        ASTNode dest = children.get(0);
        ASTNode select = children.get(1);
        List<ASTNode> rest = children.subList(2, children.size());
        // from - inclusive. to - exclusive

        List<ASTNode> nodes = tok_ctes;
        nodes.add(dest);
        nodes.add(select);
        nodes.addAll(tok_from);
        nodes.addAll(rest);

        return mkString(recTransform(nodes, prefix), "");
      } else {
        if (splitMultiInsert) {
          List<String> newList = new ArrayList<>();

          for (ASTNode node : tok_inserts) {
            List<ASTNode> children = getChildren(node);
            ASTNode dest = children.get(0);
            ASTNode select = children.get(1);
            List<ASTNode> rest = children.subList(2, children.size());

            List<ASTNode> nodes = tok_ctes;
            nodes.add(dest);
            nodes.add(select);
            nodes.addAll(tok_from);
            nodes.addAll(rest);

            newList.add(mkString(recTransform(nodes, prefix), ""));
          }
          return mkString(newList, "; "); // uncomment for no new line
        } else {
          List<ASTNode> nodes = tok_ctes;
          nodes.addAll(tok_from);
          nodes.addAll(tok_inserts);
          List<ASTNode> new_toks = new ArrayList<>();
          for (ASTNode node : nodes) {
            new_toks.addAll(getChildren(node));
          }
          return mkString(recTransform(new_toks, prefix), "");
        }
      }
    }
  };

  SingleRule tok_SUBQUERY = new SingleRule(HiveParser.TOK_SUBQUERY) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      Pair<String, String> pair = formatSubQuery(pt, prefix);
      return pair.fst + " " + pair.snd;
    }
  };

  /**
   * <pre>
   * ... where a in (select b from c)
   * ├──  └──
   * {@code
   * TOK_WHERE
   * [└── not]
   *    └──TOK_SUBQUERY_EXPR
   *        ├── TOK_SUBQUERY_OP
   *        |       └── KW_IN
   *        ├── TOK_QUERY
   *        |       └── ....
   *        └── TOK_TABLE_OR_COL
   *                └── a
  </pre>
   */
  SingleRule tok_SUBQUERY_EXPR = new SingleRule(HiveParser.TOK_SUBQUERY_EXPR) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      String col = "";
      if (pt.getChild(2) != null) {
        col = recTransform((ASTNode) pt.getChild(2)) + " ";
      }
      String op = recTransform((ASTNode) pt.getChild(0).getChild(0));

      if (pt.getParent().getType() == HiveParser.KW_NOT && op.equalsIgnoreCase("in")) {
        op = "NOT IN ";
      } else if (op.equals("")) {
        op = pt.getChild(0).getChild(0).getText();
      }
      ASTNode tok_query = (ASTNode) pt.getChild(1);
      return col + op + " (" + recTransform(tok_query, prefix.replace("\n", "") + indent) + ")"; // uncomment for no
    }
  };

  SingleRule tok_CTE = new SingleRule(HiveParser.TOK_CTE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      List<String> newList = new ArrayList<>();
      for (ASTNode node : getChildren(pt)) {
        Pair<String, String> pair = formatSubQuery(node, prefix);
        newList.add(pair.snd + " AS " + pair.fst);
      }
      return mkString(newList, "WITH ", ", ", " "); // uncomment for no new line
    }
  };

  SingleRule tok_FROM = new SingleRule(HiveParser.TOK_FROM) {
    @Override
    public String apply(ASTNode pt, String prefix) {

      List<ASTNode> children = getChildren(pt);
      if (children.get(0).getType() == HiveParser.TOK_SUBQUERY) {
        return mkString(recurseChildren(pt, prefix), prefix + "FROM (", " ", " "); // uncomment for no new line
      }

      return mkString(recurseChildren(pt, prefix), prefix + "FROM ", " ", " "); // uncomment for no new line
    }
  };

  SingleRule tok_TAB = new SingleRule(HiveParser.TOK_TAB) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "TABLE ", " ", " ");// uncomment for no new line
    }
  };

  SingleRule tok_TABREF = new SingleRule(HiveParser.TOK_TABREF) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "", " ", " ");
    }
  };

  SingleRule tok_TABNAME = new SingleRule(HiveParser.TOK_TABNAME) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), ".");
    }
  };

  SingleRule tok_PARTSPEC = new SingleRule(HiveParser.TOK_PARTSPEC) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "PARTITION(", ", ", ")");
    }
  };

  SingleRule tok_PARTVAL = new SingleRule(HiveParser.TOK_PARTVAL) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " = ");
    }
  };

  SingleRule tok_SELECT = new SingleRule(HiveParser.TOK_SELECT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt, prefix), prefix + "SELECT " + prefix, ", " + prefix, " ");
    }
  };

  SingleRule tok_SELECTDI = new SingleRule(HiveParser.TOK_SELECTDI) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt, prefix + indent), prefix + "SELECT DISTINCT " + prefix + indent,
          "," + prefix + indent, " ");
      // uncomment for no new line
    }
  };

  SingleRule tok_SELEXPR = new SingleRule(HiveParser.TOK_SELEXPR) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt, prefix), " as ");
    }
  };

  SingleRule tok_GROUPBY = new SingleRule(HiveParser.TOK_GROUPBY) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), prefix + "GROUP BY ", ", ", " "); // uncomment for no new line
    }
  };

  SingleRule tok_ORDERBY = new SingleRule(HiveParser.TOK_ORDERBY) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), prefix + "ORDER BY ", ", ", " "); // uncomment for no new line
    }
  };

  SingleRule tok_DISTRIBUTEBY = new SingleRule(HiveParser.TOK_DISTRIBUTEBY) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), prefix + "DISTRIBUTE BY ", ", ", " "); // uncomment for no new line
    }
  };

  SingleRule tok_SORTBY = new SingleRule(HiveParser.TOK_SORTBY) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), prefix + "SORT BY ", ", ", " "); // uncomment for no new line
    }
  };

  SingleRule tok_CLUSTERBY = new SingleRule(HiveParser.TOK_CLUSTERBY) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), prefix + "CLUSTER BY ", ", ", " "); // uncomment for no new line
    }
  };

  SingleRule tok_TABSORTCOLNAMEDESC = new SingleRule(HiveParser.TOK_TABSORTCOLNAMEDESC) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "", "", " DESC");
    }
  };

  SingleRule tok_TABSORTCOLNAMEASC = new SingleRule(HiveParser.TOK_TABSORTCOLNAMEASC) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "", "", " ASC");
    }
  };

  SingleRule tok_WHERE = new SingleRule(HiveParser.TOK_WHERE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      String newPrefix = prefix;
      switch (pt.getChild(0).getType()) {
      case HiveParser.KW_OR:
        newPrefix = prefix + indent + " "; // uncomment for no new line
      case HiveParser.KW_AND:
        newPrefix = prefix + indent; // uncomment for no new line
      }
      return mkString(recurseChildren(pt, newPrefix), prefix + "WHERE ", " ", " "); // uncomment for no new line
    }
  };

  SingleRule tok_HAVING = new SingleRule(HiveParser.TOK_HAVING) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt, prefix + indent), prefix + "HAVING ", " ", " "); // uncomment for no new
    }
  };

  SingleRule kw_AND = new SingleRule(HiveParser.KW_AND) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      String newPrefix = " ";
      if (prefix.contains("\n")) {
        newPrefix = prefix;
      }
      List<String> newList = new ArrayList<>();
      for (ASTNode node : getChildren(pt)) {
        if (node.getType() == HiveParser.KW_OR) {
          newList.add("(" + recTransform(node, " ") + ")");
        } else {
          newList.add(recTransform(node, prefix));
        }
      }
      return mkString(newList, newPrefix + "AND ");
    }
  };

  SingleRule kw_OR = new SingleRule(HiveParser.KW_OR) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      String newPrefix = " ";
      if (prefix.contains("\n")) {
        newPrefix = prefix;
      }
      List<String> newList = new ArrayList<>();
      for (ASTNode node : getChildren(pt)) {
        if (node.getType() == HiveParser.KW_AND) {
          newList.add("(" + recTransform(node, " ") + ")");
        } else {
          newList.add(recTransform(node, prefix));
        }
      }
      return mkString(newList, newPrefix + "OR ");
    }
  };

  SingleRule kw_NOT = new SingleRule(HiveParser.KW_NOT) {
    @Override
    public String apply(ASTNode pt, String prefix) {

      /**
       * <pre>
       * ... where a [not] in (select b from c)
       * ├──  └──
       * {@code
       * TOK_WHERE
       * [└── not]
       *    └──TOK_SUBQUERY_EXPR
       *        ├── TOK_SUBQUERY_OP
       *        |       └── KW_IN
       *        ├── TOK_QUERY
       *        |       └── ....
       *        └── TOK_TABLE_OR_COL
       *                └── a
      </pre>
       */

      if (pt.getChild(0).getText().equals("=")) {
        return mkString(recurseChildren(pt, prefix), "NOT ", "", "");
      }
      return mkString(recurseChildren(pt, prefix), "", "", "");
    }
  };

  SingleRule kw_IN = new SingleRule(HiveParser.KW_IN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "IN";
    }
  };

  SingleRule tok_JOIN = new SingleRule(HiveParser.TOK_JOIN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatJoin(prefix, pt, "JOIN");
    }
  };

  SingleRule tok_CROSSJOIN = new SingleRule(HiveParser.TOK_CROSSJOIN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatJoin(prefix, pt, "CROSS JOIN");
    }
  };

  SingleRule tok_RIGHTOUTERJOIN = new SingleRule(HiveParser.TOK_RIGHTOUTERJOIN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatJoin(prefix, pt, "RIGHT JOIN");
    }
  };

  SingleRule tok_LEFTOUTERJOIN = new SingleRule(HiveParser.TOK_LEFTOUTERJOIN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatJoin(prefix, pt, "LEFT JOIN");
    }
  };

  SingleRule tok_FULLOUTERJOIN = new SingleRule(HiveParser.TOK_FULLOUTERJOIN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatJoin(prefix, pt, "FULL JOIN");
    }
  };

  SingleRule tok_LEFTSEMIJOIN = new SingleRule(HiveParser.TOK_LEFTSEMIJOIN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatJoin(prefix, pt, "LEFT SEMI JOIN");
    }
  };

  SingleRule tok_UNIONDISTINCT = new SingleRule(HiveParser.TOK_UNIONDISTINCT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatUnion(prefix, pt, "UNION DISTINCT");
    }
  };

  SingleRule tok_UNIONALL = new SingleRule(HiveParser.TOK_UNIONALL) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatUnion(prefix, pt, "UNION ALL");
    }
  };

  SingleRule tok_LATERAL_VIEW = new SingleRule(HiveParser.TOK_LATERAL_VIEW) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatLateralView(prefix, pt, "LATERAL VIEW");
    }
  };

  SingleRule tok_LATERAL_VIEW_OUTER = new SingleRule(HiveParser.TOK_LATERAL_VIEW_OUTER) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatLateralView(prefix, pt, "LATERAL VIEW OUTER");
    }
  };

  SingleRule identifier = new SingleRule(HiveParser.Identifier) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      if (pt.getText().contains(".")) {
        return "`" + pt.getText() + "`";
      }
      return pt.getText();
    }
  };

  SingleRule stringLiteral = new SingleRule(HiveParser.StringLiteral) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return pt.getText();
    }
  };

  SingleRule number = new SingleRule(HiveParser.Number) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return pt.getText();
    }
  };

  SingleRule kw_TRUE = new SingleRule(HiveParser.KW_TRUE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "true";
    }
  };

  SingleRule kw_FALSE = new SingleRule(HiveParser.KW_FALSE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "false";
    }
  };

  SingleRule tok_ALLCOLREF = new SingleRule(HiveParser.TOK_ALLCOLREF) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      // recurseChildren(pt).headOption.map {_ + "."}.getOrElse("") + "*"
      List<String> recurseChildrenList = recurseChildren(pt);
      if (!recurseChildrenList.isEmpty()) {
        return recurseChildrenList.get(0) + "." + "*";
      } else {
        return "*";
      }
    }
  };

  SingleRule dot = new SingleRule(HiveParser.DOT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), ".");
    }
  };

  SingleRule equal = new SingleRule(HiveParser.EQUAL) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " = ");
    }
  };

  SingleRule greaterThan = new SingleRule(HiveParser.GREATERTHAN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " > ");
    }
  };

  SingleRule greaterThanOrEqualTo = new SingleRule(HiveParser.GREATERTHANOREQUALTO) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " >= ");
    }
  };

  SingleRule lessThan = new SingleRule(HiveParser.LESSTHAN) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " < ");
    }
  };

  SingleRule lessThanOrEqualTo = new SingleRule(HiveParser.LESSTHANOREQUALTO) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " <= ");
    }
  };

  SingleRule notEqual = new SingleRule(HiveParser.NOTEQUAL) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " <> ");
    }
  };

  SingleRule plus = new SingleRule(HiveParser.PLUS) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " + ");
    }
  };

  SingleRule star = new SingleRule(HiveParser.STAR) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " * ");
    }
  };

  SingleRule divide = new SingleRule(HiveParser.DIVIDE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " / ");
    }
  };

  SingleRule minus = new SingleRule(HiveParser.MINUS) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      if (pt.getChildCount() == 1) {
        return "- " + recTransform((ASTNode) pt.getChild(0));
      }
      return mkString(recurseChildren(pt), " - ");
    }
  };

  SingleRule tok_FUNCTION = new SingleRule(HiveParser.TOK_FUNCTION) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatFunction(prefix, false, pt);
    }
  };

  SingleRule tok_FUNCTIONDI = new SingleRule(HiveParser.TOK_FUNCTIONDI) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return formatFunction(prefix, true, pt);
    }
  };

  SingleRule tok_FUNCTIONSTAR = new SingleRule(HiveParser.TOK_FUNCTIONSTAR) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return recTransform((ASTNode) pt.getChild(0)) + "(*)";
    }
  };

  SingleRule lSQUARE = new SingleRule(HiveParser.LSQUARE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), "", "[", "]");
    }
  };

  /**
   * <pre>
   * ... where a not like b
   * ├──  └──
   * {@code
   * TOK_WHERE
   * [└── not]
   *    └── like
   *       └──TOK_TABLE_OR_COL
   *       |      └── a
   *       └── b
  </pre>
   */
  SingleRule kw_LIKE = new SingleRule(HiveParser.KW_LIKE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      if (pt.getParent().getType() == HiveParser.KW_NOT) {
        return mkString(recurseChildren(pt), " NOT LIKE ");
      }
      return mkString(recurseChildren(pt), " LIKE ");
    }
  };

  SingleRule kw_RLIKE = new SingleRule(HiveParser.KW_RLIKE) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return mkString(recurseChildren(pt), " RLIKE ");
    }
  };

  SingleRule tok_NULL = new SingleRule(HiveParser.TOK_NULL) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      return "NULL";
    }
  };

  SingleRule tok_LIMIT = new SingleRule(HiveParser.TOK_LIMIT) {
    @Override
    public String apply(ASTNode pt, String prefix) {
      // return mkString(recurseChildren(pt), prefix + "LIMIT ", "", "\n");
      return mkString(recurseChildren(pt), prefix + "LIMIT ", "", " "); // uncomment for no new line
    }
  };

  public Map<Integer, SingleRule> setUpTokenTypeRuleMap() {
    Map<Integer, SingleRule> tokenTypeRuleMap = new HashMap<Integer, SingleRule>() {
      {
        put(HiveParser.TOK_CREATETABLE, tok_CREATETABLE);
        put(HiveParser.TOK_LIKETABLE, tok_LIKETABLE);
        put(HiveParser.TOK_IFNOTEXISTS, tok_IFNOTEXISTS);
        put(HiveParser.TOK_TABCOLLIST, tok_TABCOLLIST);
        put(HiveParser.TOK_TABCOL, tok_TABCOL);
        put(HiveParser.TOK_INT, tok_INT);
        put(HiveParser.TOK_STRING, tok_STRING);
        put(HiveParser.TOK_TINYINT, tok_TINYINT);
        put(HiveParser.TOK_SMALLINT, tok_SMALLINT);
        put(HiveParser.TOK_BIGINT, tok_BIGINT);
        put(HiveParser.TOK_FLOAT, tok_FLOAT);
        put(HiveParser.TOK_DOUBLE, tok_DOUBLE);
        put(HiveParser.TOK_DECIMAL, tok_DECIMAL);
        put(HiveParser.TOK_TIMESTAMP, tok_TIMESTAMP);
        put(HiveParser.TOK_DATE, tok_DATE);
        put(HiveParser.TOK_VARCHAR, tok_VARCHAR);
        put(HiveParser.TOK_CHAR, tok_CHAR);
        put(HiveParser.TOK_BOOLEAN, tok_BOOLEAN);
        put(HiveParser.TOK_TABLEPARTCOLS, tok_TABLEPARTCOLS);
        put(HiveParser.TOK_FILEFORMAT_GENERIC, tok_FILEFORMAT_GENERIC);
        put(HiveParser.TOK_TABLEFILEFORMAT, tok_TABLEFILEFORMAT);
        put(HiveParser.TOK_TABLELOCATION, tok_TABLELOCATION);
        put(HiveParser.TOK_TABLEROWFORMAT, tok_TABLEROWFORMAT);
        put(HiveParser.TOK_TABLEROWFORMATFIELD, tok_TABLEROWFORMATFIELD);
        put(HiveParser.TOK_TABLEPROPERTIES, tok_TABLEPROPERTIES);
        put(HiveParser.TOK_TABLEPROPERTY, tok_TABLEPROPERTY);
        put(HiveParser.TOK_TABLESERIALIZER, tok_TABLESERIALIZER);

        put(HiveParser.TOK_INSERT, tok_INSERT);
        put(HiveParser.TOK_INSERT_INTO, tok_INSERT_INTO);
        put(HiveParser.TOK_DESTINATION, tok_DESTINATION);
        put(HiveParser.TOK_QUERY, tok_QUERY);
        put(HiveParser.TOK_SUBQUERY, tok_SUBQUERY);
        put(HiveParser.TOK_SUBQUERY_EXPR, tok_SUBQUERY_EXPR);
        put(HiveParser.TOK_CTE, tok_CTE);
        put(HiveParser.TOK_FROM, tok_FROM);
        put(HiveParser.TOK_TAB, tok_TAB);
        put(HiveParser.TOK_TABREF, tok_TABREF);
        put(HiveParser.TOK_TABNAME, tok_TABNAME);
        put(HiveParser.TOK_PARTSPEC, tok_PARTSPEC);
        put(HiveParser.TOK_PARTVAL, tok_PARTVAL);
        put(HiveParser.TOK_SELECT, tok_SELECT);
        put(HiveParser.TOK_SELECTDI, tok_SELECTDI);
        put(HiveParser.TOK_SELEXPR, tok_SELEXPR);
        put(HiveParser.TOK_GROUPBY, tok_GROUPBY);
        put(HiveParser.TOK_ORDERBY, tok_ORDERBY);
        put(HiveParser.TOK_DISTRIBUTEBY, tok_DISTRIBUTEBY);
        put(HiveParser.TOK_SORTBY, tok_SORTBY);
        put(HiveParser.TOK_CLUSTERBY, tok_CLUSTERBY);
        put(HiveParser.TOK_TABSORTCOLNAMEDESC, tok_TABSORTCOLNAMEDESC);
        put(HiveParser.TOK_TABSORTCOLNAMEASC, tok_TABSORTCOLNAMEASC);
        put(HiveParser.TOK_WHERE, tok_WHERE);
        put(HiveParser.TOK_HAVING, tok_HAVING);
        put(HiveParser.KW_AND, kw_AND);
        put(HiveParser.KW_OR, kw_OR);
        put(HiveParser.KW_NOT, kw_NOT);
        put(HiveParser.KW_IN, kw_IN);
        put(HiveParser.TOK_JOIN, tok_JOIN);
        put(HiveParser.TOK_CROSSJOIN, tok_CROSSJOIN);
        put(HiveParser.TOK_RIGHTOUTERJOIN, tok_RIGHTOUTERJOIN);
        put(HiveParser.TOK_LEFTOUTERJOIN, tok_LEFTOUTERJOIN);
        put(HiveParser.TOK_FULLOUTERJOIN, tok_FULLOUTERJOIN);
        put(HiveParser.TOK_LEFTSEMIJOIN, tok_LEFTSEMIJOIN);
        put(HiveParser.TOK_UNIONDISTINCT, tok_UNIONDISTINCT);
        put(HiveParser.TOK_UNIONALL, tok_UNIONALL);
        put(HiveParser.TOK_LATERAL_VIEW, tok_LATERAL_VIEW);
        put(HiveParser.TOK_LATERAL_VIEW_OUTER, tok_LATERAL_VIEW_OUTER);
        put(HiveParser.Identifier, identifier);
        put(HiveParser.StringLiteral, stringLiteral);
        put(HiveParser.Number, number);
        put(HiveParser.KW_TRUE, kw_TRUE);
        put(HiveParser.KW_FALSE, kw_FALSE);
        put(HiveParser.TOK_ALLCOLREF, tok_ALLCOLREF);
        put(HiveParser.DOT, dot);
        put(HiveParser.EQUAL, equal);
        put(HiveParser.GREATERTHAN, greaterThan);
        put(HiveParser.GREATERTHANOREQUALTO, greaterThanOrEqualTo);
        put(HiveParser.LESSTHAN, lessThan);
        put(HiveParser.LESSTHANOREQUALTO, lessThanOrEqualTo);
        put(HiveParser.NOTEQUAL, notEqual);
        put(HiveParser.PLUS, plus);
        put(HiveParser.STAR, star);
        put(HiveParser.DIVIDE, divide);
        put(HiveParser.MINUS, minus);
        put(HiveParser.TOK_FUNCTION, tok_FUNCTION);
        put(HiveParser.TOK_FUNCTIONDI, tok_FUNCTIONDI);
        put(HiveParser.TOK_FUNCTIONSTAR, tok_FUNCTIONSTAR);
        put(HiveParser.LSQUARE, lSQUARE);
        put(HiveParser.KW_LIKE, kw_LIKE);
        put(HiveParser.KW_RLIKE, kw_RLIKE);
        put(HiveParser.TOK_NULL, tok_NULL);
        put(HiveParser.TOK_LIMIT, tok_LIMIT);
      }
    };
    return tokenTypeRuleMap;
  }

  static <T> List<T> dropLeft(List<T> list, int itemsToDrop) {
    if (list.size() <= itemsToDrop) {
      return Collections.emptyList();
    }
    return list.subList(itemsToDrop, list.size());
  }

  static <T> List<T> dropRight(List<T> list, int itemsToDrop) {
    if (list.size() <= itemsToDrop) {
      return Collections.emptyList();
    }
    return list.subList(0, list.size() - itemsToDrop);
  }

  private List<ASTNode> take(List<ASTNode> list, int itemsToTake) {
    if (list.size() <= itemsToTake) {
      return list;
    }
    return list.subList(0, itemsToTake);
  }

  private List<String> recTransform(List<ASTNode> children) {
    return recTransform(children, "");
  }

  private List<String> recTransform(List<ASTNode> children, String prefix) {
    List<String> transformedList = new ArrayList<>();
    if (children != null) {
      for (ASTNode child : children) {
        transformedList.add(recTransform(child, prefix));
      }
    }
    return transformedList;
  }

  private String recTransform(ASTNode child) {
    return recTransform(child, "");
  }

  private String recTransform(ASTNode astNode, String prefix) {
    RuleSet recTransform = new RuleSet(defaultRule, setUpTokenTypeRuleMap());
    return recTransform.apply(astNode, prefix);
  }

  String mkString(List<String> children, String start, String sep, String end) {
    String str = start;
    Iterator<String> itr = children.iterator();
    if (itr.hasNext()) {
      str += itr.next().toString();
      while (itr.hasNext()) {
        str += sep + itr.next().toString();
      }
    }
    str += end;
    return str;
  }

  String mkString(List<String> children, String sep) {
    return mkString(children, "", sep, "");
  }

  /**
   * Transform a parsed tree back into an SQL Query
   */
  // public String treeToQuery(ASTNode tree, boolean splitMultiInsert) {
  public String treeToQuery(ASTNode tree) {
    String query = recTransform(tree, "");
    if (query.charAt(query.length() - 1) == ' ') {
      return query.substring(0, query.length() - 1);
    }
    return query;
  }

}
