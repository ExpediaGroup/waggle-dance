package com.hotels.bdp.waggledance.parse;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.ASTNode;

public class Rule {

  public Rule() {}

  public String apply(ASTNode pt, String prefix) {
    return prefix;
  }

  public static class SingleRule extends Rule {
    public SingleRule(int tokenType) {

    }

    @Override
    public String apply(ASTNode pt, String prefix) {
      return prefix;
    }

  }

  public static class RuleSet extends Rule {

    private Map<Integer, SingleRule> map = new HashMap<>();
    private final Rule defaultRule;

    public RuleSet(Rule defaultRule, Map<Integer, SingleRule> map) {
      this.defaultRule = defaultRule;
      this.map = map;
    }

    @Override
    public String apply(ASTNode pt, String prefix) {
      if (map.containsKey(pt.getType())) {
        return map.get(pt.getType()).apply(pt, prefix);
      }
      return defaultRule.apply(pt, prefix);
    }
  }

}

// the rule can just be an interface which has the apply method.
// rule set is what goes through the nodes recursively goes through the tree to build up the query string.
// fancy map which builds the string
