/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.waggledance.yaml;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.yaml.snakeyaml.DumperOptions.ScalarStyle;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.CollectionNode;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import com.google.common.base.CaseFormat;

public class AdvancedRepresenter extends Representer {

  public AdvancedRepresenter() {
    multiRepresenters.put(List.class, new RepresentList() {
      @Override
      public Node representData(Object data) {
        return representWithoutRecordingDescendents(data, super::representData);
      }
    });
  }

  private Node representWithoutRecordingDescendents(Object data, Function<Object, Node> worker) {
    Map<Object, Node> representedObjectsOnEntry = new LinkedHashMap<Object, Node>(representedObjects);
    try {
      return worker.apply(data);
    } finally {
      representedObjects.clear();
      representedObjects.putAll(representedObjectsOnEntry);
    }
  }

  @Override
  protected NodeTuple representJavaBeanProperty(
      Object javaBean,
      Property property,
      Object propertyValue,
      Tag customTag) {
    setDefaultScalarStyle(ScalarStyle.PLAIN);
    NodeTuple nodeTuple = super.representJavaBeanProperty(javaBean, property, propertyValue, customTag);
    Node valueNode = nodeTuple.getValueNode();
    if (Tag.NULL.equals(valueNode.getTag())) {
      return null; // skip 'null' values
    }

    if (valueNode instanceof CollectionNode) {
      // if (Tag.SEQ.equals(valueNode.getTag())) {
      // SequenceNode seq = (SequenceNode) valueNode;
      // if (seq.getValue().isEmpty()) {
      // return null; // skip empty lists
      // }
      // }
      if (Tag.MAP.equals(valueNode.getTag())) {
        MappingNode seq = (MappingNode) valueNode;
        if (seq.getValue().isEmpty()) {
          return null; // skip empty maps
        }
      }
    }

    Object name = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, property.getName());
    return new NodeTuple(representData(name), valueNode);
  }

}
