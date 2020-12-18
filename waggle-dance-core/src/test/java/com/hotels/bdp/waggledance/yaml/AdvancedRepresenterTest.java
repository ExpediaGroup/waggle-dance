/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.Transient;
import java.util.Collection;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.introspector.MethodProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AdvancedRepresenterTest {

  private static class TestBean {
    private String property;
    private Collection<String> collectionProperty;
    private Map<String, Long> mapProperty;
    public transient int transientField;
    private char transientProperty;

    public String getProperty() {
      return property;
    }

    public void setProperty(String property) {
      this.property = property;
    }

    public Collection<String> getCollectionProperty() {
      return collectionProperty;
    }

    public void setCollectionProperty(Collection<String> collectionProperty) {
      this.collectionProperty = collectionProperty;
    }

    public Map<String, Long> getMapProperty() {
      return mapProperty;
    }

    public void setMapProperty(Map<String, Long> mapProperty) {
      this.mapProperty = mapProperty;
    }

    @Transient
    public char getTransientProperty() {
      return transientProperty;
    }

    @Transient
    public void setTransientProperty(char transientProperty) {
      this.transientProperty = transientProperty;
    }
  }

  private final AdvancedRepresenter representer = new AdvancedRepresenter();
  private TestBean bean;
  private BeanInfo beanInfo;

  @Before
  public void init() throws Exception {
    bean = new TestBean();
    beanInfo = Introspector.getBeanInfo(TestBean.class);
  }

  private PropertyDescriptor getPropertyDescriptor(String propertyName) {
    for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
      if (propertyDescriptor.getName().equals(propertyName)) {
        return propertyDescriptor;
      }
    }
    throw new IllegalArgumentException("Property " + propertyName + " not found");
  }

  @Test
  public void notNullProperty() throws Exception {
    bean.setProperty("value");
    Property property = new MethodProperty(getPropertyDescriptor("property"));
    NodeTuple nodeTuple = representer.representJavaBeanProperty(bean, property, bean.getProperty(), null);
    assertThat(nodeTuple, is(notNullValue()));
    assertThat(nodeTuple.getKeyNode(), is(instanceOf(ScalarNode.class)));
    assertThat(((ScalarNode) nodeTuple.getKeyNode()).getValue(), is("property"));
    assertThat(nodeTuple.getValueNode(), is(instanceOf(ScalarNode.class)));
    assertThat(((ScalarNode) nodeTuple.getValueNode()).getValue(), is("value"));
  }

  @Test
  public void nullProperty() {
    Property property = new MethodProperty(getPropertyDescriptor("property"));
    NodeTuple nodeTuple = representer.representJavaBeanProperty(bean, property, bean.getProperty(), null);
    assertThat(nodeTuple, is(nullValue()));
  }

  @Test
  public void notNullCollectionProperty() {
    bean.setCollectionProperty(ImmutableList.<String>builder().add("1").add("2").build());
    Property property = new MethodProperty(getPropertyDescriptor("collectionProperty"));
    NodeTuple nodeTuple = representer.representJavaBeanProperty(bean, property, bean.getCollectionProperty(), null);
    assertThat(nodeTuple, is(notNullValue()));
    assertThat(nodeTuple.getKeyNode(), is(instanceOf(ScalarNode.class)));
    assertThat(((ScalarNode) nodeTuple.getKeyNode()).getValue(), is("collection-property"));
    assertThat(nodeTuple.getValueNode(), is(instanceOf(SequenceNode.class)));
    assertThat(((SequenceNode) nodeTuple.getValueNode()).getValue().size(), is(2));
    assertThat(((SequenceNode) nodeTuple.getValueNode()).getValue().get(0), is(instanceOf(ScalarNode.class)));
    assertThat(((ScalarNode) ((SequenceNode) nodeTuple.getValueNode()).getValue().get(0)).getValue(), is("1"));
    assertThat(((SequenceNode) nodeTuple.getValueNode()).getValue().get(1), is(instanceOf(ScalarNode.class)));
    assertThat(((ScalarNode) ((SequenceNode) nodeTuple.getValueNode()).getValue().get(1)).getValue(), is("2"));
  }

  @Test
  public void nullCollectionProperty() {
    Property property = new MethodProperty(getPropertyDescriptor("collectionProperty"));
    NodeTuple nodeTuple = representer.representJavaBeanProperty(bean, property, bean.getCollectionProperty(), null);
    assertThat(nodeTuple, is(nullValue()));
  }

  @Test
  public void emptyCollectionProperty() {
    bean.setCollectionProperty(ImmutableList.of());
    Property property = new MethodProperty(getPropertyDescriptor("collectionProperty"));
    NodeTuple nodeTuple = representer.representJavaBeanProperty(bean, property, bean.getCollectionProperty(), null);
    assertThat(nodeTuple, is(nullValue()));
  }

  @Test
  public void notNullMapProperty() {
    bean.setMapProperty(ImmutableMap.<String, Long>builder().put("first", 1L).put("second", 2L).build());
    Property property = new MethodProperty(getPropertyDescriptor("mapProperty"));
    NodeTuple nodeTuple = representer.representJavaBeanProperty(bean, property, bean.getMapProperty(), null);
    assertThat(nodeTuple, is(notNullValue()));
    assertThat(nodeTuple.getKeyNode(), is(instanceOf(ScalarNode.class)));
    assertThat(((ScalarNode) nodeTuple.getKeyNode()).getValue(), is("map-property"));
    assertThat(nodeTuple.getValueNode(), is(instanceOf(MappingNode.class)));
    assertThat(((MappingNode) nodeTuple.getValueNode()).getValue().size(), is(2));
    assertThat(((MappingNode) nodeTuple.getValueNode()).getValue().get(0), is(instanceOf(NodeTuple.class)));
    assertThat(((ScalarNode) ((MappingNode) nodeTuple.getValueNode()).getValue().get(0).getKeyNode()).getValue(),
        is("first"));
    assertThat(((ScalarNode) ((MappingNode) nodeTuple.getValueNode()).getValue().get(0).getValueNode()).getValue(),
        is("1"));
    assertThat(((ScalarNode) ((MappingNode) nodeTuple.getValueNode()).getValue().get(1).getKeyNode()).getValue(),
        is("second"));
    assertThat(((ScalarNode) ((MappingNode) nodeTuple.getValueNode()).getValue().get(1).getValueNode()).getValue(),
        is("2"));
  }

  @Test
  public void nullMapProperty() {
    Property property = new MethodProperty(getPropertyDescriptor("mapProperty"));
    NodeTuple nodeTuple = representer.representJavaBeanProperty(bean, property, bean.getMapProperty(), null);
    assertThat(nodeTuple, is(nullValue()));
  }

  @Test
  public void emptyMapProperty() {
    bean.setMapProperty(ImmutableMap.of());
    Property property = new MethodProperty(getPropertyDescriptor("mapProperty"));
    NodeTuple nodeTuple = representer.representJavaBeanProperty(bean, property, bean.getMapProperty(), null);
    assertThat(nodeTuple, is(nullValue()));
  }

}
