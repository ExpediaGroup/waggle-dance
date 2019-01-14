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
package com.hotels.bdp.waggledance.yaml;

import java.beans.PropertyDescriptor;
import java.beans.Transient;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.MethodProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import com.google.common.base.CaseFormat;

public class AdvancedPropertyUtils extends PropertyUtils {

  private boolean allowReadOnlyProperties = false;

  @Override
  public Property getProperty(Class<? extends Object> type, String name) {
    if (name.indexOf('-') > -1) {
      name = CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, name);
    }
    return super.getProperty(type, name);
  }

  @Override
  protected Set<Property> createPropertySet(Class<? extends Object> type, BeanAccess beanAccess) {
    Set<Property> properties = new TreeSet<>();
    Collection<Property> props = getPropertiesMap(type, beanAccess).values();
    for (Property property : props) {
      if (include(property)) {
        properties.add(property);
      }
    }
    return properties;
  }

  private synchronized boolean include(Property property) {
    boolean eligible = property.isReadable() && (allowReadOnlyProperties || property.isWritable());
    if (!eligible) {
      return false;
    }
    if (MethodProperty.class.isAssignableFrom(property.getClass())) {
      PropertyDescriptor propertyDescriptor = getPropertyDescriptor((MethodProperty) property);
      if (propertyDescriptor != null && isTransient(propertyDescriptor)) {
        return false;
      }
    }
    return true;
  }

  private synchronized PropertyDescriptor getPropertyDescriptor(MethodProperty methodProperty) {
    Field propertyField = null;
    try {
      propertyField = MethodProperty.class.getDeclaredField("property");
      propertyField.setAccessible(true);
      return (PropertyDescriptor) propertyField.get(methodProperty);
    } catch (Exception e) {
      return null;
    } finally {
      if (propertyField != null) {
        propertyField.setAccessible(false);
      }
    }
  }

  private boolean isTransient(PropertyDescriptor propertyDescriptor) {
    // first check for a write method to avoid NullPointerException
    Method writeMethod = propertyDescriptor.getWriteMethod();

    return propertyDescriptor.getReadMethod().getAnnotation(Transient.class) != null
        || (writeMethod != null && writeMethod.getAnnotation(Transient.class) != null);
  }

  @Override
  public void setAllowReadOnlyProperties(boolean allowReadOnlyProperties) {
    this.allowReadOnlyProperties = allowReadOnlyProperties;
    super.setAllowReadOnlyProperties(allowReadOnlyProperties);
  }

}
