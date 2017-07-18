/**
 * Copyright (C) 2016-2017 Expedia Inc.
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

import java.beans.IntrospectionException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import com.google.common.collect.Sets;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.Federations;
import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;

public final class YamlFactory {

  private YamlFactory() {}

  public static Yaml newYaml() {
    PropertyUtils propertyUtils = new AdvancedPropertyUtils();
    propertyUtils.setSkipMissingProperties(true);

    Constructor constructor = new Constructor(Federations.class);
    TypeDescription federationDescription = new TypeDescription(Federations.class);
    federationDescription.putListPropertyType("federatedMetaStores", FederatedMetaStore.class);
    constructor.addTypeDescription(federationDescription);
    constructor.setPropertyUtils(propertyUtils);

    Representer representer = new AdvancedRepresenter();
    representer.setPropertyUtils(new FieldOrderPropertyUtils());
    representer.addClassTag(Federations.class, Tag.MAP);
    representer.addClassTag(AbstractMetaStore.class, Tag.MAP);
    representer.addClassTag(WaggleDanceConfiguration.class, Tag.MAP);
    representer.addClassTag(GraphiteConfiguration.class, Tag.MAP);

    DumperOptions dumperOptions = new DumperOptions();
    dumperOptions.setIndent(2);
    dumperOptions.setDefaultFlowStyle(FlowStyle.BLOCK);

    return new Yaml(constructor, representer, dumperOptions);
  }

  private static class FieldOrderPropertyUtils extends AdvancedPropertyUtils {
    @Override
    protected Set<Property> createPropertySet(Class<? extends Object> type, BeanAccess bAccess)
      throws IntrospectionException {
      if (Federations.class.equals(type)) {
        // Serializing on Field order for Federations.
        Set<Property> fieldOrderedProperties = new LinkedHashSet<>(getPropertiesMap(type, BeanAccess.FIELD).values());
        Set<Property> defaultOrderPropertySet = super.createPropertySet(type, bAccess);
        Set<Property> filtered = Sets.difference(fieldOrderedProperties, defaultOrderPropertySet);
        fieldOrderedProperties.removeAll(filtered);
        return fieldOrderedProperties;
      } else {
        return super.createPropertySet(type, bAccess);
      }
    }
  }

}
