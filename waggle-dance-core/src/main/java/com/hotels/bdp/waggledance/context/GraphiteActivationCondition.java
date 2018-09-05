package com.hotels.bdp.waggledance.context;

import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;
import io.micrometer.graphite.GraphiteMeterRegistry;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class GraphiteActivationCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {

    GraphiteConfiguration graphiteConfiguration = (GraphiteConfiguration) context.getBeanFactory().getBean(GraphiteConfiguration.class);

    return graphiteConfiguration.isEnabled();

  }

}
