package com.hotels.bdp.waggledance.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.validation.ConstraintViolation;

import org.hibernate.validator.HibernateValidator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

public class GraphiteConfigurationTest {

  private final LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
  private final GraphiteConfiguration graphiteConfiguration = new GraphiteConfiguration();

  @Before
  public void before() {
    validator.setProviderClass(HibernateValidator.class);
    validator.afterPropertiesSet();

    graphiteConfiguration.setPort(100);
    graphiteConfiguration.setHost("host");
    graphiteConfiguration.setPrefix("prefix");
    graphiteConfiguration.setPollInterval(100);
    graphiteConfiguration.setPollIntervalTimeUnit(TimeUnit.MICROSECONDS);
    graphiteConfiguration.init();
  }

  @Test
  public void typical() {
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(0));
    assertTrue(graphiteConfiguration.isEnabled());
    assertThat(graphiteConfiguration.getPort(), is(100));
    assertThat(graphiteConfiguration.getHost(), is("host"));
    assertThat(graphiteConfiguration.getPrefix(), is("prefix"));
    assertThat(graphiteConfiguration.getPollInterval(), is((long) 100));
    assertThat(graphiteConfiguration.getPollIntervalTimeUnit(), is(TimeUnit.MICROSECONDS));
  }

  @Test
  public void zeroPort() {
    graphiteConfiguration.setPort(0);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void negativePort() {
    graphiteConfiguration.setPort(-1);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void zeroPollInterval() {
    graphiteConfiguration.setPollInterval(0);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void negativePollInterval() {
    graphiteConfiguration.setPollInterval(-1);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullPollIntervalTimeUnit() {
    graphiteConfiguration.setPollIntervalTimeUnit(null);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

}
