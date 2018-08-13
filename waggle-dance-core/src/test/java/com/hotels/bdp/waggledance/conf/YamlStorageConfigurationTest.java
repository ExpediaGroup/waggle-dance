package com.hotels.bdp.waggledance.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class YamlStorageConfigurationTest {

  private static YamlStorageConfiguration configuration = new YamlStorageConfiguration();

  @Test
  public void defaultOverwriteConfigOnShutdown() {
    assertThat(configuration.isOverwriteConfigOnShutdown(), is(true));
  }

  @Test
  public void setOverwriteConfigOnShutdown() {
    configuration.setOverwriteConfigOnShutdown(false);
    assertThat(configuration.isOverwriteConfigOnShutdown(), is(false));
  }

}
