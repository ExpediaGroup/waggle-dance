package com.hotels.bdp.waggledance.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class YamlStorageConfigurationTest {

  @Test
  public void defaultOverwriteConfigOnShutdown() {
    YamlStorageConfiguration configuration = new YamlStorageConfiguration();
    assertThat(configuration.isOverwriteConfigOnShutdown(), is(true));
  }

  @Test
  public void setOverwriteConfigOnShutdown() {
    YamlStorageConfiguration configuration = new YamlStorageConfiguration();
    configuration.setOverwriteConfigOnShutdown(false);
    assertThat(configuration.isOverwriteConfigOnShutdown(), is(false));
  }

}
