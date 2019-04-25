package com.hotels.bdp.waggledance.context;

import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.service.impl.PollingFederationService;

public class ScheduledBeansTestContext {

  @Bean
  public WaggleDanceConfiguration waggleDanceConfiguration() {
    WaggleDanceConfiguration mock = Mockito.mock(WaggleDanceConfiguration.class);
    when(mock.getStatusPollingDelay()).thenReturn(10);
    when(mock.getStatusPollingDelayTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    return mock;

  }

  @Bean
  public PollingFederationService pollingFederationService() {
    return Mockito.mock(PollingFederationService.class);
  }

}
