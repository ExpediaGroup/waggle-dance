package com.hotels.bdp.waggledance.client;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class MetastoreUnavailableExceptionTest {

  @Test
  public void messageError() {
    MetastoreUnavailableException exception = new MetastoreUnavailableException("error");
    assertThat(exception.getMessage(), is("error"));
    assertNull(exception.getCause());
  }

  @Test
  public void messageAndCauseError() {
    Throwable cause = new Throwable();
    MetastoreUnavailableException exception = new MetastoreUnavailableException("error", cause);
    assertThat(exception.getMessage(), is("error"));
    assertThat(exception.getCause(), is(cause));
  }

}
