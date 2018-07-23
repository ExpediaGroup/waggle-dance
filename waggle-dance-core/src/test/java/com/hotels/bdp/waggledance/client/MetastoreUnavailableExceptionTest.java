package com.hotels.bdp.waggledance.client;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class MetastoreUnavailableExceptionTest {

  private final String errorMessage = "error";

  @Test
  public void messageError() {
    MetastoreUnavailableException exception = new MetastoreUnavailableException(errorMessage);
    assertThat(exception.getMessage(), is(errorMessage));
    assertNull(exception.getCause());
  }

  @Test
  public void messageAndCauseError() {
    Throwable cause = new Throwable();
    MetastoreUnavailableException exception = new MetastoreUnavailableException(errorMessage, cause);
    assertThat(exception.getMessage(), is(errorMessage));
    assertThat(exception.getCause(), is(cause));
  }

}
