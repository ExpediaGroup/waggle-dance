package com.hotels.bdp.waggledance.api;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class WaggleDanceExceptionTest {

  String message = "message";
  Throwable cause = new Throwable();

  @Test
  public void messageException() {
    WaggleDanceException exception = new WaggleDanceException(message);
    assertThat(exception.getMessage(), is(message));
    assertNull(exception.getCause());
  }

  @Test
  public void messageAndCauseException() {
    WaggleDanceException exception = new WaggleDanceException(message, cause);
    assertThat(exception.getMessage(), is(message));
    assertThat(exception.getCause(), is(cause));
  }

}
