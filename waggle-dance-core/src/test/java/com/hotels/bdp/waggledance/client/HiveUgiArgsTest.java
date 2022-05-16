package com.hotels.bdp.waggledance.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;

import org.junit.Test;

public class HiveUgiArgsTest {

  @Test
  public void groups() throws Exception {
    HiveUgiArgs args = new HiveUgiArgs("user", new ArrayList<>());
    assertThat("user", is(args.getUser()));
    assertThat(args.getGroups().size(), is(0));
    // List should be mutable, Hive code mutates potentially mutates it.
    args.getGroups().add("user");
    assertThat(args.getGroups().size(), is(1));
  }

  @Test
  public void groupDefaults() throws Exception {
    HiveUgiArgs args = HiveUgiArgs.WAGGLE_DANCE_DEFAULT;
    assertThat("waggledance", is(args.getUser()));
    assertThat(args.getGroups().size(), is(0));
    // List should be mutable, Hive code mutates potentially mutates it.
    args.getGroups().add("user");
    assertThat(args.getGroups().size(), is(1));
  }

  @Test
  public void groupsImmutable() throws Exception {
    HiveUgiArgs args = new HiveUgiArgs("user", Collections.emptyList());
    assertThat("user", is(args.getUser()));
    assertThat(args.getGroups().size(), is(0));
    // List should be mutable, Hive code mutates potentially mutates it.
    args.getGroups().add("user");
    assertThat(args.getGroups().size(), is(1));
  }

  @Test
  public void groupsNull() throws Exception {
    HiveUgiArgs args = new HiveUgiArgs("user", null);
    assertThat("user", is(args.getUser()));
    assertTrue(args.getGroups().isEmpty());
  }

}
