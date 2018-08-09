package com.hotels.bdp.waggledance.client;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MetaStoreUriNormaliserTest {

  @Test
  public void normaliseValidUris() {
    MetaStoreUriNormaliser normaliser = new MetaStoreUriNormaliser();
    String firstUri = "thrift://localhost:1";
    String secondUri = "thrift://localhost:2";
    String thirdUri = "thrift://localhost:3";

    String uris = new StringBuilder(firstUri).append(",").append(secondUri).append(",").append(thirdUri).toString();
    String normalisedUris = normaliser.normaliseMetaStoreUris(uris);
    assertEquals(uris, normalisedUris);
  }

  @Test(expected = RuntimeException.class)
  public void normaliseInvalidUris() {
    MetaStoreUriNormaliser normaliser = new MetaStoreUriNormaliser();
    String firstUri = "thrift://localhost:1";
    String secondUri = "thrift://localhost:2";
    String thirdUri = "thrift://localh*;`#@32ost:3";

    String uris = new StringBuilder(firstUri).append(",").append(secondUri).append(",").append(thirdUri).toString();
    normaliser.normaliseMetaStoreUris(uris);
  }

}
