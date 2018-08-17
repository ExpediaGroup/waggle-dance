package com.hotels.bdp.waggledance.client;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MetaStoreUriNormaliserTest {

  @Test
  public void normaliseValidUri() {
    String uri = "thrift://localhost:1";

    String normalisedUris = MetaStoreUriNormaliser.normaliseMetaStoreUris(uri);
    assertEquals(uri, normalisedUris);
  }

  @Test
  public void normaliseValidUris() {
    String firstUri = "thrift://localhost:1";
    String secondUri = "thrift://localhost:2";
    String thirdUri = "thrift://localhost:3";

    String uris = new StringBuilder(firstUri).append(",").append(secondUri).append(",").append(thirdUri).toString();
    String normalisedUris = MetaStoreUriNormaliser.normaliseMetaStoreUris(uris);
    assertEquals(uris, normalisedUris);
  }

  @Test(expected = RuntimeException.class)
  public void normaliseInvalidUris() {
    String firstUri = "thrift://localhost:1";
    String secondUri = "thrift://localhost:2";
    String thirdUri = "thrift://localh*;`#@32ost:3";

    String uris = new StringBuilder(firstUri).append(",").append(secondUri).append(",").append(thirdUri).toString();
    MetaStoreUriNormaliser.normaliseMetaStoreUris(uris);
  }

}
