/**
 * Copyright (C) 2016-2018 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
