package com.hotels.bdp.waggledance.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Joiner;

public class MetaStoreUriNormaliser {

  public String normalise(String metaStoreUris) {
    try {
      String[] rawUris = metaStoreUris.split(",");
      Set<String> uris = new TreeSet<>();
      for (String rawUri : rawUris) {
        URI uri = new URI(rawUri);
        uris.add(uri.toString());
      }
      return Joiner.on(",").join(uris);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
