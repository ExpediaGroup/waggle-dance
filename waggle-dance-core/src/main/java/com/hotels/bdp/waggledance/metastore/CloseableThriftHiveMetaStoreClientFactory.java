package com.hotels.bdp.waggledance.metastore;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.base.Joiner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetastoreTunnel;
import com.hotels.hcommon.hive.metastore.client.HiveMetaStoreClientSupplier;
import com.hotels.hcommon.hive.metastore.client.ReconnectingMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.TunnellingMetaStoreClientSupplierBuilder;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;

public class CloseableThriftHiveMetaStoreClientFactory implements ThriftHiveMetaStoreClientFactory {

  @Override
  public CloseableMetaStoreClient newInstance(AbstractMetaStore metaStore) {
    Map<String, String> properties = new HashMap<>();
    String uris = normaliseMetaStoreUris(metaStore.getRemoteMetaStoreUris());
    MetastoreTunnel metastoreTunnel = metaStore.getMetastoreTunnel();
    properties.put(HiveConf.ConfVars.METASTOREURIS.varname, uris);
    HiveConfFactory confFactory = new HiveConfFactory(Collections.<String> emptyList(), properties);
    HiveConf hiveConf = confFactory.newInstance();
    MetaStoreClientFactory metaStoreClientFactory = new ReconnectingMetaStoreClientFactory(hiveConf, metaStore.getName().toLowerCase(), 10);

    if (metastoreTunnel != null) {
      return new TunnellingMetaStoreClientSupplierBuilder()
          .withRoute(metastoreTunnel.getRoute())
          .withKnownHosts(metastoreTunnel.getKnownHosts())
          .withLocalHost(metastoreTunnel.getLocalhost())
          .withPort(metastoreTunnel.getPort())
          .withPrivateKeys(metastoreTunnel.getPrivateKeys())
          .withTimeout(metastoreTunnel.getTimeout())
          .withStrictHostKeyChecking(metastoreTunnel.getStrictHostKeyChecking())
          .build(hiveConf, metaStoreClientFactory).get();
    } else {
      return new HiveMetaStoreClientSupplier(metaStoreClientFactory).get();
    }
  }

  private String normaliseMetaStoreUris(String metaStoreUris) {
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
