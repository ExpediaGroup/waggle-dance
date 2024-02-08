package com.hotels.bdp.waggledance.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SplitTrafficMetastoreClientFactoryTest {

  private @Mock CloseableThriftHiveMetastoreIface readWrite;
  private @Mock CloseableThriftHiveMetastoreIface readOnly;
  private @Mock Table readTable;
  private @Mock Table writeTable;

  @Test
  public void new_instance_getTable() throws Exception {
    CloseableThriftHiveMetastoreIface client = new SplitTrafficMetastoreClientFactory()
        .newInstance(readWrite, readOnly);
    when(readOnly.get_table("a", "b")).thenReturn(readTable);

    Table table = client.get_table("a", "b");

    assertThat(table, is(readTable));
    verifyNoInteractions(readWrite);
  }

  @Test
  public void new_instance_alterTable() throws Exception {
    CloseableThriftHiveMetastoreIface client = new SplitTrafficMetastoreClientFactory()
        .newInstance(readWrite, readOnly);

    client.alter_table("a", "b", writeTable);

    verify(readWrite).alter_table("a", "b", writeTable);
    verifyNoInteractions(readOnly);
  }

  @Test
  public void new_instance_close() throws Exception {
    CloseableThriftHiveMetastoreIface client = new SplitTrafficMetastoreClientFactory()
        .newInstance(readWrite, readOnly);

    client.close();

    verify(readWrite).close();
    verify(readOnly).close();
  }

  @Test
  public void new_instance_set_ugi() throws Exception {
    CloseableThriftHiveMetastoreIface client = new SplitTrafficMetastoreClientFactory()
        .newInstance(readWrite, readOnly);

    List<String> expected = Arrays.asList("result");
    when(readOnly.set_ugi("a", Arrays.asList("b"))).thenReturn(expected);
    when(readWrite.set_ugi("a", Arrays.asList("b"))).thenReturn(expected);
    List<String> result = client.set_ugi("a", Arrays.asList("b"));

    assertThat(result, is(expected));
    verify(readOnly).set_ugi("a", Arrays.asList("b"));
    verify(readWrite).set_ugi("a", Arrays.asList("b"));
  }

  @Test
  public void new_instance_isOpen_true() throws Exception {
    CloseableThriftHiveMetastoreIface client = new SplitTrafficMetastoreClientFactory()
        .newInstance(readWrite, readOnly);
    when(readOnly.isOpen()).thenReturn(true);
    when(readWrite.isOpen()).thenReturn(true);
    boolean isOpen = client.isOpen();

    assertThat(isOpen, is(true));
  }

  @Test
  public void new_instance_isOpen_false() throws Exception {
    CloseableThriftHiveMetastoreIface client = new SplitTrafficMetastoreClientFactory()
        .newInstance(readWrite, readOnly);
    when(readWrite.isOpen()).thenReturn(false);
    boolean isOpen = client.isOpen();

    assertThat(isOpen, is(false));
    verifyNoInteractions(readOnly);
  }

  @Test
  public void new_instance_isOpen_readOnly_false() throws Exception {
    CloseableThriftHiveMetastoreIface client = new SplitTrafficMetastoreClientFactory()
        .newInstance(readWrite, readOnly);
    when(readOnly.isOpen()).thenReturn(false);
    when(readWrite.isOpen()).thenReturn(true);
    boolean isOpen = client.isOpen();

    assertThat(isOpen, is(false));
  }

  @Test
  public void new_instance_isOpen_readWrite_false() throws Exception {
    CloseableThriftHiveMetastoreIface client = new SplitTrafficMetastoreClientFactory()
        .newInstance(readWrite, readOnly);
    when(readWrite.isOpen()).thenReturn(false);
    boolean isOpen = client.isOpen();

    assertThat(isOpen, is(false));
    verifyNoInteractions(readOnly);
  }

}
