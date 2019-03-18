/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.waggledance.spring;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

@RunWith(MockitoJUnitRunner.class)
public class CommonVFSResourceTest {

  private static final String TEST_RESOURCE_NAME = "test-resource.ext";
  private static final String CONTENT = "Content";

  public @Rule TemporaryFolder tmp = new TemporaryFolder();
  private File testResource;

  @Before
  public void init() throws Exception {
    testResource = tmp.newFile(TEST_RESOURCE_NAME);
    Files.write(CONTENT, testResource, Charsets.UTF_8);
  }

  private String toUriString(File file) {
    StringBuilder sb = new StringBuilder("file://").append(file.getAbsolutePath());
    if (file.isDirectory()) {
      sb.append("/");
    }
    return sb.toString();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidUriException() {
    new CommonVFSResource("invalid_uri@*&^");
  }

  @Test(expected = RuntimeException.class)
  public void initUnresolvableLocation() {
    new CommonVFSResource("not-existing.dat");
  }

  @Test
  public void exists() {
    assertThat(new CommonVFSResource(testResource.getAbsolutePath()).exists(), is(true));
  }

  @Test
  public void notExists() throws Exception {
    assertThat(new CommonVFSResource(new File(tmp.getRoot(), "not-existing.dat").getAbsolutePath()).exists(),
        is(false));
  }

  @Test
  public void isReadable() {
    assertThat(new CommonVFSResource(testResource.getAbsolutePath()).isReadable(), is(true));
  }

  @Test
  public void isNotReadableIfFileDoesNotExist() {
    assertThat(new CommonVFSResource(new File(tmp.getRoot(), "not-existing.dat").getAbsolutePath()).isReadable(),
        is(false));
  }

  @Test
  public void isNotReadableIfDirectory() {
    assertThat(new CommonVFSResource(tmp.getRoot().getAbsolutePath()).isReadable(), is(false));
  }

  @Test
  public void getInputStream() throws Exception {
    InputStream is = new CommonVFSResource(testResource.getAbsolutePath()).getInputStream();
    assertThat(is, is(notNullValue()));
  }

  @Test(expected = IOException.class)
  public void getInputStreamFailsIfFileDoesNotExist() throws Exception {
    new CommonVFSResource(new File(tmp.getRoot(), "not-existing.dat").getAbsolutePath()).getInputStream();
  }

  @Test(expected = IOException.class)
  public void getInputStreamFailsIfDirectory() throws Exception {
    new CommonVFSResource(tmp.getRoot().getAbsolutePath()).getInputStream();
  }

  @Test
  public void isWritable() {
    assertThat(new CommonVFSResource(testResource.getAbsolutePath()).isWritable(), is(true));
  }

  @Test
  public void isNotWritableIfFileDoesNotExist() {
    assertThat(new CommonVFSResource(new File(tmp.getRoot(), "not-existing.dat").getAbsolutePath()).isWritable(),
        is(false));
  }

  @Test
  public void isNotWritableIfDirectory() {
    assertThat(new CommonVFSResource(tmp.getRoot().getAbsolutePath()).isWritable(), is(false));
  }

  @Test
  public void getOutputStream() throws Exception {
    OutputStream os = new CommonVFSResource(testResource.getAbsolutePath()).getOutputStream();
    assertThat(os, is(notNullValue()));
  }

  @Test
  public void getOutputStreamFailsIfFileDoesNotExist() throws Exception {
    OutputStream os = new CommonVFSResource(new File(tmp.getRoot(), "not-existing.dat").getAbsolutePath())
        .getOutputStream();
    assertThat(os, is(notNullValue()));
  }

  @Test(expected = IOException.class)
  public void getOutputStreamFailsIfDirectory() throws Exception {
    new CommonVFSResource(tmp.getRoot().getAbsolutePath()).getOutputStream();
  }

  @Test
  public void getURI() throws Exception {
    URI uri = new CommonVFSResource(testResource.getAbsolutePath()).getURI();
    assertThat(uri.toString(), is(testResource.getAbsolutePath()));
  }

  @Test
  public void getURIIfFileDoesNotExist() throws Exception {
    File notExistingFile = new File(tmp.getRoot(), "not-existing.dat");
    URI uri = new CommonVFSResource(notExistingFile.getAbsolutePath()).getURI();
    assertThat(uri.toString(), is(notExistingFile.getAbsolutePath()));
  }

  @Test
  public void getURIIfDirectory() throws Exception {
    URI uri = new CommonVFSResource(tmp.getRoot().getAbsolutePath()).getURI();
    assertThat(uri.toString(), is(tmp.getRoot().getAbsolutePath()));
  }

  @Test(expected = RuntimeException.class)
  public void cannotConvertLocationToURL() throws Exception {
    new CommonVFSResource(testResource.getAbsolutePath()).getURL();
  }

  public void getURL() throws Exception {
    URL url = new CommonVFSResource(toUriString(testResource)).getURL();
    assertThat(url, is(testResource.toURI().toURL()));
  }

  @Test
  public void getURLIfFileDoesNotExist() throws Exception {
    File notExistingFile = new File(tmp.getRoot(), "not-existing.dat");
    URL url = new CommonVFSResource(toUriString(notExistingFile)).getURL();
    assertThat(url, is(notExistingFile.toURI().toURL()));
  }

  @Test
  public void getURLIfDirectory() throws Exception {
    URL url = new CommonVFSResource(toUriString(tmp.getRoot())).getURL();
    assertThat(url, is(tmp.getRoot().toURI().toURL()));
  }

  @Test(expected = RuntimeException.class)
  public void cannotConvertToFile() throws Exception {
    new CommonVFSResource(testResource.getAbsolutePath()).getFile();
  }

  @Test
  public void getFile() throws Exception {
    File file = new CommonVFSResource(toUriString(testResource)).getFile();
    assertThat(file, is(testResource));
  }

  @Test
  public void contentLength() throws Exception {
    long contentLength = new CommonVFSResource(testResource.getAbsolutePath()).contentLength();
    assertThat(contentLength, is((long) CONTENT.length()));
  }

  @Test
  public void createRelative() throws Exception {
    CommonVFSResource relativeResource = new CommonVFSResource(toUriString(testResource))
        .createRelative("relative.dat");
    assertThat(relativeResource.getURI(), is(new File(tmp.getRoot(), "relative.dat").toURI()));
  }

  @Test
  public void getFilename() {
    String filename = new CommonVFSResource(testResource.getAbsolutePath()).getFilename();
    assertThat(filename, is(TEST_RESOURCE_NAME));
  }

  @Test
  public void getDescription() {
    String description = new CommonVFSResource(toUriString(testResource)).getDescription();
    assertThat(description, is("file [file://" + testResource.getAbsolutePath() + "]"));
  }

  @Test
  public void equalsSameObject() {
    CommonVFSResource resource = new CommonVFSResource(testResource.getAbsolutePath());
    assertTrue(resource.equals(resource));
  }

  @Test
  public void equalsSameLocation() {
    CommonVFSResource firstResource = new CommonVFSResource(testResource.getAbsolutePath());
    CommonVFSResource secondResource = new CommonVFSResource(testResource.getAbsolutePath());
    assertTrue(firstResource.equals(secondResource));
  }

  @Test
  public void equalsDifferentLocation() {
    CommonVFSResource firstResource = new CommonVFSResource(testResource.getAbsolutePath());
    CommonVFSResource secondResource = new CommonVFSResource(
        new File(tmp.getRoot(), "different_location").getAbsolutePath());
    assertFalse(firstResource.equals(secondResource));
  }

  @Test
  public void equalsDifferentType() {
    assertFalse(new CommonVFSResource(testResource.getAbsolutePath()).equals("string"));
  }
}
