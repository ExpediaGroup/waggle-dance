/**
 * Copyright (C) 2016-2017 Expedia Inc.
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class CommonVFSResource extends AbstractResource implements WritableResource {

  private final URI location;
  private FileSystemManager fsManager;
  private FileObject fileObject;

  public CommonVFSResource(URI location) {
    Assert.notNull(location, "Location must not be null");
    this.location = location;
    init();
  }

  public CommonVFSResource(String uri) {
    Assert.notNull(uri, "URI must not be null");
    try {
      location = new URI(uri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("URI '" + uri + "' is invalid", e);
    }
    init();
  }

  private void init() {
    try {
      fsManager = VFS.getManager();
    } catch (FileSystemException e) {
      throw new RuntimeException("Unable to initialize Virtual File System", e);
    }
    try {
      fileObject = fsManager.resolveFile(location);
    } catch (FileSystemException e) {
      throw new RuntimeException("Unable to resolve location '" + location + "' with Virutal File System", e);
    }
  }

  /**
   * This implementation returns whether the underlying file exists.
   *
   * @see java.io.File#exists()
   */
  @Override
  public boolean exists() {
    try {
      return fileObject.exists();
    } catch (FileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isReadable() {
    try {
      return (fileObject.isFile() && fileObject.isReadable());
    } catch (FileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return fileObject.getContent().getInputStream();
  }

  @Override
  public boolean isWritable() {
    try {
      return (fileObject.isFile() && fileObject.isWriteable());
    } catch (FileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return fileObject.getContent().getOutputStream();
  }

  @Override
  public URL getURL() throws IOException {
    try {
      return location.toURL();
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Cannot convert '" + location + "' to URL", e);
    }
  }

  @Override
  public URI getURI() throws IOException {
    return location;
  }

  @Override
  public File getFile() {
    try {
      return new File(location);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Cannot convert '" + location + "' to File", e);
    }
  }

  @Override
  public long contentLength() throws IOException {
    return fileObject.getContent().getSize();
  }

  @Override
  public CommonVFSResource createRelative(String relativePath) {
    String pathToUse = StringUtils.applyRelativePath(location.getPath(), relativePath);
    URI locationToUse = null;
    try {
      locationToUse = new URI(location.getScheme(), location.getUserInfo(), location.getHost(), location.getPort(),
          pathToUse, location.getQuery(), location.getFragment());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Unable to build relative URI", e);
    }
    return new CommonVFSResource(locationToUse);
  }

  @Override
  public String getFilename() {
    return fileObject.getName().getBaseName();
  }

  @Override
  public String getDescription() {
    return "file [" + location + "]";
  }

  @Override
  public boolean equals(Object obj) {
    return (obj == this || (obj instanceof CommonVFSResource && location.equals(((CommonVFSResource) obj).location)));
  }

  @Override
  public int hashCode() {
    return location.hashCode();
  }

}
