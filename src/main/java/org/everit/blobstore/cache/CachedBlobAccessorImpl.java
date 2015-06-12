/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.blobstore.cache;

import java.util.List;
import java.util.Map;

import org.everit.blobstore.api.BlobAccessor;

/**
 * Cached version of the {@link BlobAccessor} interface that needs an actual persistent store
 * implementation.
 *
 * @param <T_CHANNEL>
 *          The actual type of the wrapped <code>BLOB</code> channel.
 */
class CachedBlobAccessorImpl<T_CHANNEL extends BlobAccessor> extends
    CachedBlobReaderImpl<T_CHANNEL> implements BlobAccessor {

  private Long newVersion;

  public CachedBlobAccessorImpl(final T_CHANNEL wrapped, final Map<List<Byte>, byte[]> cache) {
    super(wrapped, cache);
  }

  @Override
  public long newVersion() {
    if (newVersion == null) {
      newVersion = wrapped.newVersion();
    }
    return newVersion;
  }

  @Override
  public void truncate(final long len) {
    wrapped.truncate(len);
    size = len;
    // TODO Handle cache

  }

  @Override
  public void write(final byte[] b, final int off, final int len) {
    // TODO Auto-generated method stub

  }

}
