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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.everit.blobstore.api.BlobReader;

/**
 * Cached version of the {@link BlobReader} interface that needs an actual persistent store
 * implementation.
 *
 * @param <T_CHANNEL>
 *          The actual type of the wrapped <code>BLOB</code> channel.
 */
class CachedBlobReaderImpl<T_CHANNEL extends BlobReader> implements BlobReader {

  /**
   * Holder class for Blob metadata that is stored in the chunk with index zero in the cache.
   */
  protected static class SizeAndVersion {
    public long size;

    public long version;
  }

  private long blobId;

  protected final Map<List<Byte>, byte[]> cache;

  protected long position = 0;

  protected SizeAndVersion sizeAndVersion;

  protected final T_CHANNEL wrapped;

  public CachedBlobReaderImpl(final long blobId, final T_CHANNEL wrapped,
      final Map<List<Byte>, byte[]> cache) {
    this.blobId = blobId;
    this.wrapped = wrapped;
    this.cache = cache;
  }

  /**
   * Retrieves the size and version information of the <code>BLOB</code> from the first place it
   * finds: member variable, cache, wrapped reader. The retrieved information might be modified by a
   * subclass that reduces or extends the size of the <code>BLOB</code>.
   *
   * @return The size and version information of the <code>BLOB</code>.
   */
  protected SizeAndVersion getSizeAndVersion() {
    if (sizeAndVersion == null) {
      byte[] head = cache.get(Arrays.asList(Codec7BitUtil.encodeLongsTo7BitByteArray(blobId)));
      if (head != null) {
        // TODO get size and version from head
      } else {
        sizeAndVersion = new SizeAndVersion();
        sizeAndVersion.size = wrapped.size();
        sizeAndVersion.version = wrapped.version();
      }
    }
    return sizeAndVersion;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void seek(final long pos) {
    if (pos < 0 || pos > size()) {
      throw new IndexOutOfBoundsException();
    }
    position = pos;
  }

  @Override
  public long size() {
    return getSizeAndVersion().size;
  }

  @Override
  public long version() {
    return getSizeAndVersion().version;
  }

}
