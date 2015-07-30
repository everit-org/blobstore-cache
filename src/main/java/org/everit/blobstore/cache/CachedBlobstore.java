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

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.everit.blobstore.BlobAccessor;
import org.everit.blobstore.BlobReader;
import org.everit.blobstore.Blobstore;
import org.everit.blobstore.cache.internal.CacheUpdaterBlobAccessor;
import org.everit.blobstore.cache.internal.CachedBlobReaderImpl;
import org.everit.blobstore.cache.internal.Codec7BitUtil;
import org.everit.transaction.unchecked.UncheckedSystemException;

/**
 * Wrapper class for Blobstore implementations that use caching in a Map. In case no cache is set
 * for the store, caching is disabled.
 */
public class CachedBlobstore implements Blobstore {

  private final Map<List<Byte>, byte[]> cache;

  private final int defaultChunkSize;

  private final TransactionManager transactionManager;

  private final Blobstore wrapped;

  /**
   * Constructor.
   *
   * @param wrapped
   *          The wrapped {@link Blobstore} implementation.
   * @param cache
   *          The cache where the data will be stored.
   * @param defaultChunkSize
   *          The default chunk size that the {@link Blobstore} will use to store the data in cache.
   */
  public CachedBlobstore(final Blobstore wrapped, final Map<List<Byte>, byte[]> cache,
      final int defaultChunkSize, final TransactionManager transactionManager) {
    this.wrapped = wrapped;
    this.cache = cache;
    this.defaultChunkSize = defaultChunkSize;
    this.transactionManager = transactionManager;
  }

  private void checkActiveTransaction() {
    try {
      int status = transactionManager.getStatus();
      if (status != Status.STATUS_ACTIVE) {
        throw new IllegalStateException("Blobs can be accessed only in active transaction");
      }
    } catch (SystemException e) {
      throw new UncheckedSystemException(e);
    }

  }

  @Override
  public BlobAccessor createBlob() {
    return wrapped.createBlob();
  }

  @Override
  public void deleteBlob(final long blobId) {
    checkActiveTransaction();
    wrapped.deleteBlob(blobId);
    cache.remove(
        Codec7BitUtil.toUnmodifiableList(Codec7BitUtil.encodeLongsTo7BitByteArray(blobId, 0)));
  }

  @Override
  public BlobReader readBlob(final long blobId) {
    return new CachedBlobReaderImpl(blobId, wrapped, cache, transactionManager, defaultChunkSize);
  }

  @Override
  public BlobReader readBlobForUpdate(final long blobId) {
    return wrapped.readBlobForUpdate(blobId);
  }

  @Override
  public BlobAccessor updateBlob(final long blobId) {
    checkActiveTransaction();
    BlobAccessor blobAccessor = wrapped.updateBlob(blobId);

    List<Byte> blobHeadCacheId =
        Codec7BitUtil.toUnmodifiableList(Codec7BitUtil.encodeLongsTo7BitByteArray(blobId));

    // Remove must be called as in case of invalidation cache, the other nodes must be notified
    cache.remove(blobHeadCacheId);

    return new CacheUpdaterBlobAccessor(blobAccessor, cache, defaultChunkSize);

  }
}
