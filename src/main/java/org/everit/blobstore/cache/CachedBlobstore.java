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
import java.util.function.Consumer;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.blobstore.cache.CachedBlobReaderImpl.BlobCacheHeadValue;
import org.everit.osgi.transaction.helper.api.TransactionHelper;

/**
 * Wrapper class for Blobstore implementations that use caching in a Map. In case no cache is set
 * for the store, caching is disabled.
 */
public class CachedBlobstore implements Blobstore {

  private final Map<List<Byte>, byte[]> cache;

  private final int defaultChunkSize;

  private TransactionHelper transactionHelper;

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
      final int defaultChunkSize) {
    this.wrapped = wrapped;
    this.cache = cache;
    this.defaultChunkSize = defaultChunkSize;
  }

  @Override
  public long createBlob(final Consumer<BlobAccessor> createAction) {
    return wrapped.createBlob(createAction);
  }

  @Override
  public void deleteBlob(final long blobId) {
    wrapped.deleteBlob(blobId);
    cache.remove(Arrays.asList(Codec7BitUtil.encodeLongsTo7BitByteArray(blobId, 0)));
  }

  /**
   * Retrieves the size and version information of the <code>BLOB</code> from the first place it
   * finds: member variable, cache, wrapped reader. The retrieved information might be modified by a
   * subclass that reduces or extends the size of the <code>BLOB</code>.
   *
   * @return The size and version information of the <code>BLOB</code>.
   */
  protected BlobCacheHeadValue getBlobHeadValue(final long blobId) {
    List<Byte> cacheHeadId = Codec7BitUtil.toUnmodifiableList(Codec7BitUtil
        .encodeLongsTo7BitByteArray(blobId));

    byte[] head = cache.get(cacheHeadId);
    if (head != null) {
      long[] longs = Codec7BitUtil.decode7BitToLongs(head);
      BlobCacheHeadValue blobCacheHead = new BlobCacheHeadValue();
      blobCacheHead.version = longs[0];
      blobCacheHead.size = longs[1];
      blobCacheHead.chunkSize = (int) longs[2];
      return blobCacheHead;
    } else {
      return transactionHelper.requiresNew(() -> {
        BlobCacheHeadValue blobCacheHead = new BlobCacheHeadValue();
        wrapped.updateBlob(blobId, (blobAccessor) -> {
          blobCacheHead.size = blobAccessor.size();
          blobCacheHead.version = blobAccessor.version();
          blobCacheHead.chunkSize = defaultChunkSize;
          cache.put(cacheHeadId, Codec7BitUtil.encodeLongsTo7BitByteArray(blobCacheHead.version,
              blobCacheHead.size, defaultChunkSize));
        });

        return blobCacheHead;
      });

    }
  }

  Blobstore getWrapped() {
    return wrapped;
  }

  @Override
  public void readBlob(final long blobId, final Consumer<BlobReader> readingAction) {
    BlobCacheHeadValue sizeVersionAndChunkSize = getBlobHeadValue(blobId);
    transactionHelper.required(() -> {
      wrapped.readBlob(blobId,
          (blobReader -> readingAction.accept(new CachedBlobReaderImpl<BlobReader>(blobId,
              blobReader, cache, sizeVersionAndChunkSize))));
      return null;
    });

  }

  public void setTransactionHelper(final TransactionHelper transactionHelper) {
    this.transactionHelper = transactionHelper;
  }

  @Override
  public void updateBlob(final long blobId, final Consumer<BlobAccessor> updatingAction) {
    transactionHelper.required(() -> {
      wrapped.updateBlob(blobId, blobAccessor -> {
        updatingAction.accept(blobAccessor);
        long newVersion = blobAccessor.newVersion();

        cache.put(
            Codec7BitUtil.toUnmodifiableList(Codec7BitUtil.encodeLongsTo7BitByteArray(blobId)),
            Codec7BitUtil.encodeLongsTo7BitByteArray(newVersion, blobAccessor.size()));
      });
      return null;
    });
  }

}
