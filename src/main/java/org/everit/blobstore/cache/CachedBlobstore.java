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
import org.everit.osgi.transaction.helper.api.TransactionHelper;

/**
 * Wrapper class for Blobstore implementations that use caching in a Map. In case no cache is set
 * for the store, caching is disabled.
 */
public class CachedBlobstore implements Blobstore {

  private final Map<List<Byte>, byte[]> cache;

  private TransactionHelper transactionHelper;

  private final Blobstore wrapped;

  public CachedBlobstore(final Blobstore wrapped, final Map<List<Byte>, byte[]> cache) {
    this.wrapped = wrapped;
    this.cache = cache;
  }

  @Override
  public long createBlob(final Consumer<BlobAccessor> createAction) {
    return wrapped.createBlob(createAction);
  }

  @Override
  public void deleteBlob(final long blobId) {
    wrapped.deleteBlob(blobId);
    cache.remove(Arrays.asList(BlobstoreCacheUtil.encodeLongsTo7BitByteArray(blobId, 0)));
  }

  Blobstore getWrapped() {
    return wrapped;
  }

  @Override
  public void readBlob(final long blobId, final Consumer<BlobReader> readingAction) {
    transactionHelper.required(() -> {
      wrapped.readBlob(blobId,
          (blobReader -> readingAction.accept(new CachedBlobReaderImpl<BlobReader>(blobReader,
              cache))));
      return null;
    });

  }

  public void setTransactionHelper(final TransactionHelper transactionHelper) {
    this.transactionHelper = transactionHelper;
  }

  @Override
  public void updateBlob(final long blobId, final Consumer<BlobAccessor> updatingAction) {
    transactionHelper.required(() -> {
      wrapped.updateBlob(blobId, (blobAccessor -> updatingAction
          .accept(new CachedBlobAccessorImpl<BlobAccessor>(blobAccessor, cache))));
      return null;
    });
  }

}
