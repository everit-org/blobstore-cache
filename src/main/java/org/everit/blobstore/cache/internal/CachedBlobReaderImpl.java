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
package org.everit.blobstore.cache.internal;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;

/**
 * Cached version of the {@link BlobReader} interface that needs an actual persistent store
 * implementation.
 */
public class CachedBlobReaderImpl implements BlobReader {

  protected BlobCacheHeadValue blobHeadValue;

  protected final long blobId;

  protected final Map<List<Byte>, byte[]> cache;

  protected final int defaultChunkSize;

  protected final Blobstore originalBlobstore;

  protected long position = 0;

  protected final TransactionHelper transactionHelper;

  public boolean versionVerifiedFromWrapped = false;

  private BlobReader wrapped = null;

  /**
   * Constructor.
   *
   * @param blobId
   *          The id of the <code>BLOB</code> that this reader belongs to.
   * @param originalBlobstore
   *          The original blobstore that is cached.
   * @param cache
   *          The cache where the blob chunks and head eleement is stored.
   * @param defaultChunkSize
   *          The default chunk size in the cache.
   */
  public CachedBlobReaderImpl(final long blobId, final Blobstore originalBlobstore,
      final Map<List<Byte>, byte[]> cache, final TransactionManager transactionManager,
      final int defaultChunkSize) {
    this.blobId = blobId;
    this.originalBlobstore = originalBlobstore;
    this.cache = cache;
    this.defaultChunkSize = defaultChunkSize;
    TransactionHelperImpl transactionHelperImpl = new TransactionHelperImpl();
    transactionHelperImpl.setTransactionManager(transactionManager);
    this.transactionHelper = transactionHelperImpl;
  }

  private List<Byte> calculateChunkId() {
    long chunkIndex = (position / getBlobHeadValue().chunkSize);
    List<Byte> chunkId = Codec7BitUtil.toUnmodifiableList(Codec7BitUtil.encodeLongsTo7BitByteArray(
        blobId, chunkIndex));
    return chunkId;
  }

  @Override
  public void close() {
    if (wrapped != null) {
      wrapped.close();
    }
  }

  /**
   * Retrieves the size and version information of the <code>BLOB</code> from the first place it
   * finds: member variable, cache, wrapped reader. The retrieved information might be modified by a
   * subclass that reduces or extends the size of the <code>BLOB</code>.
   *
   * @return The size and version information of the <code>BLOB</code>.
   */
  protected BlobCacheHeadValue getBlobHeadValue() {
    if (blobHeadValue != null) {
      return blobHeadValue;
    }
    List<Byte> cacheHeadId = Codec7BitUtil.toUnmodifiableList(Codec7BitUtil
        .encodeLongsTo7BitByteArray(blobId));

    byte[] head = cache.get(cacheHeadId);
    if (head != null) {
      long[] longs = Codec7BitUtil.decode7BitToLongs(head);
      BlobCacheHeadValue blobCacheHead = new BlobCacheHeadValue();
      blobCacheHead.version = longs[0];
      blobCacheHead.size = longs[1];
      blobCacheHead.chunkSize = (int) longs[2];
      this.blobHeadValue = blobCacheHead;
    } else {
      transactionHelper.requiresNew(() -> {
        try (BlobReader blobReader = originalBlobstore.readBlobForUpdate(blobId)) {
          BlobCacheHeadValue blobHead = new BlobCacheHeadValue();

          blobHead.size = blobReader.size();
          blobHead.version = blobReader.version();
          blobHead.chunkSize = defaultChunkSize;
          cache.put(cacheHeadId, Codec7BitUtil.encodeLongsTo7BitByteArray(blobHead.version,
              blobHead.size, defaultChunkSize));

          this.blobHeadValue = blobHead;
          return null;
        }
      });
    }
    return blobHeadValue;
  }

  @Override
  public long getBlobId() {
    return blobId;
  }

  protected BlobReader getWrapped() {
    if (wrapped == null) {
      wrapped = originalBlobstore.readBlob(blobId);
    }
    return wrapped;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public int read(final byte[] buffer, final int off, final int len) {
    if (len > (buffer.length - off)) {
      throw new IllegalArgumentException("Byte array length without the offset is smaller"
          + " than the length that should be read from the stream");
    }

    if (len == 0) {
      return 0;
    }

    long calculatedLen = size() - position;
    if (len < calculatedLen) {
      calculatedLen = len;
    }

    if (calculatedLen == 0) {
      return -1;
    }

    return readInternal(buffer, off, (int) calculatedLen);
  }

  protected byte[] readChunkFromWrapped() {
    BlobReader lWrapped = getWrapped();
    BlobCacheHeadValue lBlobHeadValue = getBlobHeadValue();
    if (lWrapped.version() != lBlobHeadValue.version) {
      throw new ConcurrentModificationException(
          "The blob " + blobId + " has been changed since the reading was started");
    }

    int currentChunkSize = lBlobHeadValue.chunkSize;
    long chunkStartPosition = (position / currentChunkSize) * currentChunkSize;
    long wrappedPosition = lWrapped.position();
    if (wrappedPosition != chunkStartPosition) {
      lWrapped.seek(chunkStartPosition);
    }

    long remainingLenOfBlob = size() - chunkStartPosition;
    if (currentChunkSize > remainingLenOfBlob) {
      currentChunkSize = (int) remainingLenOfBlob;
    }
    byte[] buffer = new byte[currentChunkSize];
    int off = 0;
    while (off < currentChunkSize) {
      int r = lWrapped.read(buffer, off, currentChunkSize - off);
      if (r == -1) {
        throw new IllegalStateException("Blob " + blobId
            + " has less content in store than expected");
      }
      off += r;
    }

    return buffer;
  }

  protected int readInternal(final byte[] buffer, final int off, final int calculatedLen) {
    int remainingLen = calculatedLen;
    int currentBufferOffset = off;
    while (remainingLen > 0) {
      List<Byte> chunkId = calculateChunkId();
      byte[] chunk = cache.get(chunkId);
      if (chunk == null) {
        chunk = readChunkFromWrapped();
      }
      int chunkOffset = (int) (position % getBlobHeadValue().chunkSize);
      int readLength = chunk.length - chunkOffset;
      if (readLength > remainingLen) {
        readLength = remainingLen;
      }
      System.arraycopy(chunk, chunkOffset, buffer, currentBufferOffset, readLength);
      currentBufferOffset += readLength;
      position += readLength;
      remainingLen -= readLength;
    }
    return calculatedLen;
  }

  @Override
  public void seek(final long pos) {
    if (pos < 0 || pos > size()) {
      throw new IndexOutOfBoundsException("Blob " + blobId + " with size " + size()
          + " cannot set position " + pos);
    }
    position = pos;
  }

  @Override
  public long size() {
    return getBlobHeadValue().size;
  }

  @Override
  public long version() {
    return getBlobHeadValue().version;
  }

}
