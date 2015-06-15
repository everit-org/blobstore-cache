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
  protected static class BlobCacheHeadValue {
    public int chunkSize;

    public long size;

    public long version;
  }

  protected final long blobId;

  protected final Map<List<Byte>, byte[]> cache;

  protected long position = 0;

  protected BlobCacheHeadValue sizeVersionAndChunkSize;

  public boolean versionVerifiedFromWrapped = false;

  protected final T_CHANNEL wrapped;

  /**
   * Constructor.
   *
   * @param blobId
   *          The id of the <code>BLOB</code> that this reader belongs to.
   * @param wrapped
   *          The wrapped <code>BLOB</code> channel.
   * @param cache
   *          The cache where the blob chunks and head eleement is stored.
   * @param blobCacheHead
   *          Data that is stored as head element in the cache about the blob.
   */
  public CachedBlobReaderImpl(final long blobId, final T_CHANNEL wrapped,
      final Map<List<Byte>, byte[]> cache, final BlobCacheHeadValue blobCacheHead) {
    this.blobId = blobId;
    this.wrapped = wrapped;
    this.cache = cache;
    this.sizeVersionAndChunkSize = blobCacheHead;
  }

  private List<Byte> calculateChunkId() {
    long chunkIndex = (position / sizeVersionAndChunkSize.chunkSize);
    List<Byte> chunkId = Codec7BitUtil.toUnmodifiableList(Codec7BitUtil.encodeLongsTo7BitByteArray(
        blobId, chunkIndex));
    return chunkId;
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
      calculatedLen = 0;
    }

    if (calculatedLen == 0) {
      return -1;
    }

    return readInternal(buffer, off, (int) calculatedLen);
  }

  protected byte[] readChunkFromWrapped() {
    long wrappedPosition = wrapped.position();
    if (wrappedPosition != position) {
      wrapped.seek(position);
    }
    int currentChunkSize = sizeVersionAndChunkSize.chunkSize;
    long remainingLenOfBlob = size() - position;
    if (currentChunkSize > remainingLenOfBlob) {
      currentChunkSize = (int) remainingLenOfBlob;
    }
    byte[] buffer = new byte[currentChunkSize];
    int off = 0;
    while (off < currentChunkSize) {
      int r = wrapped.read(buffer, off, currentChunkSize - off);
      if (r == -1) {
        throw new IllegalStateException("Blob " + blobId
            + " has less content in store than expected");
      }
      off += r;
    }

    return null;
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
      int chunkOffset = (int) (position % sizeVersionAndChunkSize.chunkSize);
      int readLength = chunk.length - chunkOffset;
      if (readLength < remainingLen) {
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
    return sizeVersionAndChunkSize.size;
  }

  @Override
  public long version() {
    return sizeVersionAndChunkSize.version;
  }

}
