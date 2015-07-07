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

/**
 * Holder class for Blob metadata that is stored in the chunk with index zero in the cache.
 */
public class BlobCacheHeadValue {

  /**
   * Creates the {@link BlobCacheHeadValue} based on the format as it is stored in the cache.
   *
   * @param bytea
   *          The cache representation of the head value.
   * @return The {@link BlobCacheHeadValue} instance.
   */
  public static BlobCacheHeadValue fromByteArray(final byte[] bytea) {
    long[] longs = Codec7BitUtil.decode7BitToLongs(bytea);
    return new BlobCacheHeadValue(longs[0], longs[1], (int) longs[2]);
  }

  public int chunkSize;

  public long size;

  public long version;

  /**
   * Constructor.
   *
   * @param version
   *          The version of the blob.
   * @param size
   *          The size of the blob.
   * @param chunkSize
   *          The chunk size that is used for caching the blob.
   */
  public BlobCacheHeadValue(final long version, final long size, final int chunkSize) {
    this.version = version;
    this.size = size;
    this.chunkSize = chunkSize;
  }

  /**
   * Converts the blob head value into the format as it is stored in the cache.
   *
   * @return The byte array format of the blob head value.
   */
  public byte[] toByteArray() {
    return Codec7BitUtil.encodeLongsTo7BitByteArray(version,
        size, chunkSize);
  }
}
