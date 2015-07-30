package org.everit.blobstore.cache.internal;

import java.util.List;
import java.util.Map;

import org.everit.blobstore.BlobAccessor;

/**
 * Blob accessor that updates the cache during calling the close method.
 *
 */
public class CacheUpdaterBlobAccessor implements BlobAccessor {

  private final Map<List<Byte>, byte[]> cache;

  private final int defaultChunkSize;

  private final BlobAccessor wrapped;

  /**
   * Constructor.
   * 
   * @param wrapped
   *          The original blob accessor that is cached.
   * @param cache
   *          The cache.
   * @param defaultChunkSize
   *          The default chunk size that is used for this blob in the cache.
   */
  public CacheUpdaterBlobAccessor(final BlobAccessor wrapped, final Map<List<Byte>, byte[]> cache,
      final int defaultChunkSize) {
    this.wrapped = wrapped;
    this.cache = cache;
    this.defaultChunkSize = defaultChunkSize;
  }

  @Override
  public void close() {
    cache.put(
        Codec7BitUtil
            .toUnmodifiableList(Codec7BitUtil.encodeLongsTo7BitByteArray(wrapped.getBlobId())),
        new BlobCacheHeadValue(wrapped.getNewVersion(), wrapped.getSize(), defaultChunkSize)
            .toByteArray());
    wrapped.close();
  }

  @Override
  public long getBlobId() {
    return wrapped.getBlobId();
  }

  @Override
  public long getNewVersion() {
    return wrapped.getNewVersion();
  }

  @Override
  public long getPosition() {
    return wrapped.getPosition();
  }

  @Override
  public int read(final byte[] b, final int off, final int len) {
    return wrapped.read(b, off, len);
  }

  @Override
  public void seek(final long pos) {
    wrapped.seek(pos);
  }

  @Override
  public long getSize() {
    return wrapped.getSize();
  }

  @Override
  public void truncate(final long newLength) {
    wrapped.truncate(newLength);
  }

  @Override
  public long getVersion() {
    return wrapped.getVersion();
  }

  @Override
  public void write(final byte[] b, final int off, final int len) {
    wrapped.write(b, off, len);
  }

}
