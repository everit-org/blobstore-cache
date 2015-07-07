package org.everit.blobstore.cache.internal;

import java.util.List;
import java.util.Map;

import org.everit.blobstore.api.BlobAccessor;

public class CacheUpdaterBlobAccessor implements BlobAccessor {

  private final Map<List<Byte>, byte[]> cache;

  private final int defaultChunkSize;

  private final BlobAccessor wrapped;

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
        Codec7BitUtil.encodeLongsTo7BitByteArray(wrapped.newVersion(),
            wrapped.size(), defaultChunkSize));
    wrapped.close();
  }

  @Override
  public long getBlobId() {
    return wrapped.getBlobId();
  }

  @Override
  public long newVersion() {
    return wrapped.newVersion();
  }

  @Override
  public long position() {
    return wrapped.position();
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
  public long size() {
    return wrapped.size();
  }

  @Override
  public void truncate(final long newLength) {
    wrapped.truncate(newLength);
  }

  @Override
  public long version() {
    return wrapped.version();
  }

  @Override
  public void write(final byte[] b, final int off, final int len) {
    wrapped.write(b, off, len);
  }

}
