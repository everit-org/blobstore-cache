package org.everit.blobstore.cache;

import java.util.ArrayList;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.cache.MemBlobstore.BlobData;

public class MemBlobAccessorImpl implements BlobAccessor {

  private final BlobData data;

  private long position = 0;

  private final long previousVersion;

  public MemBlobAccessorImpl(final BlobData data, final long previousVersion) {
    this.data = data;
    this.previousVersion = previousVersion;
  }

  @Override
  public long newVersion() {
    return data.version;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) {
    int n = (int) (size() - position);
    if (n == 0) {
      return -1;
    }
    if (len < n) {
      n = len;
    }

    int loff = off;
    for (int i = 0; i < n; i++) {
      b[loff] = data.content.get((int) position);
      loff++;
      position++;
    }
    return n;
  }

  @Override
  public void seek(final long pos) {
    if (pos > size() || pos < 0) {
      throw new IllegalArgumentException();
    }
    position = pos;
  }

  @Override
  public long size() {
    return data.content.size();
  }

  @Override
  public void truncate(final long newLength) {
    if (newLength < 0 || newLength > size() || position > newLength) {
      throw new IllegalArgumentException();
    }
    data.content = new ArrayList<Byte>(data.content.subList(0, (int) newLength));
  }

  @Override
  public long version() {
    return previousVersion;
  }

  @Override
  public void write(final byte[] b, final int off, final int len) {
    for (int i = 0; i < len; i++) {
      if (position < size()) {
        data.content.set((int) position, b[off + i]);
      } else {
        data.content.add(b[off + i]);
      }
      position++;
    }
  }

}
