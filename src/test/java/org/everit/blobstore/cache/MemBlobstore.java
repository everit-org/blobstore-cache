package org.everit.blobstore.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.blobstore.api.BlobstoreException;

public class MemBlobstore implements Blobstore {

  public static class BlobData {
    public List<Byte> content;

    public long version;

    public BlobData(final long version, final List<Byte> content) {
      this.version = version;
      this.content = content;
    }

  }

  private Map<Long, BlobData> blobs = new ConcurrentHashMap<>();

  private AtomicLong nextBlobId = new AtomicLong();

  @Override
  public long createBlob(final Consumer<BlobAccessor> createAction) {
    BlobData data = new BlobData(0, new ArrayList<>());
    MemBlobAccessorImpl blobAccessor = new MemBlobAccessorImpl(data, 0);
    long blobId = nextBlobId.getAndIncrement();
    createAction.accept(blobAccessor);
    blobs.put(blobId, data);
    return blobId;
  }

  @Override
  public void deleteBlob(final long blobId) {
    blobs.remove(blobId);
  }

  @Override
  public void readBlob(final long blobId, final Consumer<BlobReader> readingAction) {
    BlobData data = blobs.get(blobId);
    if (data == null) {
      throw new BlobstoreException("Blob not available");
    }
    readingAction.accept(new MemBlobAccessorImpl(data, data.version));
  }

  @Override
  public void updateBlob(final long blobId, final Consumer<BlobAccessor> updatingAction) {
    BlobData data = blobs.get(blobId);
    if (data == null) {
      throw new BlobstoreException("No blob available with id " + blobId);
    }
    // TODO instead of synchronization, use locks until the transaction is done
    synchronized (data) {
      BlobData newData = new BlobData(data.version + 1, new ArrayList<Byte>(data.content));
      updatingAction.accept(new MemBlobAccessorImpl(newData, data.version));
      blobs.put(blobId, newData);
    }
  }

}
