# blobstore-cache

Wraps any Blobstore instance and caches the binary content in chunks.

## Usage

    // Get the original blobstore from somewhere
    Blobstore originalBlobstore = getBlobstore();
    
    // Get a JTA Transaction aware cache from somewhere that implements
    // the Map interface.
    Map<List<Byte>, byte[]> cache = getCache();
    
    // Get a JTA Transaction Manager from somewhere
    TransactionManager transactionManager = getTransactionManager();
    
    Blobstore cachedBlobstore = new CachedBlobstore(originalBlobstore,
        cache, 1024, transactionManager);

After that, you can use the _cachedBlobstore_ instance as it is described
in the documentation of [blobstore-api][1].

Please note that to make the content of the cached blobstore consistent,
the original blobstore should not be used directly.

[1]: https://github.com/everit-org/blobstore-api