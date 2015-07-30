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

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;

import org.apache.geronimo.transaction.manager.GeronimoTransactionManager;
import org.everit.blobstore.Blobstore;
import org.everit.blobstore.mem.MemBlobstore;
import org.everit.blobstore.testbase.AbstractBlobstoreTest;
import org.everit.blobstore.testbase.BlobstoreStressAndConsistencyTester;
import org.everit.blobstore.testbase.BlobstoreStressAndConsistencyTester.BlobstoreStressTestConfiguration;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;
import org.everit.transaction.map.managed.ManagedMap;
import org.everit.transaction.map.readcommited.ReadCommitedTransactionalMap;
import org.everit.transaction.unchecked.xa.UncheckedXAException;
import org.junit.BeforeClass;
import org.junit.Test;

public class CachedBlobstoreTest extends AbstractBlobstoreTest {

  private static TransactionManager transactionManager;

  @BeforeClass
  public static void beforeClass() {
    try {
      transactionManager = new GeronimoTransactionManager(6000);
    } catch (XAException e) {
      throw new UncheckedXAException(e);
    }
  }

  @Override
  protected Blobstore getBlobStore() {
    return new CachedBlobstore(new MemBlobstore(transactionManager),
        new ManagedMap<>(new ReadCommitedTransactionalMap<>(null), transactionManager), 1024,
        transactionManager);
  }

  @Override
  protected TransactionHelper getTransactionHelper() {
    TransactionHelperImpl transactionHelperImpl = new TransactionHelperImpl();
    transactionHelperImpl.setTransactionManager(transactionManager);
    return transactionHelperImpl;
  }

  @Test
  public void testConsistency() {
    Blobstore cachedBlobstore = new CachedBlobstore(new MemBlobstore(transactionManager),
        new ManagedMap<>(new ReadCommitedTransactionalMap<>(null), transactionManager), 1024,
        transactionManager);

    Blobstore memBlobstore = new MemBlobstore(transactionManager);

    TransactionHelperImpl transactionHelper = new TransactionHelperImpl();
    transactionHelper.setTransactionManager(transactionManager);

    BlobstoreStressAndConsistencyTester.runStressTest(new BlobstoreStressTestConfiguration(), transactionHelper,
        cachedBlobstore, memBlobstore);
  }

}
