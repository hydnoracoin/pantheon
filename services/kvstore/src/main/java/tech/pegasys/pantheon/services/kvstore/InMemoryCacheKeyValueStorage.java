/*
 * Copyright 2018 ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package tech.pegasys.pantheon.services.kvstore;

import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class InMemoryCacheKeyValueStorage implements KeyValueStorage {

  private final int OBJECT_OVERHEAD = 16;
  private final int ARRAY_OVERHEAD = 20;

  private final Cache<BytesValue, BytesValue> cache;
  private final KeyValueStorage backingStore;
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  public InMemoryCacheKeyValueStorage(
      final KeyValueStorage backingStore, final long cacheSizeInBytes) {
    this.backingStore = backingStore;
    cache =
        CacheBuilder.newBuilder()
            .weigher(
                (BytesValue k, BytesValue v) ->
                    k.size() + v.size() + OBJECT_OVERHEAD * 2 + ARRAY_OVERHEAD * 2)
            .maximumWeight(cacheSizeInBytes)
            .build();
  }

  @Override
  public Optional<BytesValue> get(final BytesValue key) {
    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      final BytesValue val = cache.getIfPresent(key);
      if (val == null) {
        final Optional<BytesValue> oVal = backingStore.get(key);
        oVal.ifPresent(bytesValue -> cache.put(key, bytesValue));
        return oVal;
      } else {
        return Optional.of(val);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Transaction startTransaction() {
    return new InMemoryCacheTransaction();
  }

  @Override
  public Stream<Entry> entries() {
    return backingStore.entries();
  }

  @Override
  public void close() throws IOException {
    backingStore.close();
  }

  private class InMemoryCacheTransaction extends AbstractTransaction {

    private Map<BytesValue, BytesValue> updatedValues = new HashMap<>();
    private Set<BytesValue> removedKeys = new HashSet<>();
    final Transaction tx = backingStore.startTransaction();

    @Override
    protected void doPut(final BytesValue key, final BytesValue value) {
      updatedValues.put(key, value);
      removedKeys.remove(key);
      tx.put(key, value);
    }

    @Override
    protected void doRemove(final BytesValue key) {
      removedKeys.add(key);
      updatedValues.remove(key);
      tx.remove(key);
    }

    @Override
    protected void doCommit() {
      final Lock lock = rwLock.writeLock();
      lock.lock();
      try {
        cache.putAll(updatedValues);
        removedKeys.forEach(cache::invalidate);
        tx.commit();
        updatedValues = null;
        removedKeys = null;
      } finally {
        lock.unlock();
      }
    }

    @Override
    protected void doRollback() {
      updatedValues = null;
      removedKeys = null;
      tx.rollback();
    }
  }
}
