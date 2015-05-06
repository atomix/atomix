/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.log;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.BufferPool;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.HashFunction;
import net.kuujo.copycat.io.util.Murmur3HashFunction;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.Persistence;
import net.kuujo.copycat.protocol.ProtocolFilter;
import net.kuujo.copycat.util.Managed;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared commit log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SharedCommitLog<KEY, VALUE> implements Managed<SharedCommitLog<KEY, VALUE>> {
  protected final CommitLog<KEY, VALUE> log;
  protected final Map<String, Integer> hashMap = new ConcurrentHashMap<>();
  protected final Map<Integer, RawCommitHandler> handlerMap = new ConcurrentHashMap<>();
  protected final HashFunction hash = new Murmur3HashFunction();
  protected final BufferPool bufferPool = new HeapBufferPool();
  private CompletableFuture<SharedCommitLog<KEY, VALUE>> openFuture;
  private CompletableFuture<Void> closeFuture;
  private AtomicInteger open = new AtomicInteger();

  public SharedCommitLog(CommitLog<KEY, VALUE> log) {
    this.log = log;
    log.handler(this::handle);
  }

  /**
   * Handles a commit.
   */
  @SuppressWarnings("unchecked")
  private synchronized Buffer handle(long index, Buffer key, Buffer entry, Buffer result) {
    int hash = key.readInt();
    RawCommitHandler handler = handlerMap.get(hash);
    if (handler != null) {
      return handler.commit(index, key, entry, result);
    }
    return log.cluster().serializer().writeObject(null, result);
  }

  /**
   * Calculates the 32-bit hash for the given value.
   */
  protected int calculateHash(String value) {
    return hashMap.computeIfAbsent(value, r -> this.hash.hash32(r.getBytes()));
  }

  /**
   * Commits an entry on behalf of a named resource.
   *
   * @param resource The resource name.
   * @param key The entry key.
   * @param entry The entry value.
   * @param persistence The entry persistence level.
   * @param consistency The entry consistency.
   * @param <RESULT> The result type.
   * @return A completable future to be completed with the commit result.
   */
  public <RESULT> CompletableFuture<RESULT> commit(String resource, KEY key, VALUE entry, Persistence persistence, Consistency consistency) {
    int hash = calculateHash(resource);
    Buffer keyBuffer = log.cluster().serializer().writeObject(key, bufferPool.acquire().writeInt(hash)).flip();
    Buffer entryBuffer = log.cluster().serializer().writeObject(entry, bufferPool.acquire()).flip();
    return log.commit(keyBuffer, entryBuffer, persistence, consistency).thenApply((result) -> {
      keyBuffer.close();
      entryBuffer.close();
      return log.cluster().serializer().readObject(result);
    });
  }

  /**
   * Commits an entry on behalf of a named resource.
   *
   * @param resource The resource name.
   * @param keyBuffer The entry key.
   * @param entryBuffer The entry value.
   * @param persistence The entry persistence level.
   * @param consistency The entry consistency.
   * @return A completable future to be completed with the commit result.
   */
  protected CompletableFuture<Buffer> commit(String resource, Buffer keyBuffer, Buffer entryBuffer, Persistence persistence, Consistency consistency) {
    int hash = hashMap.computeIfAbsent(resource, r -> this.hash.hash32(r.getBytes()));
    Buffer resourceKeyBuffer = bufferPool.acquire().writeInt(hash).write(keyBuffer).flip();
    return log.commit(resourceKeyBuffer, entryBuffer.acquire(), persistence, consistency).thenApply(result -> {
      resourceKeyBuffer.close();
      entryBuffer.release();
      return result;
    });
  }

  /**
   * Registers a log commit handler on behalf of a named resource.
   *
   * @param resource The resource name.
   * @param handler The commit handler.
   * @param <RESULT> The commit result type.
   * @return The commit log.
   */
  public <RESULT> SharedCommitLog<KEY, VALUE> handler(String resource, CommitHandler<KEY, VALUE, RESULT> handler) {
    handlerMap.put(hashMap.computeIfAbsent(resource, r -> hash.hash32(r.getBytes())), wrapHandler(handler));
    return this;
  }

  /**
   * Registers a raw log commit handler on behalf of a named resource.
   *
   * @param resource The resource name.
   * @param handler The commit handler.
   * @return The commit log.
   */
  protected SharedCommitLog<KEY, VALUE> handler(String resource, RawCommitHandler handler) {
    handlerMap.put(hashMap.computeIfAbsent(resource, r -> hash.hash32(r.getBytes())), handler);
    return this;
  }

  /**
   * Sets a protocol filter.
   */
  protected SharedCommitLog<KEY, VALUE> filter(ProtocolFilter filter) {
    log.filter(filter);
    return this;
  }

  /**
   * Wraps a raw commit handler.
   */
  private RawCommitHandler wrapHandler(CommitHandler<KEY, VALUE, ?> handler) {
    return (index, key, entry, result) -> log.cluster().serializer().writeObject(handler.commit(index, log.cluster().serializer().readObject(key), log.cluster().serializer().readObject(entry)), result);
  }

  @Override
  public synchronized CompletableFuture<SharedCommitLog<KEY, VALUE>> open() {
    if (open.incrementAndGet() == 1) {
      if (closeFuture != null) {
        openFuture = closeFuture.thenCompose(c -> log.open().thenApply(o -> this));
      } else {
        openFuture = log.open().thenApply(o -> this);
      }
    }
    return openFuture != null ? openFuture : CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open.get() > 0 && openFuture == null;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    int open = this.open.decrementAndGet();
    if (open == 0) {
      if (openFuture != null) {
        closeFuture = openFuture.thenCompose(o -> log.close());
      } else {
        closeFuture = log.close();
      }
    } else if (open < 0) {
      this.open.set(0);
    }
    return closeFuture != null ? closeFuture : CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return open.get() == 0 && closeFuture == null;
  }

}
