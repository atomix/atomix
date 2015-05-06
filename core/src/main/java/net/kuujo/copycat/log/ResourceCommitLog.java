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

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.Persistence;
import net.kuujo.copycat.protocol.ProtocolFilter;

import java.util.concurrent.CompletableFuture;

/**
 * Resource commit log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceCommitLog<KEY, VALUE> extends CommitLog<KEY, VALUE> {
  private final String name;
  private final SharedCommitLog<KEY, VALUE> log;
  private CompletableFuture<CommitLog<KEY, VALUE>> openFuture;
  private CompletableFuture<Void> closeFuture;
  private boolean open;

  public ResourceCommitLog(String name, SharedCommitLog<KEY, VALUE> log) {
    this.name = name;
    this.log = log;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Cluster cluster() {
    return log.log.cluster();
  }

  @Override
  public <RESULT> CompletableFuture<RESULT> commit(KEY key, VALUE value, Persistence persistence, Consistency consistency) {
    return log.commit(name, key, value, persistence, consistency);
  }

  @Override
  protected CompletableFuture<Buffer> commit(Buffer key, Buffer value, Persistence persistence, Consistency consistency) {
    return log.commit(name, key, value, persistence, consistency);
  }

  @Override
  protected CommitLog<KEY, VALUE> filter(ProtocolFilter filter) {
    log.filter(filter);
    return this;
  }

  @Override
  public <RESULT> CommitLog<KEY, VALUE> handler(CommitHandler<KEY, VALUE, RESULT> handler) {
    log.handler(name, handler);
    return this;
  }

  @Override
  protected CommitLog<KEY, VALUE> handler(RawCommitHandler handler) {
    log.handler(name, handler);
    return this;
  }

  @Override
  public CompletableFuture<CommitLog<KEY, VALUE>> open() {
    if (open)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          if (closeFuture != null) {
            openFuture = closeFuture.thenCompose(v -> log.open().thenRun(() -> open = true).thenApply(o -> this));
          } else {
            openFuture = log.open().thenRun(() -> open = true).thenApply(o -> this);
          }
        }
      }
    }
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          if (openFuture != null) {
            closeFuture = openFuture.thenCompose(v -> log.close().thenRun(() -> open = false));
          } else {
            closeFuture = log.close().thenRun(() -> open = false);
          }
        }
      }
    }
    return null;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
