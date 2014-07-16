/*
 * Copyright 2014 the original author or authors.
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

import java.util.List;
import java.util.concurrent.Executors;

import net.kuujo.copycat.util.AsyncAction;
import net.kuujo.copycat.util.AsyncExecutor;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * Asynchronous logger.<p>
 *
 * This logger logs entries to a <code>Log</code> instance by writing and reading
 * using a single background thread. Each call will be placed on a queue from
 * which operations are executed in the background. Using a single thread ensures
 * that operations are executed in order and locking is not necessary.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncLogger implements AsyncLog {
  private final Log log;
  private final AsyncExecutor executor = new AsyncExecutor(Executors.newSingleThreadExecutor());

  public AsyncLogger(Log log) {
    this.log = log;
  }

  @Override
  public Log log() {
    return log;
  }

  @Override
  public AsyncLog setMaxSize(long maxSize) {
    log.setMaxSize(maxSize);
    return this;
  }

  @Override
  public long getMaxSize() {
    return log.getMaxSize();
  }

  @Override
  public void open(Handler<AsyncResult<Void>> doneHandler) {
    executor.execute(new AsyncAction<Void>() {
      @Override
      public Void execute() {
        log.open();
        return null;
      }
    }, doneHandler);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> doneHandler) {
    executor.execute(new AsyncAction<Void>() {
      @Override
      public Void execute() {
        log.close();
        return null;
      }
    }, doneHandler);
  }

  @Override
  public void delete(Handler<AsyncResult<Void>> doneHandler) {
    executor.execute(new AsyncAction<Void>() {
      @Override
      public Void execute() {
        log.delete();
        return null;
      }
    }, doneHandler);
  }

  @Override
  public AsyncLog fullHandler(Handler<Void> handler) {
    log.fullHandler(handler);
    return this;
  }

  @Override
  public AsyncLog drainHandler(Handler<Void> handler) {
    log.drainHandler(handler);
    return this;
  }

  @Override
  public AsyncLog appendEntry(final Entry entry, Handler<AsyncResult<Long>> doneHandler) {
    executor.execute(new AsyncAction<Long>() {
      @Override
      public Long execute() {
        return log.appendEntry(entry);
      }
    }, doneHandler);
    return this;
  }

  @Override
  public AsyncLog containsEntry(final long index, Handler<AsyncResult<Boolean>> containsHandler) {
    executor.execute(new AsyncAction<Boolean>() {
      @Override
      public Boolean execute() {
        return log.containsEntry(index);
      }
    }, containsHandler);
    return this;
  }

  @Override
  public AsyncLog getEntry(final long index, Handler<AsyncResult<Entry>> entryHandler) {
    executor.execute(new AsyncAction<Entry>() {
      @Override
      public Entry execute() {
        return log.getEntry(index);
      }
    }, entryHandler);
    return this;
  }

  @Override
  public AsyncLog setEntry(final long index, final Entry entry, Handler<AsyncResult<Void>> doneHandler) {
    executor.execute(new AsyncAction<Void>() {
      @Override
      public Void execute() {
        log.setEntry(index, entry);
        return null;
      }
    }, doneHandler);
    return this;
  }

  @Override
  public AsyncLog firstIndex(Handler<AsyncResult<Long>> resultHandler) {
    executor.execute(new AsyncAction<Long>() {
      @Override
      public Long execute() {
        return log.firstIndex();
      }
    }, resultHandler);
    return this;
  }

  @Override
  public AsyncLog firstEntry(Handler<AsyncResult<Entry>> doneHandler) {
    executor.execute(new AsyncAction<Entry>() {
      @Override
      public Entry execute() {
        return log.firstEntry();
      }
    }, doneHandler);
    return this;
  }

  @Override
  public AsyncLog lastIndex(Handler<AsyncResult<Long>> resultHandler) {
    executor.execute(new AsyncAction<Long>() {
      @Override
      public Long execute() {
        return log.lastIndex();
      }
    }, resultHandler);
    return this;
  }

  @Override
  public AsyncLog lastEntry(Handler<AsyncResult<Entry>> doneHandler) {
    executor.execute(new AsyncAction<Entry>() {
      @Override
      public Entry execute() {
        return log.lastEntry();
      }
    }, doneHandler);
    return this;
  }

  @Override
  public AsyncLog getEntries(final long start, final long end, Handler<AsyncResult<List<Entry>>> doneHandler) {
    executor.execute(new AsyncAction<List<Entry>>() {
      @Override
      public List<Entry> execute() {
        return log.getEntries(start, end);
      }
    }, doneHandler);
    return this;
  }

  @Override
  public AsyncLog removeBefore(final long index, Handler<AsyncResult<Void>> doneHandler) {
    executor.execute(new AsyncAction<Void>() {
      @Override
      public Void execute() {
        log.removeBefore(index);
        return null;
      }
    }, doneHandler);
    return this;
  }

  @Override
  public AsyncLog removeAfter(final long index, Handler<AsyncResult<Void>> doneHandler) {
    executor.execute(new AsyncAction<Void>() {
      @Override
      public Void execute() {
        log.removeBefore(index);
        return null;
      }
    }, doneHandler);
    return this;
  }

}
