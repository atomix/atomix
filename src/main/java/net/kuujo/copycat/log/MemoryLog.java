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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;

/**
 * A default log implementation.
 * 
 * @author Jordan Halterman
 */
public class MemoryLog implements Log {
  private static final long DEFAULT_MAX_SIZE = 1000;
  private final TreeMap<Long, Entry> log = new TreeMap<>();
  private long maxSize = DEFAULT_MAX_SIZE;
  private Handler<Void> fullHandler;
  private Handler<Void> drainHandler;
  private boolean full;

  @Override
  public void init(Handler<AsyncResult<Void>> doneHandler) {
    result(null, doneHandler);
  }

  @Override
  public Log setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    return this;
  }

  @Override
  public long getMaxSize() {
    return maxSize;
  }

  @Override
  public Log fullHandler(Handler<Void> handler) {
    fullHandler = handler;
    return this;
  }

  @Override
  public Log drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public Log appendEntry(Entry entry, Handler<AsyncResult<Long>> doneHandler) {
    long index = (!log.isEmpty() ? log.lastKey() : 0) + 1;
    log.put(index, entry);
    return result(index, doneHandler);
  }

  @Override
  public Log containsEntry(long index, Handler<AsyncResult<Boolean>> containsHandler) {
    return result(log.containsKey(index), containsHandler);
  }

  @Override
  public Log getEntry(long index, Handler<AsyncResult<Entry>> entryHandler) {
    return result(log.get(index), entryHandler);
  }

  @Override
  public long firstIndex() {
    return !log.isEmpty() ? log.firstKey() : 0;
  }

  @Override
  public Log firstTerm(Handler<AsyncResult<Long>> handler) {
    return result(!log.isEmpty() ? log.firstEntry().getValue().term() : 0, handler);
  }

  @Override
  public Log firstEntry(Handler<AsyncResult<Entry>> handler) {
    return result(!log.isEmpty() ? log.firstEntry().getValue() : null, handler);
  }

  @Override
  public long lastIndex() {
    return !log.isEmpty() ? log.lastKey() : 0;
  }

  @Override
  public Log lastTerm(Handler<AsyncResult<Long>> handler) {
    return result(!log.isEmpty() ? log.lastEntry().getValue().term() : 0, handler);
  }

  @Override
  public Log lastEntry(Handler<AsyncResult<Entry>> handler) {
    return result(!log.isEmpty() ? log.lastEntry().getValue() : null, handler);
  }

  @Override
  public Log getEntries(long start, long end, Handler<AsyncResult<List<Entry>>> doneHandler) {
    List<Entry> entries = new ArrayList<>();
    for (Map.Entry<Long, Entry> entry : log.subMap(start, end+1).entrySet()) {
      entries.add(entry.getValue());
    }
    return result(entries, doneHandler);
  }

  @Override
  public Log removeEntry(long index, Handler<AsyncResult<Entry>> doneHandler) {
    return result(log.remove(index), doneHandler);
  }

  @Override
  public Log removeBefore(long index, Handler<AsyncResult<Void>> doneHandler) {
    log.headMap(index);
    return result(null, doneHandler);
  }

  @Override
  public Log removeAfter(long index, Handler<AsyncResult<Void>> doneHandler) {
    log.tailMap(index);
    return result(null, doneHandler);
  }

  /**
   * Creates a triggers a result.
   */
  private <T> Log result(T result, Handler<AsyncResult<T>> handler) {
    if (!full) {
      if (log.size() >= maxSize) {
        full = true;
        if (fullHandler != null) {
          fullHandler.handle((Void) null);
        }
      }
    }
    else {
      if (log.size() < maxSize) {
        full = false;
        if (drainHandler != null) {
          drainHandler.handle((Void) null);
        }
      }
    }
    new DefaultFutureResult<T>().setHandler(handler).setResult(result);
    return this;
  }

}
