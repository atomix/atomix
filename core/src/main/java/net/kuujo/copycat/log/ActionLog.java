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

import net.kuujo.copycat.Action;
import net.kuujo.copycat.ActionOptions;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ActionLog implements Log {
  private final Log log;
  private final Map<String, ActionInfo> actions = new HashMap<>();

  public ActionLog(Log log) {
    this.log = log;
  }

  private static class ActionInfo {
    private final Action action;
    private final ActionOptions options;
    private ActionInfo(Action action, ActionOptions options) {
      this.action = action;
      this.options = options;
    }
  }

  /**
   * Registers a named action.
   *
   * @param name The action name.
   * @param action The action instance.
   * @return The action log.
   */
  public ActionLog register(String name, Action action) {
    return register(name, action, new ActionOptions());
  }

  /**
   * Registers a named action.
   *
   * @param name The action name.
   * @param action The action instance.
   * @param options The action options.
   * @return The action log.
   */
  public ActionLog register(String name, Action action, ActionOptions options) {
    actions.put(name, new ActionInfo(action, options));
    return this;
  }

  /**
   * Unregisters a named action.
   *
   * @param name The action name.
   * @return The action log.
   */
  public ActionLog unregister(String name) {
    actions.remove(name);
    return this;
  }

  /**
   * Returns a map of actions registered in the log.
   *
   * @return A map of actions registered in the log.
   */
  public Map<String, ActionOptions> actions() {
    Map<String, ActionOptions> actions = new HashMap<>();
    for (Map.Entry<String, ActionInfo> entry : this.actions.entrySet()) {
      actions.put(entry.getKey(), entry.getValue().options);
    }
    return actions;
  }

  /**
   * Returns options for a specific action.
   *
   * @param name The action name.
   * @return The action options.
   */
  public ActionOptions action(String name) {
    ActionInfo info = actions.get(name);
    return info != null ? info.options : null;
  }

  /**
   * Commits the entry at the given index.
   *
   * @param index The index of the entry to commit.
   * @return The action log.
   */
  public ActionLog commit(long index) {

  }

  @Override
  public File base() {
    return log.base();
  }

  @Override
  public File directory() {
    return log.directory();
  }

  @Override
  public Collection<LogSegment> segments() {
    return log.segments();
  }

  @Override
  public LogSegment segment() {
    return log.segment();
  }

  @Override
  public LogSegment segment(long index) {
    return log.segment();
  }

  @Override
  public LogSegment firstSegment() {
    return log.firstSegment();
  }

  @Override
  public LogSegment lastSegment() {
    return log.lastSegment();
  }

  @Override
  public void open() {
    log.open();
  }

  @Override
  public boolean isOpen() {
    return log.isOpen();
  }

  @Override
  public int size() {
    return log.size();
  }

  @Override
  public long appendEntry(ByteBuffer entry) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(entry.capacity() + 12);
    return log.appendEntry(entry);
  }

  @Override
  public List<Long> appendEntries(List<ByteBuffer> entries) {
    return null;
  }

  @Override
  public long firstIndex() {
    return 0;
  }

  @Override
  public long lastIndex() {
    return 0;
  }

  @Override
  public boolean containsIndex(long index) {
    return false;
  }

  @Override
  public ByteBuffer getEntry(long index) {
    return null;
  }

  @Override
  public List<ByteBuffer> getEntries(long from, long to) {
    return null;
  }

  @Override
  public void removeAfter(long index) {

  }

  @Override
  public void compact(long index) {

  }

  @Override
  public void compact(long index, ByteBuffer entry) {

  }

  @Override
  public void flush() {

  }

  @Override
  public void flush(boolean force) {

  }

  @Override
  public void close() {

  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void delete() {

  }
}
