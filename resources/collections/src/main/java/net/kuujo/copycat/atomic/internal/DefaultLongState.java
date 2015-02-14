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
package net.kuujo.copycat.atomic.internal;

import net.kuujo.copycat.state.StateContext;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Default atomic long status implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultLongState implements LongState {
  private AtomicLong value;

  @Override
  public void init(StateContext<LongState> context) {
    value = context.get("value");
    if (value == null) {
      value = new AtomicLong();
      context.put("value", value);
    }
  }

  @Override
  public long get() {
    return this.value.get();
  }

  @Override
  public void set(long value) {
    this.value.set(value);
  }

  @Override
  public long addAndGet(long value) {
    return this.value.addAndGet(value);
  }

  @Override
  public long getAndAdd(long value) {
    return this.value.getAndAdd(value);
  }

  @Override
  public long getAndSet(long value) {
    return this.value.getAndSet(value);
  }

  @Override
  public long getAndIncrement() {
    return this.value.getAndIncrement();
  }

  @Override
  public long getAndDecrement() {
    return this.value.getAndDecrement();
  }

  @Override
  public long incrementAndGet() {
    return this.value.incrementAndGet();
  }

  @Override
  public long decrementAndGet() {
    return this.value.decrementAndGet();
  }

  @Override
  public boolean compareAndSet(long expect, long update) {
    return this.value.compareAndSet(expect, update);
  }

}
