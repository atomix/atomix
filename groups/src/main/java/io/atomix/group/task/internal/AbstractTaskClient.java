/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.task.internal;

import io.atomix.catalyst.util.Assert;
import io.atomix.group.task.TaskClient;
import io.atomix.group.task.TaskProducer;
import io.atomix.group.util.Submitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract message client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractTaskClient implements TaskClient {
  private final Submitter submitter;
  private final Map<String, AbstractTaskProducer> producers = new ConcurrentHashMap<>();

  protected AbstractTaskClient(Submitter submitter) {
    this.submitter = Assert.notNull(submitter, "submitter");
  }

  protected abstract <T> AbstractTaskProducer<T> createProducer(String name, TaskProducer.Options options);

  /**
   * Returns the client submitter.
   *
   * @return The client submitter.
   */
  Submitter submitter() {
    return submitter;
  }

  @Override
  public <T> AbstractTaskProducer<T> producer(String name) {
    return producer(name, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> AbstractTaskProducer<T> producer(String name, TaskProducer.Options options) {
    AbstractTaskProducer<T> producer = producers.get(name);
    if (producer == null) {
      synchronized (producers) {
        producer = producers.get(name);
        if (producer == null) {
          producer = createProducer(name, options != null ? options : new TaskProducer.Options());
          producers.put(name, producer);
        }
      }
    }

    if (options != null) {
      producer.setOptions(options);
    }
    return producer;
  }

  /**
   * Closes the given producer.
   *
   * @param producer The producer to close.
   */
  void close(AbstractTaskProducer producer) {
    producers.remove(producer.name());
  }

}
