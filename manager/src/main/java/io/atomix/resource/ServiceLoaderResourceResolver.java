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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.catalyst.CatalystException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Resource type resolver based on {@link java.util.ServiceLoader}.
 * <p>
 * This resource type resolver resolves resource types via {@link java.util.ServiceLoader}.
 * <p>
 * All classes that are loadable as services must provide a default no-argument constructor.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServiceLoaderResourceResolver implements ResourceTypeResolver {

  @Override
  public void resolve(ResourceRegistry registry) {
    for (Class<? extends Resource> resourceType : load(Resource.class)) {
      registry.register(new ResourceType<>(resourceType));
    }
  }

  /**
   * Loads a list of classes from the classpath.
   */
  @SuppressWarnings("unchecked")
  private <T> List<Class<? extends T>> load(Class<T> clazz) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = ClassLoader.getSystemClassLoader();
    }

    Enumeration<URL> urls;
    try {
      urls = cl.getResources(String.format("META-INF/services/%s", clazz.getName()));
    } catch (IOException e) {
      throw new CatalystException(e);
    }

    List<Class<? extends T>> classes = new ArrayList<>();
    while (urls.hasMoreElements()) {
      URL url = urls.nextElement();
      try {
        InputStream is = url.openStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String line;
        while ((line = reader.readLine()) != null) {
          int comment = line.lastIndexOf('#');
          if (comment >= 0) line = line.substring(0, line.lastIndexOf('#'));
          line = line.trim();
          if (line.length() > 0) {
            try {
              classes.add((Class<? extends T>) cl.loadClass(line));
            } catch (ClassNotFoundException e) {
              throw new CatalystException("failed to load class: " + line, e);
            }
          }
        }
        is.close();
      } catch (IOException | IllegalArgumentException | SecurityException e) {
        throw new CatalystException("failed to read services", e);
      }
    }
    return classes;
  }

}
