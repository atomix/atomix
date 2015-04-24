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
package net.kuujo.copycat.util;

import java.io.*;
import java.net.URL;
import java.util.*;

/**
 * Service loader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServiceLoader {

  /**
   * Loads a service.
   *
   * @param service The service class.
   * @return A collection of services.
   */
  public static Collection<ServiceInfo> load(Class<?> service) {
    return load(service.getName());
  }

  /**
   * Loads a service.
   *
   * @param service The service name, e.g. <code>net.kuujo.copycat.Serializer</code>
   * @return A collection of services.
   * @throws ServiceNotFoundException if the service cannot be found on the classpath.
   */
  public static Collection<ServiceInfo> load(String service) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Enumeration<URL> serializerUrls;
    try {
      serializerUrls = cl.getResources(String.format("META-INF%sservices%s%s", File.separator, File.separator, service.replace(".", File.separator)));
    } catch (IOException e) {
      throw new ServiceNotFoundException(e);
    }

    List<ServiceInfo> services = new ArrayList<>();
    while (serializerUrls.hasMoreElements()) {
      URL serializerUrl = serializerUrls.nextElement();
      try (InputStream sis = serializerUrl.openStream()) {
        BufferedReader sreader = new BufferedReader(new InputStreamReader(sis, "UTF-8"));
        String serviceName;
        while ((serviceName = sreader.readLine()) != null) {
          try (InputStream is = cl.getResourceAsStream(String.format("META-INF%sservices%s%s%s%s", File.separator, File.separator, service.replace(".", File.separator), File.separator, serviceName))) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String line;
            Map<String, String> serviceOptions = new HashMap<>();
            serviceOptions.put("name", serviceName);
            while ((line = reader.readLine()) != null) {
              int comment = line.lastIndexOf('#');
              if (comment >= 0) line = line.substring(0, line.lastIndexOf('#'));
              line = line.trim();
              if (line.contains("=")) {
                String property = line.substring(0, line.indexOf("="));
                String value = line.substring(line.indexOf("=") + 1);
                serviceOptions.put(property, value);
              } else {
                serviceOptions.put("class", line);
              }
            }
            services.add(new ServiceInfo(serviceName, serviceOptions));
          }
        }
      } catch (IOException e) {
        throw new ServiceNotFoundException(e);
      }
      return services;
    }
    return services;
  }

}
