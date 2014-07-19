package net.kuujo.copycat.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * Service loader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServiceLoader {

  /**
   * Loads a service.
   *
   * @param service The fully qualified service name, e.g. <code>net.kuujo.juno.Serializer</code>
   * @return The loaded service info.
   * @throws {@link ServiceNotFoundException} if the service cannot be found on the classpath.
   */
  public static ServiceInfo load(String service) {
    return load(service.substring(0, service.lastIndexOf('.')), service.substring(service.lastIndexOf('.') + 1));
  }

  /**
   * Loads a service.
   *
   * @param namespace The service namespace, e.g. <code>net.kuujo.juno.component</code>
   * @param service The service name, e.g. <code>eventbus</code>
   * @return The loaded service info.
   * @throws {@link ServiceNotFoundException} if the service cannot be found on the classpath.
   */
  public static ServiceInfo load(String namespace, String service) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Enumeration<URL> urls;
    try {
      urls = cl.getResources(String.format("META-INF%sservices%s%s%s%s", File.separator, File.separator, namespace.replace(".", File.separator), File.separator, service));
    } catch (IOException e) {
      throw new ServiceNotFoundException(e);
    }

    while (urls.hasMoreElements()) {
      URL url = urls.nextElement();
      try {
        InputStream is = url.openStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String line;
        String serviceName = url.getPath().substring(url.getPath().lastIndexOf('/') + 1);
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
        is.close();
        return new ServiceInfo(serviceName, serviceOptions);
      } catch (IOException | IllegalArgumentException | SecurityException e) {
        throw new ServiceNotFoundException(e);
      }
    }
    throw new ServiceNotFoundException(String.format("Service not found: %s", service));
  }

}
