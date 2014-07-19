package net.kuujo.copycat.util;

import java.util.Map;

/**
 * Service information.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServiceInfo {
  private final String name;
  protected final Map<String, String> properties;

  public ServiceInfo(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = properties;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @SuppressWarnings("unchecked")
  public <T> T getProperty(String name) {
    return (T) properties.get(name);
  }

  public <T> T getProperty(String name, Class<T> type) {
    if (Class.class.isAssignableFrom(type)) {
      String value = properties.get(name);
      if (value != null) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
          return type.cast(cl.loadClass(value));
        } catch (ClassNotFoundException e) {
          throw new IllegalStateException(e);
        }
      }
      return null;
    }
    return type.cast(properties.get(name));
  }

}
