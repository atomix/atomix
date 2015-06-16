/*
 * Copyright (C) 2005 The Guava Authors
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
package net.kuujo.copycat.io.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.function.Predicate;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Scans the source of a {@link ClassLoader} and finds all loadable classes and resources.
 *
 * @author Ben Yu
 * @since 14.0
 */
public final class ClassPath {
  private static final Logger logger = Logger.getLogger(ClassPath.class.getName());

  private static final Predicate<ClassInfo> IS_TOP_LEVEL = new Predicate<ClassInfo>() {
    @Override public boolean test(ClassInfo info) {
      return info.className.indexOf('$') == -1;
    }
  };

  private static final String CLASS_FILE_NAME_EXTENSION = ".class";

  private final Set<ResourceInfo> resources;

  private ClassPath(Set<ResourceInfo> resources) {
    this.resources = resources;
  }

  /**
   * Returns a {@code ClassPath} representing all classes and resources loadable from {@code
   * classloader} and its parent class loaders.
   *
   * <p>Currently only {@link URLClassLoader} and only {@code file://} urls are supported.
   *
   * @throws IOException if the attempt to read class path resources (jar files or directories)
   *         failed.
   */
  public static ClassPath from(ClassLoader classloader) throws IOException {
    DefaultScanner scanner = new DefaultScanner();
    scanner.scan(classloader);
    return new ClassPath(scanner.getResources());
  }

  /**
   * Returns all resources loadable from the current class path, including the class files of all
   * loadable classes but excluding the "META-INF/MANIFEST.MF" file.
   */
  public Set<ResourceInfo> getResources() {
    return resources;
  }

  /**
   * Returns all classes loadable from the current class path.
   *
   * @since 16.0
   */
  public Set<ClassInfo> getAllClasses() {
    return resources.stream().filter(r -> r instanceof ClassInfo).map(r -> (ClassInfo) r).collect(Collectors.toSet());
  }

  /** Returns all top level classes loadable from the current class path. */
  public Set<ClassInfo> getTopLevelClasses() {
    return resources.stream().filter(r -> r instanceof ClassInfo).map(r -> (ClassInfo) r).filter(IS_TOP_LEVEL).collect(Collectors.toSet());
  }

  /** Returns all top level classes whose package name is {@code packageName}. */
  public Set<ClassInfo> getTopLevelClasses(String packageName) {
    if (packageName == null)
      throw new NullPointerException("packageName cannot be null");
    Set<ClassInfo> info = new HashSet<>();
    for (ClassInfo classInfo : getTopLevelClasses()) {
      if (classInfo.getPackageName().equals(packageName)) {
        info.add(classInfo);
      }
    }
    return info;
  }

  /**
   * Returns all top level classes whose package name is {@code packageName} or starts with
   * {@code packageName} followed by a '.'.
   */
  public Set<ClassInfo> getTopLevelClassesRecursive(String packageName) {
    if (packageName == null)
      throw new NullPointerException("packageName cannot be null");
    String packagePrefix = packageName + '.';
    Set<ClassInfo> info = new HashSet<>();
    for (ClassInfo classInfo : getTopLevelClasses()) {
      if (classInfo.getName().startsWith(packagePrefix)) {
        info.add(classInfo);
      }
    }
    return info;
  }

  /**
   * Represents a class path resource that can be either a class file or any other resource file
   * loadable from the class path.
   *
   * @since 14.0
   */
  public static class ResourceInfo {
    private final String resourceName;

    final ClassLoader loader;

    static ResourceInfo of(String resourceName, ClassLoader loader) {
      if (resourceName.endsWith(CLASS_FILE_NAME_EXTENSION)) {
        return new ClassInfo(resourceName, loader);
      } else {
        return new ResourceInfo(resourceName, loader);
      }
    }

    ResourceInfo(String resourceName,  ClassLoader loader) {
      if (resourceName == null)
        throw new NullPointerException("resourceName cannot be null");
      if (loader == null)
        throw new NullPointerException("loader cannot be null");
      this.resourceName = resourceName;
      this.loader = loader;
    }

    /**
     * Returns the url identifying the resource.
     *
     * <p>See {@link ClassLoader#getResource}
     * @throws NoSuchElementException if the resource cannot be loaded through the class loader,
     *         despite physically existing in the class path.
     */
    public final URL url() throws NoSuchElementException {
      URL url = loader.getResource(resourceName);
      if (url == null) {
        throw new NoSuchElementException(resourceName);
      }
      return url;
    }

    /** Returns the fully qualified name of the resource. Such as "com/mycomp/foo/bar.txt". */
    public final String getResourceName() {
      return resourceName;
    }

    @Override
    public int hashCode() {
      return resourceName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ResourceInfo) {
        ResourceInfo that = (ResourceInfo) obj;
        return resourceName.equals(that.resourceName)
          && loader == that.loader;
      }
      return false;
    }

    // Do not change this arbitrarily. We rely on it for sorting ResourceInfo.
    @Override
    public String toString() {
      return resourceName;
    }
  }

  /**
   * Represents a class that can be loaded through {@link #load}.
   *
   * @since 14.0
   */
  public static final class ClassInfo extends ResourceInfo {
    private final String className;

    ClassInfo(String resourceName, ClassLoader loader) {
      super(resourceName, loader);
      this.className = getClassName(resourceName);
    }

    /**
     * Returns the package name of the class, without attempting to load the class.
     *
     * <p>Behaves identically to {@link Package#getName()} but does not require the class (or
     * package) to be loaded.
     */
    public String getPackageName() {
      int lastDot = className.lastIndexOf('.');
      return (lastDot < 0) ? "" : className.substring(0, lastDot);
    }

    /**
     * Returns the fully qualified name of the class.
     *
     * <p>Behaves identically to {@link Class#getName()} but does not require the class to be
     * loaded.
     */
    public String getName() {
      return className;
    }

    /**
     * Loads (but doesn't link or initialize) the class.
     *
     * @throws LinkageError when there were errors in loading classes that this class depends on.
     *         For example, {@link NoClassDefFoundError}.
     */
    public Class<?> load() {
      try {
        return loader.loadClass(className);
      } catch (ClassNotFoundException e) {
        // Shouldn't happen, since the class name is read from the class path.
        throw new IllegalStateException(e);
      }
    }

    @Override
    public String toString() {
      return className;
    }
  }

  /**
   * Abstract class that scans through the class path represented by a {@link ClassLoader} and calls
   * {@link #scanDirectory} and {@link #scanJarFile} for directories and jar files on the class path
   * respectively.
   */
  abstract static class Scanner {

    // We only scan each file once independent of the classloader that resource might be associated
    // with.
    private final Set<File> scannedUris = new HashSet<>();

    public final void scan(ClassLoader classloader) throws IOException {
      for (Map.Entry<File, ClassLoader> entry : getClassPathEntries(classloader).entrySet()) {
        scan(entry.getKey(), entry.getValue());
      }
    }

    /** Called when a directory is scanned for resource files. */
    protected abstract void scanDirectory(ClassLoader loader, File directory)
      throws IOException;

    /** Called when a jar file is scanned for resource entries. */
    protected abstract void scanJarFile(ClassLoader loader, JarFile file) throws IOException;

    final void scan(File file, ClassLoader classloader) throws IOException {
      if (scannedUris.add(file.getCanonicalFile())) {
        scanFrom(file, classloader);
      }
    }

    private void scanFrom(File file, ClassLoader classloader) throws IOException {
      if (!file.exists()) {
        return;
      }

      if (file.isDirectory()) {
        scanDirectory(classloader, file);
      } else {
        scanJar(file, classloader);
      }
    }

    private void scanJar(File file, ClassLoader classloader) throws IOException {
      JarFile jarFile;
      try {
        jarFile = new JarFile(file);
      } catch (IOException e) {
        // Not a jar file
        return;
      }

      try {
        for (File path : getClassPathFromManifest(file, jarFile.getManifest())) {
          scan(path, classloader);
        }
        scanJarFile(classloader, jarFile);
      } finally {
        try {
          jarFile.close();
        } catch (IOException ignored) {}
      }
    }

    /**
     * Returns the class path URIs specified by the {@code Class-Path} manifest attribute, according
     * to
     * <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html#Main_Attributes">
     * JAR File Specification</a>. If {@code manifest} is null, it means the jar file has no
     * manifest, and an empty set will be returned.
     */
    static Set<File> getClassPathFromManifest(File jarFile, Manifest manifest) {
      if (manifest == null) {
        return new HashSet<>();
      }

      Set<File> files = new HashSet<>();
      String classpathAttribute = manifest.getMainAttributes()
        .getValue(Attributes.Name.CLASS_PATH.toString());

      if (classpathAttribute != null) {
        for (String path : classpathAttribute.trim().split(" ")) {
          URL url;
          try {
            url = getClassPathEntry(jarFile, path);
          } catch (MalformedURLException e) {
            // Ignore bad entry
            logger.warning("Invalid Class-Path entry: " + path);
            continue;
          }
          if (url.getProtocol().equals("file")) {
            files.add(new File(url.getFile()));
          }
        }
      }
      return files;
    }

    static Map<File, ClassLoader> getClassPathEntries(ClassLoader classloader) {
      LinkedHashMap<File, ClassLoader> entries = new LinkedHashMap<>();
      // Search parent first, since it's the order ClassLoader#loadClass() uses.
      ClassLoader parent = classloader.getParent();
      if (parent != null) {
        entries.putAll(getClassPathEntries(parent));
      }

      if (classloader instanceof URLClassLoader) {
        URLClassLoader urlClassLoader = (URLClassLoader) classloader;
        for (URL entry : urlClassLoader.getURLs()) {
          if (entry.getProtocol().equals("file")) {
            File file = new File(entry.getFile());
            if (!entries.containsKey(file)) {
              entries.put(file, classloader);
            }
          }
        }
      }
      return new LinkedHashMap<>(entries);
    }

    /**
     * Returns the absolute uri of the Class-Path entry value as specified in
     * <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html#Main_Attributes">
     * JAR File Specification</a>. Even though the specification only talks about relative urls,
     * absolute urls are actually supported too (for example, in Maven surefire plugin).
     */
    static URL getClassPathEntry(File jarFile, String path)
      throws MalformedURLException {
      return new URL(jarFile.toURI().toURL(), path);
    }
  }

  static final class DefaultScanner extends Scanner {
    private final Map<ClassLoader, Set<String>> resources = new HashMap<>();

    Set<ResourceInfo> getResources() {
      Set<ResourceInfo> entries = new HashSet<>();
      for (Map.Entry<ClassLoader, Set<String>> entry : resources.entrySet()) {
        for (String value : entry.getValue()) {
          entries.add(ResourceInfo.of(value, entry.getKey()));
        }
      }
      return entries;
    }

    @Override
    protected void scanJarFile(ClassLoader classloader, JarFile file) {
      Enumeration<JarEntry> entries = file.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        if (entry.isDirectory() || entry.getName().equals(JarFile.MANIFEST_NAME)) {
          continue;
        }
        resources.computeIfAbsent(classloader, cl -> new LinkedHashSet<>()).add(entry.getName());
      }
    }

    @Override
    protected void scanDirectory(ClassLoader classloader, File directory)
      throws IOException {
      scanDirectory(directory, classloader, "");
    }

    private void scanDirectory(
      File directory, ClassLoader classloader, String packagePrefix) throws IOException {
      File[] files = directory.listFiles();
      if (files == null) {
        logger.warning("Cannot read directory " + directory);
        // IO error, just skip the directory
        return;
      }

      for (File f : files) {
        String name = f.getName();
        if (f.isDirectory()) {
          scanDirectory(f, classloader, packagePrefix + name + "/");
        } else {
          String resourceName = packagePrefix + name;
          if (!resourceName.equals(JarFile.MANIFEST_NAME)) {
            resources.computeIfAbsent(classloader, cl -> new LinkedHashSet<>()).add(resourceName);
          }
        }
      }
    }
  }

  static String getClassName(String filename) {
    int classNameEnd = filename.length() - CLASS_FILE_NAME_EXTENSION.length();
    return filename.substring(0, classNameEnd).replace('/', '.');
  }
}
