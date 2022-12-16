/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.vectorized;

import org.apache.spark.util.GazelleShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Vector;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.io.FileUtils;

/** Helper class for JNI related operations. */
public class JniUtils {
  private static final String LIBRARY_NAME = "spark_columnar_jni";
  private static final String ARROW_LIBRARY_NAME = "libarrow.so.400.0.0";
  private static final String ARROW_PARENT_LIBRARY_NAME = "libarrow.so.400";
  private static final String ARROW_PARENT_LIBRARY_SHORT = "libarrow.so";
  private static final String GANDIVA_LIBRARY_NAME = "libgandiva.so.400.0.0";
  private static final String GANDIVA_PARENT_LIBRARY_NAME = "libgandiva.so.400";
  private static final String GANDIVA_PARENT_LIBRARY_SHORT = "libgandiva.so";
  private static AtomicBoolean isLoaded = new AtomicBoolean(false);
  private static AtomicBoolean isCodegenDependencyLoaded = new AtomicBoolean(false);
  private static List<String> codegenJarsLoadedCache = new ArrayList<>();
  private static volatile JniUtils INSTANCE;
  private static String tmp_dir;
  private static final Logger LOG =
  LoggerFactory.getLogger(JniUtils.class);

  public static Set<String> LOADED_LIBRARY_PATHS = new HashSet<>();
  public static Set<String> REQUIRE_UNLOAD_LIBRARY_PATHS = new LinkedHashSet<>();

  static {
    GazelleShutdownManager.registerUnloadLibShutdownHook(new Function0<BoxedUnit>() {
      @Override
      public BoxedUnit apply() {
        List<String> loaded = new ArrayList<>(REQUIRE_UNLOAD_LIBRARY_PATHS);
        Collections.reverse(loaded); // use reversed order to unload
        loaded.forEach(JniUtils::unloadFromPath);
        return BoxedUnit.UNIT;
      }
    });
  }

  public static JniUtils getInstance() throws IOException {
    String tmp_dir = System.getProperty("java.io.tmpdir");
    return getInstance(tmp_dir);
  }

  public static JniUtils getInstance(String tmp_dir) throws IOException {
    if (INSTANCE == null) {
      synchronized (JniUtils.class) {
        if (INSTANCE == null) {
          try {
            INSTANCE = new JniUtils(tmp_dir);
          } catch (IllegalAccessException ex) {
            throw new IOException("IllegalAccess", ex);
          }
        }
      }
    }
    return INSTANCE;
  }

  private JniUtils(String _tmp_dir) throws IOException, IllegalAccessException, IllegalStateException {
    if (!isLoaded.get()) {
      if (tmp_dir == null) {
        if (_tmp_dir.contains("nativesql")) {
          tmp_dir = _tmp_dir;
        } else {
          Path folder = Paths.get(_tmp_dir);
          Path path = Files.createTempDirectory(folder, "spark_columnar_plugin_");
          Runtime.getRuntime().addShutdownHook(
              new Thread(() -> FileUtils.deleteQuietly(path.toFile())));
          tmp_dir = path.toAbsolutePath().toString();
        }
      }
      try {
        loadLibraryFromJar(tmp_dir);
      } catch (IOException ex) {
        System.load(ARROW_LIBRARY_NAME);
        System.load(GANDIVA_LIBRARY_NAME);
        System.loadLibrary(LIBRARY_NAME);
      }
      isLoaded.set(true);
    }
  }

  public void setTempDir() throws IOException, IllegalAccessException {
    if (!isCodegenDependencyLoaded.get()) {
      loadIncludeFromJar(tmp_dir);
      isCodegenDependencyLoaded.set(true);
    }
  }

  public String getTempDir() {
    return tmp_dir;
  }

  public void setJars(List<String> list_jars) throws IOException, IllegalAccessException {
    for (String jar : list_jars) {
      if (!codegenJarsLoadedCache.contains(jar)) {
        loadLibraryFromJar(jar, tmp_dir);
        codegenJarsLoadedCache.add(jar);
      }
    }
  }

  private static synchronized void loadFromPath0(String libPath, boolean requireUnload) {
    if (LOADED_LIBRARY_PATHS.contains(libPath)) {
      LOG.debug("Library in path {} has already been loaded, skipping", libPath);
    } else {
      System.load(libPath);
      LOADED_LIBRARY_PATHS.add(libPath);
      LOG.info("Library {} has been loaded using path-loading method", libPath);
    }
    if (requireUnload) {
      REQUIRE_UNLOAD_LIBRARY_PATHS.add(libPath);
    }
  }

  static void loadLibraryFromJar(String tmp_dir) throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
      }
      final File arrowlibraryFile = moveFileFromJarToTemp(tmp_dir, ARROW_LIBRARY_NAME);
      Path arrowMiddleLink = createSoftLink(arrowlibraryFile, ARROW_PARENT_LIBRARY_NAME);
      Path arrowShortLink = createSoftLink(new File(arrowMiddleLink.toString()), ARROW_PARENT_LIBRARY_SHORT);
      System.load(arrowShortLink.toAbsolutePath().toString());
      loadFromPath0(arrowShortLink.toAbsolutePath().toString(), true);

      final File gandivalibraryFile = moveFileFromJarToTemp(tmp_dir, GANDIVA_LIBRARY_NAME);
      Path gandivaMiddleLink = createSoftLink(gandivalibraryFile, GANDIVA_PARENT_LIBRARY_NAME);
      Path gandivaShortLink = createSoftLink(new File(gandivaMiddleLink.toString()), GANDIVA_PARENT_LIBRARY_SHORT);
      System.load(gandivaShortLink.toAbsolutePath().toString());
      loadFromPath0(gandivaShortLink.toAbsolutePath().toString(), true);

      final String libraryToLoad = System.mapLibraryName(LIBRARY_NAME);
      final File libraryFile = moveFileFromJarToTemp(tmp_dir, libraryToLoad);
      System.load(libraryFile.getAbsolutePath());
      loadFromPath0(libraryFile.getAbsolutePath(), true);
    }
  }

  private static void loadLibraryFromJar(String source_jar, String tmp_dir) throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
      }
      final String folderToLoad = "";
      URL url = new URL("jar:file:" + source_jar + "!/");
      final URLConnection urlConnection = (JarURLConnection) url.openConnection();
      File tmp_dir_handler = new File(tmp_dir + "/tmp");
      if (!tmp_dir_handler.exists()) {
        tmp_dir_handler.mkdirs();
        FileUtils.forceDeleteOnExit(tmp_dir_handler);
      }

      if (urlConnection instanceof JarURLConnection) {
        final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
        extractResourcesToDirectory(jarFile, folderToLoad, tmp_dir + "/tmp/");
      } else {
        throw new IOException(urlConnection.toString() + " is not JarUrlConnection");
      }
      /*
       * System.out.println("Current content under " + tmp_dir + "/tmp/");
       * Files.list(new File(tmp_dir + "/tmp/").toPath()).forEach(path -> {
       * System.out.println(path); });
       */
    }
  }

  private static void loadIncludeFromJar(String tmp_dir) throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
      }
      final String folderToLoad = "include";
      // only find all include file in the jar that contains JniUtils.class
      final String jarPath =
              JniUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath();
      if (jarPath.endsWith(".jar")) {
        extractResourcesToDirectory(
                new JarFile(new File(jarPath)), folderToLoad, tmp_dir + "/" + "nativesql_include");
      } else {
        // For Maven test only
        final URLConnection urlConnection =
                JniUtils.class.getClassLoader().getResource("include").openConnection();
        String path = urlConnection.getURL().toString();
        if (urlConnection.getURL().toString().startsWith("file:")) {
          // remove the prefix of "file:" from includePath
          path = urlConnection.getURL().toString().substring(5);
        }
        final File folder = new File(path);
        copyResourcesToDirectory(urlConnection,
                tmp_dir + "/" + "nativesql_include", folder);
      }
    }
  }

  private static File moveFileFromJarToTemp(String tmpDir, String libraryToLoad) throws IOException {
    // final File temp = File.createTempFile(tmpDir, libraryToLoad);
    Path lib_path = Paths.get(tmpDir + "/" + libraryToLoad);
    if (Files.exists(lib_path)) {
      // origin lib file may load failed while using speculation, thus we delete it and create a new one
      Files.delete(lib_path);
    }
    final File temp = new File(tmpDir + "/" + libraryToLoad);
    try (final InputStream is = JniUtils.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      }
      try {
        Files.copy(is, temp.toPath());
      } catch (Exception e) {
      }
    }
    return temp;
  }

  private static Path createSoftLink(File srcFile, String destFileName) throws IOException {
    Path arrow_target = Paths.get(srcFile.getPath());
    Path arrow_link = Paths.get(tmp_dir, destFileName);
    if (Files.exists(arrow_link)) {
      Files.delete(arrow_link);
    }
    return Files.createLink(arrow_link, arrow_target);
  }

  public static void extractResourcesToDirectory(JarFile origJar, String jarPath, String destPath) throws IOException {
    for (Enumeration<JarEntry> entries = origJar.entries(); entries.hasMoreElements();) {
      JarEntry oneEntry = entries.nextElement();
      if (((jarPath == "" && !oneEntry.getName().contains("META-INF")) || (oneEntry.getName().startsWith(jarPath + "/")))
          && !oneEntry.isDirectory()) {
        int rm_length = jarPath.length() == 0 ? 0 : jarPath.length() + 1;
        Path dest_path = Paths.get(destPath + "/" + oneEntry.getName().substring(rm_length));
        if (Files.exists(dest_path)) {
          continue;
        }
        File destFile = new File(destPath + "/" + oneEntry.getName().substring(rm_length));
        File parentFile = destFile.getParentFile();
        if (parentFile != null) {
          parentFile.mkdirs();
        }

        FileOutputStream outFile = new FileOutputStream(destFile);
        InputStream inFile = origJar.getInputStream(oneEntry);

        try {
          byte[] buffer = new byte[4 * 1024];

          int s = 0;
          while ((s = inFile.read(buffer)) > 0) {
            outFile.write(buffer, 0, s);
          }
        } catch (IOException e) {
          throw new IOException("Could not extract resource from jar", e);
        } finally {
          try {
            inFile.close();
          } catch (IOException ignored) {
          }
          try {
            outFile.close();
          } catch (IOException ignored) {
          }
        }
      }
    }
  }

  public static void copyResourcesToDirectory(URLConnection urlConnection,
                                              String destPath, File folder) throws IOException {
    for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
      String destFilePath = destPath + "/" + fileEntry.getName();
      File destFile = new File(destFilePath);
      if (fileEntry.isDirectory()) {
        destFile.mkdirs();
        copyResourcesToDirectory(urlConnection, destFilePath, fileEntry);
      } else {
        try {
          Files.copy(fileEntry.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
        }
      }
    }
  }

  public static synchronized void unloadFromPath(String libPath) {
    if (!LOADED_LIBRARY_PATHS.remove(libPath)) {
      throw new IllegalStateException("Library not exist: " + libPath);
    }

    REQUIRE_UNLOAD_LIBRARY_PATHS.remove(libPath);

    try {
      while (Files.isSymbolicLink(Paths.get(libPath))) {
        libPath = Files.readSymbolicLink(Paths.get(libPath)).toString();
      }

      ClassLoader classLoader = JniUtils.class.getClassLoader();
      Field field = ClassLoader.class.getDeclaredField("nativeLibraries");
      field.setAccessible(true);
      Vector<Object> libs = (Vector<Object>) field.get(classLoader);
      Iterator it = libs.iterator();
      while (it.hasNext()) {
        Object object = it.next();
        Field[] fs = object.getClass().getDeclaredFields();
        for (int k = 0; k < fs.length; k++) {
          if (fs[k].getName().equals("name")) {
            fs[k].setAccessible(true);
            String verbosePath = fs[k].get(object).toString();
            if (verbosePath.endsWith(libPath)) {
              Method finalize = object.getClass().getDeclaredMethod("finalize");
              finalize.setAccessible(true);
              finalize.invoke(object);
            }
          }
        }
      }
    } catch (Throwable th) {
      LOG.error("Unload native library error: ", th);
    }
  }
}
