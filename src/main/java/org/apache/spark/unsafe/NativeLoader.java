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


package org.apache.spark.unsafe;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NativeLoader {
  private static final Logger logger = LoggerFactory.getLogger(NativeLoader.class);

  public static void loadLibrary(String libName) {
    assertOsArchSupport();
    try {
      logger.info("Trying to load library " + libName + " from system library path.");
      logger.info("system library path:" + System.getProperty("java.library.path")
      + System.getProperty("user.dir"));
      System.loadLibrary(libName);
      logger.info("load libvmemcachejni succeed.");
      return;
    } catch (UnsatisfiedLinkError e) {
      logger.info("load from system library path failed and will try to load from package.");
    }
    logger.info("Trying to load library " + libName + " from package.");
    loadFromPackage(libName);
  }

  private static void loadFromPackage(String libName) {
    String fullName = appendPrefixAndSuffix(libName);
    String path = "/lib/linux64/" + fullName;
    logger.info("library path is " + path);
    InputStream input = NativeLoader.class.getResourceAsStream(path);
    if (input == null) {
      throw new RuntimeException("The library " + path + " doesn't exist");
    }

    File tmpFile = null;
    OutputStream output = null;
    try {
      tmpFile = File.createTempFile("lib", libName + ".so.tmp");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try {
      output = new FileOutputStream(tmpFile);
      byte[] buffer = new byte[1024];
      int len = -1;
      while ((len = input.read(buffer)) != -1) {
        output.write(buffer, 0, len);
      }

      try {
        output.flush();
        output.close();
      } catch (Exception e) {
        // ignore it
      }

      System.load(tmpFile.getCanonicalPath());
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (UnsatisfiedLinkError e) {
      throw new RuntimeException(e);
    } finally {
      if (input != null) {
        try {
          input.close();
          input = null;
        } catch (Exception e) {
          // ignore it
        }
      }

      if (output != null) {
        try {
          output.close();
          output = null;
        } catch (Exception e) {
          // ignore it
        }
      }

      if (tmpFile != null && tmpFile.exists()) {
        tmpFile.delete();
        tmpFile = null;
      }
    }
  }

  private static void assertOsArchSupport() {
    String osProp = System.getProperty("os.name");
    String archProp = System.getProperty("os.arch");
    if (!osProp.contains("Linux") && !archProp.contains("64")) {
      throw new UnsupportedOperationException("We only tested on linux64. It doesn't support on "
                                                + osProp + archProp + "currently");
    }
  }

  private static String appendPrefixAndSuffix(String libName) {
    // Currently, we only support linux64
    return "lib" + libName + ".so";
  }
}
