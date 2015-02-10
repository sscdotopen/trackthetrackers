/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ssc.trackthetrackers.extraction.hadoop.util;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

//taken from Apache Mahout's HadoopUtil
public class DistributedCacheHelper {

  public static void cacheFile(Path path, Configuration conf) {
    DistributedCache.addCacheFile(path.toUri(), conf);
  }

  public static Path[] getCachedFiles(Configuration conf) throws IOException {
    LocalFileSystem localFs = FileSystem.getLocal(conf);
    Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
    URI[] fallbackFiles = DistributedCache.getCacheFiles(conf);
    // fallback for local execution
    if (cacheFiles == null) {
      Preconditions.checkState(fallbackFiles != null, "Unable to find cached files!");
      cacheFiles = new Path[fallbackFiles.length];
      for (int n = 0; n < fallbackFiles.length; n++) {
        cacheFiles[n] = new Path(fallbackFiles[n].getPath());
      }
    } else {
      for (int n = 0; n < cacheFiles.length; n++) {
        cacheFiles[n] = localFs.makeQualified(cacheFiles[n]);
        // fallback for local execution
        if (!localFs.exists(cacheFiles[n])) {
          cacheFiles[n] = new Path(fallbackFiles[n].getPath());
        }
      }
    }
    Preconditions.checkState(cacheFiles.length > 0, "Unable to find cached files!");
    return cacheFiles;
  }
}
