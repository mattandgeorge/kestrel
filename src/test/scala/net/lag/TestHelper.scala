/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag

import java.io.File


trait TestHelper {
  private val _folderName = new ThreadLocal[File]

  /**
   * Recursively delete a folder. Should be built in; bad java.
   */
  def deleteFolder(folder: File): Unit = {
    val files = folder.listFiles
    if (files != null) {
      for (val f <- files) {
        if (f.isDirectory) {
          deleteFolder(f)
        } else {
          f.delete
        }
      }
    }
    folder.delete
  }

  def withTempFolder(f: => Any): Unit = {
    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null
    do {
      folder = new File(tempFolder, "scala-test-" + System.currentTimeMillis)
    } while (! folder.mkdir)
    _folderName.set(folder)

    try {
      f
    } finally {
      deleteFolder(folder)
    }
  }

  def folderName = { _folderName.get.getPath }

  def canonicalFolderName = { _folderName.get.getCanonicalPath }
}
