/*
 * Copyright (c) 2009 Robey Pointer <robeypointer@lag.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package net.lag.kestrel

import net.lag.logging.Logger
import scala.collection.mutable


class InaccessibleIdGeneratorPath extends Exception("Inaccessible ID generator path: Must be a directory and writable")


class IdGeneratorCollection(idGeneratorFolder: String) {
  private val path = new File(idGeneratorFolder)

  if (! path.isDirectory) {
    path.mkdirs()
  }
  if (! path.isDirectory || ! path.canWrite) {
    throw new InaccessibleIdGeneratorPath
  }

  private val generators = new mutable.HashMap[String, IdGenerator]
  private var shuttingDown = false


  def generatorNames: List[String] = synchronized {
    generators.keys.toList
  }

  /**
   * Get an id generator, initalizing it if necessary.
   * Exposed only to unit tests.
   */
  private[kestrel] def get(name: String): Option[IdGenerator] = {
    var setup = false
    var generator: Option[IdGenerator] = None

    synchronized {
      if (shuttingDown) {
        return None
      }

      generator = generators.get(name) match {
        case g @ Some(_) => g
        case None =>
          try {
            val g = new IdGenerator(path.getPath, name)
            setup = true
            generators(name) = g
            Some(g)
          } except {
            case _ => None
          }
      }
    }

    if (setup) {
      /* race is handled by having PersistentQueue start up with an
       * un-initialized flag that blocks all operations until this
       * method is called and completed:
       */
      queue.get.setup
      synchronized {
        _currentBytes += queue.get.bytes
        _currentItems += queue.get.length
      }
    }
    queue
  }
  
}