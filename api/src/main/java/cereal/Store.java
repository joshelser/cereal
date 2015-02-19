/*
 * Copyright 2015 Josh Elser
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cereal;

import java.util.Collection;

/**
 * An interface designed to allow the storing of simple objects into some backing store.
 */
public interface Store extends AutoCloseable {

  /**
   * Write the provided objects into the store.
   *
   * @param msgs
   *          The objects to write
   * @throws Exception
   *           If something bad happened
   */
  <T> void write(Collection<T> msgs) throws Exception;

  /**
   * Force any objects locally cached for writing but not yet written to the backing store, to be sent to the backing store.
   *
   * @throws Exception
   *           If something bad happened
   */
  void flush() throws Exception;

  /**
   * Fetch an object from the backing store using the provided value as the primary key. The object's type is controlled by the provided {@code clz}.
   *
   * @param id
   *          The value from the primary key.
   * @param clz
   *          The type of the object from the store.
   * @return The typed code from the backing store, or null if it's not found.
   * @throws Exception
   *           If something went wrong.
   */
  <T> T read(String id, Class<T> clz) throws Exception;
}
