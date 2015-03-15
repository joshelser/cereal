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

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * The control over how a simple object is serialized and deserialized.
 *
 * @param <T>
 *          The class which this {@link Mapping} describes and controls
 */
public interface Mapping<T> {

  /**
   * The primary key for this object. Must be unique for this object.
   *
   * @param obj
   *          The object to act upon
   * @return The primary key for this object
   */
  public Text getRowId(T obj);

  /**
   * The {@link Field}s, attributes, for this object.
   *
   * @param obj
   *          THe object to act upon
   * @return A list of the {@link Field}s for this object.
   */
  public List<Field> getFields(T obj);

  /**
   * Update the provided object with the given {@link Key} {@link Value} pair.
   *
   * @param entry
   *          The serialized representation for a {@link Field}.
   * @param obj
   *          The object to update
   */
  public void update(Iterable<Entry<Key,Value>> entry, InstanceOrBuilder<T> obj);

  /**
   * @return The class for which this {@link Mapping} is designed to act upon.
   */
  public Class<T> objectType();
}
