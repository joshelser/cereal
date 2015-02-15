/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

/**
 * Manages the means, a {@link Mapping}, by which an object is serialized to a {@link Store}
 */
public interface Registry {

  /**
   * Add a new {@link Mapping} to the registry. Will replace any existing {@link Mapping} for {@code T}.
   *
   * @param mapping
   *          The {@link Mapping} for {@code T}
   */
  <T> void add(Mapping<T> mapping);

  /**
   * Fetch the current {@link Mapping} for the class {@code T}.
   * 
   * @param obj
   *          The object to fetch the {@link Mapping} for
   * @return The {@link Mapping} for {@code T}
   */
  <T> Mapping<T> get(T obj);
}
