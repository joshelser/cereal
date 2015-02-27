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
package cereal.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

import cereal.InstanceOrBuilder;
import cereal.Mapping;
import cereal.Registry;

/**
 * Simple Class to {@link Mapping} registry implemented with a {@link HashMap}
 */
public class RegistryImpl implements Registry {

  Map<Class<?>,Mapping<?>> mappings = new HashMap<>();

  @Override
  public <T> void add(Mapping<T> mapping) {
    Class<T> type = mapping.objectType();
    checkNotNull(type, "objectType on Mapping returned null");
    mappings.put(type, mapping);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Mapping<T> get(InstanceOrBuilder<T> obj) {
    checkNotNull(obj, "InstanceOrBuilder was null");
    Class<T> wrappedClass = obj.getWrappedClass();
    checkNotNull(wrappedClass, "wrappedClass on InstanceOrBuilder was null");
    return (Mapping<T>) mappings.get(wrappedClass);
  }

}
