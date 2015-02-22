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

import com.google.protobuf.MessageOrBuilder;

/**
 * Implementation for {@link InstanceOrBuilder}.
 */
public class InstanceOrBuilderImpl<T> implements InstanceOrBuilder<T> {
  private Type type;
  private Object wrappedObject;
  private Class<T> wrappedClass;

  /**
   * Constructor for concrete messages (POJO or Thrift)
   *
   * @param instance
   *          A message
   */
  @SuppressWarnings("unchecked")
  public InstanceOrBuilderImpl(T instance) {
    // Can't think of a good way to do a compile-time check to avoid Builders only -- should call the other constructor
    type = Type.INSTANCE;
    wrappedObject = instance;
    wrappedClass = (Class<T>) instance.getClass();
  }

  /**
   * Constructor for Protocol Buffer {@link com.google.protobuf.GeneratedMessage.Builder}s.
   *
   * @param msgBuilder
   *          An instance of the builder.
   */
  public InstanceOrBuilderImpl(MessageOrBuilder msgBuilder, Class<T> msg) {
    type = Type.BUILDER;
    wrappedObject = msgBuilder;
    wrappedClass = msg;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public Object get() {
    return wrappedObject;
  }

  @Override
  public Class<T> getWrappedClass() {
    return wrappedClass;
  }
}
