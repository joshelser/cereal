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

import org.apache.thrift.TBase;

import com.google.protobuf.MessageOrBuilder;

/**
 * Implementation for {@link InstanceOrBuilder}.
 */
public class InstanceOrBuilderImpl<T> implements InstanceOrBuilder<T> {

  private Type type;
  private Object wrappedObject;
  private Class<T> wrappedClass;

  /**
   * Constructor for POJO-type messages
   *
   * @param instance
   *          An instance of the POJO
   */
  @SuppressWarnings("unchecked")
  public InstanceOrBuilderImpl(T instance) {
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

  /**
   * Constructor for Thrift {@link TBase} message
   *
   * @param tMsg
   *          The Thrift message
   */
  // TODO is this right? Is it better to just do {@code TBase<?,? extends TFieldIdEnum>}?
  @SuppressWarnings("unchecked")
  public InstanceOrBuilderImpl(TBase<?,?> tMsg) {
    type = Type.INSTANCE;
    wrappedObject = tMsg;
    // TODO This is definitely not right
    wrappedClass = (Class<T>) tMsg.getClass();
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
