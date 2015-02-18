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
 * A wrapper around the message of type {@code T}. Protobuf messages (among others) are immutable. Therefore,
 * {@link Mapping#update(java.util.Map.Entry, InstanceOrBuilder)} needs to be able to handle either and instance of the message to update or some builder class
 * for the message.
 */
public interface InstanceOrBuilder<T> {

  /**
   * The type of the Object being wrapped. Informs the implementation whether to treat it as a message builder or an instance of the message
   */
  public enum Type {
    INSTANCE, BUILDER
  }

  /**
   * The {@link Type} of the object being wrapped
   */
  Type getType();

  /**
   * The wrapped object. See {@link #getType()} for what the object actually is.
   */
  Object get();

  /**
   * The class of the object being wrapped
   */
  Class<T> getWrappedClass();
}
