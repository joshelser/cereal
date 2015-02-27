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

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import cereal.Field;
import cereal.InstanceOrBuilder;
import cereal.Mapping;
import cereal.Registry;
import cereal.Store;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

/**
 * {@link Store} implementation that uses Accumulo.
 */
public class StoreImpl implements Store {
  Registry reg;
  Connector conn;
  String table;
  BatchWriter bw;

  public StoreImpl(Registry reg, Connector conn, String table) {
    checkNotNull(reg, "Registry was null");
    checkNotNull(conn, "Connector was null");
    checkNotNull(table, "Accumulo table was null");
    this.reg = reg;
    this.conn = conn;
    this.table = table;
  }

  BatchWriter getOrCreateBatchWriter() throws TableNotFoundException {
    if (null == bw) {
      bw = conn.createBatchWriter(table, new BatchWriterConfig());
    }
    return bw;
  }

  /**
   * @return Get the internal {@link BatchWriter}, or null if it's not initialized.
   */
  BatchWriter getBatchWriter() {
    return bw;
  }

  @Override
  public <T> void write(Collection<T> msgs) throws Exception {
    checkNotNull(msgs, "Message to write were null");
    BatchWriter bw = getOrCreateBatchWriter();
    Mapping<T> mapping = null;
    for (T m : msgs) {
      if (null == mapping) {
        InstanceOrBuilder<T> instOrBuilder = toInstanceOrBuilder(m);
        mapping = reg.get(instOrBuilder);
        if (null == mapping) {
          throw new IllegalStateException("Registry doesn't contain Mapping for " + instOrBuilder);
        }
      }
      Mutation mut = new Mutation(mapping.getRowId(m));
      for (Field f : mapping.getFields(m)) {
        mut.put(f.grouping(), f.name(), f.visibility(), f.value());
      }
      bw.addMutation(mut);
    }
  }

  @Override
  public void flush() throws Exception {
    if (bw != null) {
      bw.flush();
    }
  }

  @Override
  public <T> T read(Text id, Class<T> clz) throws Exception {
    checkNotNull(id, "ID was null");
    checkNotNull(clz, "Target class was null");
    Scanner s = conn.createScanner(table, Authorizations.EMPTY);
    s.setRange(Range.exact(id));
    InstanceOrBuilder<T> instOrBuilder = toInstanceOrBuilder(clz);
    Mapping<T> mapping = reg.get(instOrBuilder);
    for (Entry<Key,Value> entry : s) {
      mapping.update(entry, instOrBuilder);
    }

    switch (instOrBuilder.getType()) {
      case INSTANCE:
        @SuppressWarnings("unchecked")
        T obj = (T) instOrBuilder.get();
        return obj;
      case BUILDER:
        GeneratedMessage.Builder<?> builder = (GeneratedMessage.Builder<?>) instOrBuilder.get();
        @SuppressWarnings("unchecked")
        T pb = (T) builder.build();
        return pb;
      default:
        throw new IllegalArgumentException("Cannot handle unknown InstanceOrBuilder.Type");
    }
  }

  protected <T> InstanceOrBuilder<T> toInstanceOrBuilder(T instance) {
    if (instance instanceof GeneratedMessage) {
      try {
        Message.Builder builder = ((GeneratedMessage) instance).newBuilderForType();
        @SuppressWarnings("unchecked")
        Class<T> typedClz = (Class<T>) instance.getClass();
        return new InstanceOrBuilderImpl<T>(builder, typedClz);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      // POJO or Thrift
      return new InstanceOrBuilderImpl<T>(instance);
    }
  }

  protected <T> InstanceOrBuilder<T> toInstanceOrBuilder(Class<T> clz) {
    try {
      if (GeneratedMessage.class.isAssignableFrom(clz)) {
        // Protobuf Message
        @SuppressWarnings("unchecked")
        Class<? extends GeneratedMessage> msgClz = (Class<GeneratedMessage>) clz;
        Method newBuilderMethod = msgClz.getMethod("newBuilder");
        Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);
        return new InstanceOrBuilderImpl<T>(builder, clz);
      } else {
        // A POJO or Thrift
        return new InstanceOrBuilderImpl<T>(clz.newInstance());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws MutationsRejectedException {
    if (null != bw) {
      bw.close();
      bw = null;
    }
  }
}
