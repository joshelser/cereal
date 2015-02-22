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

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

public class StoreImpl implements Store {
  Registry reg;
  Connector conn;
  String table;
  BatchWriter bw;

  public StoreImpl(Registry reg, Connector conn, String table) {
    this.reg = reg;
    this.conn = conn;
    this.table = table;
  }

  BatchWriter getBatchWriter() throws TableNotFoundException {
    if (null == bw) {
      bw = conn.createBatchWriter(table, new BatchWriterConfig());
    }
    return bw;
  }

  @Override
  public <T> void write(Collection<T> msgs) throws Exception {
    BatchWriter bw = getBatchWriter();
    for (T m : msgs) {
      Mapping<T> mapping = reg.get(toInstanceOrBuilder(m));
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
  public <T> T read(String id, Class<T> clz) throws Exception {
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
    }
  }
}
