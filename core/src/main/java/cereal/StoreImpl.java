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

import java.util.Collection;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

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

  BatchWriter getBatchWriter() throws Exception {
    if (null == bw) {
      bw = conn.createBatchWriter(table, new BatchWriterConfig());
    }
    return bw;
  }

  @Override
  public <T> void write(Collection<T> msgs) throws Exception {
    BatchWriter bw = getBatchWriter();
    for (T m : msgs) {
      Mapping<T> mapping = reg.get(m);
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
    T obj = clz.newInstance();
    Mapping<T> mapping = reg.get(obj);
    for (Entry<Key,Value> entry : s) {
      mapping.update(entry, obj);
    }
    return obj;
  }

  @Override
  public void close() throws Exception {
    BatchWriter bw = getBatchWriter();
    if (null != bw) {
      bw.close();
    }
  }
}
