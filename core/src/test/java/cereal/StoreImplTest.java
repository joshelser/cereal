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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import cereal.objects.pojo.SimplePojo;
import cereal.objects.protobuf.SimpleOuter.Simple;

import com.google.protobuf.ByteString;

public class StoreImplTest {

  private StoreImpl store;
  private Registry registry;
  private Connector conn;
  private final String table = "table";

  private static final SimpleMapping INSTANCE = new SimpleMapping();

  private static class SimpleMapping extends ProtobufMessageMapping<Simple> {
    @Override
    public Text getRowId(Simple msg) {
      return new Text(msg.getByteStr().toStringUtf8());
    }

    @Override
    public Class<Simple> objectType() {
      return Simple.class;
    }
  }

  @Before
  public void setup() {
    conn = createMock(Connector.class);
    registry = new RegistryImpl();
    registry.add(INSTANCE);
    store = new StoreImpl(registry, conn, table);
  }

  @Test(expected = NullPointerException.class)
  public void testNullMessages() throws Exception {
    store.write(null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullReadId() throws Exception {
    store.read(null, Simple.class);
  }

  @Test(expected = NullPointerException.class)
  public void testNullReadClass() throws Exception {
    store.read("id", null);
  }

  @Test
  public void testWrite() throws Exception {
    Simple msg = Simple.newBuilder().setBoolean(true).setInt(42).setByteStr(ByteString.copyFromUtf8("string")).setDub(3.14159d).build();

    // The order of put()'s correlates to the equals() on Mutation (horribly). Therefore order here has to match iteration of the msg
    Mutation m = new Mutation(INSTANCE.getRowId(msg));
    m.put("", "dub", "3.14159");
    m.put("", "int", "42");
    m.put("", "boolean", "true");
    m.put("", "byte_str", "string");

    BatchWriter bw = createMock(BatchWriter.class);

    expect(conn.createBatchWriter(eq(table), anyObject(BatchWriterConfig.class))).andReturn(bw);
    bw.addMutation(m);
    expectLastCall();
    bw.flush();
    expectLastCall();
    bw.close();
    expectLastCall();

    replay(conn, bw);

    store.write(Collections.singleton(msg));
    store.flush();
    store.close();

    verify(conn, bw);

    assertNull(store.getBatchWriter());
  }

  @Test(expected = IllegalStateException.class)
  public void testMissingMapping() throws Exception {
    SimplePojo msg = new SimplePojo();

    BatchWriter bw = createMock(BatchWriter.class);

    expect(conn.createBatchWriter(eq(table), anyObject(BatchWriterConfig.class))).andReturn(bw);

    replay(conn, bw);

    store.write(Collections.singleton(msg));

    verify(conn, bw);
  }

  @Test
  public void testRead() throws Exception {
    Simple msg = Simple.newBuilder().setBoolean(true).setInt(42).setByteStr(ByteString.copyFromUtf8("string")).setDub(3.14159d).build();
    final String row = "read";

    TreeMap<Key,Value> entries = new TreeMap<>();
    entries.put(new Key(row, "", "dub"), new Value("3.14159".getBytes(UTF_8)));
    entries.put(new Key(row, "", "int"), new Value("42".getBytes(UTF_8)));
    entries.put(new Key(row, "", "boolean"), new Value("true".getBytes(UTF_8)));
    entries.put(new Key(row, "", "byte_str"), new Value("string".getBytes(UTF_8)));

    Scanner scanner = createMock(Scanner.class);
    expect(conn.createScanner(table, Authorizations.EMPTY)).andReturn(scanner);
    scanner.setRange(Range.exact(row));
    expect(scanner.iterator()).andReturn(entries.entrySet().iterator());

    replay(conn, scanner);

    Simple msgCopy = store.read(row, Simple.class);

    verify(conn, scanner);

    assertEquals(msg, msgCopy);
  }

  @Test
  public void testEmptyRead() throws Exception {
    Simple msg = Simple.newBuilder().build();
    final String row = "read";

    Scanner scanner = createMock(Scanner.class);
    expect(conn.createScanner(table, Authorizations.EMPTY)).andReturn(scanner);
    scanner.setRange(Range.exact(row));
    expect(scanner.iterator()).andReturn(Collections.<Entry<Key,Value>> emptyIterator());

    replay(conn, scanner);

    Simple msgCopy = store.read(row, Simple.class);

    verify(conn, scanner);

    assertEquals(msg, msgCopy);
  }
}
