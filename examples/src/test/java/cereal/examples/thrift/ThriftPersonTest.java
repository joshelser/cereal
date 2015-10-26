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
package cereal.examples.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import cereal.Registry;
import cereal.Store;
import cereal.examples.thrift.generated.Person;
import cereal.impl.RegistryImpl;
import cereal.impl.StoreImpl;

public class ThriftPersonTest {
  private static MiniAccumuloClusterImpl mac;
  private static final String PASSWORD = "password";

  @Rule
  public TestName test = new TestName();

  @BeforeClass
  public static void start() throws IOException, InterruptedException {
    File target = new File(System.getProperty("user.dir") + "/target");
    assertTrue(target.exists());
    assertTrue(target.isDirectory());
    File macParent = new File(target, "minicluster");
    macParent.mkdirs();
    File macDir = new File(macParent, ThriftPersonTest.class.getName());
    if (macDir.exists()) {
      FileUtils.deleteQuietly(macDir);
    }
    MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(macDir, PASSWORD);
    cfg.setNumTservers(1);
    cfg.setZooKeeperStartupTime(40 * 1000);
    mac = new MiniAccumuloClusterImpl(cfg);
    mac.start();
  }

  @AfterClass
  public static void stop() throws IOException, InterruptedException {
    if (null != mac) {
      mac.stop();
    }
  }

  @Test(timeout = 30 * 1000)
  public void testSerialization() throws Exception {
    Registry registry = new RegistryImpl();
    ThriftPersonMapping mapping = new ThriftPersonMapping();
    registry.add(mapping);

    Person p = new Person();
    p.setFirstName("Bob");
    p.setMiddleName("Joe");
    p.setLastName("Franklin");
    p.setAge(30);
    p.setHeight(72);
    p.setWeight(220);

    String tableName = test.getMethodName();
    Connector conn = mac.getConnector("root", PASSWORD);
    conn.tableOperations().create(tableName);

    try (Store store = new StoreImpl(registry, conn, tableName)) {
      store.write(Collections.singleton(p));
      store.flush();

      Text row = mapping.getRowId(p);

      Person copy = store.read(row, Person.class);

      assertEquals(p, copy);
    }
  }
}
