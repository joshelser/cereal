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
package cereal.examples.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import cereal.Registry;
import cereal.Store;
import cereal.examples.protobuf.generated.PersonOuter.Engine;
import cereal.examples.protobuf.generated.PersonOuter.Person;
import cereal.examples.protobuf.generated.PersonOuter.Vehicle;
import cereal.impl.RegistryImpl;
import cereal.impl.StoreImpl;

public class ProtobufPersonTest {
  private static MiniAccumuloCluster mac;
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
    File macDir = new File(macParent, ProtobufPersonTest.class.getName());
    if (macDir.exists()) {
      FileUtils.deleteQuietly(macDir);
    }
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(macDir, PASSWORD);
    cfg.setNumTservers(1);
    mac = new MiniAccumuloCluster(cfg);
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
    ProtobufPersonMapping mapping = new ProtobufPersonMapping(registry);
    registry.add(mapping);
    registry.add(new ProtobufVehicleMapping(registry));
    registry.add(new ProtobufEngineMapping(registry));

    Person.Builder builder = Person.newBuilder().setFirstName("Bob").setMiddleName("Joe").setLastName("Franklin").setAge(30).setHeight(72).setWeight(220);

    Person brother = Person.newBuilder().setFirstName("Steve").setMiddleName("Michael").setLastName("Franklin").setAge(28).setHeight(71).setWeight(230).build();
    Person sister = Person.newBuilder().setFirstName("Mary").setMiddleName("Lou").setLastName("Franklin").setAge(33).setHeight(68).setWeight(180).build();
    Person father = Person.newBuilder().setFirstName("Harry").setMiddleName("Daniel").setLastName("Franklin").setAge(58).setHeight(70).setWeight(250).build();
    Person mother = Person.newBuilder().setFirstName("Loretta").setMiddleName("Anne").setLastName("Franklin").setAge(59).setHeight(66).setWeight(190).build();

    Engine civicEngine = Engine.newBuilder().setCylinders(4).setDisplacement(1.8).setHorsepower(160).setTorque(180).build();
    Engine accordEngine = Engine.newBuilder().setCylinders(6).setDisplacement(2.0).setHorsepower(180).setTorque(150).build();

    Vehicle civic = Vehicle.newBuilder().setMake("Honda").setModel("Civic").setWheels(4).setEngine(civicEngine).build();
    Vehicle accord = Vehicle.newBuilder().setMake("Honda").setModel("Accord").setWheels(4).setEngine(accordEngine).build();

    builder.addVehicles(civic);
    builder.addVehicles(accord);
    builder.addParents(father);
    builder.addParents(mother);
    builder.addSiblings(sister);
    builder.addSiblings(brother);

    Person p = builder.build();

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
