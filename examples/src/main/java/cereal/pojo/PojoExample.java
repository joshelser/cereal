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
package cereal.pojo;

import java.util.Collections;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import cereal.Registry;
import cereal.RegistryImpl;
import cereal.Store;
import cereal.StoreImpl;

public class PojoExample {

  public static void main(String[] args) throws Exception {
    Person p = new Person();
    p.setFirstName("Bob");
    p.setMiddleName("Joe");
    p.setLastName("Franklin");
    p.setAge(30);
    p.setHeight(72);
    p.setWeight(220);

    Registry registry = new RegistryImpl();
    registry.add(new PersonMapping());
    String tableName = "pojo_people";
    ZooKeeperInstance inst = new ZooKeeperInstance("accumulo", "127.0.0.1");
    Connector conn = inst.getConnector("root", new PasswordToken("secret"));
    if (!conn.tableOperations().exists(tableName)) {
      conn.tableOperations().create(tableName);
    }

    System.out.println("Person: " + p);

    try (Store store = new StoreImpl(registry, conn, tableName)) {
      store.write(Collections.singleton(p));
      store.flush();

      Person pCopy = store.read("Bob_Joe_Franklin", Person.class);
      System.out.println("Copy: " + pCopy);
    }
  }
}
