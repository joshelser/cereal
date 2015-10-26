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

/**
 * Interface that defines how primitive types are converted to bytes.
 */
public interface Serialization {

  byte[] toBytes(String value);

  byte[] toBytes(int value);

  byte[] toBytes(long value);

  byte[] toBytes(float value);

  byte[] toBytes(double value);

  byte[] toBytes(boolean value);

  String toString(byte[] bytes);

  int toInt(byte[] bytes);

  long toLong(byte[] bytes);

  float toFloat(byte[] bytes);

  double toDouble(byte[] bytes);

  boolean toBoolean(byte[] bytes);
}
