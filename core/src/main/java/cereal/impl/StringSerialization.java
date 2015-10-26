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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

import cereal.Serialization;

/**
 * Trivial String-based serialization.
 */
public class StringSerialization implements Serialization {
  private static final byte[] TRUE_BYTES = "true".getBytes();
  private static final byte[] FALSE_BYTES = "false".getBytes();

  @Override
  public byte[] toBytes(String value) {
    return value.getBytes(UTF_8);
  }

  @Override
  public byte[] toBytes(int value) {
    return Integer.toString(value).getBytes(UTF_8);
  }

  @Override
  public byte[] toBytes(long value) {
    return Long.toString(value).getBytes(UTF_8);
  }

  @Override
  public byte[] toBytes(float value) {
    return Float.toString(value).getBytes(UTF_8);
  }

  @Override
  public byte[] toBytes(double value) {
    return Double.toString(value).getBytes(UTF_8);
  }

  @Override
  public byte[] toBytes(boolean value) {
    return value ? TRUE_BYTES : FALSE_BYTES;
  }

  @Override
  public String toString(byte[] bytes) {
    return new String(bytes, UTF_8);
  }

  @Override
  public int toInt(byte[] bytes) {
    return Integer.parseInt(toString(bytes));
  }

  @Override
  public long toLong(byte[] bytes) {
    return Long.parseLong(toString(bytes));
  }

  @Override
  public float toFloat(byte[] bytes) {
    return Float.parseFloat(toString(bytes));
  }

  @Override
  public double toDouble(byte[] bytes) {
    return Double.parseDouble(toString(bytes));
  }

  @Override
  public boolean toBoolean(byte[] bytes) {
    if (Arrays.equals(bytes, TRUE_BYTES)) {
      return true;
    } else if (Arrays.equals(bytes, FALSE_BYTES)) {
      return false;
    }

    throw new IllegalArgumentException("Unrecognized bytes: " + Arrays.toString(bytes));
  }
}
