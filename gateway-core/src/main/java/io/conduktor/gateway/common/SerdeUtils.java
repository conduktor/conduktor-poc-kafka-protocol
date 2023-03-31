/*
 * Copyright 2023 Conduktor, Inc
 *
 * Licensed under the Conduktor Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * https://www.conduktor.io/conduktor-community-license-agreement-v1.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.conduktor.gateway.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SerdeUtils {

    public static byte[] writeToBytes(Object data) throws Exception {
        try (var bos = new ByteArrayOutputStream();
             var ois = new ObjectOutputStream(bos)) {
            ois.writeObject(data);
            return bos.toByteArray();
        }
    }

    public static Object writeToObject(byte[] data) throws Exception {
        try (var in = new ByteArrayInputStream(data);
             var is = new ObjectInputStream(in)) {
            return is.readObject();
        }
    }

    public static Object writeToObject(ByteBuffer data) throws Exception {
        return writeToObject(writeToBytes(data));
    }

    public static byte[] writeToBytes(ByteBuffer buf) {
        var data = new byte[buf.remaining()];
        buf.rewind();
        buf.get(data);
        return data;
    }

    public static ByteBuffer writeToByteBuffer(byte[] data) {
        return ByteBuffer.wrap(data);
    }
}
