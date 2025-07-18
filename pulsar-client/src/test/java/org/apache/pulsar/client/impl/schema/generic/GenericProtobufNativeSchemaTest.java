/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl.schema.generic;

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.schema.proto.Test.TestMessage;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GenericProtobufNativeSchemaTest {

    private TestMessage message;
    private GenericRecord genericmessage;
    private GenericProtobufNativeSchema genericProtobufNativeSchema;
    private ProtobufNativeSchema<TestMessage> clazzBasedProtobufNativeSchema;

    @BeforeMethod
    public void init() {
        clazzBasedProtobufNativeSchema = ProtobufNativeSchema.of(SchemaDefinition.<TestMessage>builder()
                .withPojo(TestMessage.class).build());
        genericProtobufNativeSchema = (GenericProtobufNativeSchema) GenericProtobufNativeSchema
                .of(clazzBasedProtobufNativeSchema.getSchemaInfo());

    }

    @Test
    public void testGenericReaderByClazzBasedWriterSchema() {
        message = TestMessage.newBuilder().setStringField(STRING_FIELD_VLUE).setDoubleField(DOUBLE_FIELD_VLUE).build();
        byte[] clazzBasedProtobufBytes = clazzBasedProtobufNativeSchema.encode(message);
        GenericRecord genericRecord = genericProtobufNativeSchema.decode(clazzBasedProtobufBytes);
        assertEquals(genericRecord.getField("stringField"), STRING_FIELD_VLUE);
        assertEquals(genericRecord.getField("doubleField"), DOUBLE_FIELD_VLUE);
    }

    @Test
    public void testClazzBasedReaderByClazzGenericWriterSchema() {
        genericmessage = genericProtobufNativeSchema.newRecordBuilder().set("stringField", STRING_FIELD_VLUE)
                .set("doubleField", DOUBLE_FIELD_VLUE).build();
        byte[] messageBytes = genericProtobufNativeSchema.encode(genericmessage);
        message = clazzBasedProtobufNativeSchema.decode(messageBytes);
        assertEquals(message.getStringField(), STRING_FIELD_VLUE);
        assertEquals(message.getDoubleField(), DOUBLE_FIELD_VLUE);
    }

    private static final String STRING_FIELD_VLUE = "stringFieldValue";
    private static final double DOUBLE_FIELD_VLUE = 0.2D;

}
