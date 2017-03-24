/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streever.data.generator.fields;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class NestedField extends AbstractFieldType implements FieldType<String> {
    private boolean orderForced = false;
    private Map<Integer,FieldType> fields = new TreeMap<Integer,FieldType>();
    private String delimiter = "\t";

    private int repeats = 1;
    private String repeatsDelimiter = ",";

    private Random generator = new Random();

    public NestedField(JsonNode node) {
        super(node);

        delimiter = node.get("delimiter").asText();

        if(node.has("repeats")) {
            repeats = node.get("repeats").asInt();
            repeatsDelimiter = node.get("repeatsDelimiter").asText();
        }

        JsonNode fieldsNode = node.get("fields");

        for (int i=0;i < fieldsNode.size();i++) {
            JsonNode fieldNode = fieldsNode.get(i);
            if (fieldNode.has("string")) {
                FieldType field = new StringField(fieldNode.get("string"));
                addFields(field);
            } else if (fieldNode.has("number")) {
                FieldType field = new NumberField(fieldNode.get("number"));
                addFields(field);
            } else if (fieldNode.has("ip")) {
                FieldType field = new IPAddressField(fieldNode.get("ip"));
                addFields(field);
            } else if (fieldNode.has("boolean")) {
                FieldType field = new BooleanField(fieldNode.get("boolean"));
                addFields(field);
            } else if (fieldNode.has("date")) {
                FieldType field = new DateField(fieldNode.get("date"));
                addFields(field);
            } else if (fieldNode.has("null")) {
                FieldType field = new NullField(fieldNode.get("null"));
                addFields(field);
            } else if (fieldNode.has("start.stop")) {
                StartStopFields fields = new StartStopFields(fieldNode.get("start.stop"));
                addFields(fields.getStartField());
                addFields(fields.getStopField());
            } else if (fieldNode.has("nested")) {
                FieldType field = new NestedField(fieldNode.get("nested"));
                addFields(field);
            }
        }
    }

    private void addFields(FieldType field) {
        boolean fieldHasOrder = field.hasOrder();
        if (orderForced | fieldHasOrder) {
            orderForced = true;
            if (!fieldHasOrder) {
                throw new RuntimeException("Once order is used to control field positions, it must be used in every field definition.");
            } else {
                fields.put(field.getOrder(), field);
            }
        } else {
            fields.put(fields.size(),field);
        }
    }

    @Override
    public String getValue() {
        StringBuilder sb = new StringBuilder();

        // Fix for random number generator going from 0 <= x < repeats
        int numRepeats = generator.nextInt(repeats) + 1;
        for(int i=0; i < numRepeats; i++) {
            Iterator<Map.Entry<Integer,FieldType>> fieldsIterator = fields.entrySet().iterator();

            while (fieldsIterator.hasNext()) {
                Map.Entry<Integer, FieldType> fieldMapRec = fieldsIterator.next();
                sb.append(fieldMapRec.getValue().getValue());
                if (fieldsIterator.hasNext())
                    sb.append(delimiter);
            }

            if(i+1 < numRepeats) {
                sb.append(repeatsDelimiter);
            }
        }

        return sb.toString();
    }

    @Override
    public String getPoolValue() {
        return null;
    }
}
