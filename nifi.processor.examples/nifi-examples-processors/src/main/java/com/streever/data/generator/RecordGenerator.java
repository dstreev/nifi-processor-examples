/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streever.data.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.streever.data.generator.fields.*;

import java.util.*;

public class RecordGenerator {
    private Map<Integer, FieldType> keyFields = new TreeMap<Integer, FieldType>();
    private Map<Integer, FieldType> fields = new TreeMap<Integer, FieldType>();
    private String delimiter = "\t";
    private boolean orderForced = false;
    private boolean cdc = false;
    private int cdctype = 1;
    private Random rand = new Random(new Date().getTime());

    public RecordGenerator(JsonNode node) {
        // This node should be either the "root" node OR the "fields" node.
        delimiter = node.get("delimiter").asText();
        if (node.has("cdc")) {
            cdc = node.get("cdc").asBoolean();
            if (node.has("cdctype")) {
                cdctype = node.get("cdctype").asInt();
            }
        }

        JsonNode fieldsNode = node.get("fields");

        for (int i = 0; i < fieldsNode.size(); i++) {
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
                if (field.isKey()) {
                    keyFields.put(field.getOrder(), field);
                } else {
                    fields.put(field.getOrder(), field);
                }
            }
        } else {
            if (field.isKey()) {
                keyFields.put(keyFields.size(), field);
            } else {
                fields.put(fields.size(), field);
            }
        }
    }

    /*
    Generate New Record.
     */
    public String next() {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<Integer, FieldType>> keyFieldsIterator = keyFields.entrySet().iterator();
        Iterator<Map.Entry<Integer, FieldType>> fieldsIterator = fields.entrySet().iterator();

        while (keyFieldsIterator.hasNext()) {
            Map.Entry<Integer, FieldType> fieldMapRec = keyFieldsIterator.next();
            sb.append(fieldMapRec.getValue().getValue());
//            if (keyFieldsIterator.hasNext())
            sb.append(delimiter);
        }

        if (this.cdc) {
            // Only need one field and value
            int fieldNum = rand.nextInt(fields.size());
            int pos = 0;
            while (fieldsIterator.hasNext()) {
                switch (this.cdctype) {
                    case 1:
                        if (fieldNum == pos++) {
                            Map.Entry<Integer, FieldType> fieldMapRec = fieldsIterator.next();
                            sb.append(fieldMapRec.getValue().getName());
                            sb.append(delimiter);
                            sb.append(fieldMapRec.getValue().getValue());
                        } else {
                            fieldsIterator.next();
                        }
                        break;
                    case 2:
                        Map.Entry<Integer, FieldType> fieldMapRec = fieldsIterator.next();
                        if (fieldNum == pos++) {
                            sb.append(fieldMapRec.getValue().getValue());
                            if (fieldsIterator.hasNext())
                                sb.append(delimiter);
                        } else {
                            if (fieldsIterator.hasNext())
                                sb.append(delimiter);
                        }
                        break;
                    default:
                }
            }
        } else {
            while (fieldsIterator.hasNext()) {
                Map.Entry<Integer, FieldType> fieldMapRec = fieldsIterator.next();
                sb.append(fieldMapRec.getValue().getValue());
                if (fieldsIterator.hasNext())
                    sb.append(delimiter);
            }
        }

        return sb.toString();
    }

}
