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

public abstract class AbstractFieldType {
    private Integer order;
    private JsonNode node;
    private String name;
    private boolean key = false;
    
    public boolean hasOrder() {
        return order != null ? true : false;
    }

    public Integer getOrder() {
        return order;
    }

    public String getName() {return name; }
    public boolean isKey() {return key; }

    public void setOrder(Integer order) {
        this.order = order;
    }

    public JsonNode getJsonNode() {
        return node;
    }

    public AbstractFieldType(JsonNode node) {
        if (node != null) {
            this.node = node;
            if (node.has("order")) {
                order = node.get("order").asInt();
            }
            if (node.has("name")) {
                name = node.get("name").asText();
            }
            if (node.has("key")) {
                key = node.get("key").asBoolean();
            }
        }
    }

}
