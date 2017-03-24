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
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Iterator;

public class StringField extends AbstractFieldType implements FieldType<String> {

    private enum TYPE {
        RANDOM,SET;
    }
    private TYPE type = TYPE.RANDOM;
    private String[] pool;
    private int min = 15;
    private int max = 15;
    private int diff = 0;
    private int poolSize = 100;
    private String[] set;
    private boolean hasPool = false;
    private String charStr = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public StringField(JsonNode node) {
        super(node);
        if (node.has("random")) {
            JsonNode rNode = node.get("random");
            if (rNode.get("min") != null)
                min = rNode.get("min").asInt();
            if (rNode.get("max") != null)
                max = rNode.get("max").asInt();
            if (min < max)
                diff = Math.abs(max - min);
            if (rNode.get("chars") != null) {
                charStr = rNode.get("chars").asText();
            }
            if (rNode.get("pool") != null) {
                // size is required
                poolSize = rNode.get("pool").get("size").asInt();
                fillPool();
            }
        } else if (node.has("set")) {
            JsonNode setNode = node.get("set");
            type = TYPE.SET;
            set = new String[setNode.size()];
            int i=0;
            for (Iterator<JsonNode> it=setNode.elements();it.hasNext();) {
                JsonNode element = it.next();
                set[i++] = element.asText();
            }
        }
    }

    private int getStringSize() {
        if (diff == 0) {
            return min;
        } else {
            double ran = Math.random();
            long vector = Math.round(diff * ran);
            return (int)(min + vector);
        }

    }

    private void fillPool() {
        hasPool = true;
        pool = new String[poolSize];
        for (int i=0;i < poolSize;i++) {
            pool[i] = RandomStringUtils.random(getStringSize(), charStr);
        }
    }

    public String getPoolValue() {
        int ran = (int)Math.round((poolSize-1) * Math.random());
        return pool[ran];
    }

    public String getValue() {
        String rtn = null;
        if (hasPool) {
            rtn = getPoolValue();
        } else {
            switch (type) {
                case RANDOM:
                    rtn = RandomStringUtils.random(getStringSize(),charStr);
                    break;
                case SET:
                    double multiplier = Math.random();
                    rtn = set[(int)(set.length * multiplier)];
                    break;
            }
        }
        return rtn;
    }


}
