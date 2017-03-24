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

import java.text.DecimalFormat;
import java.util.Random;

public class NumberField extends AbstractFieldType implements FieldType<Number> {

    private enum TYPE {
        INT,LONG,FLOAT,DOUBLE;
    }
    private Number[] pool;
    private Number min = 0;
    private Number max = 100;
    private DecimalFormat decimalFormat = new DecimalFormat("#.##");
    private TYPE type = TYPE.INT;
    private Number diff;
    private int poolSize = 100;
    private boolean hasPool = false;
    Random random = new Random(127);

    public NumberField(JsonNode node) {
        super(node);
        if (node.has("type")) {
            String typeStr = node.get("type").asText();
            type = TYPE.valueOf(typeStr.toUpperCase());
        }
        if (node.has("min")) {
            switch (type) {
                case INT:
                    min = node.get("min").intValue();
                    break;
                case LONG:
                    min = node.get("min").longValue();
                    break;
                case FLOAT:
                    min = node.get("min").floatValue();
                    break;
                case DOUBLE:
                    min = node.get("min").doubleValue();
                    break;
            }
        }
        if (node.has("max")) {
            switch (type) {
                case INT:
                    max = node.get("max").intValue();
                    break;
                case LONG:
                    max = node.get("max").longValue();
                    break;
                case FLOAT:
                    max = node.get("max").floatValue();
                    break;
                case DOUBLE:
                    max = node.get("max").doubleValue();
                    break;
            }
        }
        if (node.has("decimals")) {
            StringBuilder sb = new StringBuilder("#.");
            int dec = node.get("decimals").asInt();
            for (int i=0;i<dec;i++) {
                sb.append("#");
            }
            decimalFormat = new DecimalFormat(sb.toString());
        }
        if (node.has("pool")) {
            // size is required
            poolSize = node.get("pool").get("size").asInt();
            fillPool();
        }
    }

    private Number getDiff() {
        if (diff == null) {
            switch (type) {
                case INT:
                    diff = (Integer)max - (Integer)min;
                    break;
                case LONG:
                    diff = (Long)max - (Long)min;
                    break;
                case FLOAT:
                    diff = (Float)max - (Float)min;
                    break;
                case DOUBLE:
                    diff = (Double)max - (Double)min;
                    break;
            }
        }
        return diff;
    }

    private void fillPool() {
        hasPool = true;
        pool = new Number[poolSize];
        for (int i=0;i < poolSize;i++) {
            pool[i] = newValue();
        }
    }

    protected Number newValue() {
        switch (type) {
            case INT:
                return (Integer)min + random.nextInt((Integer)getDiff());
            case LONG:
                double multiplierD = random.nextDouble();
                return (Long)min + Math.round((Long)getDiff() * multiplierD);
            case FLOAT:
                float multiplierF = random.nextFloat();
                Float valF =  (Float)min + ((Float)getDiff() * multiplierF);
                return Float.valueOf(decimalFormat.format(valF));
            case DOUBLE:
                double multiplierD2 = random.nextDouble();
                Double valD = (Double)min + ((Double)getDiff() * multiplierD2);
                return Double.valueOf(decimalFormat.format(valD));
            default:
                return (Integer)min + random.nextInt((Integer)getDiff());
        }
    }

    public Number getPoolValue() {
        int ran = (int)Math.round((poolSize-1) * random.nextFloat());
        return pool[ran];
    }

    public Number getValue() {
        if (hasPool) {
            return getPoolValue();
        } else {
            return newValue();
        }
    }


}
