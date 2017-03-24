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

import java.util.Random;

public class IPAddressField extends AbstractFieldType implements FieldType<String> {

    private String[] pool;
    private Long min = 0l;
    private Long max = 100000l;
    private Long diff;
    private int poolSize = 100;
    private boolean hasPool = false;
    Random random = new Random(127);

    public IPAddressField(JsonNode node) {
        super(node);
        if (node.has("min")) {
            min = node.get("min").longValue();
        }
        if (node.has("minIp")) {
            min = ipToLong(node.get("minIp").asText());
        }
        if (node.has("max")) {
            max = node.get("max").longValue();
        }
        if (node.has("maxIp")) {
            max = ipToLong(node.get("maxIp").asText());
        }
        if (node.has("pool")) {
            // size is required
            poolSize = node.get("pool").get("size").asInt();
            fillPool();
        }
    }

    private Number getDiff() {
        if (diff == null) {
            diff = max - min;
        }
        return diff;
    }

    private void fillPool() {
        hasPool = true;
        pool = new String[poolSize];
        for (int i=0;i < poolSize;i++) {
            pool[i] = newValue();
        }
    }

    protected String newValue() {
        double multiplierD = random.nextDouble();
        long ipLong =  (Long)min + Math.round((Long)getDiff() * multiplierD);
        return longToIp(ipLong);
    }

    public String getPoolValue() {
        int ran = (int)Math.round((poolSize-1) * random.nextFloat());
        return pool[ran];
    }

    public String getValue() {
        if (hasPool) {
            return getPoolValue();
        } else {
            return newValue();
        }
    }

    public static long ipToLong(String ipAddress) {
        long result = 0;
        String[] atoms = ipAddress.split("\\.");

        for (int i = 3; i >= 0; i--) {
            result |= (Long.parseLong(atoms[3 - i]) << (i * 8));
        }

        return result & 0xFFFFFFFF;
    }

    public static String longToIp(long ip) {
        StringBuilder sb = new StringBuilder(15);

        for (int i = 0; i < 4; i++) {
            sb.insert(0, Long.toString(ip & 0xff));

            if (i < 3) {
                sb.insert(0, '.');
            }

            ip >>= 8;
        }

        return sb.toString();
    }

}
