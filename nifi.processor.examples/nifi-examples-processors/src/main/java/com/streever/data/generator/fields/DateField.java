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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class DateField extends AbstractFieldType implements FieldType<String> {

    private String[] pool;
    private Long min = 0l;
    private Long max = 100000l;
    private Date minDt = new Date(0); // epoch
    private Date maxDt = new Date();
    private String format = "yyyy-MM-dd HH:mm:ss";
    private Long diff;
    private DateFormat df = null;
    //    private int poolSize = 100;
//    private boolean hasPool = false;
    Random random = new Random();

    public DateField(JsonNode node) {
        super(node);
        if (node.has("format")) {
            format = node.get("format").asText();
            System.out.println("Date Format Set: " + format);
        }
        if (df == null) {
            df = new SimpleDateFormat(format);
        }
        try {
            if (node.has("range")) {
                JsonNode rangeNode = node.get("range");
                if (rangeNode.has("min")) {
                    String minDtStr = rangeNode.get("min").asText();
                    minDt = df.parse(minDtStr);
                    System.out.println("Min Date: " + minDt.toString());
                    min = minDt.getTime();
                }
                if (rangeNode.has("max")) {
                    String maxDtStr = rangeNode.get("max").asText();
                    maxDt = df.parse(maxDtStr);
                    System.out.println("Max Date: " + maxDt.toString());
                    max = maxDt.getTime();
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

//        if (node.has("pool")) {
//            // size is required
//            poolSize = node.get("pool").get("size").asInt();
//            fillPool();
//        }
    }

    private Number getDiff() {
        if (diff == null) {
            diff = max - min;
        }
        return diff;
    }

    protected String newValue() {
        double multiplierD = random.nextDouble();
        long dateValue = (Long) min + Math.round((Long) getDiff() * multiplierD);
        return df.format(new Date(dateValue));
    }

    public String getPoolValue() {
//        int ran = (int) Math.round((poolSize - 1) * random.nextFloat());
//        return pool[ran];
        return newValue();

    }

    public String getValue() {
//        if (hasPool) {
//            return getPoolValue();
//        } else {
        return newValue();
//        }
    }

}
