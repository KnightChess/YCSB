/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * @author wulingqi
 * @date 2022/3/10
 */
public class KafkaClient extends DB {

  public static final String BROKER_LIST = "broker.list";
  public static final String TOPIC = "topic";
  public static final String KEY_SERIALIZER = "key.serializer";
  public static final String VALUE_SERIALIZER = "value.serializer";
  public static final String STATIC_COL_PRE = "static_col";
  public static final String LINGER_MS = "linger.ms";
  public static final String BATCH_SIZE = "batch.size";

  private String topic;
  private KafkaProducer producer;
  private Map<String, String> staticCols = new HashMap<>();

  @Override
  public void init() throws DBException {
    try {
      Properties propsLoad = new Properties();
      InputStream propFile = KafkaClient.class.getClassLoader()
          .getResourceAsStream("kafka.properties");
      propsLoad.load(propFile);

      System.out.println(propsLoad.getProperty(BROKER_LIST));

      this.topic = propsLoad.getProperty(TOPIC);
      Properties props = new Properties();
      props.put("bootstrap.servers", propsLoad.getProperty(BROKER_LIST));
      //压缩方式，在平衡存储与cpu使用率后推荐使用lz4
      props.put("compression.type", "lz4");
      // 建议500,务必要改
      props.put(LINGER_MS, 500);
      //每个请求的批量大小
      props.put(BATCH_SIZE, 102400);
      //根据实际场景选择序列化类
      props.put("key.serializer", propsLoad.getProperty(KEY_SERIALIZER));
      props.put("value.serializer", propsLoad.getProperty(VALUE_SERIALIZER));
      producer = new KafkaProducer<String, String>(props);

      // add static columns
      Properties properties = getProperties();
      properties.stringPropertyNames().forEach(key -> {
          if (key.startsWith(STATIC_COL_PRE)) {
            String colName = key.split("\\.")[1];
            staticCols.put(colName, properties.getProperty(key));
          }
        });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return null;
  }

  @Override
  public Status scan(
      String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    stringMap.put("uuid", key);
    stringMap.put("ts", String.valueOf(System.currentTimeMillis()));

    // static col
    stringMap.putAll(staticCols);

    ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(stringMap));
    producer.send(record);

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    stringMap.put("uuid", key);
    stringMap.put("ts", String.valueOf(System.currentTimeMillis()));

    // static col
    stringMap.putAll(staticCols);

    ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(stringMap));
    producer.send(record);

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }

  @Override
  public void cleanup() throws DBException {
    producer.close();
  }
}
