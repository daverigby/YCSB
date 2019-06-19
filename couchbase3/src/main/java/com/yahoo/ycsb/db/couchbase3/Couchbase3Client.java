/**
 * Copyright (c) 2019 Yahoo! Inc. All rights reserved.
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
 */

package com.yahoo.ycsb.db.couchbase3;

import com.couchbase.client.core.env.ServiceConfig;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.service.KeyValueServiceConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.time.Duration;

import java.util.*;

/**
 * Full YCSB implementation based on the new Couchbase Java SDK 3.x.
 */
public class Couchbase3Client extends DB {

  private static final String KEY_SEPARATOR = ":";

  private volatile ClusterEnvironment environment;
  private volatile Cluster cluster;
  private volatile Collection collection;
  private DurabilityLevel durabilityLevel;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;
  private boolean useDurabilityLevels;
  private int kvTimeoutMillis;

  @Override
  public synchronized void init() throws DBException {
    if (environment == null) {
      Properties props = getProperties();

      String hostname = props.getProperty("couchbase.host", "127.0.0.1");
      String bucketName = props.getProperty("couchbase.bucket", "ycsb");
      String username = props.getProperty("couchbase.username", "Administrator");
      String password = props.getProperty("couchbase.password", "password");

      kvTimeoutMillis = Integer.parseInt(props.getProperty("couchbase.kvTimeoutMillis", "10000"));

      // durability options
      String rawDurabilityLevel = props.getProperty("couchbase.durability", null);
      if (rawDurabilityLevel == null) {
        persistTo = parsePersistTo(props.getProperty("couchbase.persistTo", "0"));
        replicateTo = parseReplicateTo(props.getProperty("couchbase.replicateTo", "0"));
        useDurabilityLevels = false;
      } else {
        durabilityLevel = parseDurabilityLevel(rawDurabilityLevel);
        useDurabilityLevels = true;
      }

      int kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));

      environment = ClusterEnvironment
          .builder(hostname, username, password)
          .ioConfig(IoConfig.mutationTokensEnabled(true))
          .serviceConfig(ServiceConfig.keyValueServiceConfig(KeyValueServiceConfig.builder().endpoints(kvEndpoints)))
          .build();
      cluster = Cluster.connect(environment);
      Bucket bucket = cluster.bucket(bucketName);
      collection = bucket.defaultCollection();
    }
  }

  private static ReplicateTo parseReplicateTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return ReplicateTo.NONE;
    case 1:
      return ReplicateTo.ONE;
    case 2:
      return ReplicateTo.TWO;
    case 3:
      return ReplicateTo.THREE;
    default:
      throw new DBException("\"couchbase.replicateTo\" must be between 0 and 3");
    }
  }

  private static PersistTo parsePersistTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return PersistTo.NONE;
    case 1:
      return PersistTo.ONE;
    case 2:
      return PersistTo.TWO;
    case 3:
      return PersistTo.THREE;
    case 4:
      return PersistTo.FOUR;
    default:
      throw new DBException("\"couchbase.persistTo\" must be between 0 and 4");
    }
  }

  private static DurabilityLevel parseDurabilityLevel(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return DurabilityLevel.NONE;
    case 1:
      return DurabilityLevel.MAJORITY;
    case 2:
      return DurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER;
    case 3:
      return DurabilityLevel.PERSIST_TO_MAJORITY;
    default:
      throw new DBException("\"couchbase.durability\" must be between 0 and 3");
    }
  }

  @Override
  public synchronized void cleanup() {
    if (environment != null) {
      cluster.shutdown();
      environment.shutdown();
      environment = null;
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    Optional<GetResult> document = collection.get(formatId(table, key),
        getOptions().timeout(Duration.ofMillis(kvTimeoutMillis)));
    if (!document.isPresent()) {
      return Status.NOT_FOUND;
    }
    extractFields(document.get().contentAsObject(), fields, result);
    return Status.OK;
  }

  private static void extractFields(final JsonObject content, Set<String> fields,
                                    final Map<String, ByteIterator> result) {
    if (fields == null || fields.isEmpty()) {
      fields = content.getNames();
    }

    for (String field : fields) {
      result.put(field, new StringByteIterator(content.getString(field)));
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      if (useDurabilityLevels) {
        collection.replace(formatId(table, key), encode(values),
            replaceOptions()
                .durabilityLevel(durabilityLevel)
                .timeout(Duration.ofMillis(kvTimeoutMillis)));
      } else {
        collection.replace(formatId(table, key), encode(values),
            replaceOptions()
                .durability(persistTo, replicateTo)
                .timeout(Duration.ofMillis(kvTimeoutMillis)));
      }
      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      if (useDurabilityLevels) {
        collection.insert(formatId(table, key), encode(values),
            insertOptions()
                .durabilityLevel(durabilityLevel)
                .timeout(Duration.ofMillis(kvTimeoutMillis)));
      } else {
        collection.insert(formatId(table, key), encode(values),
            insertOptions()
                .durability(persistTo, replicateTo)
                .timeout(Duration.ofMillis(kvTimeoutMillis)));
      }

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  /**
   * Helper method to turn the passed in iterator values into a map we can encode to json.
   *
   * @param values the values to encode.
   * @return the map of encoded values.
   */
  private static Map<String, String> encode(final Map<String, ByteIterator> values) {
    Map<String, String> result = new HashMap<>(values.size());
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      result.put(value.getKey(), value.getValue().toString());
    }
    return result;
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      if (useDurabilityLevels) {
        collection.remove(formatId(table, key),
            removeOptions()
                .durabilityLevel(durabilityLevel)
                .timeout(Duration.ofMillis(kvTimeoutMillis)));
      } else {
        collection.remove(formatId(table, key),
            removeOptions()
                .durability(persistTo, replicateTo)
                .timeout(Duration.ofMillis(kvTimeoutMillis)));
      }

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  /**
   * Helper method to turn the prefix and key into a proper document ID.
   *
   * @param prefix the prefix (table).
   * @param key the key itself.
   * @return a document ID that can be used with Couchbase.
   */
  private static String formatId(final String prefix, final String key) {
    return prefix + KEY_SEPARATOR + key;
  }

}
