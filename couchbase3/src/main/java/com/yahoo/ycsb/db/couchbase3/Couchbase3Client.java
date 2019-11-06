/*
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

import com.couchbase.client.core.error.KeyNotFoundException;

import com.couchbase.client.core.env.ServiceConfig;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.service.KeyValueServiceConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.ClusterOptions;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Full YCSB implementation based on the new Couchbase Java SDK 3.x.
 */
public class Couchbase3Client extends DB {

  private static final String KEY_SEPARATOR = ":";

  private static volatile ClusterEnvironment environment;
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);

  private static final Object INIT_COORDINATOR = new Object();

  private Cluster cluster;
  private ClusterOptions clusterOptions;
  private Collection collection;
  private volatile DurabilityLevel durabilityLevel;
  private volatile PersistTo persistTo;
  private volatile ReplicateTo replicateTo;
  private volatile boolean useDurabilityLevels;
  private  volatile  HashSet errors = new HashSet<Throwable>();

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String hostname = props.getProperty("couchbase.host", "127.0.0.1");
    String username = props.getProperty("couchbase.username", "Administrator");
    String password = props.getProperty("couchbase.password", "password");
    String bucketName = props.getProperty("couchbase.bucket", "ycsb");
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

    synchronized (INIT_COORDINATOR) {
      if (environment == null) {
        boolean enableMutationToken = Boolean.parseBoolean(props.getProperty("couchbase.enableMutationToken", "false"));

        long kvTimeoutMillis = Integer.parseInt(props.getProperty("couchbase.kvTimeoutMillis", "60000"));
        int kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));
        environment = ClusterEnvironment
            .builder()
            .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeoutMillis)))
            .ioConfig(IoConfig.mutationTokensEnabled(enableMutationToken))
            .serviceConfig(ServiceConfig.keyValueServiceConfig(KeyValueServiceConfig.builder().endpoints(kvEndpoints)))
            .build();
      }
    }

    clusterOptions = ClusterOptions.clusterOptions(username, password);
    clusterOptions.environment(environment);
    cluster = Cluster.connect(hostname, clusterOptions);

    Bucket bucket = cluster.bucket(bucketName);
    collection = bucket.defaultCollection();

    OPEN_CLIENTS.incrementAndGet();
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
    OPEN_CLIENTS.decrementAndGet();
    if (OPEN_CLIENTS.get() == 0 && environment != null) {
      cluster.disconnect();
      environment.shutdown();
      environment = null;
      Iterator<Throwable> it = errors.iterator();
      while(it.hasNext()){
        it.next().printStackTrace();
      }
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      GetResult document = collection.get(formatId(table, key));
      extractFields(document.contentAsObject(), fields, result);
      return Status.OK;
    } catch (KeyNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      errors.add(t);
      return Status.ERROR;
    }
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
        collection.replace(formatId(table, key), encode(values), replaceOptions().durability(durabilityLevel));
      } else {
        collection.replace(formatId(table, key), encode(values), replaceOptions().durability(persistTo, replicateTo));
      }
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      if (useDurabilityLevels) {
        collection.insert(formatId(table, key), encode(values), insertOptions().durability(durabilityLevel));
      } else {
        collection.insert(formatId(table, key), encode(values), insertOptions().durability(persistTo, replicateTo));
      }

      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
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
        collection.remove(formatId(table, key), removeOptions().durability(durabilityLevel));
      } else {
        collection.remove(formatId(table, key), removeOptions().durability(persistTo, replicateTo));
      }

      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
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
