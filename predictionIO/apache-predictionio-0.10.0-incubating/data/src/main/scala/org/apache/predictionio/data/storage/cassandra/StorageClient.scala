/*
 * Created by sanal_s on 1/24/2017.
 */

package org.apache.predictionio.data.storage.cassandra

import grizzled.slf4j.Logging
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import org.apache.predictionio.data.storage.{BaseStorageClient, StorageClientConfig, StorageClientException}

case class CassandraClient (
                     cluster : Cluster,
                     session: Session
                   )

class StorageClient (val config: StorageClientConfig)
  extends BaseStorageClient with Logging{

  if (!config.properties.contains("IP")) {
    throw new StorageClientException("The IP variable is not set!", null)
  }
  if (!config.properties.contains("PORT")) {
    throw new StorageClientException("The PORT variable is not set!", null)
  }

    val cluster = Cluster.builder().addContactPoint(config.properties("IP"))
      .withPort(config.properties("PORT").toInt).build();
    val metadata = cluster.getMetadata();
    val session = cluster.connect();

  val client = CassandraClient (
    cluster = cluster,
    session = session
  )

  override
  val prefix = "Cassandra"
}
