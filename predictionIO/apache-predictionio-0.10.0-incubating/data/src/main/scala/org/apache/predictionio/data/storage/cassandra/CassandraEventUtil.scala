/*
 * Created by sanal_s on 1/24/2017.
 */


package org.apache.predictionio.data.storage.cassandra

object CassandraEventUtil {
  def eventTableName(namespace: String, appId: Int, channelId: Option[Int]): String =
    s"${namespace}_${appId}${channelId.map("_" + _).getOrElse("")}"

  def generateId: String = java.util.UUID.randomUUID().toString.replace("-", "")

}
