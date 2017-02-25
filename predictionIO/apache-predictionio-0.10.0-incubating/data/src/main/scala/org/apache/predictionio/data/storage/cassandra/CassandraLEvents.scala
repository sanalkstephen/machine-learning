/*
 * Created by sanal_s on 1/24/2017.
 */

package org.apache.predictionio.data.storage.cassandra

import grizzled.slf4j.Logging
import com.datastax.driver.core.Row
import org.apache.predictionio.data.storage._
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JObject
import org.json4s.native.Serialization.{read, write}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._

class CassandraLEvents (val client: CassandraClient, config: StorageClientConfig,
                        val namespace: String)
  extends LEvents with Logging {
  implicit private val formats = org.json4s.DefaultFormats
  override
  def init(appId: Int, channelId: Option[Int] = None): Boolean = {
    // check namespace exist
    val ks = client.cluster.getMetadata().getKeyspace("namespace");
    if(ks == null) {
      info(s"The namespace ${namespace} doesn't exist yet. Creating now...")
      val query = "CREATE KEYSPACE " + namespace + " WITH replication " +
        "= {'class':'SimpleStrategy', 'replication_factor':1};";
      client.session.execute(query);
      client.session.execute("USE " + namespace);
    }
    val tableName = ks.getTable(CassandraEventUtil.eventTableName(namespace, appId, channelId));
    if (tableName == null) {
      info(s"The table ${tableName} doesn't exist yet." +
        " Creating now...")
      val createTableCql =
        "create table IF NOT EXISTS " + tableName + " (" +
        "id varchar(32) not null primary key," +
        "event varchar(255) not null," +
        "entityType varchar(255) not null," +
        "entityId varchar(255) not null," +
        "targetEntityType text," +
        "targetEntityId text," +
        "properties text," +
        "eventTime timestamp DEFAULT CURRENT_TIMESTAMP," +
        "eventTimeZone varchar(50) not null," +
        "tags text," +
        "prId text," +
        "creationTime timestamp DEFAULT CURRENT_TIMESTAMP," +
        "creationTimeZone varchar(50) not null);"
        client.session.execute(createTableCql)
    }
    true
  }

  override
  def futureInsert(
                    event: Event, appId: Int, channelId: Option[Int])
                  (implicit ec: ExecutionContext): Future[String] = Future {
    val eventName = "";
    val id = event.eventId.getOrElse(CassandraEventUtil.generateId)
    client.session.execute(
      "INSERT INTO " + eventName +
        "(event, entityType, entityId, targetEntityType, targetEntityId, properties, eventTime" +
        "eventTimeZone, prId, creationTime, creationTimeZone) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      event.event, event.entityType, event.entityId, event.targetEntityType, event.targetEntityId,
      write(event.properties.toJObject), event.eventTime,
      event.eventTime.getZone.getID,
      if (event.tags.nonEmpty) Some(event.tags.mkString(",")) else None,
      event.prId, event.creationTime, event.creationTime.getZone.getID)
      id
    }

  def resultToEvent(row: Row , appId: Int): Event = {
    def getOptStringCol(value : String): Option[String] = {
      Some(value)
    }
    val eventId = row.getString("eventId")
    val eventTimeZone = getOptStringCol("eventTimeZone")
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)

    val eventTime = new DateTime(
      row.getLong("eventTime"), eventTimeZone)
    val creationTimeZone = getOptStringCol("creationTimeZone")
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)
    val creationTime: DateTime = new DateTime(
      row.getLong("creationTime"), creationTimeZone)
    Event(
      eventId = Some(eventId),
      event = row.getString("event"),
      entityType = row.getString("entityType"),
      entityId = row.getString("entityId"),
      targetEntityType = Some(row.getString("targetEntityType")),
      targetEntityId = Some(row.getString("targetEntityId")),
      properties = Some(row.getString("properties"))
        .map(s => DataMap(read[JObject](s))).getOrElse(DataMap()),
      eventTime = eventTime,
      tags = Seq(),
      prId = getOptStringCol(row.getString("prId")),
      creationTime = creationTime
    )
  }

  override
  def remove(appId: Int, channelId: Option[Int] = None): Boolean = {
    val tableName = CassandraEventUtil.eventTableName(namespace, appId, channelId)
    val ks = client.cluster.getMetadata.getKeyspace(namespace)
    val table = ks.getTable(tableName);
    try {
      if (table != null) {
        info(s"Removing table ${tableName}...")
        // Write remove table code here
      } else {
        info(s"Table ${tableName}} doesn't exist." +
          s" Nothing is deleted.")
      }
      true
    } catch {
      case e: Exception => {
        error(s"Fail to remove table for appId ${appId}. Exception: ${e}")
        false
      }
    }
  }

  override
  def futureDelete(
                    eventId: String, appId: Int, channelId: Option[Int])
                  (implicit ec: ExecutionContext):
  Future[Boolean] = {
    Future {
      val tableName = CassandraEventUtil.eventTableName(namespace, appId, channelId)
      val ks = client.cluster.getMetadata.getKeyspace(namespace)
      val table = ks.getTable(tableName);
      try {
        if (table != null) {
          // Write code to delete row
        } else {
          info(s"Table ${tableName}} doesn't exist." +
            s" Nothing is deleted.")
        }
        true
      }
      catch {
        case e: Exception => {
          error(s"Fail to delete row for event ${eventId}. Exception: ${e}")
          false
        }
      }
      true
    }
  }

  override
  def close(): Unit = {
    client.session.close()
  }

  override
  def futureGet(
                 eventId: String, appId: Int, channelId: Option[Int])
               (implicit ec: ExecutionContext):
  Future[Option[Event]] = {
    Future {
      val resultSet = client.session.execute("") // TODO
      val row = resultSet.one
      if (row != null) {
        val event = resultToEvent(row, appId)
        Some(event)
      } else {
        None
      }
    }
  }

  // TODO
  override
  def futureFind(
                  appId: Int,
                  channelId: Option[Int] = None,
                  startTime: Option[DateTime] = None,
                  untilTime: Option[DateTime] = None,
                  entityType: Option[String] = None,
                  entityId: Option[String] = None,
                  eventNames: Option[Seq[String]] = None,
                  targetEntityType: Option[Option[String]] = None,
                  targetEntityId: Option[Option[String]] = None,
                  limit: Option[Int] = None,
                  reversed: Option[Boolean] = None)(implicit ec: ExecutionContext):
  Future[Iterator[Event]] = {
    Future {
      require(!((reversed == Some(true)) && (entityType.isEmpty || entityId.isEmpty)),
        "the parameter reversed can only be used with both entityType and entityId specified.")
      val resultSet = client.session.execute("") // TODO
      val resultsIter = resultSet.iterator()

      // Get all events if None or Some(-1)
      val results: Iterator[Row] = limit match {
        case Some(-1) => resultsIter
        case None => resultsIter
        case Some(x) => resultsIter.take(x)
      }

      val eventsIt = results.map{ resultToEvent(_, appId) }
      eventsIt
    }
  }
}

