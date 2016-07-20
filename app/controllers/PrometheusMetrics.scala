/**
  * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
  * See accompanying LICENSE file.
  */

package controllers

import java.io.StringWriter

import features.ApplicationFeatures
import io.prometheus.client.exporter.common.TextFormat
import io.prometheus.client.{CollectorRegistry, Gauge}
import kafka.manager.{ApiError, ConsumerListExtended}
import models.navigation.Menus
import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.mvc._

import scala.concurrent.Future
import scalaz._

/**
  * @author jmwong
  */
class PrometheusMetrics(val messagesApi: MessagesApi, val kafkaManagerContext: KafkaManagerContext)
                       (implicit af: ApplicationFeatures, menus: Menus) extends Controller with I18nSupport {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import PrometheusMetrics._


  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  def metrics = Action.async {
    val futureClusterNames: Future[Option[IndexedSeq[String]]] =
      kafkaManager.getClusterList
        .map(_.toOption)
        .map { clusterListOpt =>
          clusterListOpt.map(_.active.map(_.name))
        }

    val futureClusterToConsumerMap: Future[Map[String, ConsumerListExtended]] = futureClusterNames.flatMap { clusterNamesOpt =>
      clusterNamesOpt.map { clusterNames => {
          val futures = clusterNames.map(kafkaManager.getConsumerListExtended)

          Future.sequence(futures).map { responses =>
            (clusterNames zip responses).collect { case (cluster, \/-(consumerList)) =>
                cluster -> consumerList
              }.toMap
          }
        }
      }.getOrElse(Future.successful(Map()))
    }

    futureClusterToConsumerMap.map { clusterToConsumerMap =>
      recordLags(clusterToConsumerMap)

      val writer = new StringWriter()
      TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples())

      Ok(writer.toString)
    }
  }

}


object PrometheusMetrics {
  val lagGauge = Gauge.build()
    .name("kafka_manager_consumer_lag")
    .help("Kafka consumer lag")
    .labelNames("cluster", "consumer", "topic", "type")
    .register()

  def recordLags(clusterToConsumerList: Map[String, ConsumerListExtended]) = {
    clusterToConsumerList foreach { case (cluster, consumerList) =>
      for {
        ((consumer, consumerType), consumerIdentityOpt) <- consumerList.list
        consumerIdentity <- consumerIdentityOpt
        (topic, state) <- consumerIdentity.topicMap
        totalLag <- state.totalLag
      } {
        lagGauge
          .labels(cluster, consumer, topic, consumerType.toString)
          .set(totalLag)
      }
    }
  }
}
