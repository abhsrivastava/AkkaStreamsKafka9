package com.abhi

/**
  * Created by ASrivastava on 9/29/17.
  */

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import com.softwaremill.react.kafka.KafkaMessages._
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer, StringDeserializer}
import akka.stream.scaladsl._
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object AkkaStreamsKafka9 extends App {
   implicit val actorSystem = ActorSystem()
   implicit val actorMaterializer = ActorMaterializer()
   val kafka = new ReactiveKafka()

   val props = Map[String, String](
      "bootstrap.servers" -> "foo:9092",
      "auto.offset.reset" -> "latest",
      "group.id" -> "abhi"
   )
   val consumerProperties = ConsumerProperties(
      params = props,
      topic = "my-topic",
      groupId = "abhi",
      keyDeserializer = new ByteArrayDeserializer(),
      valueDeserializer = new StringDeserializer()
   )

   val source = Source.fromPublisher(kafka.consume(consumerProperties))
   val flow = Flow[ConsumerRecord[Array[Byte], String]].map(r => r.value())
   val sink = Sink.foreach[String](println)
   val graph = RunnableGraph.fromGraph(GraphDSL.create(sink) {implicit builder =>
    s =>
      import GraphDSL.Implicits._
       source ~> flow ~> s.in
      ClosedShape
   })
   val future = graph.run()
   future.onComplete{_ =>
      actorSystem.terminate()
   }
   Await.result(actorSystem.whenTerminated, Duration.Inf)
}
