/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import joptsimple._
import kafka.utils.Implicits._
import kafka.utils._
import org.apache.kafka.clients.consumer.{AcknowledgeType, ConsumerConfig, ConsumerRecord, KafkaShareConsumer}
import org.apache.kafka.common.MessageFormatter
import org.apache.kafka.common.errors.{AuthenticationException, TimeoutException, WakeupException}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}

import java.io.PrintStream
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.{Collections, Optional, Properties, Random}
import scala.jdk.CollectionConverters._

/**
 * Share group consumer that dumps messages to standard out.
 */
object ConsoleShareConsumer extends Logging {

  var messageCount = 0

  private val shutdownLatch = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    val conf = new ConsumerConfig(args)
    try {
      run(conf)
    } catch {
      case e: AuthenticationException =>
        error("Authentication failed: terminating consumer process", e)
        Exit.exit(1)
      case e: Throwable =>
        error("Unknown error when running consumer: ", e)
        Exit.exit(1)
    }
  }

  def run(conf: ConsumerConfig): Unit = {
    val timeoutMs = if (conf.timeoutMs >= 0) conf.timeoutMs.toLong else Long.MaxValue
    val consumer = new KafkaShareConsumer(consumerProps(conf), new ByteArrayDeserializer, new ByteArrayDeserializer)

    val consumerWrapper =
      new ConsumerWrapper(Option(conf.topicArg), consumer, timeoutMs)

    addShutdownHook(consumerWrapper, conf)

    try process(conf.maxMessages, conf.formatter, consumerWrapper, System.out, conf.skipMessageOnError, conf.acknowledgeType)
    finally {
      consumerWrapper.cleanup()
      conf.formatter.close()
      reportRecordCount()

      shutdownLatch.countDown()
    }
  }

  def addShutdownHook(consumer: ConsumerWrapper, conf: ConsumerConfig): Unit = {
    Exit.addShutdownHook("consumer-shutdown-hook", {
      consumer.wakeup()

      shutdownLatch.await()

      if (conf.enableSystestEventsLogging) {
        System.out.println("shutdown_complete")
      }
    })
  }

  def process(maxMessages: Integer, formatter: MessageFormatter, consumer: ConsumerWrapper, output: PrintStream,
              skipMessageOnError: Boolean, acknowledgeType: AcknowledgeType): Unit = {
    while (messageCount < maxMessages || maxMessages == -1) {
      val msg: ConsumerRecord[Array[Byte], Array[Byte]] = try {
        consumer.receive()
      } catch {
        case _: WakeupException =>
          trace("Caught WakeupException because consumer is shutdown, ignore and terminate.")
          // Consumer will be closed
          return
        case e: Throwable =>
          error("Error processing message, terminating consumer process: ", e)
          // Consumer will be closed
          return
      }
      messageCount += 1
      try {
        formatter.writeTo(new ConsumerRecord(msg.topic, msg.partition, msg.offset, msg.timestamp, msg.timestampType,
          0, 0, msg.key, msg.value, msg.headers, Optional.empty[Integer], msg.deliveryCount()), output)
        consumer.acknowledge(msg, acknowledgeType)
      } catch {
        case e: Throwable =>
          if (skipMessageOnError) {
            error("Error processing message, skipping this message: ", e)
            consumer.acknowledge(msg, AcknowledgeType.RELEASE)
          } else {
            // Consumer will be closed
            throw e
          }
      }
      if (checkErr(output, formatter)) {
        // Consumer will be closed
        return
      }
    }
  }

  def reportRecordCount(): Unit = {
    System.err.println(s"Processed a total of $messageCount messages")
  }

  def checkErr(output: PrintStream, formatter: MessageFormatter): Boolean = {
    val gotError = output.checkError()
    if (gotError) {
      // This means no one is listening to our output stream anymore, time to shutdown
      System.err.println("Unable to write to standard out, closing consumer.")
    }
    gotError
  }

  private[tools] def consumerProps(config: ConsumerConfig): Properties = {
    val props = new Properties
    props ++= config.consumerProps
    props ++= config.extraConsumerProps
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
    if (props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null)
      props.put(ConsumerConfig.CLIENT_ID_CONFIG, "console-share-consumer")
    CommandLineUtils.maybeMergeOptions(
      props, ConsumerConfig.ISOLATION_LEVEL_CONFIG, config.options, config.isolationLevelOpt)
    props
  }

  class ConsumerConfig(args: Array[String]) extends CommandDefaultOptions(args) {
    val topicOpt = parser.accepts("topic", "The topic to consume on.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
      .withRequiredArg
      .describedAs("consumer_prop")
      .ofType(classOf[String])
    val consumerConfigOpt = parser.accepts("consumer.config", s"Consumer config properties file. Note that $consumerPropertyOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
      .withRequiredArg
      .describedAs("class")
      .ofType(classOf[String])
      .defaultsTo(classOf[DefaultMessageFormatter].getName)
    val messageFormatterArgOpt = parser.accepts("property",
      """The properties to initialize the message formatter. Default properties include:
        | print.timestamp=true|false
        | print.key=true|false
        | print.offset=true|false
        | print.delivery=true|false
        | print.partition=true|false
        | print.headers=true|false
        | print.value=true|false
        | key.separator=<key.separator>
        | line.separator=<line.separator>
        | headers.separator=<line.separator>
        | null.literal=<null.literal>
        | key.deserializer=<key.deserializer>
        | value.deserializer=<value.deserializer>
        | header.deserializer=<header.deserializer>
        |
        |Users can also pass in customized properties for their formatter; more specifically, users can pass in properties keyed with 'key.deserializer.', 'value.deserializer.' and 'headers.deserializer.' prefixes to configure their deserializers."""
        .stripMargin)
      .withRequiredArg
      .describedAs("prop")
      .ofType(classOf[String])
    val messageFormatterConfigOpt = parser.accepts("formatter-config", s"Config properties file to initialize the message formatter. Note that $messageFormatterArgOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
      .withRequiredArg
      .describedAs("num_messages")
      .ofType(classOf[java.lang.Integer])
    val timeoutMsOpt = parser.accepts("timeout-ms", "If specified, exit if no message is available for consumption for the specified interval.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Integer])
    val skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
      "skip it instead of halt.")
    val rejectOpt = parser.accepts("reject", "If specified, messages are rejected as they are consumed.")
    val releaseOpt = parser.accepts("release", "If specified, messages are released as they are consumed.")
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server(s) to connect to.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    val keyDeserializerOpt = parser.accepts("key-deserializer")
      .withRequiredArg
      .describedAs("deserializer for key")
      .ofType(classOf[String])
    val valueDeserializerOpt = parser.accepts("value-deserializer")
      .withRequiredArg
      .describedAs("deserializer for values")
      .ofType(classOf[String])
    val enableSystestEventsLoggingOpt = parser.accepts("enable-systest-events",
      "Log lifecycle events of the consumer in addition to logging consumed " +
        "messages. (This is specific for system tests.)")
    val isolationLevelOpt = parser.accepts("isolation-level",
      "Set to read_committed in order to filter out transactional messages which are not committed. Set to read_uncommitted " +
        "to read all messages.")
      .withRequiredArg()
      .ofType(classOf[String])
      .defaultsTo("read_uncommitted")

    val groupIdOpt = parser.accepts("group", "The share group id of the consumer.")
      .withRequiredArg
      .describedAs("share group id")
      .ofType(classOf[String])
      .defaultsTo("share")

    options = tryParse(parser, args)

    CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to read data from Kafka topics using a share group and outputs it to standard output.")

    var groupIdPassed = true
    val enableSystestEventsLogging = options.has(enableSystestEventsLoggingOpt)

    // topic must be specified.
    var topicArg: String = _
    val extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt))
    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties()
    val skipMessageOnError = options.has(skipMessageOnErrorOpt)
    val acknowledgeType = if (options.has(rejectOpt))
      AcknowledgeType.REJECT
    else if (options.has(releaseOpt))
      AcknowledgeType.RELEASE
    else
      AcknowledgeType.ACCEPT;
    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs = if (options.has(messageFormatterConfigOpt))
      Utils.loadProps(options.valueOf(messageFormatterConfigOpt))
    else
      new Properties()
    formatterArgs ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(messageFormatterArgOpt))
    val maxMessages = if (options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1
    val timeoutMs = if (options.has(timeoutMsOpt)) options.valueOf(timeoutMsOpt).intValue else -1
    val bootstrapServer = options.valueOf(bootstrapServerOpt)
    val keyDeserializer = options.valueOf(keyDeserializerOpt)
    val valueDeserializer = options.valueOf(valueDeserializerOpt)
    val formatter: MessageFormatter = messageFormatterClass.getDeclaredConstructor().newInstance().asInstanceOf[MessageFormatter]

    if (keyDeserializer != null && keyDeserializer.nonEmpty) {
      formatterArgs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
    }
    if (valueDeserializer != null && valueDeserializer.nonEmpty) {
      formatterArgs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
    }

    formatter.configure(formatterArgs.asScala.asJava)

    topicArg = options.valueOf(topicOpt)
    if (!options.has(topicOpt))
      CommandLineUtils.printUsageAndExit(parser, "The topic is required.")

    CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

    // if the group id is provided in more than place (through different means) all values must be the same
    val groupIdsProvided = Set(
      Option(options.valueOf(groupIdOpt)), // via --group
      Option(consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG)), // via --consumer-property
      Option(extraConsumerProps.get(ConsumerConfig.GROUP_ID_CONFIG)) // via --consumer.config
    ).flatten

    if (groupIdsProvided.size > 1) {
      CommandLineUtils.printUsageAndExit(parser, "The group ids provided in different places (directly using '--group', "
        + "via '--consumer-property', or via '--consumer.config') do not match. "
        + s"Detected group ids: ${groupIdsProvided.mkString("'", "', '", "'")}")
    }

    groupIdsProvided.headOption match {
      case Some(group) =>
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group)
      case None =>
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, s"console-share-consumer-${new Random().nextInt(100000)}")
        // By default, avoid unnecessary expansion of the coordinator cache since
        // the auto-generated group and its offsets is not intended to be used again
        if (!consumerProps.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
          consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        groupIdPassed = false
    }

    def tryParse(parser: OptionParser, args: Array[String]): OptionSet = {
      try
        parser.parse(args: _*)
      catch {
        case e: OptionException =>
          ToolsUtils.printUsageAndExit(parser, e.getMessage)
      }
    }
  }

  private[tools] class ConsumerWrapper(
                                        topic: Option[String],
                                        consumer: KafkaShareConsumer[Array[Byte], Array[Byte]],
                                        timeoutMs: Long = Long.MaxValue,
                                        time: Time = Time.SYSTEM
                                      ) {
    consumerInit()
    var commitRequired = false
    var recordIter = Collections.emptyList[ConsumerRecord[Array[Byte], Array[Byte]]]().iterator()

    def consumerInit(): Unit = {
      topic match {
        case Some(topic) =>
          consumer.subscribe(Collections.singletonList(topic))
        case _ =>
          throw new IllegalArgumentException("'topic' must be provided.")
      }
    }

    def receive(): ConsumerRecord[Array[Byte], Array[Byte]] = {
      val startTimeMs = time.milliseconds
      while (!recordIter.hasNext) {
        if (commitRequired) {
          consumer.commitSync()
          Thread.sleep(1000)
          commitRequired = false
        }
        recordIter = consumer.poll(Duration.ofMillis(timeoutMs)).iterator
        if (!recordIter.hasNext && (time.milliseconds - startTimeMs > timeoutMs)) {
          throw new TimeoutException()
        }
      }

      recordIter.next
    }

    def acknowledge(rec: ConsumerRecord[Array[Byte], Array[Byte]], acknowledgeType: AcknowledgeType): Unit = {
      this.consumer.acknowledge(rec, acknowledgeType)
      commitRequired = true
    }

    def wakeup(): Unit = {
      this.consumer.wakeup()
    }

    def cleanup(): Unit = {
      this.consumer.close()
    }

  }
}
