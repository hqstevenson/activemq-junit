/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pronoia.junit.activemq;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.plugin.StatisticsBrokerPlugin;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit Rule that embeds an ActiveMQ broker into a test.
 */
public class EmbeddedActiveMQBroker extends ExternalResource {
  Logger log = LoggerFactory.getLogger(this.getClass());

  BrokerService brokerService;
  InternalClient internalClient;

  /**
   * Create an embedded ActiveMQ broker using defaults
   * <p>
   * The defaults are:
   * - the broker name is 'embedded-broker'
   * - JMX is disabled
   * - Persistence is disabled
   */
  public EmbeddedActiveMQBroker() {
    brokerService = new BrokerService();
    brokerService.setUseJmx(false);
    brokerService.setUseShutdownHook(false);
    brokerService.setPersistent(false);
    brokerService.setBrokerName("embedded-broker");
  }

  /**
   * Create an embedded ActiveMQ broker using a configuration URI
   */
  public EmbeddedActiveMQBroker(String configurationURI) {
    try {
      brokerService = BrokerFactory.createBroker(configurationURI);
    } catch (Exception ex) {
      throw new RuntimeException("Exception encountered creating embedded ActiveMQ broker from configuration URI: " + configurationURI, ex);
    }
  }

  /**
   * Create an embedded ActiveMQ broker using a configuration URI
   */
  public EmbeddedActiveMQBroker(URI configurationURI) {
    try {
      brokerService = BrokerFactory.createBroker(configurationURI);
    } catch (Exception ex) {
      throw new RuntimeException("Exception encountered creating embedded ActiveMQ broker from configuration URI: " + configurationURI, ex);
    }
  }

  /**
   * Customize the configuration of the embedded ActiveMQ broker
   * <p>
   * This method is called before the embedded ActiveMQ broker is started, and can
   * be overridden to this method to customize the broker configuration.
   */
  protected void configure() {
  }


  /**
   * Start the embedded ActiveMQ Broker
   * <p/>
   * Invoked by JUnit to setup the resource
   */
  @Override
  protected void before() throws Throwable {
    log.info("Starting embedded ActiveMQ broker: {}", this.getBrokerName());

    this.start();

    super.before();
  }

  /**
   * Stop the embedded ActiveMQ Broker
   * <p/>
   * Invoked by JUnit to tear down the resource
   */
  @Override
  protected void after() {
    log.info("Stopping Embedded ActiveMQ Broker: {}", this.getBrokerName());

    super.after();

    this.stop();
  }

  public void setMessageHeaders(Message message, Map<String, Object> headers) {
    if (headers != null && headers.size() > 0) {
      for (Map.Entry<String, Object> header : headers.entrySet()) {
        try {
          Object value = header.getValue();
          switch (header.getKey()) {
            case "JMSDestination":
              message.setJMSMessageID(value.toString());
              break;
            case "JMSDeliveryMode":
              if (value instanceof Integer) {
                message.setJMSDeliveryMode((Integer) value);
              } else if (value instanceof Long) {
                message.setJMSDeliveryMode(((Long) value).intValue());
              } else {
                message.setJMSDeliveryMode(Integer.parseInt(value.toString()));
              }
              break;
            case "JMSExpiration":
              if (value instanceof Integer) {
                message.setJMSExpiration((Integer) value);
              } else if (value instanceof Long) {
                message.setJMSExpiration((Long) value);
              } else {
                message.setJMSExpiration(Long.parseLong(value.toString()));
              }
              break;
            case "JMSPriority":
              if (value instanceof Integer) {
                message.setJMSPriority((Integer) value);
              } else if (value instanceof Long) {
                message.setJMSPriority(((Long) value).intValue());
              } else {
                message.setJMSPriority(Integer.parseInt(value.toString()));
              }
              break;
            case "JMSMessageID":
              message.setJMSMessageID(value.toString());
              break;
            case "JMSTimestamp":
              if (value instanceof Integer) {
                message.setJMSTimestamp((Integer) value);
              } else if (value instanceof Long) {
                message.setJMSTimestamp((Long) value);
              } else {
                message.setJMSTimestamp(Long.parseLong(value.toString()));
              }
              break;
            case "JMSCorrelationID":
              message.setJMSCorrelationID(value.toString());
              break;
            case "JMSReplyTo":
              message.setJMSReplyTo(createDestination(value.toString()));
              break;
            case "JMSType":
              message.setJMSType(value.toString());
              break;
            default:
              log.warn("Ignoring value <{}> of type {} for unknown/unsupported header <{}>",
                  header.getValue(), header.getValue().getClass().getName(), header.getKey());
          }
        } catch (JMSException jmsEx) {
          log.warn(
              String.format("Ignoring unexpected exception encountered when attempting to set header <%s> of type <%s> to value %s.",
                  header.getKey(), header.getValue().getClass().getName(), header.getValue()), jmsEx);
        }
      }
    }
  }

  public static void setMessageProperties(Message message, Map<String, Object> properties) {
    if (properties != null && properties.size() > 0) {
      for (Map.Entry<String, Object> property : properties.entrySet()) {
        try {
          message.setObjectProperty(property.getKey(), property.getValue());
        } catch (JMSException jmsEx) {
          throw new EmbeddedActiveMQBrokerException(String.format("Failed to set property {%s = %s}", property.getKey(), property.getValue().toString()), jmsEx);
        }
      }
    }
  }

  /**
   * Create an {@link org.apache.activemq.command.ActiveMQDestination} for the given destination name.
   *
   * @param destinationName
   */
  static ActiveMQDestination createDestination(String destinationName) {
    ActiveMQDestination tmpDestination;

    if (destinationName.startsWith("queue://")) {
      tmpDestination = new ActiveMQQueue(destinationName.substring("queue://".length()));
    } else if (destinationName.startsWith("queue:/")) {
      tmpDestination = new ActiveMQQueue(destinationName.substring("queue:/".length()));
    } else if (destinationName.startsWith("queue:")) {
      tmpDestination = new ActiveMQQueue(destinationName.substring("queue:".length()));
    } else if (destinationName.startsWith("topic://")) {
      tmpDestination = new ActiveMQTopic(destinationName.substring("topic://".length()));
    } else if (destinationName.startsWith("topic:/")) {
      tmpDestination = new ActiveMQTopic(destinationName.substring("topic:/".length()));
    } else if (destinationName.startsWith("topic:")) {
      tmpDestination = new ActiveMQTopic(destinationName.substring("topic:".length()));
    } else if (destinationName.startsWith("temp-queue://")) {
      tmpDestination = new ActiveMQTempQueue(destinationName.substring("temp-queue://".length()));
    } else if (destinationName.startsWith("temp-queue:/")) {
      tmpDestination = new ActiveMQTempQueue(destinationName.substring("temp-queue:/".length()));
    } else if (destinationName.startsWith("temp-queue:")) {
      tmpDestination = new ActiveMQTempQueue(destinationName.substring("temp-queue:".length()));
    } else if (destinationName.startsWith("temp-topic://")) {
      tmpDestination = new ActiveMQTempTopic(destinationName.substring("temp-topic://".length()));
    } else if (destinationName.startsWith("temp-topic:/")) {
      tmpDestination = new ActiveMQTempTopic(destinationName.substring("temp-topic:/".length()));
    } else if (destinationName.startsWith("temp-topic:")) {
      tmpDestination = new ActiveMQTempTopic(destinationName.substring("temp-topic:".length()));
    } else {
      tmpDestination = new ActiveMQQueue(destinationName);
    }

    return tmpDestination;
  }

  /**
   * Start the embedded ActiveMQ broker, blocking until the broker has successfully started.
   * <p/>
   * The broker will normally be started by JUnit using the before() method.  This method allows the broker to
   * be started manually to support advanced testing scenarios.
   */
  public void start() {
    try {
      this.configure();
      brokerService.start();
      internalClient = new InternalClient();
      internalClient.start();
    } catch (Exception ex) {
      throw new RuntimeException("Exception encountered starting embedded ActiveMQ broker: {}" + this.getBrokerName(), ex);
    }

    brokerService.waitUntilStarted();
  }

  /**
   * Stop the embedded ActiveMQ broker, blocking until the broker has stopped.
   * <p/>
   * The broker will normally be stopped by JUnit using the after() method.  This method allows the broker to
   * be stopped manually to support advanced testing scenarios.
   */
  public void stop() {
    if (internalClient != null) {
      internalClient.stop();
      internalClient = null;
    }
    if (!brokerService.isStopped()) {
      try {
        brokerService.stop();
      } catch (Exception ex) {
        log.warn("Exception encountered stopping embedded ActiveMQ broker: {}" + this.getBrokerName(), ex);
      }
    }

    brokerService.waitUntilStopped();
  }

  /**
   * Get the name of the embedded ActiveMQ Broker
   *
   * @return name of the embedded broker
   */
  public String getBrokerName() {
    return brokerService.getBrokerName();
  }

  /**
   * Set the name of the embedded ActiveMQ Broker
   *
   * @param brokerName
   */
  public void setBrokerName(String brokerName) {
    brokerService.setBrokerName(brokerName);
  }

  /**
   * Builder-style setter for the name of the embedded ActiveMQ Broker
   *
   * @param brokerName
   */
  public EmbeddedActiveMQBroker brokerName(String brokerName) {
    brokerService.setBrokerName(brokerName);
    return this;
  }

  /**
   * Get the BrokerService for the embedded ActiveMQ broker.
   * <p/>
   * This may be required for advanced configuration of the BrokerService.
   *
   * @return the embedded ActiveMQ broker
   */
  public BrokerService getBrokerService() {
    return brokerService;
  }

  /**
   * Get the failover VM URL for the embedded ActiveMQ Broker
   * <p/>
   * NOTE:  The create=false option is appended to the URL to avoid the automatic creation of brokers
   * and the resulting duplicate broker errors
   *
   * @return the VM URL for the embedded broker
   */
  public String getVmURL() {
    return getVmURL(true);
  }

  /**
   * Get the VM URL for the embedded ActiveMQ Broker
   * <p/>
   * NOTE:  The create=false option is appended to the URL to avoid the automatic creation of brokers
   * and the resulting duplicate broker errors
   *
   * @param failoverURL if true a failover URL will be returned
   *
   * @return the VM URL for the embedded broker
   */
  public String getVmURL(boolean failoverURL) {
    if (failoverURL) {
      return String.format("failover:(%s?create=false)", brokerService.getVmConnectorURI().toString());
    }

    return brokerService.getVmConnectorURI().toString() + "?create=false";
  }

  /**
   * Get the failover VM URI for the embedded ActiveMQ Broker
   * <p/>
   * NOTE:  The create=false option is appended to the URI to avoid the automatic creation of brokers
   * and the resulting duplicate broker errors
   *
   * @return the VM URI for the embedded broker
   */
  public URI getVmURI() {
    return getVmURI(true);
  }

  /**
   * Get the VM URI for the embedded ActiveMQ Broker
   * <p/>
   * NOTE:  The create=false option is appended to the URI to avoid the automatic creation of brokers
   * and the resulting duplicate broker errors
   *
   * @param failoverURI if true a failover URI will be returned
   *
   * @return the VM URI for the embedded broker
   */
  public URI getVmURI(boolean failoverURI) {
    URI result;
    try {
      result = new URI(getVmURL(failoverURI));
    } catch (URISyntaxException uriEx) {
      throw new RuntimeException("Unable to create failover URI", uriEx);
    }

    return result;
  }

  /**
   * Get the state of the ActiveMQ Statistics Plugin.
   * <p>
   * See <a href="http://activemq.apache.org/statisticsplugin.html" />
   *
   * @return true if the plugin is enabled; false otherwise
   */
  public boolean isStatisticsPluginEnabled() {
    BrokerPlugin[] plugins = brokerService.getPlugins();

    if (null != plugins) {
      for (BrokerPlugin plugin : plugins) {
        if (plugin instanceof StatisticsBrokerPlugin) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Enable the ActiveMQ Statistics Plugin.
   */
  public void enableStatisticsPlugin() {
    if (!isStatisticsPluginEnabled()) {
      BrokerPlugin[] newPlugins;
      BrokerPlugin[] currentPlugins = brokerService.getPlugins();
      if (null != currentPlugins && 0 < currentPlugins.length) {
        newPlugins = new BrokerPlugin[currentPlugins.length + 1];

        System.arraycopy(currentPlugins, 0, newPlugins, 0, currentPlugins.length);
      } else {
        newPlugins = new BrokerPlugin[1];
      }

      newPlugins[newPlugins.length - 1] = new StatisticsBrokerPlugin();

      brokerService.setPlugins(newPlugins);
    }
  }

  /**
   * Disable the ActiveMQ Statistics Plugin.
   */
  public void disableStatisticsPlugin() {
    if (isStatisticsPluginEnabled()) {
      BrokerPlugin[] currentPlugins = brokerService.getPlugins();
      if (1 < currentPlugins.length) {
        BrokerPlugin[] newPlugins = new BrokerPlugin[currentPlugins.length - 1];

        int i = 0;
        for (BrokerPlugin plugin : currentPlugins) {
          if (!(plugin instanceof StatisticsBrokerPlugin)) {
            newPlugins[i++] = plugin;
          }
        }
        brokerService.setPlugins(newPlugins);
      } else {
        brokerService.setPlugins(null);
      }

    }
  }

  /**
   * Get the state of the ActiveMQ advisoryForDelivery Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   *
   * @return
   */
  public boolean isAdvisoryForDeliveryEnabled() {
    return getDefaultPolicyEntry().isAdvisoryForDelivery();
  }

  /**
   * Enable the ActiveMQ advisoryForDelivery Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void enableAdvisoryForDelivery() {
    getDefaultPolicyEntry().setAdvisoryForDelivery(true);
  }

  /**
   * Disable the ActiveMQ advisoryForDelivery Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void disableAdvisoryForDelivery() {
    getDefaultPolicyEntry().setAdvisoryForDelivery(false);
  }

  /**
   * Get the state of the ActiveMQ advisoryForDelivery Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   *
   * @return
   */
  public boolean isAdvisoryForConsumedEnabled() {
    return getDefaultPolicyEntry().isAdvisoryForConsumed();
  }

  /**
   * Enable the ActiveMQ advisoryForDelivery Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void enableAdvisoryForConsumed() {
    getDefaultPolicyEntry().setAdvisoryForConsumed(true);
  }

  /**
   * Disable the ActiveMQ advisoryForDelivery Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void disableAdvisoryForConsumed() {
    getDefaultPolicyEntry().setAdvisoryForConsumed(false);
  }

  /**
   * Get the state of the ActiveMQ sendAdvisoryIfNoConsumers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   *
   * @return
   */
  public boolean isAdvisoryForNoConsumers() {
    return getDefaultPolicyEntry().isSendAdvisoryIfNoConsumers();
  }

  /**
   * Enable the ActiveMQ sendAdvisoryIfNoConsumers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void enableAdvisoryNoConsumers() {
    getDefaultPolicyEntry().setSendAdvisoryIfNoConsumers(true);
  }

  /**
   * Disable the ActiveMQ sendAdvisoryIfNoConsumers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void disableAdvisoryNoConsumers() {
    getDefaultPolicyEntry().setSendAdvisoryIfNoConsumers(false);
  }

  /**
   * Get the state of the ActiveMQ sendAdvisoryIfNoConsumers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   *
   * @return
   */
  public boolean isAdvisoryForDiscardingMessagesEnabled() {
    return getDefaultPolicyEntry().isAdvisoryForDiscardingMessages();
  }

  /**
   * Enable the ActiveMQ advisoryForDiscardingMessages Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void enableAdvisoryForDiscardingMessages() {
    getDefaultPolicyEntry().setAdvisoryForDiscardingMessages(true);
  }

  /**
   * Disable the ActiveMQ advisoryForDiscardingMessages Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void disableAdvisoryForDiscardingMessages() {
    getDefaultPolicyEntry().setAdvisoryForDiscardingMessages(false);
  }

  /**
   * Get the state of the ActiveMQ advisoryForFastProducers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   *
   * @return
   */
  public boolean isAdvisoryForFastProducersEnabled() {
    return getDefaultPolicyEntry().isAdvisoryForFastProducers();
  }

  /**
   * Enable the ActiveMQ advisoryForFastProducers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void enableAdvisoryForFastProducers() {
    getDefaultPolicyEntry().setAdvisoryForFastProducers(true);
  }

  /**
   * Disable the ActiveMQ advisoryForFastProducers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void disableAdvisoryForFastProducers() {
    getDefaultPolicyEntry().setAdvisoryForFastProducers(false);
  }

  /**
   * Get the state of the ActiveMQ advisoryForSlowConsumers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   *
   * @return
   */
  public boolean isAdvisoryForSlowConsumersEnabled() {
    return getDefaultPolicyEntry().isAdvisoryForSlowConsumers();
  }

  /**
   * Enable the ActiveMQ advisoryForSlowConsumers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void enableAdvisoryForSlowConsumers() {
    getDefaultPolicyEntry().setAdvisoryForSlowConsumers(true);
  }

  /**
   * Disable the ActiveMQ advisoryForSlowConsumers Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void disableAdvisoryForSlowConsumers() {
    getDefaultPolicyEntry().setAdvisoryForSlowConsumers(false);
  }

  /**
   * Get the state of the ActiveMQ includeBodyForAdvisory Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   *
   * @return
   */
  public boolean isBodyForAdvisoryIncluded() {
    return getDefaultPolicyEntry().isIncludeBodyForAdvisory();
  }

  /**
   * Enable the ActiveMQ includeBodyForAdvisory Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void enableIncludeBodyForAdvisory() {
    getDefaultPolicyEntry().setIncludeBodyForAdvisory(true);
  }

  /**
   * Disable the ActiveMQ includeBodyForAdvisory Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void disableIncludeBodyForAdvisory() {
    getDefaultPolicyEntry().setIncludeBodyForAdvisory(false);
  }

  /**
   * Get the state of the ActiveMQ advisoryWhenFull Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   *
   * @return
   */
  public boolean isAdvisoryWhenFullEnabled() {
    return getDefaultPolicyEntry().isAdvisoryWhenFull();
  }

  /**
   * Enable the ActiveMQ advisoryWhenFull Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void enableAdvisoryWhenFull() {
    getDefaultPolicyEntry().setAdvisoryWhenFull(true);
  }

  /**
   * Disable the ActiveMQ advisoryWhenFull Policy Entry.
   * <p>
   * See <a href="http://activemq.apache.org/advisory-message.html"/>
   */
  public void disableAdvisoryWhenFull() {
    getDefaultPolicyEntry().setAdvisoryWhenFull(false);
  }

  /**
   * Get the number of messages in a specific JMS Destination.
   * <p/>
   * The full name of the JMS destination including the prefix should be provided - i.e. queue://myQueue
   * or topic://myTopic.  If the destination type prefix is not included in the destination name, a prefix
   * of "queue://" is assumed.
   *
   * @param destinationName the full name of the JMS Destination
   *
   * @return the number of messages in the JMS Destination
   */
  public long getMessageCount(String destinationName) {
    if (null == brokerService) {
      throw new IllegalStateException("BrokerService has not yet been created - was before() called?");
    }

    Destination destination = getDestination(destinationName);
    if (destination == null) {
      throw new RuntimeException("Failed to find destination: " + destinationName);
    }

    // return destination.getMessageStore().getMessageCount();
    return destination.getDestinationStatistics().getMessages().getCount();
  }

  /**
   * Get the ActiveMQ destination
   * <p/>
   * The full name of the JMS destination including the prefix should be provided - i.e. queue://myQueue
   * or topic://myTopic.  If the destination type prefix is not included in the destination name, a prefix
   * of "queue://" is assumed.
   *
   * @param destinationName the full name of the JMS Destination
   *
   * @return the {@link org.apache.activemq.broker.region.Destination}, null if not found
   *
   * @throws EmbeddedActiveMQBrokerException if some exception occurs retrieving the {@link org.apache.activemq.broker.region.Destination}
   *                                         from the {@link org.apache.activemq.broker.BrokerService}
   *                                         IllegalStateException if the {@link org.apache.activemq.broker.BrokerService}
   *                                         hasn't been created
   */
  public Destination getDestination(String destinationName) {
    if (null == brokerService) {
      throw new IllegalStateException("BrokerService has not yet been created - was before() called?");
    }

    Destination answer;

    try {
      ActiveMQDestination tmpDestination = createDestination(destinationName);

      answer = brokerService.getDestination(tmpDestination);
    } catch (Exception unexpectedEx) {
      throw new EmbeddedActiveMQBrokerException("Unexpected exception getting destination from broker", unexpectedEx);
    }

    return answer;
  }

  /**
   * Create a JMS {@link javax.jms.BytesMessage}.
   */
  public BytesMessage createBytesMessage() {
    return internalClient.createBytesMessage();
  }

  /**
   * Create a JMS {@link javax.jms.BytesMessage} with the specified body.
   *
   * @param body
   */
  public BytesMessage createBytesMessage(byte[] body) {
    return this.createBytesMessage(body, null);
  }

  /**
   * Create a JMS {@link javax.jms.BytesMessage} with the specified body and message properties.
   *
   * @param body
   * @param properties
   */
  public BytesMessage createBytesMessage(byte[] body, Map<String, Object> properties) {
    BytesMessage message = this.createBytesMessage();
    if (body != null) {
      try {
        message.writeBytes(body);
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException(String.format("Failed to set body {%s} on BytesMessage", new String(body)), jmsEx);
      }
    }

    setMessageProperties(message, properties);

    return message;
  }

  /**
   * Create a JMS {@link javax.jms.TextMessage}
   */
  public TextMessage createTextMessage() {
    return internalClient.createTextMessage();
  }

  /**
   * Create a JMS {@link javax.jms.TextMessage} with the specified body.
   *
   * @param body
   */
  public TextMessage createTextMessage(String body) {
    return this.createTextMessage(body, null);
  }

  /**
   * Create a JMS {@link javax.jms.TextMessage} with the specified body and message properties.
   *
   * @param body
   * @param properties
   */
  public TextMessage createTextMessage(String body, Map<String, Object> properties) {
    TextMessage message = this.createTextMessage();
    if (body != null) {
      try {
        message.setText(body);
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException(String.format("Failed to set body {%s} on TextMessage", body), jmsEx);
      }
    }

    setMessageProperties(message, properties);

    return message;
  }

  /**
   * Create a JMS {@link javax.jms.MapMessage}
   */
  public MapMessage createMapMessage() {
    return internalClient.createMapMessage();
  }

  /**
   * Create a JMS {@link javax.jms.MapMessage} with the specified body.
   *
   * @param body
   */
  public MapMessage createMapMessage(Map<String, Object> body) {
    return this.createMapMessage(body, null);
  }

  /**
   * Create a JMS {@link javax.jms.MapMessage} with the specified body and message properties.
   *
   * @param body
   * @param properties
   */
  public MapMessage createMapMessage(Map<String, Object> body, Map<String, Object> properties) {
    MapMessage message = this.createMapMessage();

    if (body != null) {
      for (Map.Entry<String, Object> entry : body.entrySet()) {
        try {
          message.setObject(entry.getKey(), entry.getValue());
        } catch (JMSException jmsEx) {
          throw new EmbeddedActiveMQBrokerException(String.format("Failed to set body entry {%s = %s} on MapMessage", entry.getKey(), entry.getValue().toString()), jmsEx);
        }
      }
    }

    setMessageProperties(message, properties);

    return message;
  }

  /**
   * Create a JMS {@link javax.jms.ObjectMessage}
   */
  public ObjectMessage createObjectMessage() {
    return internalClient.createObjectMessage();
  }

  /**
   * Create a JMS {@link javax.jms.ObjectMessage} with the specified body.
   *
   * @param body
   */
  public ObjectMessage createObjectMessage(Serializable body) {
    return this.createObjectMessage(body, null);
  }

  /**
   * Create a JMS {@link javax.jms.ObjectMessage} with the specified body and message properties.
   *
   * @param body
   * @param properties
   */
  public ObjectMessage createObjectMessage(Serializable body, Map<String, Object> properties) {
    ObjectMessage message = this.createObjectMessage();

    if (body != null) {
      try {
        message.setObject(body);
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException(String.format("Failed to set body {%s} on ObjectMessage", body.toString()), jmsEx);
      }
    }

    setMessageProperties(message, properties);

    return message;
  }

  /**
   * Create a JMS {@link javax.jms.StreamMessage}
   */
  public StreamMessage createStreamMessage() {
    return internalClient.createStreamMessage();
  }

  public StreamMessage createStreamMessage(Map<String, Object> properties) {
    StreamMessage message = this.createStreamMessage();

    setMessageProperties(message, properties);

    return message;
  }

  /**
   * Send the specified JMS {@link javax.jms.Message} to the specified destination.
   *
   * @param destinationName
   * @param message
   *
   * @return the {@link javax.jms.Message} sent to the destination
   */
  public <T extends Message> T sendMessage(String destinationName, T message) {
    if (destinationName == null || destinationName.isEmpty()) {
      throw new IllegalArgumentException("putMessage failure - destination name is required");
    } else if (message == null) {
      throw new IllegalArgumentException("putMessage failure - a Message is required");
    }

    internalClient.sendMessage(destinationName, message);

    return message;
  }

  /**
   * Send a JMS {@link javax.jms.BytesMessage} with the specified body to the specified destination.
   *
   * @param destinationName
   * @param body
   *
   * @return the {@link javax.jms.BytesMessage} sent to the destination
   */
  public BytesMessage sendBytesMessage(String destinationName, byte[] body) {
    return sendMessage(destinationName, createBytesMessage(body));
  }

  /**
   * Send a JMS {@link javax.jms.BytesMessage} with the specified body and message properties to the specified
   * destination.
   *
   * @param destinationName
   * @param body
   *
   * @return the {@link javax.jms.BytesMessage} sent to the destination
   */
  public BytesMessage sendBytesMessage(String destinationName, byte[] body, Map<String, Object> properties) {
    return sendMessage(destinationName, createBytesMessage(body, properties));
  }

  /**
   * Send a JMS {@link javax.jms.TextMessage} with the specified body to the specified destination.
   *
   * @param destinationName
   * @param body
   *
   * @return the {@link javax.jms.TextMessage} sent to the destination
   */
  public TextMessage sendTextMessage(String destinationName, String body) {
    return sendMessage(destinationName, createTextMessage(body));
  }

  /**
   * Send a JMS {@link javax.jms.TextMessage} with the specified body and message properties to the specified
   * destination.
   *
   * @param destinationName
   * @param body
   *
   * @return the {@link javax.jms.TextMessage} sent to the destination
   */
  public TextMessage sendTextMessage(String destinationName, String body, Map<String, Object> properties) {
    return sendMessage(destinationName, createTextMessage(body, properties));
  }

  /**
   * Send a JMS {@link javax.jms.MapMessage} with the specified body to the specified destination.
   *
   * @param destinationName
   * @param body
   *
   * @return the {@link javax.jms.MapMessage} sent to the destination
   */
  public MapMessage sendMapMessage(String destinationName, Map<String, Object> body) {
    return sendMessage(destinationName, createMapMessage(body));
  }

  /**
   * Send a JMS {@link javax.jms.MapMessage} with the specified body and message properties to the specified
   * destination.
   *
   * @param destinationName
   * @param body
   *
   * @return the {@link javax.jms.MapMessage} sent to the destination
   */
  public MapMessage sendMapMessage(String destinationName, Map<String, Object> body, Map<String, Object> properties) {
    return sendMessage(destinationName, createMapMessage(body));
  }

  /**
   * Send a JMS {@link javax.jms.ObjectMessage} with the specified body to the specified destination.
   *
   * @param destinationName
   * @param body
   *
   * @return the {@link javax.jms.ObjectMessage} sent to the destination
   */
  public ObjectMessage sendObjectMessage(String destinationName, Serializable body) {
    return sendMessage(destinationName, createObjectMessage(body));
  }


  /**
   * Send a JMS {@link javax.jms.ObjectMessage} with the specified body and message properties to the specified
   * destination.
   *
   * @param destinationName
   * @param body
   *
   * @return the {@link javax.jms.ObjectMessage} sent to the destination
   */
  public ObjectMessage sendObjectMessage(String destinationName, Serializable body, Map<String, Object> properties) {
    return sendMessage(destinationName, createObjectMessage(body));
  }

  /**
   * Get the next {@link javax.jms.Message} from the specified destination without consuming the message.
   *
   * @param destinationName
   *
   * @return the next {@link javax.jms.Message}
   */
  public Message peekMessage(String destinationName) {
    if (null == brokerService) {
      throw new NullPointerException("peekMessage failure  - BrokerService is null");
    }

    if (destinationName == null) {
      throw new IllegalArgumentException("peekMessage failure - destination name is required");
    }

    ActiveMQDestination destination = createDestination(destinationName);
    Destination brokerDestination = null;

    try {
      brokerDestination = brokerService.getDestination(destination);
    } catch (Exception ex) {
      throw new EmbeddedActiveMQBrokerException("peekMessage failure - unexpected exception getting destination from BrokerService", ex);
    }

    if (brokerDestination == null) {
      throw new IllegalStateException(String.format("peekMessage failure - destination %s not found in broker %s", destination.toString(), brokerService.getBrokerName()));
    }

    org.apache.activemq.command.Message[] messages = brokerDestination.browse();
    if (messages != null && messages.length > 0) {
      return (Message) messages[0];
    }

    return null;
  }

  /**
   * Get the next {@link javax.jms.Message} from the specified destination without consuming the message.
   *
   * @param destinationName
   *
   * @return the next {@link javax.jms.BytesMessage}
   *
   * @throws {@link ClassCastException} if the message is not a {@link javax.jms.BytesMessage}
   */
  public BytesMessage peekBytesMessage(String destinationName) {
    return (BytesMessage) peekMessage(destinationName);
  }

  /**
   * Get the next {@link javax.jms.Message} from the specified destination without consuming the message.
   *
   * @param destinationName
   *
   * @return the next {@link javax.jms.TextMessage}
   *
   * @throws {@link ClassCastException} if the message is not a {@link javax.jms.TextMessage}
   */
  public TextMessage peekTextMessage(String destinationName) {
    return (TextMessage) peekMessage(destinationName);
  }

  /**
   * Get the next {@link javax.jms.Message} from the specified destination without consuming the message.
   *
   * @param destinationName
   *
   * @return the next {@link javax.jms.MapMessage}
   *
   * @throws {@link ClassCastException} if the message is not a {@link javax.jms.MapMessage}
   */
  public MapMessage peekMapMessage(String destinationName) {
    return (MapMessage) peekMessage(destinationName);
  }

  /**
   * Get the next {@link javax.jms.Message} from the specified destination without consuming the message.
   *
   * @param destinationName
   *
   * @return the next {@link javax.jms.ObjectMessage}
   *
   * @throws {@link ClassCastException} if the message is not a {@link javax.jms.ObjectMessage}
   */
  public ObjectMessage peekObjectMessage(String destinationName) {
    return (ObjectMessage) peekMessage(destinationName);
  }

  /**
   * Get the next {@link javax.jms.Message} from the specified destination without consuming the message.
   *
   * @param destinationName
   *
   * @return the next {@link javax.jms.StreamMessage}
   *
   * @throws {@link ClassCastException} if the message is not a {@link javax.jms.StreamMessage}
   */
  public StreamMessage peekStreamMessage(String destinationName) {
    return (StreamMessage) peekMessage(destinationName);
  }

  private PolicyEntry getDefaultPolicyEntry() {
    PolicyMap destinationPolicy = brokerService.getDestinationPolicy();
    if (null == destinationPolicy) {
      destinationPolicy = new PolicyMap();
      brokerService.setDestinationPolicy(destinationPolicy);
    }

    PolicyEntry defaultEntry = destinationPolicy.getDefaultEntry();
    if (null == defaultEntry) {
      defaultEntry = new PolicyEntry();
      destinationPolicy.setDefaultEntry(defaultEntry);
    }

    return defaultEntry;
  }

  /**
   * Exception class for all Embedded broker exceptions.
   */
  public static class EmbeddedActiveMQBrokerException extends RuntimeException {
    public EmbeddedActiveMQBrokerException(String message) {
      super(message);
    }

    public EmbeddedActiveMQBrokerException(String message, Exception cause) {
      super(message, cause);
    }
  }

  /**
   * An Internal JMS Client for this broker.
   * <p>
   * The client will be used for creating messages and putting them on destinations.
   */
  private class InternalClient {
    ActiveMQConnectionFactory connectionFactory;
    Connection connection;
    Session session;
    MessageProducer producer;

    /**
     * Start the internal client
     */
    void start() {
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
      connectionFactory.setBrokerURL(brokerService.getVmConnectorURI().toString() + "?create=false");
      try {
        connection = connectionFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);
        connection.start();
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException("Internal Client creation failure", jmsEx);
      }
    }

    /**
     * Stop the internal client and clean-up
     */
    void stop() {
      if (producer != null) {
        try {
          producer.close();
        } catch (JMSException jmsEx) {
          log.warn("JMSException encounter closing InternalClient JMS Producer - ignoring", jmsEx);
        }
      }
      if (session != null) {
        try {
          session.close();
        } catch (JMSException jmsEx) {
          log.warn("JMSException encounter closing InternalClient JMS Session - ignoring", jmsEx);
        }
      }
      if (null != connection) {
        try {
          connection.close();
        } catch (JMSException jmsEx) {
          log.warn("JMSException encounter closing InternalClient JMS Connection - ignoring", jmsEx);
        }
      }
      connectionFactory = null;
      connection = null;
      session = null;
      producer = null;
    }

    /**
     * Create an empty {@link javax.jms.BytesMessage}
     */
    public BytesMessage createBytesMessage() {
      checkSession();

      try {
        return session.createBytesMessage();
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException("Failed to create BytesMessage", jmsEx);
      }
    }

    void checkSession() {
      if (session == null) {
        throw new IllegalStateException("JMS Session is null - has the InternalClient been started?");
      }
    }

    /**
     * Create an empty {@link javax.jms.TextMessage}
     */
    public TextMessage createTextMessage() {
      checkSession();

      try {
        return session.createTextMessage();
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException("Failed to create TextMessage", jmsEx);
      }
    }

    /**
     * Create an empty {@link javax.jms.MapMessage}
     */
    public MapMessage createMapMessage() {
      checkSession();

      try {
        return session.createMapMessage();
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException("Failed to create MapMessage", jmsEx);
      }
    }

    /**
     * Create an empty {@link javax.jms.ObjectMessage}
     */
    public ObjectMessage createObjectMessage() {
      checkSession();

      try {
        return session.createObjectMessage();
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException("Failed to create ObjectMessage", jmsEx);
      }
    }

    /**
     * Create an empty {@link javax.jms.StreamMessage}
     */
    public StreamMessage createStreamMessage() {
      checkSession();
      try {
        return session.createStreamMessage();
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException("Failed to create StreamMessage", jmsEx);
      }
    }

    /**
     * Send a JMS Message to the ActiveMQ Destination.
     *
     * @param destinationName
     * @param message
     */
    public void sendMessage(String destinationName, Message message) {
      if (producer == null) {
        throw new IllegalStateException("JMS MessageProducer is null - has the InternalClient been started?");
      }

      try {
        producer.send(createDestination(destinationName), message);
      } catch (JMSException jmsEx) {
        throw new EmbeddedActiveMQBrokerException(String.format("Failed to push %s to %s", message.getClass().getSimpleName(), destinationName), jmsEx);
      }
    }

  }
}
