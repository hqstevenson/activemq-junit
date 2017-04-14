/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pronoia.junit.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.pool.PooledConnectionFactory;

public class VmConnectionFactoryUtils {
  /**
   * Create an {@link org.apache.activemq.ActiveMQConnectionFactory} for the embedded ActiveMQ Broker
   *
   * @param embeddedActiveMQBrokers
   *
   * @return a new {@link org.apache.activemq.ActiveMQConnectionFactory}
   */
  public static ActiveMQConnectionFactory createConnectionFactoryWithFailover(EmbeddedActiveMQBroker... embeddedActiveMQBrokers) {
    BrokerService[] brokerServices = new BrokerService[embeddedActiveMQBrokers.length];
    for (int i=0; i<embeddedActiveMQBrokers.length; ++i) {
      brokerServices[i] = embeddedActiveMQBrokers[i].getBrokerService();
    }

    return createConnectionFactoryWithFailover(brokerServices);
  }

  /**
   * Create an {@link org.apache.activemq.pool.PooledConnectionFactory} for the embedded ActiveMQ Broker
   *
   * @param embeddedActiveMQBrokers
   *
   * @return a new {@link org.apache.activemq.pool.PooledConnectionFactory}
   */
  public static PooledConnectionFactory createPooledConnectionFactoryWithFailover(EmbeddedActiveMQBroker... embeddedActiveMQBrokers) {
    return new PooledConnectionFactory(createConnectionFactoryWithFailover(embeddedActiveMQBrokers));
  }

  /**
   * Create an {@link org.apache.activemq.pool.PooledConnectionFactory} for the embedded ActiveMQ Broker
   *
   * @return a new {@link org.apache.activemq.pool.PooledConnectionFactory}
   */
  public static ActiveMQConnectionFactory createConnectionFactoryWithFailover(BrokerService... brokerServices) {
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();

    if(brokerServices.length == 1) {
      connectionFactory.setBrokerURL(String.format("failover:(%s?create=false)", brokerServices[0].getVmConnectorURI().toString()));
    } else {
      StringBuilder urlBuilder = new StringBuilder();

      urlBuilder.append("failover:(");
      for (int i = 0; i < brokerServices.length; ++i) {
        urlBuilder.append(brokerServices[i].getVmConnectorURI().toString());
        urlBuilder.append("?create=false");
        if (i < brokerServices.length - 1) {
          urlBuilder.append(",");
        }
      }
      urlBuilder.append(')');

      connectionFactory.setBrokerURL(urlBuilder.toString());
    }

    return connectionFactory;
  }

  /**
   * Create an {@link org.apache.activemq.pool.PooledConnectionFactory} for the embedded ActiveMQ Broker
   *
   * @return a new {@link org.apache.activemq.pool.PooledConnectionFactory}
   */
  public static PooledConnectionFactory createPooledConnectionFactoryWithFailover(BrokerService... brokerServices) {
    return new PooledConnectionFactory(createConnectionFactoryWithFailover(brokerServices));
  }

}
