package com.pronoia.junit.asserts.activemq;

import com.pronoia.junit.activemq.EmbeddedActiveMQBroker;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Assert;

public class EmbeddedBrokerAssert {
  static final String ASSERT_NOT_CONTAINS_DESTINATION_FORMAT = "Destination %s found in broker";

  protected EmbeddedBrokerAssert() {
  }

  /**
   * Assert that the specified destination has the expected message count.
   *
   * @param broker
   * @param destinationName
   * @param expected
   */
  public static void assertMessageCount(EmbeddedActiveMQBroker broker, String destinationName, long expected) {
    ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
    assertMessageCount(
        String.format("Message Count for destination %s in broker %s did not match expected value", destination.toString(), broker.getBrokerName()),
        broker, destinationName, expected);
  }

  /**
   * Assert that the specified destination has the expected message count.
   *
   * @param message
   * @param broker
   * @param destinationName
   * @param expected
   *
   * @throws RuntimeException if the destination is not found in the broker
   */
  public static void assertMessageCount(String message, EmbeddedActiveMQBroker broker, String destinationName, long expected) {
    Assert.assertEquals(message, expected, broker.getMessageCount(destinationName));
  }

  /**
   * Assert that the specified destination exists
   *
   * @param broker
   * @param expected
   *
   * @throws RuntimeException if there is an exception retrieving the destination from the broker service
   */
  public static void assertContainsDestination(EmbeddedActiveMQBroker broker, String expected) {
    assertContainsDestination(
        String.format("Destination %s not found in broker %s", expected, broker.getBrokerName()),
        broker, expected);
  }

  public static void assertContainsDestination(String message, EmbeddedActiveMQBroker broker, String expected) {
    Assert.assertTrue(message, (null != broker.getDestination(expected)));
  }

  public static void assertNotContainsDestination(EmbeddedActiveMQBroker broker, String expected) {
    assertNotContainsDestination(
        String.format("Destination %s found in broker %s", expected, broker.getBrokerName()),
        broker, expected);
  }

  public static void assertNotContainsDestination(String message, EmbeddedActiveMQBroker broker, String expected) {
    Assert.assertFalse(message, (null != broker.getDestination(expected)));
  }


}
