package com.pronoia.junit.asserts.activemq;

import com.pronoia.junit.activemq.EmbeddedActiveMQBroker;

import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Assert;

/**
 * JUnit assertions for embedded brokers.
 */
public class EmbeddedBrokerAssert {
    static final String ASSERT_NOT_CONTAINS_DESTINATION_FORMAT = "Destination %s found in broker";

    protected EmbeddedBrokerAssert() {
    }

    /**
     * Assert that the specified destination has the expected message count.
     *
     * @param broker          the EmbeddedActiveMQBroker to check
     * @param destinationName the name of the destination to check.
     * @param expected        the expected number of messages
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
     * @param message         the message to use in the event of an assertion failure
     * @param broker          the EmbeddedActiveMQBroker to check
     * @param destinationName the name of the destination to check.
     * @param expected        the expected number of messages
     */
    public static void assertMessageCount(String message, EmbeddedActiveMQBroker broker, String destinationName, long expected) {
        Assert.assertEquals(message, expected, broker.getMessageCount(destinationName));
    }

    /**
     * Assert that the specified destination exists
     *
     * @param broker   the EmbeddedActiveMQBroker to check
     * @param expected the name of the destination to check for.
     */
    public static void assertContainsDestination(EmbeddedActiveMQBroker broker, String expected) {
        assertContainsDestination(
            String.format("Destination %s not found in broker %s", expected, broker.getBrokerName()),
            broker, expected);
    }

    /**
     * Assert that the specified destination exists
     *
     * @param message  the message to use in the event of an assertion failure
     * @param broker   the EmbeddedActiveMQBroker to check
     * @param expected the name of the destination to check for.
     */
    public static void assertContainsDestination(String message, EmbeddedActiveMQBroker broker, String expected) {
        Assert.assertTrue(message, (null != broker.getDestination(expected)));
    }

    /**
     * Assert that the specified destination does not exist
     *
     * @param broker   the EmbeddedActiveMQBroker to check
     * @param expected the name of the destination to check for.
     */
    public static void assertNotContainsDestination(EmbeddedActiveMQBroker broker, String expected) {
        assertNotContainsDestination(
            String.format("Destination %s found in broker %s", expected, broker.getBrokerName()),
            broker, expected);
    }

    /**
     * Assert that the specified destination does not exist
     *
     * @param message  the message to use in the event of an assertion failure
     * @param broker   the EmbeddedActiveMQBroker to check
     * @param expected the name of the destination to check for.
     */
    public static void assertNotContainsDestination(String message, EmbeddedActiveMQBroker broker, String expected) {
        Assert.assertFalse(message, (null != broker.getDestination(expected)));
    }

}
