package com.pronoia.junit.activemq;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Assert;

public class EmbeddedBrokerAssert {
    static final String ASSERT_CONTAINS_DESTINATION_FORMAT = "Destination %s not found in broker";
    static final String ASSERT_NOT_CONTAINS_DESTINATION_FORMAT = "Destination %s found in broker";

    protected EmbeddedBrokerAssert() {
    }

    /**
     * Assert that the specified destination has the expected message count
     */
    public static void assertMessageCount(String message, String destinationName, long expected, EmbeddedActiveMQBroker broker) {
        ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        Destination brokerDestination = getDestination(destination, broker);
        if (brokerDestination == null) {
            throw new RuntimeException(String.format("Destination % not found in broker %s", destination, broker.getBrokerName()));
        }

        Assert.assertEquals(String.format(message, destination.toString(), broker.getBrokerName()),
                expected, brokerDestination.getDestinationStatistics().getMessages().getCount());
    }

    public static void assertMessageCount(String destinationName, long expected, EmbeddedActiveMQBroker broker) {
        ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        assertMessageCount(String.format("Message Count for destination %s in broker %s did not match expected value",
                destination.toString(), broker.getBrokerName()), destinationName, expected, broker);
    }

    /**
     * Assert that the specified destination exists
     *
     * @throws Exception
     */
    public static void assertContainsDestination(String message, String destinationName, EmbeddedActiveMQBroker broker) {
        ActiveMQDestination expected = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        Assert.assertNotNull(message, getDestination(expected, broker));
    }

    public static void assertContainsDestination(String destinationName, EmbeddedActiveMQBroker broker) {
        ActiveMQDestination expected = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        assertContainsDestination(String.format(ASSERT_CONTAINS_DESTINATION_FORMAT, expected.toString(), broker.getBrokerName()),
                destinationName, broker);
    }

    public static void assertNotContainsDestination(String message, String destinationName, EmbeddedActiveMQBroker broker) {
        ActiveMQDestination expected = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        Assert.assertNull(message, getDestination(expected, broker));
    }

    public static void assertNotContainsDestination(String destinationName, EmbeddedActiveMQBroker broker) {
        ActiveMQDestination expected = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        assertNotContainsDestination(String.format(ASSERT_CONTAINS_DESTINATION_FORMAT, expected.toString(), broker.getBrokerName()),
                destinationName, broker);
    }

    static Destination getDestination(ActiveMQDestination destination, EmbeddedActiveMQBroker broker) {
        Destination brokerDestination = null;
        try {
            brokerDestination = broker.getBrokerService().getDestination(destination);
        } catch (RuntimeException runtimeEx) {
            throw runtimeEx;
        } catch (Exception unexpectedEx) {
            throw new RuntimeException(String.format("Unexpected exception encountered retrieving destination %s from BrokerService %s", destination.toString(), broker.getBrokerName() ), unexpectedEx);
        }

        return brokerDestination;
    }
}
