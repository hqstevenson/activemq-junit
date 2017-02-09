# activemq-junit

Provides a set of JUnit Rules to simplify testing with ActiveMQ

All required libraries have a provided scope in the project so that 
the activemq-junit Rules are independent of the ActiveMQ version - the JUnit rules will use
whatever ActiveMQ libraries are found on the classpath.

NOTE:  If XML configuration is used for embedded ActiveMQ brokers, ensure the version of 
spring-context is compatible with the version of ActiveMQ.