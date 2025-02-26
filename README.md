A brief description of the components of the system：
I have implemented all the functionalities of the Assignment, including the bonus part.
Deployment and Startup of My Project:
java -jar directory.jar <port>
java -jar broker.jar <port> <directory_ip> <directory_port>
java -jar publisher.jar <username> <directory_ip> <directory_port>
java -jar subscriber.jar <username> <directory_ip> <directory_port>
My project consists of five packages, and communication between
components is implemented using Java RMI:
1. broker: Contains BrokerImpl.java, BrokerServer.java, and Topic.java. It is
responsible for implementing the core functionality and logic of the message broker.
2. directory ： Contains DirectoryServer.java and DirectoryServiceImpl.java.
Responsible for registration and management of the directory service.
3. publisher：Contains PublisherClient.java. It implements the functions for creating,
publishing, showing, and deleting on publisher site.
4. subscriber ： Contains SubscriberClient.java and SubscriberImpl.java. It
implements the subscriber functions for listing, subscribing, showing, and
unsubscribing.
5. Remote ： remote interfaces and data structures, BrokerInterface.java,
DirectoryServiceInterface.java, BrokerInfo.java, and
SubscriberCallbackInterface.java.
broker package
BrokerImpl.java:
1. BrokerImpl.java is the core part of the project, implementing the BrokerInterface
and handling requests from publishers and subscribers, also synchronizing with
other brokers.
2. Main methods:
1. initializeConnections(): Initializes connections with other brokers and
synchronizes existing topic information.
2. registerNewBroker(): Registers and synchronizes topic information when a
new broker joins.
3. createTopic(): Creates a new topic and synchronizes it with other brokers.
4. publishMessage(): Publishes a message to the specified topic and
forwards it to other brokers.
5. subscribe(): Allows a subscriber to subscribe to a topic and synchronizes
the subscription information with other brokers.
6. 7. unsubscribe(): Cancels a subscription and synchronizes the cancellation.
listTopics(): Lists all available topics for subscription, including those on
other brokers.
8. deleteTopic(): Deletes a topic, notifies subscribers, and synchronizes the
deletion with other brokers.
9. handlePublisherCrash() and handleSubscriberCrash(): Handles crashes of
publishers and subscribers, cleans up resources, and synchronizes the
changes with other brokers.
10. publisherHeartbeat() and subscriberHeartbeat(): Receives heartbeat
signals from publishers and subscribers to check connection status.
11. startPublisherHeartbeatChecker() and startSubscriberHeartbeatChecker():
Starts heartbeat monitoring threads to regularly check heartbeat signals.
BrokerServer.java:
BrokerServer is the startup class for the broker. After obtaining the broker's port number
and the directory service's IP and port, it creates an instance of BrokerImpl and binds it to
the service name in the RMI registry. It also calls the initializeConnections() method to
establish connections with other brokers.
Topic.java: Responsible for managing the subscriber list and message publishing:
1. 2. 3. addSubscriber() and removeSubscriber(): Add or remove subscribers.
publishMessage(): Publish a message to all subscribers.
notifySubscribersOfTopicDeletion(): When a topic is deleted, iterate through all
subscribers of that topic and call each subscriber's notifySubscriber() to inform
them that the topic has been deleted.
directory package
DirectoryServiceImpl.java:
1. Implements the DirectoryServiceInterface. It uses a ConcurrentHashMap to store
all registered broker information. The registerAndGetBrokerList() method
registers the broker itself and returns the broker list. getBrokerList() is used to
retrieve all registered brokers.
DirectoryServer.java: The startup class for the directory service. After specifying a
custom port, it creates an instance of DirectoryServiceImpl and waits for brokers to
register.
publisher package
PublisherClient.java:
1. Implements the required functionality for the publisher client:
1. connectToBroker(): Connects to the directory service and randomly
selects a broker to connect to.
2. start(): Starts the client.
3. The publisher manages topics through createTopic() and deleteTopic()
methods, and ensures synchronization between all brokers using
synchronizeTopic() and synchronizeTopicDeletion() methods.
4. publishMessage(): Publishes a message. After verifying the publisher's
permissions, the broker forwards the message to all relevant subscribers
and other brokers using forwardMessage().
5. showSubscriberCount(): Calls the getLocalSubscriberCount() method to
get the local subscriber count, and uses RMI to call the same method on
other brokers to obtain the total number of subscribers across the
network.
6. startHeartbeat(): Sends a heartbeat signal to the broker every 3 seconds.
subscriber package
SubscriberClient.java:
1. Implements the required functionality for the subscriber client, with methods like
those of the publisher, using synchronization to ensure consistency:
1. connectToBroker(), startHeartbeat(), start()
2. listTopics(), subscribeTopic(), showCurrentSubscriptions(),
unsubscribeTopic()
SubscriberImpl.java:
1. Function overview: Implements the SubscriberCallbackInterface. When a broker
publishes a message or a topic is deleted, the broker calls notifySubscriber() to
inform the subscriber.
remote package
BrokerInterface.java: Used by publishers and subscribers, as well as for
synchronization calls between brokers.
DirectoryServiceInterface.java: Used by brokers to register themselves and retrieve the
broker list.
BrokerInfo.java: Defines the data structure for broker information, including the broker's
ID, IP address, and port.
SubscriberCallbackInterface.java: Implemented by subscribers, this interface is called
by brokers to push messages to subscribers.
3. A critical analysis of the work completed, including design choices
and their justifications.
Reason for Choosing Java RMI Instead of Socket:
1. Reduced Communication Protocol Design: In Assignment 1, I used sockets and
TCP, which required custom application-layer communication protocols between
the server and client. With Java RMI, remote calls hide the network communication
details at the application layer, greatly simplifying development. For example, the
publisher can directly call the broker's createTopic() method to create a topic.
2. Suitability for the Project Complexity: Given the complexity of Assignment 2 and
the true distributed nature of the system, Java RMI was a better choice compared
to sockets. Java RMI offers a more straightforward approach for implementing
distributed systems with less complexity.
3. No Thread Pool Management: In Assignment 1, I had to decide between a work
pool and a thread-per-connection model. With RMI, I did not need to spend effort
on managing this aspect of the architecture, as RMI abstracts the concurrency
handling.
4. Improved Reliability and Stability: In Assignment 1, I had to consider many
exceptional cases, such as data transmission errors and multithreaded
synchronization, and explicitly catch and handle different exceptions. Java RMI's
built-in RemoteException helps handle exceptions more effectively. Additionally,
the implementation of heartbeat mechanisms for detecting publishers and
subscribers is simplified: the broker only needs to update the corresponding client's
last heartbeat time, without handling detailed heartbeat packet parsing or state
management logic.
Broker Connection and Message Passing Method:
In my implementation, brokers establish connections via RMI. Each broker, upon
startup, performs the following steps:
1. Register with Directory Service: After startup, the broker registers its IP and port
information with the directory service.
2. Retrieve Other Broker List: It retrieves the list of other brokers currently registered
with the directory service.
3. Establish RMI Connections: For each known broker, it uses its IP and port to find
the remote object through the RMI Registry, saving the connection in the
otherBrokers map.
4. Random Connection by Publishers and Subscribers: Publishers and subscribers
randomly connect to one of the online brokers, which helps distribute the load and
prevents a single broker from being overwhelmed.
5. Subscriber Location Synchronization: When a subscriber subscribes to or
unsubscribes from a topic, the broker synchronizes this information with other
brokers. This way, each broker knows which brokers have subscribers interested
in a specific topic.
6. Selective Message Forwarding: When a publisher publishes a message, the broker
first sends the message to its local subscribers. Then, based on the subscriber
locations, the broker only forwards the message to brokers that have interested
subscribers. This avoids unnecessary message transmission and reduces network
load.
Handling Publisher and Subscriber Crashes: Heartbeat Mechanism:
1. Heartbeat Signal: After connecting to a broker, both publisher and subscriber
clients start a separate thread that sends a heartbeat signal to the broker every 3
seconds by calling the publisherHeartbeat() or subscriberHeartbeat() method.
2. Broker Heartbeat Monitoring: Internally, the broker starts a heartbeat detection
thread that periodically checks the heartbeat timestamps of publishers and
subscribers. If no heartbeat is received within a specified 6-second timeout, the
client is considered crashed.
o For a publisher crash, the system calls the handlePublisherCrash() method,
which deletes all topics created by that publisher, notifies subscribers, and
synchronizes the deletion with other brokers.
o Similarly, for a subscriber crash, the system calls the
handleSubscriberCrash() method, which removes the subscription,
updates the subscriber location mapping, and synchronizes this
information with other brokers.
3. Concurrency and Thread Safety Mechanisms: To ensure thread safety when
accessing and modifying shared data such as heartbeat timestamps and
subscription information in high-concurrency situations, synchronized blocks and
ConcurrentHashMap are used.
Future Improvements:
1. Data Storage and Persistence:
o According to project requirements, the user ID for publishers and
subscribers is unique, and no other users can perform actions on their
behalf. In the future, a login mechanism could be introduced using Firebase
to facilitate two-factor authentication for user registration and login. This
would enhance both account security and overall system security.
o Although data storage wasn't explicitly required in the current assignment,
storing data in Firebase, combined with a message re-sending mechanism,
could help prevent data loss during system crashes. Additionally, it would
allow subscribers and publishers to log in and out while being able to
access their historical records.
2. Improving Concurrency:
o In the current implementation, synchronized blocks are used extensively to
prevent deadlocks and because of the low level of concurrency. However,
using ReadWriteLock could enhance system concurrency, especially for a
publish-subscribe application where reads are more common than writes.
In most cases, the number of subscribers will be greater than the number
of publishers, making ReadWriteLock a better fit.
3. Reducing Code Duplication:
o Some code logic between the publisher and subscriber components is
repetitive. This can be improved by consolidating the common logic and
moving it into the remote package in the future.
4. Broker and Client Connection Mechanisms:
o Currently, the three brokers in the system are fully connected, meaning
each broker communicates directly with every other broker. If the number
of brokers increases, a more efficient network structure could be adopted.
o The current random connection approach used by subscribers and
publishers to connect to brokers could be improved. In the future, the
system could distribute connections more evenly by considering the user's
IP and the load on each broker.
5. Security Enhancements:
o Sensitive data transmitted over the network could be encrypted to prevent
interception or tampering, thereby enhancing the overall security of the
system.
