# distributed-snake
Building a distributed snake with event sourcing and kafka streams

## Prerequisites
* Docker
* Java 17+
* Gradle
## Goals

In this workshop, we will build the classic snake game with an event driven architecture using Kafka and the Kafka Streams API.

| **Goal**              | *Implement a functioning snake game in Kafka*                                                   |
| ----------------------------- |-------------------------------------------------------------------------------------------------|
| **What will you learn**       | *Event-driven architecture, basic of the Kafka eco-system, Kafka Streams API*                   |
| **What you'll need**          | *https://www.docker.com/products/docker-desktop/ <br /> https://adoptium.net/en-GB/temurin/releases/* |
| **Duration**                  | *1h 30m*                                                                                        |

## Building the project

Clone the repo and setup project and gradle JDK.
Run the gradle task generateAvroJava
gradle generateAvroJava

## Goal

In this workshop we will build a distributed snake using a client-server architecture and event sourcing.
The goal is to implement the core kafka processing logic in the server using the Kafka Streams DSL API and finally
connect a client to the server and play.

The client is provided as a .jar can be run with:
java -jar snakeclient.jar

## Structure

The application can be divided into two parts, kafka messaging logic and business logic. 
Today we are focused on the Kafka Streams API and that is what we will implement. This code can be found in 
the config package. Each processor in the topology is exposed as a bean within this package. These beans will
have some missing core logic, and it is your job to fix it using the DSL API and the classes in the processor package.

The business logic is contained within the processor package and is fully implemented. You will use this domain code
to complete your tasks for each processor.

## Milestone 1 (Branching)

In this segment, you will learn about branching in Kafka and implement the source processor of the app
which will receive and route user input.

Navigate to config/GameInputRouterConfig

Your job is to route input to two different topics. The game movement topic and the game administration topic.

## Milestone 2 (Joins and Filters)

After routing our input events to our movement topic we need to process the event to assert its validity based on some state.

By using joins we can decorate our event with necessary data and use it in filters to validate state.

Navigate to config/MovementProcessorConfig

Your job is to validate the movement event by joining the stream with different tables and filter the data.

## Putting it together

After finishing these two milestones, navigate to config/TickProcessorConfig and scroll down to
the tickProcessingTopology bean.

In this processor branching, joining and filtering is used in conjunction to handle game ticks and the
updates of the game. Read and try to understand the code and don't be afraid to ask questions.

In the end of this processor, the event are branched into two different topics, game status and
game table entries. Next up we will discuss game table entries, which holds the state of positions on the map.
Before moving on to the next step, take a look at the classes GameTableEntry and GameTableEntryType.

## Milestone 3 (Reduce and Aggregate)

So we now we have a topic which holds the current state of positions on the map. We can make use of this data
to hold current references to the snake head and tail positions. Which are the only parts of the snake that
needs to be updated in order for the snake to move.

In config/TickProcessorConfig navigate to the snakeHead bean. Here we will handle the snake head state.

Just below it is the snakeTail which handles the state of the snake tail.
Implement these.

## Milestone 4

If you have come this far, try to run the application and start the client.
Firstly we need to setup the environment with "docker-compose up -d" and add topics.
Let one of us know, and we will help you with that.

java -jar snakeclient.jar

## Milestone 5

Implement the logic for eating an apple. 
Randomly spawn apples which increases the size of the snake on collision.
