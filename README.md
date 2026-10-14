# Day 23 — DStreams Basics

## Objective
Learn Apache Spark Streaming using the DStreams API.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.9
- Spark Streaming
- Java 17
- SBT

## Features
- Creates a StreamingContext with a 5-second batch interval.
- Reads application logs through a TCP socket.
- Applies map, filter, and flatMap transformations.
- Filters ERROR messages, ignoring letter case.
- Counts ERROR messages in each micro-batch.

## How to Run

### 1. Compile the project

```bash
sbt compile
```

### 2. Start the socket server in Terminal 1

```bash
nc -lk 9999
```

### 3. Start Spark in Terminal 2

```bash
sbt "runMain Day23DStreamsBasics"
```

### 4. Send test messages in Terminal 1

```text
ERROR Database connection failed
ERROR Null pointer exception
INFO Application started
```

Spark prints the ERROR count for each five-second batch.

## Concepts Learned
- StreamingContext
- DStreams
- Socket text streams
- map, filter, flatMap
- Micro-batch processing
- Error counting per batch
