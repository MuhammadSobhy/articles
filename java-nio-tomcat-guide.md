# Deep Dive: Java NIO and Apache Tomcat Internal Request Architecture

## Introduction
In high-performance Java enterprise applications, I/O efficiency is the defining bottleneck for scalability. Traditionally, Java's networking relied on synchronous, blocking APIs that forced a "one thread per client connection" model. This approach quickly ran into structural limitations under massive traffic loads. 

This comprehensive technical guide explores how **Java NIO (New I/O)** solves these bottlenecks using non-blocking, multiplexed mechanics, and how **Apache Tomcat** utilizes this foundation internally to manage thousands of concurrent connections. Finally, we will examine how modern developments like **Spring WebFlux (Reactive Stack)** and **JDK 21+ Virtual Threads (Project Loom)** evolve these scaling paradigms.

---

## Agenda
1. **Foundations of Java Non-Blocking I/O (NIO)**
   - Traditional Blocking I/O vs. Java NIO
   - The Three Pillars of Java NIO
   - The Java NIO Selector Lifecycle & Interest Operations
   - Selection Keys and the Event Loop
   - Trade-offs & Modern Alternatives
2. **Tomcat's Internal Network Architecture**
   - The Coyote Connector Layer
   - The Core Triad: Acceptor, Poller, and Worker Pool
3. **The Lifecycle of an HTTP Request in Tomcat**
   - Step-by-Step Execution Journey
   - Crucial Performance Tuning: `maxConnections` vs. `maxThreads`
4. **Under the Hood: Native OS Kernel Integration**
   - Symmetrical Sockets: Inbound (RMEM) & Outbound (WMEM) Buffers
   - The Silent Actor: Background OS Ingestion
   - The Poller Wakeup Mechanism (`epoll`, `kqueue`, `IOCP`)
5. **Decoupling Acceptor and Poller: The Poller Event Queue**
   - Solving the Selector Blocking Dilemma
   - The Concurrency Blueprint: Queue Hand-off & `selector.wakeup()`
6. **Concurrency Over a Single TCP Connection**
   - HTTP/1.1 Sequential Model & Head-of-Line (HoL) Blocking
   - HTTP/2 Multiplexed Model & Stream De-multiplexing
   - Socket Locking and Thread-Safety Guardrails
7. **Scaling the Execution Layer: MVC, WebFlux, and Virtual Threads**
   - The Limitation of Tomcat's Blocking Servlets
   - Netty and Spring WebFlux (Reactive Multiplexing)
   - JDK 21+ Virtual Threads (Project Loom)
   - The Synchronization Gotcha: Thread Pinning
8. **Summary Matrix & Architectural Comparison**

---

## 1. Foundations of Java Non-Blocking I/O (NIO)

### Traditional Blocking I/O vs. Java NIO
Traditional Java I/O (`java.io`) is **stream-oriented and blocking**. It reads or writes data one byte or character at a time. If a socket has no incoming data, the reading thread blocks, freezing its execution until data arrives. This forces architectures into a rigid **"one thread per client connection"** model. Under heavy load, this model consumes significant system memory and induces high Operating System (OS) context-switching overhead as the CPU jumps between thousands of blocked and active threads.

**Java NIO (New I/O)**, introduced in JDK 1.4, is **buffer-oriented and non-blocking**. Instead of single bytes, NIO tracks blocks of data. When an execution thread requests data from a source, it receives whatever is currently available right away without waiting. If no data is present, the thread is not blocked; it is immediately released to perform other tasks. This allows a single thread to manage thousands of active network connections simultaneously.

### The Three Pillars of Java NIO
NIO's foundational architecture depends on three core components [2]:
1. **Channels:** Gateways to physical entities like sockets, files, or hardware devices [2]. Unlike one-way Streams, Channels are bidirectional, meaning they can both read and write [2]. Common socket-based implementations include `SocketChannel` and `ServerSocketChannel` [2].
2. **Buffers:** Dedicated blocks of memory used to temporarily hold data [2]. In NIO, you never read directly from or write directly to a channel; instead, you read from a channel into a buffer, and write from a buffer into a channel [2]. The most common buffer is `ByteBuffer` [2].
3. **Selectors:** An I/O multiplexer that enables a single thread to monitor multiple selectable channels for active traffic events [2].

### The Java NIO Selector Lifecycle & Interest Operations
The Selector acts like an air traffic controller [3]. Instead of spawning thousands of threads to wait on individual client connections, all connections are registered with a single Selector object, letting a single thread query the Selector for events [3].

```
+-----+------------+                 +-----+------------+
|  SocketChannel   |                 |  SocketChannel   |
| (Client Conn 1)  |                 | (Client Conn 2)  |
+---------┬--------+                 +---------┬--------+
          │                                    │
          └─────────────────┬──────────────────┘
                            ▼
                  ┌──────────────────┐
                  │  Java Selector   │◀─── Registered for Events
                  └──────────────────┘
```

To use a Selector, an application steps through a sequential registration and polling loop [4]:
1. **Open the Selector:** Instantiated via a static factory method:
   ```java
   Selector selector = Selector.open();
   ```
2. **Configure Non-Blocking Mode:** A channel must be explicitly switched out of blocking mode before registration. Note that `FileChannel`s cannot use selectors because they cannot be made non-blocking [4].
   ```java
   channel.configureBlocking(false);
   ```
3. **Register the Channel:** Bind the channel to the selector while identifying its **Interest Set** (the exact events the selector should watch for) [4, 5]:
   ```java
   SelectionKey key = channel.register(selector, SelectionKey.OP_READ);
   ```

#### The Four Interest Operations:
- `SelectionKey.OP_ACCEPT`: The server socket channel has a new client connection attempt ready to be accepted [5].
- `SelectionKey.OP_CONNECT`: A client socket successfully finalized its connection to a remote server [5].
- `SelectionKey.OP_READ`: The channel has data waiting in its low-level kernel buffer, ready to be read into application memory [5].
- `SelectionKey.OP_WRITE`: The channel's internal network buffers have space available to accept outgoing payloads [5].

### Selection Keys and the Event Loop
The `register()` method yields a `SelectionKey` token that encapsulates the precise relationship between a specific channel and the selector [6]. The server runs inside an **Event Loop** structured around the `select()` call:
1. `selector.select()`: Blocks the thread only until at least one registered event occurs on any monitored channel [6].
2. `selector.selectedKeys()`: Returns a set of `SelectionKey` objects representing the channels ready for processing [6].
3. **Iteration & Reset:** The thread loops through the active keys, determines what action is required (`isAcceptable()`, `isReadable()`), handles the corresponding I/O, and removes the processed key from the selected set to prevent it from being processed twice [6].

### Blueprint for a Single-Threaded Server
Below is the structural skeleton of a single-threaded server using a Selector [7]:
```java
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

public class NioEchoServer {
    // The selector coordinates incoming connections and read events.
}
```

---

## 2. Tomcat's Internal Network Architecture

Historically, Apache Tomcat used a BIO (Blocking I/O) connector, which adhered to the "one thread per connection" model [10]. Modern versions of Tomcat use the **NIO Connector** (`org.apache.coyote.http11.Http11NioProtocol`) as the default engine to manage massive traffic loads efficiently [10].

Tomcat combines a Java NIO Selector network layer with a traditional Thread Pool worker layer, preventing a single thread from handling everything [11]. When an HTTP request hits Tomcat, it flows through a specialized group of internal components known as **Coyote** (Tomcat’s connector framework) [11].

```
[ Client Sockets ]
┌────────────────────────────────────────────────────────┐
│ COYOTE CONNECTOR (Http11NioProtocol)                   │
│                                                        │
│  ┌──────────────────┐                                  │
│  │   Acceptor       │  (Listens for new connections)   │
│  └────────┬─────────┘                                  │
│           │ passes SocketChannel                       │
│           ▼                                            │
│  ┌──────────────────┐                                  │
│  │    Poller        │◀─── [ Java NIO Selector ]        │
│  └────────┬─────────┘                                  │
│           │ event triggers (OP_READ)                   │
│           ▼                                            │
│  ┌──────────────────┐                                  │
│  │   Worker Pool    │  (Tomcat Executor Threads)       │
│  └────────┬─────────┘                                  │
└───────────┼────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────┐
│ CATALINA CONTAINER                                     │
│  Engine ── Host ── Context ── Wrapper (Your Servlet)   │
└────────────────────────────────────────────────────────┘
```

### The Core Triad of Components:
1. **The Acceptor (The Gatekeeper):** A dedicated thread running a standard blocking loop around `serverSocketChannel.accept()`. Its sole job is to accept new incoming TCP handshakes from clients. Once caught, it configures the client's `SocketChannel` to non-blocking mode (`configureBlocking(false)`), wraps it in an internal `NioChannel` metadata object, and hands it directly to the Poller [14].
2. **The Poller (The Air Traffic Controller):** A thread that owns and manages the Java NIO Selector instance. Tomcat usually spawns 1 to 2 Poller threads by default [15]. It registers the newly arrived `SocketChannel` onto its Selector for `OP_READ` events and runs the `selector.select()` event loop. Persistent Keep-Alive connections sit quietly inside this Selector without wasting any worker threads. The moment the client transmits an HTTP payload, the Selector wakes up and marks that channel's SelectionKey as readable (`isReadable()`) [15].
3. **The Worker Pool (The Heavy Lifters):** A dedicated thread pool controlled by Tomcat (prefixed as `http-nio-8080-exec-*`) [16]. When the Poller thread spots an active `OP_READ` event, it extracts the `SocketChannel` and assigns it to an idle Worker Thread from the pool to avoid freezing the Poller's Selector loop [16].

---

## 3. The Lifecycle of an HTTP Request in Tomcat

To understand how Java NIO translates into a web application framework, we can follow a single HTTP request from start to finish [16]:

1. **Connection:** The client clicks a link. The Acceptor thread picks up the TCP handshake and registers the channel to the Poller's Selector [17].
2. **Waiting (Keep-Alive):** Before sending data, the connection consumes virtually zero bytes of thread memory because it is stored purely as an entry inside the Poller's Selector registry [17].
3. **Data Arrival:** The browser sends the HTTP payload (e.g., `GET /api/users`). The OS triggers the Selector to wake up the Poller thread [17].
4. **Handoff:** The Poller packages the channel and assigns it to an idle Worker Thread from the pool [17].
5. **Parsing (Coyote):** The Worker thread allocates a Java NIO `ByteBuffer`, reads the raw bytes out of the channel, and passes them to the Coyote engine. Coyote parses the raw text into standard `HttpServletRequest` and `HttpServletResponse` objects [17].
6. **Container Routing (Catalina):** The Worker thread carries these request and response objects down the Catalina container hierarchy [17]:
   - **Engine:** The top-level container [17].
   - **Host:** Selects the virtual host (e.g., `localhost`) [17].
   - **Context:** Pinpoints the specific web application deployment (e.g., `/my-api`) [17].
   - **Wrapper:** Locates the specific Servlet instance mapping to the target URL [17].
7. **Execution:** The Worker thread runs the application code via `servlet.service(request, response)`. In Spring Boot applications, this is where `@RestController` code is executed [17].
8. **Response & Return:** The application code writes data to the response stream. The Worker thread flushes the bytes back down into the NIO channel, resets the channel configuration, and returns itself to the thread pool. The channel goes back into the Poller's Selector to wait for the next request [17].

### Performance Tuning: maxConnections vs. maxThreads
Understanding Tomcat's internals clarifies the difference between two critical configuration parameters [18]:
- **`maxConnections` (Default ~8,192+):** The absolute maximum number of concurrent TCP connections Tomcat can keep open at once. Because of the Poller's Selector, thousands of idle browsers can stay connected simultaneously without crashing the server [18].
- **`maxThreads` (Default ~200):** The maximum number of Worker Threads allocated for executing actual backend code simultaneously [18].

This separation prevents resource starvation: even if there are 5,000 concurrent connected users, Tomcat only needs enough execution threads (`maxThreads`) to handle the subset of users actively clicking buttons and sending payloads at that exact millisecond [18].

---

## 4. Under the Hood: Native OS Kernel Integration

Neither the Acceptor thread, the Poller thread, nor the application code moves raw data from the network card into the Java channel. The OS Kernel handles this entirely in the background [41].

### Symmetrical Sockets: Inbound (RMEM) & Outbound (WMEM) Buffers
At the OS level, every TCP socket contains two completely separate, independent, one-way memory highways [59, 60]:
- **Socket Receive Buffer (RMEM):** Inbound traffic only (Client → Server) [60].
- **Socket Send Buffer (WMEM):** Outbound traffic only (Server → Client) [60].

These two buffers operate independently. Inbound data flows do not block, overwrite, or collide with outbound data writes [60].

### The Silent Actor: Background OS Ingestion
1. **The Client Sends Data:** The client transmits TCP packets across the network [44].
2. **The Hardware Handles Arrival:** The server's physical Network Interface Card (NIC) receives the signal [44].
3. **The OS Kernel Takes Over:** The NIC fires a hardware interrupt to the CPU. The OS Kernel intercepts these packets, verifies the TCP sequence, strips network headers, and copies the raw bytes into the socket's **Socket Receive Buffer (RMEM)** [44].
4. **The Channel "Fills":** In Java NIO, a `SocketChannel` is not a physical bucket of data but a programmatic pointer (file descriptor) to this OS kernel buffer. As the OS kernel fills the RMEM, the `SocketChannel` is effectively "filled" and marked ready for reading [44].

During this background process, Java is completely uninvolved. The Acceptor thread is busy waiting for new clients, and the Poller thread is asleep [44].

### How the Poller Wakes Up
Once the OS Kernel successfully writes data into the RMEM, it updates the state of that connection's file descriptor in the OS native multiplexer (such as **`epoll`** on Linux, **`kqueue`** on macOS, or **`IOCP`** on Windows) [45].
- The OS flags the socket descriptor as "Ready for Reading" [45].
- The Tomcat Poller thread, sitting blocked inside `selector.select()`, is shaken awake by the OS Kernel [45].
- The Poller thread detects the `OP_READ` interest flag, extracts the `SocketChannel`, and hands it to a Worker Thread [45].
- The Worker thread executes `channel.read(byteBuffer)`. This triggers a native system call (`sys_read`), streaming the bytes out of the kernel's RMEM directly into Java's application buffer (typically optimized via **Zero-Copy mechanics** using **Direct ByteBuffers**) [45, 46].

---

## 5. Decoupling Acceptor and Poller: The Poller Event Queue

How can the Poller thread accept new incoming channels from the Acceptor if it is currently blocked inside a `selector.select()` system call? 

To solve this classic concurrency problem, Tomcat decouples the hand-off using a thread-safe internal queue combined with an explicit interrupt mechanism (`selector.wakeup()`) [47].

### The Poller Event Queue
The Poller thread owns a concurrent, thread-safe queue of pending tasks called the **Poller Event Queue** (implemented as `SynchronizedQueue` or `ConcurrentLinkedQueue`) [48].

### The Step-by-Step Hand-off Flow
1. **The Poller is Asleep:** The Poller loop calls `selector.select()`, delegating down to the native OS kernel (`epoll_wait` on Linux) and putting the Poller thread into a blocked sleep state [49].
2. **The Acceptor Catches a Connection:** A new client connects. The Acceptor thread wakes up from `server.accept()` and wraps the connection in a Tomcat metadata object (`PollerEvent` or `NioSocketWrapper`) [50].
3. **The Acceptor Bypasses the Selector:** Rather than interacting directly with the Selector (which would block or throw exceptions), the Acceptor quietly pushes the connection object onto the Poller's Event Queue. At this point, the Selector does not yet know the connection exists [50].
4. **The Alarm Clock (`selector.wakeup()`):** The Acceptor thread immediately executes:
   ```java
   poller.getSelector().wakeup();
   ```
   `wakeup()` is a highly optimized, thread-safe method. Under the hood, the OS kernel opens a tiny internal loopback pipe or socket. Executing `wakeup()` writes a single byte to this loopback pipe [51].
5. **The Poller Drains the Queue:** The byte written to the loopback pipe instantly unblocks the Poller thread, causing `selector.select()` to exit [51]. Before the Poller loops back to check client network traffic, it runs a maintenance step to empty its Event Queue [52]:
   ```java
   // Simplified view of Tomcat's Poller queue maintenance
   while (true) {
       // Pulls SocketChannel from queue, registers it for OP_READ, and clears event
   }
   ```
   The Poller registers the new `SocketChannel` onto the Selector for `OP_READ` traffic, then invokes `selector.select()` again, now monitoring both existing clients and the new client [52].

---

## 6. Concurrency Over a Single TCP Connection

Whether a single TCP connection (`SocketChannel`) can process multiple concurrent requests simultaneously depends on the protocol version [53].

### Scenario A: HTTP/1.1 (The Sequential Model)
HTTP/1.1 connections support Keep-Alive, allowing the TCP connection to stay open. However, it enforces strict **Head-of-Line (HoL) Blocking** at the protocol level, requiring sequential requests and responses [55].

```
[Client] ───(Request 1)───► [SocketChannel] ──► [Worker Thread A processes...]
[Client] ───(Request 2)───► [BLOCKED IN OS BUFFER]     ▼
```

1. **Request 1 Processed:** The Poller detects Request 1 and assigns the `SocketChannel` to Worker Thread A [55].
2. **OP_READ Disabled:** While Worker Thread A processes the request, Tomcat temporarily **removes the `OP_READ` interest flag** for this specific channel from the Poller's Selector [55].
3. **Request 2 Queued:** If the client browser streams Request 2 on the same connection, the raw bytes arrive and sit in the OS RMEM. The Poller ignores them because the Selector is not currently listening to this channel [55].
4. **Response Sent & OP_READ Restored:** Worker Thread A writes Response 1. Once completed, Tomcat tells the Poller to re-add the `OP_READ` flag to this channel [55].
5. **Request 2 Processed:** The Poller instantly wakes up (due to the bytes waiting in the RMEM) and hands the channel to Worker Thread B [55].
*Result:* Two worker threads will never process requests from the same `SocketChannel` concurrently under HTTP/1.1; execution is strictly serial [55].

### Scenario B: HTTP/2 (The Multiplexed Model)
If HTTP/2 is enabled (`org.apache.coyote.http2.Http2Protocol`), HTTP/2 **Streams** allow a single TCP connection (`SocketChannel`) to carry hundreds of interleaved data frames simultaneously [56].

```
[SocketChannel] ──┼─── Stream 1 (Bytes) ──► [Worker Thread A processes Request 1]
(Single TCP)     │
                 └─── Stream 3 (Bytes) ──► [Worker Thread B processes Request 2]
```

1. **Interleaved Arrival:** Client sends Request 1 and Request 2 chopped into frames over the same wire simultaneously [56].
2. **Poller Reads Frames:** The Poller wakes up, reads raw multiplexed frames, and hands them to Tomcat's HTTP/2 Stream Handler Engine [56].
3. **De-multiplexing:** Tomcat parses the stream IDs embedded in the frames, recognizing that Stream 1 is Request 1 and Stream 3 is Request 2 [56].
4. **Concurrent Allocation:** Tomcat splits these streams into separate logical sub-requests and schedules them into the worker pool. Worker Thread A processes Request 1, while Worker Thread B concurrently processes Request 2 [56].
5. **Merged Response:** As the threads finish, responses are chopped into frames, merged back into the single `SocketChannel`, and sent back to the client [56].

### Socket Locking Guardrail
To prevent data corruption from threads writing concurrently to the same connection, Tomcat uses internal **write locks** (`ReentrantLock` or `synchronized` monitors) on the `NioChannel` object [57]. While execution runs completely concurrently, the final physical writing phase is tightly synchronized so that thread outputs do not corrupt each other [57].

---

## 7. Scaling the Execution Layer: MVC, WebFlux, and Virtual Threads

### The Limitation of Tomcat's Blocking Servlets
Even though Tomcat uses Java NIO to fetch bytes off the wire in a non-blocking manner, the moment it hands the request to a Worker Thread, it switches to a traditional synchronous model [21, 22].

If servlet code performs a slow blocking operation (e.g., a database query or external API call), that specific Worker Thread is completely frozen [22, 26]:
```
[Tomcat Worker Thread] ──► [Sends DB Query] ──► [BLOCKED / IDLE FOR 500ms] ──► [Processes Response]
```
If `maxThreads` is 200, and 200 users trigger a slow query simultaneously, Tomcat runs out of worker threads, forcing subsequent users to wait in a queue even if the server's CPU usage is under 5% [22, 27]. To scale Spring MVC, you must increase thread count, wasting memory (each platform thread allocates ~1MB for its stack frame) [27].

### Netty and Spring WebFlux (Reactive Multiplexing)
Spring WebFlux completely departs from Tomcat's architecture, defaulting to **Netty** (or Eclipse Jetty) [27]. Instead of hundreds of worker threads, Netty uses a tiny, fixed number of event loop threads (matching CPU cores) [28].

```
[Event Loop Thread] ──► [Triggers DB Query] ──► [Registers Callback & Moves On]
                                                        │
[Event Loop Thread] ◀─── [DB Event Fires] ◀─────────────┘
```

1. **Reactive Triggers:** Code triggers database queries or API calls using a reactive driver (e.g., R2DBC or WebClient) [28].
2. **Immediate Release:** Instead of waiting, the Event Loop thread registers a callback and immediately moves on to handle requests for other users [28].
3. **Callback Execution:** When the database returns data, it triggers an event. An Event Loop thread captures this event, executes the callback, and returns the response [28].
*Result:* A WebFlux application with only a few threads can process tens of thousands of concurrent active requests that would otherwise crash a standard Tomcat server [29].

### JDK 21+ Virtual Threads (Project Loom)
Introduced in Java 21, **Virtual Threads** decouple Java threads from OS threads, switching from a 1:1 mapping to an **M:N mapping** where millions of lightweight virtual threads run on top of a few physical OS "Carrier Threads" [32, 34].

```
[Virtual Thread 1] (User A - Waiting on DB) ──┐
[Virtual Thread 2] (User B - Processing)     ──┼──► [Carrier Thread 1] (OS Thread)
[Virtual Thread 3] (User C - Waiting on API) ──┘
```

When a Virtual Thread hits a blocking operation:
1. **JVM Interception:** The JVM automatically intercepts the blocking call [35].
2. **Unmounting:** The JVM detaches ("unmounts") the Virtual Thread from the physical OS Carrier Thread. The Virtual Thread's state is moved to the JVM's heap memory (consuming only a few hundred bytes instead of 1MB) [35].
3. **Carrier Thread Free:** The OS Carrier Thread is instantly free to run other Virtual Threads [35].
4. **Mounting:** When the database responds, the JVM schedules the Virtual Thread to mount onto any available Carrier Thread to finish execution [35].

#### What This Means for Tomcat:
- **"Infinite Threads":** When Virtual Threads are enabled in Tomcat, every incoming HTTP request is assigned its own dedicated Virtual Thread [36]. Tomcat can spawn 10,000 Virtual Threads for 10,000 slow database calls without crashing, using only a handful of physical OS threads [36].
- **Clean Imperative Code:** You can write standard, readable sequential code instead of complex reactive streams (Mono/Flux):
  ```java
  // Looks blocking, but is 100% non-blocking to the OS under Java 21!
  User user = database.fetchUser(id);
  Order order = api.getOrder(id);
  return new Dashboard(user, order);
  ```

### The Synchronization Gotcha: Thread Pinning
While Virtual Threads are highly effective, they suffer from **Thread Pinning** [38, 39]. 
If a Virtual Thread encounters a blocking operation while inside a **`synchronized` block or method**, it cannot unmount from the Carrier Thread [39]. It pins the Carrier Thread, blocking physical OS resources just like traditional platform threads [39].
- *Mitigation:* Modern frameworks and libraries have updated their internal code to replace `synchronized` locks with `java.util.concurrent.locks.ReentrantLock` [39]. However, old, unmaintained third-party libraries can still cause thread pinning under load [39].

---

## 8. Summary Matrix & Architectural Comparison

### Tomcat (Spring MVC) vs. Netty (Spring WebFlux)

| Feature | Tomcat + Spring MVC | Netty + Spring WebFlux |
| :--- | :--- | :--- |
| **Threading Model** | Thread-Per-Request Worker Pool (Large) [29] | Event Loop Multiplexing (Small/Fixed) [30] |
| **Typical Thread Count** | 200+ threads [30] | Matches CPU cores (e.g., 4, 8, 16) [30] |
| **Memory Footprint** | Higher (1MB per thread stack) [30] | Extremely Low [30] |
| **Where it blocks** | Blocks during DB calls, API calls, file I/O [30] | Never blocks anywhere in the stack [30] |
| **Best Used For** | CPU-heavy tasks, traditional relational DBs [30] | High-concurrency, streaming data, microservices [30] |

### Spring WebFlux vs. Java 21+ Virtual Threads

| Feature | Spring WebFlux (Reactive) | Spring MVC + Virtual Threads (Java 21+) |
| :--- | :--- | :--- |
| **Code Style** | Asynchronous / Reactive (Mono/Flux) [38] | Traditional / Imperative (Sequential) [38] |
| **Learning Curve** | High (Steep paradigm shift) [38] | Zero (Standard sequential Java) [38] |
| **Memory Footprint** | Extremely low [38] | Extremely low [38] |
| **Concurrency Limit** | Bound by CPU & I/O limits [38] | Bound by CPU & I/O limits [38] |
| **Debugging / Profiling** | Difficult (Fragmented stack traces) [38] | Easy (Standard step-by-step debugging) [38] |
