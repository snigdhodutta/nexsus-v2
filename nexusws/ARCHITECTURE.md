## 1. Architecture & Data Flow

### Message Flow Diagram

```
┌─────────────┐      WebSocket       ┌─────────────┐      NATS JetStream     ┌─────────────┐      WebSocket       ┌─────────────┐
│   Client A  │ ◄──────────────────► │  WS Node 1  │ ◄─────────────────────► │    NATS     │ ◄─────────────────────► │  WS Node 2  │ ◄──────────────────► │   Client B  │
│             │   ws://nexus/ws      │ (Edge Pod)  │   nats://nats:4222      │   Server    │   nats://nats:4222      │ (Edge Pod)  │   ws://nexus/ws      │             │
└─────────────┘                      └─────────────┘                          └─────────────┘                         └─────────────┘                      └─────────────┘
       │                                    │                                        │                                        │                                    │
       │ 1. Send Event "chat.message"       │                                        │                                        │                                    │
       │───────────────────────────────────►│                                        │                                        │                                    │
       │                                    │ 2. Encode: [FrameType|Subject|CorrelationID|Payload]                          │                                    │
       │                                    │────────────────────────────────────────►│                                        │                                    │
       │                                    │                                        │ 3. Publish to subject "chat.message"   │                                    │
       │                                    │                                        │────────────────────────────────────────►│                                    │
       │                                    │                                        │                                        │ 4. Subscribe receives message      │
       │                                    │                                        │                                        │◄─────────────────────────────────│
       │                                    │ 5. Decode & Route to subscribed clients │                                        │ 5. Forward to WS connections       │
       │                                    │◄────────────────────────────────────────│                                        │─────────────────────────────────►│
       │ 6. Receive Event                   │                                        │                                        │                                    │ 6. Receive Event
       │◄───────────────────────────────────│                                        │                                        │                                    │───────────────────────────────────►
```

### Binary Wire Protocol

The framework uses a length-prefixed binary protocol over WebSocket frames to support multiplexing of different message types (`@action`, `@event`, `@producer`):

```
+----------------+------------------+-------------------+------------------+
|  FrameType (1) | SubjectLen (2)   | Subject (var)     | CorrelationID (8)|
+----------------+------------------+-------------------+------------------+
| PayloadLen (4) | Payload (var)    | ...               |                  |
+----------------+------------------+-------------------+------------------+

Field Descriptions:
- FrameType (uint8): 0x01=Event, 0x02=Action Request, 0x03=Action Response, 0x04=Producer Ack
- SubjectLen (uint16): Length of the subject string (big-endian)
- Subject ([]byte): The NATS subject / WS topic (e.g., "chat.message")
- CorrelationID (uint64): For RPC/Request-Reply patterns (0 for fire-and-forget events)
- PayloadLen (uint32): Length of the payload (big-endian)
- Payload ([]byte): Raw bytes (codec-agnostic)
```

This design allows:
- Zero-copy reads when using `github.com/coder/websocket`
- Multiplexing multiple logical streams over a single WS connection
- Efficient routing without JSON parsing overhead
