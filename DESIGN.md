# Smoke Network Architecture: HTTP over WebRTC

This document explains the architecture of the Smoke library, which implements HTTP and other networking protocols over WebRTC connections. The design allows for peer-to-peer connections that can serve web services without traditional server infrastructure.

## Architecture Overview

The Smoke library implements a layered network stack that builds HTTP functionality on top of WebRTC connections:

```
┌─────────────────────────────────┐
│           HTTP Module           │
├─────────────────────────────────┤
│            Net Module           │
├─────────────────────────────────┤
│          WebRTC Module          │
├─────────────────────────────────┤
│         Hub (Signaling)         │
└─────────────────────────────────┘
```

The system uses a modular design where each layer builds upon the capabilities of the layer below it:
- **Hub**: Provides signaling capabilities for WebRTC connection establishment
- **WebRTC**: Manages WebRTC peer connections and data channels
- **Net**: Provides socket abstractions on top of WebRTC data channels
- **HTTP**: Implements HTTP protocol over the Net sockets

## Peer Discovery and Connection Establishment

### 1. Signaling through Hubs

The system uses a "Hub" for signaling - the critical first step in establishing WebRTC connections. The library implements:

- **Private Hub**: Used for self-contained signaling
- **Public Hub**: Could be used for wider network connectivity

The Hub acts as a signaling channel to exchange connection metadata (SDP offers/answers and ICE candidates) between peers.

```javascript
// From network.mts
constructor(options: NetworkOptions = {}) {
  this.#hub = options.hub ?? new Hubs.Private()
  this.#webrtc = new WebRtc.WebRtcModule(this.#hub)
  // ...
}
```

### 2. WebRTC Connection Establishment

The WebRTC module handles creating and managing peer connections. The connection process:

1. Creates an RTCPeerConnection
2. Sets up event listeners
3. Uses the Hub for signaling to exchange:
   - Session descriptions (offers/answers)
   - ICE candidates

```javascript
// From webrtc.mts
async #resolvePeer(remoteAddress: string): Promise<WebRtcPeer> {
  if (this.#peers.has(remoteAddress)) return this.#peers.get(remoteAddress)!
  const configuration = await this.#hub.configuration()
  const localAddress = await this.#hub.address()
  const connection = new RTCPeerConnection(configuration)
  const peer: WebRtcPeer = { 
    connection, 
    datachannels: new Set<RTCDataChannel>(), 
    localAddress, 
    remoteAddress, 
    makingOffer: false, 
    ignoreOffer: true, 
    bytesSent: 0, 
    bytesReceived: 0 
  }
  this.#setupPeerEvents(peer)
  this.#peers.set(remoteAddress, peer)
  return peer
}
```

## Network Implementation

### Net Module: Socket Abstraction

The Net module creates a socket abstraction over WebRTC data channels, providing familiar networking primitives:

1. **NetListener**: Listens for incoming connections on a "port" (actually a WebRTC data channel with a specific label)
2. **NetSocket**: Represents a connection to a peer, allowing bidirectional communication

```javascript
// From net.mts
/** Establishes a connection to a remote Net listener */
public async connect(options: NetConnectOptions): Promise<NetSocket> {
  const [hostname, port] = [options.hostname ?? 'localhost', options.port]
  const [peer, datachannel] = await this.#webrtc.connect(hostname, port, { ordered: true, maxRetransmits: 16 })
  return new NetSocket(peer, datachannel)
}
```

### HTTP Implementation

The HTTP module builds on top of the Net module to provide HTTP functionality:

1. **HTTP Server**: Listens for incoming HTTP requests on a specific port
2. **HTTP Client**: Makes requests to HTTP servers using fetch-like API

## HTTP Server Flow

When serving HTTP over WebRTC:

1. Create a new HTTP listener on a specific port:
   ```javascript
   const httpListener = network.Http.listen({ port: 8080 }, async (request, info) => {
     // Handle the request and return a Response
     return new Response("Hello from WebRTC HTTP server!")
   });
   ```

2. Behind the scenes, the listener:
   - Creates a NetListener on the specified port
   - Waits for incoming connections
   - When a connection arrives, it reads HTTP request data from the socket
   - Processes the request as a standard Request object
   - Passes it to the user-provided handler
   - Sends the Response back over the socket

The `HttpListener` class in `listener.mts` handles incoming connections:

```javascript
async #onRequest(socket: Net.NetSocket) {
  const stream = new Stream.FrameDuplex(socket)
  const listenerRequestInit = await this.#readListenerRequestInit(stream)
  if (listenerRequestInit === null) return await stream.close()
  
  // Create standard Request object
  const url = new URL(`http://${socket.local.hostname}:${socket.local.port}${listenerRequestInit.url}`)
  const headers = new Headers(listenerRequestInit.headers)
  const body = await this.#createBodyFromRequestInit(listenerRequestInit, stream)
  const request = new Request(url, {
    method: listenerRequestInit.method,
    headers: headers,
    body,
    duplex: 'half',
  } as RequestInit)
  
  // Handle the request with user-provided callback
  const info = { local: socket.local, remote: socket.remote }
  const response = await this.#accept(request, info)
  
  // Send response back
  await stream.write(Signal.RESPONSE)
  await this.#sendResponse(response, stream)
}
```

## HTTP Client Flow

The fetch mechanism:

1. The client calls `network.Http.fetch(url, options)`
2. The HTTP module:
   - Uses the Net module to establish a connection to the target peer and port
   - Sends the HTTP request over the WebRTC data channel
   - Waits for and receives the response
   - Returns it as a standard Response object

The fetch implementation in `fetch.mts`:

```javascript
export async function fetch(net: Net.NetModule, endpoint: string, requestInit: RequestInit = {}) {
  const url = Url.parse(endpoint)
  const [hostname, port] = resolveHostnameAndPort(endpoint)
  
  // Connect to the remote peer
  const socket = await net.connect({ hostname, port })
  const duplex = new Stream.FrameDuplex(socket)
  
  // Send request
  await sendListenerRequestInit(duplex, url, requestInit)
  sendRequestBody(duplex, requestInit).catch((error) => console.error(error))
  
  // Read response
  const signal = await Async.timeout(readResponseSignal(duplex), 
    { timeout: 4000, error: new Error('A timeout occured reading the http response signal') })
  
  // Process response
  // ...
  
  // Create and return standard Response object
  return new Response(readable, responseInit)
}
```

## WebSocket Support

The system also supports WebSockets over the same WebRTC data channel infrastructure:

1. The server can upgrade HTTP requests to WebSocket connections
2. The client can establish WebSocket connections to the server

This uses the same underlying WebRTC data channels but with a different protocol format that maintains an open bidirectional stream.

## Data Flow and Framing

Data sent over WebRTC data channels is framed using a `FrameDuplex` abstraction, which:
1. Chunks data into manageable frames
2. Handles flow control
3. Ensures reliable delivery

## Loopback Capability

For local testing, the system includes a loopback implementation:

```javascript
#setupLocalhost(): void {
  if(this.#peers.has('loopback:1') || this.#peers.has('loopback:0')) {
    return
  }
  const connection0 = new RTCPeerConnection({})
  const connection1 = new RTCPeerConnection({})
  // Create peer connections for local testing
}
```

## Replacing Peer Discovery Mechanisms

To replace the peer discovery and rendezvous mechanisms with your own implementation:

1. **Create a custom Hub implementation**:
   - Implement the Hub interface from `src/hubs/hub.mts`
   - Provide your own signaling mechanism for peer discovery and connection negotiation
   - Your implementation needs to handle:
     - Address assignment for peers
     - Message passing between peers for WebRTC signaling
     - Tracking connected peers

2. **Use your custom hub when initializing the Network**:
   ```javascript
   const customHub = new MyCustomHub();
   const network = new Network({ hub: customHub });
   ```

3. **Key considerations for custom implementations**:
   - Provide unique addresses for each peer
   - Support reliable message passing for signaling
   - Handle peer discovery in your preferred way (centralized directory, DHT, etc.)
   - Consider authentication mechanisms for secure peer connections

## Conclusion

The Smoke library provides a full HTTP stack over WebRTC peer connections, enabling direct peer-to-peer web applications without traditional servers. The layered architecture makes it possible to use familiar HTTP and WebSocket APIs while leveraging WebRTC's NAT traversal capabilities and direct connections.

By providing abstraction layers from signaling through HTTP, the system hides much of the complexity of WebRTC setup and data channel management, making P2P web applications more accessible to developers.