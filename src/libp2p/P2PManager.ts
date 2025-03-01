import { createLibp2p } from 'libp2p'
import { FaultTolerance, type Libp2p, type PubSub } from '@libp2p/interface'
import { createDelegatedRoutingV1HttpApiClient, DelegatedRoutingV1HttpApiClient } from '@helia/delegated-routing-v1-http-api-client'
import { identify } from '@libp2p/identify'
import { peerIdFromString } from '@libp2p/peer-id'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { Multiaddr } from '@multiformats/multiaddr'
import { sha256 } from 'multiformats/hashes/sha2'
import type { Connection, Message, SignedMessage, PeerId } from '@libp2p/interface'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { webSockets } from '@libp2p/websockets'
import { webTransport } from '@libp2p/webtransport'
import { webRTC, webRTCDirect } from '@libp2p/webrtc'
import { circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { ping } from '@libp2p/ping'
import { 
  BOOTSTRAP_PEER_IDS, 
  CHAT_TOPIC, 
  CHAT_FILE_TOPIC, 
  PUBSUB_PEER_DISCOVERY 
} from './constants'
import first from 'it-first'
import { Channel } from './Channel'
import { forComponent, enable } from './logger'
import type { Identify } from '@libp2p/identify'

// Define proper types for our libp2p instance with all required services
export type Libp2pType = Libp2p<{
  pubsub: PubSub
  identify: Identify
  delegatedRouting: DelegatedRoutingV1HttpApiClient
  ping: any
}>

const log = forComponent('p2p')

// Singleton instance tracking
let loaded = false

export class P2PManager {
  private node?: Libp2pType
  private _initialized = false
  private delegatedClient?: DelegatedRoutingV1HttpApiClient
  
  isInitialized() {
    return this._initialized
  }

  /**
   * Message IDs are used to deduplicate inbound messages.
   * Every agent in the network should use the same message ID function.
   */
  async msgIdFnStrictNoSign(msg: Message): Promise<Uint8Array> {
    const encoder = new TextEncoder()
    const signedMessage = msg as SignedMessage
    const encodedSeqNum = encoder.encode(signedMessage.sequenceNumber.toString())
    return await sha256.encode(encodedSeqNum)
  }

  /**
   * Resolves PeerIDs of bootstrap nodes to multiaddrs dialable from the browser
   */
  private async getRelayListenAddrs(client: DelegatedRoutingV1HttpApiClient): Promise<string[]> {
    const peers = await Promise.all(BOOTSTRAP_PEER_IDS.map((peerId) => 
      first(client.getPeers(peerIdFromString(peerId)))
    ))
  
    const relayListenAddrs: string[] = []
    for (const p of peers) {
      if (p && p.Addrs.length > 0) {
        for (const maddr of p.Addrs) {
          const protos = maddr.protoNames()
          // Note: narrowing to Secure WebSockets and IP4 addresses to avoid potential issues with ipv6
          if (protos.includes('tls') && protos.includes('ws')) {
            if (maddr.nodeAddress().address === '127.0.0.1') continue // skip loopback
            relayListenAddrs.push(this.getRelayListenAddr(maddr, p.ID))
          }
        }
      }
    }
    return relayListenAddrs
  }
  
  /**
   * Constructs a multiaddr string representing the circuit relay v2 listen address
   */
  private getRelayListenAddr(maddr: Multiaddr, peer: PeerId): string {
    return `${maddr.toString()}/p2p/${peer.toString()}/p2p-circuit`
  }

  /**
   * Dials one multiaddr at a time to avoid establishing multiple connections to the same peer
   */
  private async dialWebRTCMaddrs(multiaddrs: Multiaddr[]): Promise<void> {
    if (!this.node) return

    const webRTCMadrs = multiaddrs.filter((maddr) => maddr.protoNames().includes('webrtc'))
    log('Dialling WebRTC multiaddrs: %o', webRTCMadrs)

    for (const addr of webRTCMadrs) {
      try {
        log('Attempting to dial webrtc multiaddr: %o', addr)
        await this.node.dial(addr)
        return // if we succeed dialing the peer, no need to try another address
      } catch (error) {
        log.error('Failed to dial webrtc multiaddr: %o', addr)
      }
    }
  }
  
  /**
   * Connect to a specific multiaddr
   */
  async connectToMultiaddr(multiaddr: Multiaddr): Promise<Connection | undefined> {
    if (!this.node) {
      throw new Error('P2P not initialized. Call initialize() first.')
    }

    console.log(`Dialling:`, multiaddr)
    try {
      const conn = await this.node.dial(multiaddr)
      console.log('Connected to', conn.remotePeer, 'on', conn.remoteAddr)
      return conn
    } catch (e) {
      console.error('Connection error:', e)
      throw e
    }
  }

  /**
   * Initialize the P2P network with all transports and services
   */
  async initialize() {
    if (loaded) {
      log('P2P already initialized, reusing existing instance')
      return true
    }

    // Enable verbose logging
    enable('ui*,libp2p*,-libp2p:connection-manager*,-*:trace')
    
    log('Starting P2P network initialization...')
    try {
      // Set up the delegated routing client for peer discovery
      log('Setting up delegated routing client...')
      this.delegatedClient = createDelegatedRoutingV1HttpApiClient('https://delegated-ipfs.dev')
  
      // Get relay listen addresses for better connectivity
      log('Fetching relay listen addresses...')
      const relayListenAddrs = await this.getRelayListenAddrs(this.delegatedClient)
      log('Starting with relay addresses: %o', relayListenAddrs)

      log('Creating libp2p node...')
      this.node = await createLibp2p({
        addresses: {
          listen: [
            // Listen for webRTC connections
            '/webrtc',
            ...relayListenAddrs,
          ],
        },
        transports: [
          webTransport(),
          webSockets(),
          webRTC(),
          webRTCDirect(),
          circuitRelayTransport(),
        ],
        connectionEncrypters: [noise()],
        streamMuxers: [yamux()],
        connectionGater: {
          denyDialMultiaddr: async () => false,
        },
        transportManager: {
          // Don't fail if some transports fail to listen
          faultTolerance: FaultTolerance.NO_FATAL
        },
        peerDiscovery: [
          pubsubPeerDiscovery({
            interval: 10_000,
            topics: [PUBSUB_PEER_DISCOVERY],
            listenOnly: false,
          }),
        ],
        services: {
          pubsub: gossipsub({
            allowPublishToZeroTopicPeers: true,
            msgIdFn: (msg) => this.msgIdFnStrictNoSign(msg),
            ignoreDuplicatePublishError: true,
            emitSelf: true // Allow self-messaging for testing
          }),
          // Delegated routing helps discover ephemeral multiaddrs of bootstrap peers
          delegatedRouting: () => this.delegatedClient!,
          identify: identify(),
          ping: ping(),
        }
      }) as Libp2pType
      log('Libp2p node created successfully');
  
      if (!this.node) {
        throw new Error('Failed to create libp2p node')
      }

      // Expose globally for debugging like the reference implementation
      if (typeof window !== 'undefined') {
        (window as any).libp2p = this.node;
      }
  
      log('Subscribing to chat topics...');
      await this.node.services.pubsub.subscribe(CHAT_TOPIC)
      await this.node.services.pubsub.subscribe(CHAT_FILE_TOPIC)
      log('Subscribed to chat topics');
  
      // Track when our multiaddrs change
      this.node.addEventListener('self:peer:update', ({ detail: { peer } }: any) => {
        const multiaddrs = peer.addresses.map(({ multiaddr }: any) => multiaddr)
        log('Changed multiaddrs: peer %s multiaddrs: %s', peer.id.toString(), multiaddrs)
      })
  
      // Enhanced peer discovery logging
      this.node.addEventListener('peer:discovery', (event: any) => {
        const { multiaddrs, id } = event.detail
        log('Discovered peer %s', id)
        log('Peer multiaddrs: %o', multiaddrs.map((m: any) => m.toString()))
  
        if ((this.node?.getConnections(id) ?? []).length > 0) {
          log('Already connected to peer %s. Will not try dialling', id)
          return
        }
  
        log('Attempting to connect to discovered peer %s', id)
        this.dialWebRTCMaddrs(multiaddrs)
      })
  
      // Enhanced connection logging
      this.node.addEventListener('connection:open', (event: any) => {
        const conn = event.detail;
        log('New connection opened with peer %s', conn.remotePeer.toString())
        log('Connection details: %o', {
          remotePeer: conn.remotePeer.toString(),
          remoteAddr: conn.remoteAddr.toString(),
          direction: conn.direction,
          multiplexer: conn.multiplexer,
          encryption: conn.encryption
        })
      })

      this.node.addEventListener('connection:close', (event: any) => {
        const conn = event.detail;
        log('Connection closed with peer %s', conn.remotePeer.toString())
        log('Checking connectivity...')
        this.checkConnectivity()
      })

      log('Starting libp2p node...');
      await this.node.start()
      log('Libp2p node started');
      
      this._initialized = true
      loaded = true
      
      // Log initial connection state
      const initialConnections = this.getConnectionInfo()
      log('Initial connections: %o', initialConnections)
      
      log('P2P network initialization completed successfully')
      return true
    } catch (error) {
      log.error('Failed to initialize P2P network:', error)
      this._initialized = false
      loaded = false
      throw error
    }
  }

  /**
   * Check connectivity and attempt to reconnect if necessary
   */
  private async checkConnectivity() {
    if (!this.node || !this._initialized) return
    
    const connections = this.node.getConnections()
    if (connections.length === 0) {
      log('No connections, attempting to reconnect...')
      try {
        // Restart the node to re-establish connections
        await this.node.start()
        log('Reconnected successfully')
      } catch (error) {
        log.error('Failed to reconnect', error)
      }
    }
  }

  /**
   * Create a channel for pubsub communication
   */
  createChannel(schema: string, name: string): Channel {
    if (!this.node) {
      throw new Error('P2P not initialized. Call initialize() first.')
    }
    return new Channel(this.node, `skibidi.${schema}.${name}`)
  }

  /**
   * Publish a message to a specific topic
   */
  async publish(schema: string, table: string, type: 'INSERT' | 'UPDATE' | 'DELETE', payload: any) {
    if (!this.node) {
      throw new Error('P2P not initialized. Call initialize() first.')
    }
    
    const topic = `skibidi.${schema}.${table}`
    const message = {
      type,
      payload
    }
    
    const maxRetries = 3
    let attempts = 0
    
    while (attempts < maxRetries) {
      try {
        await this.node.services.pubsub.publish(topic, new TextEncoder().encode(JSON.stringify(message)))
        return
      } catch (error) {
        attempts++
        log.error(`Failed to publish message (attempt ${attempts}/${maxRetries}):`, error)
        
        if (attempts >= maxRetries) {
          throw new Error('Failed to publish message after max retries')
        }
        
        // Wait before retrying
        await new Promise(resolve => setTimeout(resolve, 1000))
      }
    }
  }
  
  /**
   * Get information about current connections
   */
  getConnectionInfo() {
    if (!this.node) {
      throw new Error('P2P not initialized. Call initialize() first.')
    }
    
    const connections = this.node.getConnections()
    return connections.map((conn) => ({
      peerId: conn.remotePeer.toString(),
      protocols: [...new Set(conn.remoteAddr.protoNames())],
      address: conn.remoteAddr.toString()
    }))
  }

  /**
   * Shutdown the P2P network gracefully
   */
  async stop() {
    if (this.node && this._initialized) {
      await this.node.stop()
      this._initialized = false
      loaded = false
      log('P2P network stopped')
    }
  }
}