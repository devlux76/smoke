import { Hub, HubMessage, HubMessageCallback } from '../hubs/hub.mjs'
import * as Events from '../events/index.mjs'
import * as Crypto from '../crypto/index.mjs'
import { P2PManager } from './P2PManager'
import { PeerDiscovery } from './PeerDiscovery'

/**
 * Implements the Smoke Hub interface using libp2p as the transport layer
 * for peer discovery and messaging.
 */
export class Libp2pHub implements Hub {
  private p2pManager: P2PManager
  private events: Events.Events
  private address: string
  private signalChannel: string
  private initialized = false
  private peerDiscovery: PeerDiscovery

  /**
   * Creates a new libp2p-based Hub
   * @param p2pManager - Optional P2PManager instance. If not provided, a new one will be created.
   */
  constructor(p2pManager?: P2PManager) {
    // Generate a unique address for this hub
    this.address = Crypto.randomUUID()
    
    // Create or use the provided P2PManager
    this.p2pManager = p2pManager || new P2PManager()
    
    // Event system for handling incoming messages
    this.events = new Events.Events()
    
    // Channel name for signaling messages
    this.signalChannel = 'smoke.signaling'
    
    // Peer discovery helper
    this.peerDiscovery = new PeerDiscovery(this)
  }

  /**
   * Initializes the hub and its underlying P2P network
   */
  public async initialize(): Promise<void> {
    if (this.initialized) return
    
    // Initialize the P2P network
    await this.p2pManager.initialize()
    
    // Create a signaling channel for WebRTC connection setup
    const channel = this.p2pManager.createChannel('smoke', this.signalChannel)
    
    // Subscribe to incoming messages
    channel.subscribe(async (data) => {
      try {
        const message = JSON.parse(data)
        
        // Only process messages directed to this hub
        if (message.to === this.address) {
          const hubMessage: HubMessage = {
            from: message.from,
            to: message.to,
            data: message.data
          }
          
          // Dispatch the message to registered handlers
          this.events.send('message', hubMessage)
        }
      } catch (error) {
        console.error('Error processing incoming message:', error)
      }
    })
    
    // Start peer discovery
    await this.peerDiscovery.start()
    
    // Set up peer discovery event listener
    this.peerDiscovery.onPeerDiscovered((peerInfo) => {
      this.events.send('peer:discovered', peerInfo)
      console.log(`Discovered peer: ${peerInfo.address}`)
    })
    
    this.initialized = true
    console.log(`Libp2p Hub initialized with address: ${this.address}`)
  }

  /**
   * Gets the RTCConfiguration object for this Hub.
   * This implementation uses the default configuration as we rely on libp2p
   * for connectivity.
   */
  public async configuration(): Promise<RTCConfiguration> {
    await this.ensureInitialized()
    // Return a minimal configuration as we're using libp2p for connectivity
    return {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' }
      ]
    }
  }

  /**
   * Gets the address of this Hub
   */
  public async address(): Promise<string> {
    await this.ensureInitialized()
    return this.address
  }

  /**
   * Sends a message to another peer via the libp2p network
   */
  public async send(message: { to: string; data: unknown }): Promise<void> {
    await this.ensureInitialized()
    
    const fullMessage = {
      from: this.address,
      to: message.to,
      data: message.data
    }
    
    try {
      // Publish to the signaling channel
      const channel = this.p2pManager.createChannel('smoke', this.signalChannel)
      await channel.publish(JSON.stringify(fullMessage))
    } catch (error) {
      console.error('Error sending message:', error)
      throw error
    }
  }

  /**
   * Registers a callback to receive messages from this Hub
   */
  public receive(callback: HubMessageCallback): void {
    this.events.on('message', callback)
  }

  /**
   * Disposes of this Hub and its resources
   */
  public async dispose(): Promise<void> {
    if (!this.initialized) return
    
    // Stop peer discovery
    this.peerDiscovery.stop()
    
    this.events.dispose()
    await this.p2pManager.stop()
    this.initialized = false
  }

  /**
   * Ensures the hub is initialized before performing operations
   */
  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.initialize()
    }
  }

  /**
   * Returns the underlying P2PManager instance
   */
  public getP2PManager(): P2PManager {
    return this.p2pManager
  }

  /**
   * Returns the peer discovery helper
   */
  public getPeerDiscovery(): PeerDiscovery {
    return this.peerDiscovery
  }
  
  /**
   * Gets a list of all discovered peers
   */
  public getDiscoveredPeers() {
    return this.peerDiscovery.getPeers()
  }
}