import { Libp2pHub } from './Libp2pHub'
import { P2PManager } from './P2PManager'
import * as Events from '../events/index.mjs'

interface PeerInfo {
  address: string;
  connectionInfo: any;
  timestamp: number;
}

/**
 * Helps Smoke nodes discover each other in the libp2p network.
 * 
 * This class periodically announces the hub's presence and maintains
 * a directory of known peers that can be used for establishing
 * WebRTC connections through Smoke's network stack.
 */
export class PeerDiscovery {
  private hub: Libp2pHub;
  private events: Events.Events;
  private knownPeers: Map<string, PeerInfo>;
  private announceInterval: any;
  private directoryChannel = 'smoke.directory';
  private ttlMs = 5 * 60 * 1000; // 5 minutes TTL for peer entries
  
  constructor(hub: Libp2pHub) {
    this.hub = hub;
    this.events = new Events.Events();
    this.knownPeers = new Map<string, PeerInfo>();
  }
  
  /**
   * Start peer discovery
   */
  public async start(): Promise<void> {
    const p2pManager = this.hub.getP2PManager();
    const channel = p2pManager.createChannel('smoke', this.directoryChannel);
    
    // Subscribe to peer announcements
    channel.subscribe(async (data) => {
      try {
        const announcement = JSON.parse(data);
        if (announcement && announcement.type === 'announce') {
          const peerInfo: PeerInfo = {
            address: announcement.address,
            connectionInfo: announcement.connectionInfo,
            timestamp: Date.now()
          };
          
          // Don't add ourselves to the peer list
          const selfAddress = await this.hub.address();
          if (peerInfo.address !== selfAddress) {
            this.knownPeers.set(peerInfo.address, peerInfo);
            this.events.send('peer:discovered', peerInfo);
          }
        }
      } catch (error) {
        console.error('Error processing peer announcement:', error);
      }
    });
    
    // Start announcing our presence
    this.startAnnouncing();
    
    // Start cleanup task
    this.startCleanup();
  }
  
  /**
   * Stop peer discovery
   */
  public stop(): void {
    if (this.announceInterval) {
      clearInterval(this.announceInterval);
      this.announceInterval = null;
    }
    this.events.dispose();
  }
  
  /**
   * Get all currently known peers
   */
  public getPeers(): PeerInfo[] {
    return Array.from(this.knownPeers.values());
  }
  
  /**
   * Listen for peer discovery events
   */
  public onPeerDiscovered(callback: (peer: PeerInfo) => void): void {
    this.events.on('peer:discovered', callback);
  }
  
  /**
   * Periodically announce our presence to other peers
   */
  private startAnnouncing(): void {
    // Announce immediately
    this.announcePresence();
    
    // Then announce periodically (every 30 seconds)
    this.announceInterval = setInterval(() => {
      this.announcePresence();
    }, 30 * 1000);
  }
  
  /**
   * Publish our presence to the directory channel
   */
  private async announcePresence(): Promise<void> {
    try {
      const p2pManager = this.hub.getP2PManager();
      const channel = p2pManager.createChannel('smoke', this.directoryChannel);
      const address = await this.hub.address();
      const connectionInfo = p2pManager.getConnectionInfo();
      
      const announcement = {
        type: 'announce',
        address,
        connectionInfo,
        timestamp: Date.now()
      };
      
      await channel.publish(JSON.stringify(announcement));
    } catch (error) {
      console.error('Error announcing presence:', error);
    }
  }
  
  /**
   * Start cleaning up stale peer entries
   */
  private startCleanup(): void {
    setInterval(() => {
      const now = Date.now();
      for (const [address, info] of this.knownPeers.entries()) {
        if (now - info.timestamp > this.ttlMs) {
          this.knownPeers.delete(address);
        }
      }
    }, 60 * 1000); // Run cleanup every minute
  }
}