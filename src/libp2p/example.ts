import { Network } from '../network.mjs'
import { Libp2pHub } from './Libp2pHub'

/**
 * Example demonstrating how to use the Libp2pHub with Smoke's network stack
 */
async function main() {
  try {
    console.log('Initializing Libp2p Hub...')
    const hub = new Libp2pHub()
    await hub.initialize()
    
    console.log('Creating network with Libp2p Hub...')
    const network = new Network({ hub })
    
    // Set up a simple HTTP server
    console.log('Setting up HTTP server...')
    network.Http.listen({ port: 8080 }, async (request) => {
      console.log(`Received request for ${request.url}`)
      return new Response('Hello from libp2p-powered Smoke server!', {
        headers: { 'Content-Type': 'text/plain' }
      })
    })
    
    // Get our peer address that others can connect to
    const peerAddress = await hub.address()
    console.log(`Server running at: ${peerAddress}:8080`)
    console.log('Connected peers:', hub.getP2PManager().getConnectionInfo())
    
    // Listen for peer discovery events
    hub.getPeerDiscovery().onPeerDiscovered((peerInfo) => {
      console.log(`✨ Discovered new peer: ${peerInfo.address}`)
      
      // Example: Try to fetch from the discovered peer
      setTimeout(async () => {
        try {
          console.log(`Attempting to connect to discovered peer: ${peerInfo.address}:8080`)
          const response = await network.Http.fetch(`http://${peerInfo.address}:8080`)
          const text = await response.text()
          console.log(`Response from ${peerInfo.address}: ${text}`)
        } catch (error) {
          console.log(`Failed to connect to peer ${peerInfo.address}: ${error.message}`)
        }
      }, 5000)
    })
    
    // Add CLI commands for testing
    const showPeersCommand = setInterval(() => {
      const peers = hub.getDiscoveredPeers()
      console.log(`\n--- Known Peers (${peers.length}) ---`)
      peers.forEach((peer, i) => {
        console.log(`${i + 1}. ${peer.address} (discovered ${new Date(peer.timestamp).toLocaleTimeString()})`)
      })
      console.log('------------------------')
      console.log('To manually connect to a specific peer, use:')
      console.log(`network.Http.fetch('http://PEER_ADDRESS:8080')\n`)
    }, 30000)
    
    // Clean up on process exit
    process.on('SIGINT', async () => {
      console.log('Shutting down...')
      clearInterval(showPeersCommand)
      await hub.dispose()
      process.exit(0)
    })
    
    console.log('Server running. Press Ctrl+C to exit.')
  } catch (error) {
    console.error('Error:', error)
  }
}

// For browser environments, we'd call main() directly
// For Node.js, we need to export it
export { main }

// Auto-start in Node.js environment
if (typeof process !== 'undefined') {
  main().catch(console.error)
}