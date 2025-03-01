import { Network } from '../network.mjs'
import { Libp2pHub } from './Libp2pHub'

/**
 * Test script for verifying the Libp2pHub implementation.
 * 
 * This script tests basic functionality:
 * 1. Creates two instances of Libp2pHub
 * 2. Initializes both and allows them to discover each other
 * 3. Sets up HTTP servers on each
 * 4. Tests HTTP requests between them
 * 
 * Run with: node -r ts-node/register src/libp2p/test.ts
 */
async function test() {
  console.log('🧪 Starting Libp2pHub integration test...')
  
  try {
    // Create two hub instances to simulate two peers
    console.log('Creating Hub A...')
    const hubA = new Libp2pHub()
    await hubA.initialize()
    const networkA = new Network({ hub: hubA })
    
    console.log('Creating Hub B...')
    const hubB = new Libp2pHub()
    await hubB.initialize()
    const networkB = new Network({ hub: hubB })
    
    // Setup HTTP servers on both instances
    console.log('Setting up HTTP servers...')
    networkA.Http.listen({ port: 8080 }, async (request) => {
      return new Response(`Hello from peer A (${await hubA.address()})`, {
        headers: { 'Content-Type': 'text/plain' }
      })
    })
    
    networkB.Http.listen({ port: 8080 }, async (request) => {
      return new Response(`Hello from peer B (${await hubB.address()})`, {
        headers: { 'Content-Type': 'text/plain' }
      })
    })
    
    const addressA = await hubA.address()
    const addressB = await hubB.address()
    
    console.log(`Hub A address: ${addressA}`)
    console.log(`Hub B address: ${addressB}`)
    
    // Track discovered peers
    const peerDiscoveryPromiseA = new Promise<void>((resolve) => {
      hubA.getPeerDiscovery().onPeerDiscovered((peerInfo) => {
        if (peerInfo.address === addressB) {
          console.log('Hub A discovered Hub B!')
          resolve()
        }
      })
    })
    
    const peerDiscoveryPromiseB = new Promise<void>((resolve) => {
      hubB.getPeerDiscovery().onPeerDiscovered((peerInfo) => {
        if (peerInfo.address === addressA) {
          console.log('Hub B discovered Hub A!')
          resolve()
        }
      })
    })
    
    // Wait for peer discovery (with timeout)
    console.log('Waiting for mutual peer discovery (timeout: 60s)...')
    await Promise.race([
      Promise.all([peerDiscoveryPromiseA, peerDiscoveryPromiseB]),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Peer discovery timeout')), 60000))
    ])
    
    console.log('✅ Mutual peer discovery successful')
    
    // Test HTTP communication
    console.log('Testing HTTP communication...')
    try {
      // Hub A fetches from Hub B
      console.log('Hub A -> Hub B:')
      const responseFromB = await networkA.Http.fetch(`http://${addressB}:8080`)
      const textFromB = await responseFromB.text()
      console.log(`  Response: ${textFromB}`)
      
      // Hub B fetches from Hub A
      console.log('Hub B -> Hub A:')
      const responseFromA = await networkB.Http.fetch(`http://${addressA}:8080`)
      const textFromA = await responseFromA.text()
      console.log(`  Response: ${textFromA}`)
      
      console.log('✅ HTTP communication test successful')
    } catch (error) {
      console.error('❌ HTTP communication test failed:', error)
      throw error
    }
    
    // Test WebSocket (if implemented)
    console.log('\nAll tests completed successfully! Cleaning up...')
    
    // Clean up
    await hubA.dispose()
    await hubB.dispose()
    
    console.log('Test completed successfully!')
  } catch (error) {
    console.error('❌ Test failed:', error)
    process.exit(1)
  }
}

// Run the test
test().catch(console.error)