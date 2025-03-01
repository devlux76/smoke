import type { GossipSub } from '@chainsafe/libp2p-gossipsub'
import type { Libp2p } from 'libp2p'

export interface ChannelCallbacks {
    onInsert?: (payload: any) => void;
    onUpdate?: (payload: any) => void;
    onDelete?: (payload: any) => void;
    onError?: (error: Error) => void;
}
  
export class Channel {
    private subscriptions: Map<string, ChannelCallbacks> = new Map()
    private pubsub: GossipSub
    
    constructor(
      private readonly node: Libp2p,
      private readonly topic: string,
    ) {
      this.pubsub = node.services.pubsub as GossipSub
    }
  
    on(event: 'INSERT' | 'UPDATE' | 'DELETE', callback: (payload: any) => void): this {
      const id = Math.random().toString(36).substring(7)
      this.subscriptions.set(id, {
        [`on${event.charAt(0)}${event.slice(1).toLowerCase()}`]: callback
      })
      return this
    }
  
    async subscribe() {
      await this.pubsub.subscribe(this.topic)
      this.pubsub.addEventListener('message', (evt: CustomEvent<{ topic: string, data: Uint8Array }>) => {
        const message = evt.detail
        if (message.topic === this.topic) {
          try {
            const data = JSON.parse(new TextDecoder().decode(message.data))
            this.subscriptions.forEach((callbacks) => {
              if (data.type === 'INSERT' && callbacks.onInsert) {
                callbacks.onInsert(data.payload)
              } else if (data.type === 'UPDATE' && callbacks.onUpdate) {
                callbacks.onUpdate(data.payload)
              } else if (data.type === 'DELETE' && callbacks.onDelete) {
                callbacks.onDelete(data.payload)
              }
            })
          } catch (error) {
            this.subscriptions.forEach((callbacks) => {
              if (callbacks.onError) callbacks.onError(error as Error)
            })
          }
        }
      })
    }
  
    unsubscribe() {
      return this.pubsub.unsubscribe(this.topic)
    }
}