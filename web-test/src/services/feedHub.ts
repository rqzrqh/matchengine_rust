import { WebSocket } from "ws";

/** Broadcast quote/settle writes to WebSocket clients */
export class FeedHub {
  private clients = new Set<WebSocket>();

  add(ws: WebSocket): void {
    this.clients.add(ws);
  }

  remove(ws: WebSocket): void {
    this.clients.delete(ws);
  }

  broadcast(channel: string, data: Record<string, unknown>): void {
    const msg = JSON.stringify({ channel, data });
    for (const ws of this.clients) {
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(msg);
        } catch {
          this.clients.delete(ws);
        }
      }
    }
  }
}
