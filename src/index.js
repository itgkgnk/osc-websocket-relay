export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/room\/([^/]+)$/);
    if (!match) return new Response('OSC WebSocket Relay', { status: 200 });
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('WebSocket required', { status: 426 });
    const room = decodeURIComponent(match[1]);
    const id = env.ROOMS.idFromName(room);
    return env.ROOMS.get(id).fetch(request);
  }
};

export class OscRoom {
  constructor(state) {
    this.state = state;
    this.clients = new Map();
    this.latest = new Map();
  }

  async fetch(request) {
    const url = new URL(request.url);
    const role = url.searchParams.get('role') === 'bridge' ? 'bridge' : 'controller';
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();
    this.clients.set(server, role);

    server.addEventListener('message', event => this.onMessage(server, role, event.data));
    const close = () => this.onClose(server, role);
    server.addEventListener('close', close);
    server.addEventListener('error', close);

    if (role === 'bridge') {
      this.sendLatest(server);
      this.broadcastStatus();
    } else {
      this.sendStatus(server);
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  onMessage(sender, role, data) {
    if (role !== 'controller' || typeof data !== 'string') return;
    try {
      const message = JSON.parse(data);
      if (message.type !== 'osc') return;
      const key = Number.isInteger(message.channel) ? `ch:${message.channel}` : `addr:${message.address}`;
      this.latest.set(key, data);
      for (const [socket, socketRole] of this.clients) {
        if (socketRole === 'bridge' && socket !== sender) this.safeSend(socket, data);
      }
    } catch (_) {}
  }

  onClose(socket, role) {
    if (!this.clients.has(socket)) return;
    this.clients.delete(socket);
    if (role === 'bridge') this.broadcastStatus();
  }

  bridgeCount() {
    let count = 0;
    for (const role of this.clients.values()) if (role === 'bridge') count++;
    return count;
  }

  sendStatus(socket) {
    this.safeSend(socket, JSON.stringify({ type: 'bridge-status', online: this.bridgeCount() > 0 }));
  }

  broadcastStatus() {
    const data = JSON.stringify({ type: 'bridge-status', online: this.bridgeCount() > 0 });
    for (const [socket, role] of this.clients) if (role === 'controller') this.safeSend(socket, data);
  }

  sendLatest(socket) {
    for (const data of this.latest.values()) this.safeSend(socket, data);
  }

  safeSend(socket, data) {
    try { socket.send(data); } catch (_) { this.clients.delete(socket); }
  }
}
