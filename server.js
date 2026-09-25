/**
 * 生存战争小游戏 — 自建 WebSocket 实时通信服务器
 * 功能：玩家连接/断开、按房间隔离广播、心跳保活、通用 JSON 消息转发
 * 部署：Render（Free 档即可），监听 process.env.PORT
 */

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const HEARTBEAT_INTERVAL = 30000; // 30 秒发一次 ping
const ROOM_DEFAULT = 'sc_main';

// 简单 HTTP 服务（Render 健康检查用，访问根路径返回 OK）
const server = http.createServer((req, res) => {
  if (req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('生存战争 WebSocket 服务器运行中 ✅\n连接方式：wss://你的地址/?room=sc_main&uid=xxx&name=xxx\n');
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

const wss = new WebSocketServer({ server });

// 房间表：roomName → Set<ws>
const rooms = {};

function getRoom(name) {
  if (!rooms[name]) rooms[name] = new Set();
  return rooms[name];
}

function removeFromRoom(ws) {
  if (ws._room && rooms[ws._room]) {
    rooms[ws._room].delete(ws);
    if (rooms[ws._room].size === 0) delete rooms[ws._room];
  }
}

// 心跳：定期 ping，未响应 pong 的连接标记为死连接并 terminate
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch (e) {}
      return;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, HEARTBEAT_INTERVAL);

wss.on('connection', (ws, req) => {
  // 解析 URL query：?room=sc_main&uid=xxx&name=xxx
  let room = ROOM_DEFAULT;
  let uid = '';
  let name = '';
  try {
    const url = new URL(req.url, 'http://localhost');
    room = url.searchParams.get('room') || ROOM_DEFAULT;
    uid = url.searchParams.get('uid') || '';
    name = url.searchParams.get('name') || '';
  } catch (e) { /* URL 解析失败用默认值 */ }

  ws._room = room;
  ws._uid = uid;
  ws._name = name;
  ws.isAlive = true;

  // 加入房间
  getRoom(room).add(ws);

  console.log(`[连接] uid=${uid} name=${name} room=${room} | 当前房间 ${rooms[room].size} 人`);

  // 收到 pong 标记存活
  ws.on('pong', () => { ws.isAlive = true; });

  // 收到消息：解析 JSON，转发给同房间其他成员（不回发发送者）
  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      return; // 非 JSON 消息忽略
    }

    // 确保消息带发送者 uid（客户端应已带，这里兜底）
    if (!msg.f && uid) msg.f = uid;

    const payload = JSON.stringify(msg);
    const roomSet = rooms[room];
    if (!roomSet) return;

    roomSet.forEach((client) => {
      if (client !== ws && client.readyState === 1) { // OPEN
        try { client.send(payload); } catch (e) {}
      }
    });
  });

  // 断开：从房间移除
  ws.on('close', () => {
    removeFromRoom(ws);
    console.log(`[断开] uid=${uid} name=${name} room=${room}`);
  });

  ws.on('error', () => {
    removeFromRoom(ws);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ 生存战争 WebSocket 服务器已启动`);
  console.log(`   监听端口: ${PORT}`);
  console.log(`   本地测试: ws://localhost:${PORT}/?room=sc_main&uid=test&name=测试`);
  console.log(`   心跳间隔: ${HEARTBEAT_INTERVAL / 1000}s`);
});

// 优雅关闭
process.on('SIGTERM', () => {
  clearInterval(heartbeat);
  wss.close(() => server.close());
});
