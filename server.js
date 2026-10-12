const http = require("http");
const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const clients = new Map();

app.get("/", (req, res) => {
res.json({
name: "LJ Chat Server",
status: "online",
version: "1.0.0"
});
});

wss.on("connection", (socket) => {
const clientId =
Math.random().toString(36).slice(2, 12);

clients.set(clientId, socket);

socket.send(JSON.stringify({
type: "connected",
clientId
}));

socket.on("message", (data) => {
let message;

try {
  message = JSON.parse(data.toString());
} catch {
  socket.send(JSON.stringify({
    type: "error",
    message: "Invalid JSON"
  }));
  return;
}

if (message.type === "chat") {
  if (typeof message.text !== "string") return;

  const text = message.text.trim();

  if (!text || text.length > 2000) return;

  const outgoing = JSON.stringify({
    type: "chat",
    clientId,
    text,
    timestamp: Date.now()
  });

  for (const client of clients.values()) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(outgoing);
    }
  }
}

});

socket.on("close", () => {
clients.delete(clientId);
});
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
console.log("LJ Chat Server running on port ${PORT}");
});