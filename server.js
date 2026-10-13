const http = require("http");
const express = require("express");
const WebSocket = require("ws");
const { createClient } = require("@supabase/supabase-js");

const app = express();
app.use(express.json({ limit: "20kb" }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_PUBLISHABLE_KEY =
  process.env.SUPABASE_PUBLISHABLE_KEY;

if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY) {
  throw new Error("Missing Supabase environment variables");
}

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_PUBLISHABLE_KEY,
  {
    auth: {
      autoRefreshToken: false,
      persistSession: false
    }
  }
);

const clients = new Map();

app.get("/", (req, res) => {
  res.json({
    name: "LJ Chat Server",
    status: "online",
    version: "2.0.0"
  });
});

// Verify a Supabase access token.
async function verifyToken(token) {
  if (typeof token !== "string" || !token) {
    return null;
  }

  const { data, error } = await supabase.auth.getUser(token);

  if (error || !data.user) {
    return null;
  }

  return data.user;
}

function send(socket, payload) {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(payload));
  }
}

// Authenticate a connection using its Supabase access token.
wss.on("connection", (socket) => {
  let user = null;
  let authenticating = false;
  let authenticated = false;

  socket.on("message", async (data) => {
    let message;

    try {
      message = JSON.parse(data.toString());
    } catch {
      send(socket, {
        type: "error",
        message: "Invalid JSON"
      });
      return;
    }

    // First message must authenticate the connection.
    if (!authenticated) {
      if (authenticating) return;

      if (
        message.type !== "authenticate" ||
        typeof message.accessToken !== "string"
      ) {
        send(socket, {
          type: "error",
          message: "Authenticate first"
        });
        socket.close(1008, "Authentication required");
        return;
      }

      authenticating = true;

      try {
        user = await verifyToken(message.accessToken);

        if (!user) {
          send(socket, {
            type: "error",
            message: "Invalid or expired access token"
          });
          socket.close(1008, "Invalid token");
          return;
        }

        // Only one active connection per user is stored here.
        // A later connection replaces the earlier one.
        const previous = clients.get(user.id);

        if (previous && previous !== socket) {
          send(previous, {
            type: "error",
            message: "Connected from another session"
          });
          previous.close();
        }

        clients.set(user.id, socket);
        authenticated = true;

        send(socket, {
          type: "authenticated",
          userId: user.id
        });
      } catch (error) {
        console.error("Authentication error:", error.message);
        send(socket, {
          type: "error",
          message: "Authentication failed"
        });
        socket.close(1011, "Authentication error");
      } finally {
        authenticating = false;
      }

      return;
    }

    if (!user) return;

    if (message.type === "chat") {
      const receiverId = message.receiverId;
      const body =
        typeof message.text === "string"
          ? message.text.trim()
          : "";

      if (
        typeof receiverId !== "string" ||
        receiverId === user.id ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(receiverId)
      ) {
        send(socket, {
          type: "error",
          message: "Invalid recipient"
        });
        return;
      }

      if (!body || body.length > 4000) {
        send(socket, {
          type: "error",
          message: "Message must contain 1 to 4000 characters"
        });
        return;
      }

      try {
        // The authenticated user ID, not a client-supplied sender ID,
        // is used as the sender.
        const { data, error } = await supabase
          .from("messages")
          .insert({
            sender_id: user.id,
            receiver_id: receiverId,
            body
          })
          .select("id, sender_id, receiver_id, body, created_at")
          .single();

        if (error) {
          console.error("Message save failed:", error.message);
          send(socket, {
            type: "error",
            message: "Could not save message"
          });
          return;
        }

        const outgoing = {
          type: "chat",
          message: data
        };

        // Deliver only to the sender and intended recipient.
        send(socket, outgoing);

        const recipientSocket = clients.get(receiverId);

        if (recipientSocket && recipientSocket !== socket) {
          send(recipientSocket, outgoing);
        }
      } catch (error) {
        console.error("Message error:", error.message);
        send(socket, {
          type: "error",
          message: "Message failed"
        });
      }
    }
  });

  socket.on("close", () => {
    if (user && clients.get(user.id) === socket) {
      clients.delete(user.id);
    }
  });

  socket.on("error", (error) => {
    console.error("WebSocket error:", error.message);
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`LJ Chat Server running on port ${PORT}`);
});