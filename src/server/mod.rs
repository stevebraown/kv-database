use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::protocol;

// A very small, placeholder in-memory store.
//
// Right now, this struct doesn't actually hold any data. The goal
// is to focus on teaching how `Arc` and `tokio::spawn` work with
// ownership and lifetimes. Later, this type will grow into a real
// sharded key–value store.
#[derive(Debug, Default)]
pub struct InMemoryStore;

impl InMemoryStore {
    // Constructor function. Returning `Self` by value means the caller
    // owns the new instance and can decide how to wrap/share it.
    pub fn new() -> Self {
        Self
    }
}

// Public entry point for starting the TCP server.
//
// The lifetime of `addr` is limited to this function call, but we only
// need it long enough to bind the listener. After binding, Tokio owns
// the socket, so there is no need for explicit lifetimes here.
pub async fn run(addr: &str) -> Result<()> {
    // Initialize logging once at server startup. In a bigger application
    // this might move to `main`, but keeping it here helps make this
    // module self-contained while teaching.
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    tracing::info!("binding TCP listener on {addr}");

    // `TcpListener::bind` is async because it may need to perform OS calls
    // that can yield. The owned `TcpListener` is returned to us and will
    // live for the duration of this function.
    let listener = TcpListener::bind(addr).await?;

    // Create a single shared store instance. We wrap it in `Arc` so the
    // reference-counted pointer can be cloned and moved into each spawned
    // task. `Arc` is necessary because multiple tasks will access it
    // concurrently and we want shared ownership.
    let store = Arc::new(InMemoryStore::new());

    loop {
        // Accept a new connection. This returns a new owned `TcpStream`
        // and the peer's socket address. If this await is canceled
        // (e.g., on shutdown), the loop will exit.
        let (stream, peer_addr) = listener.accept().await?;

        tracing::info!(%peer_addr, "accepted connection");

        // Clone the `Arc`. This does *not* clone the underlying store
        // data; it only increments the reference count so the same
        // instance can be shared. This clone is cheap (a pointer + count).
        let store = Arc::clone(&store);

        // Spawn a new asynchronous task to handle this connection.
        //
        // - `tokio::spawn` takes ownership of all values it moves into
        //   the async block. Those values must be `'static`, meaning
        //   they are valid for the entire duration of the program or
        //   owned by the task (no borrowing from the stack).
        //
        // - `stream` and `store` are both owned values here, so they
        //   can be moved into the task safely.
        //
        // - The async block returns a `Result<()>`, which we just log
        //   on error for now.
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, store, peer_addr).await {
                tracing::warn!(%peer_addr, error = %err, "connection closed with error");
            }
        });
    }
}

// Handle a single client connection.
//
// Ownership and lifetimes:
// - `stream` is owned by this function; when the function returns, the
//   stream is dropped and the connection is closed.
// - `store` is an `Arc<InMemoryStore>`; cloning the Arc increases the
//   reference count, and dropping it decreases the count. The underlying
//   store lives as long as at least one `Arc` exists.
// - `peer_addr` is copied in (SocketAddr is `Copy`); it is used only
//   for logging.
async fn handle_connection(
    mut stream: TcpStream,
    store: Arc<InMemoryStore>,
    peer_addr: SocketAddr,
) -> Result<()> {
    tracing::info!(%peer_addr, "handling connection");

    // A reusable, growable buffer for reading bytes from the socket.
    // `BytesMut` is a convenient type from the `bytes` crate that
    // supports efficient appends and splits, which is ideal for
    // protocol framing.
    let mut read_buf = BytesMut::with_capacity(4096);

    loop {
        // Read more data from the client into the buffer.
        //
        // - `read_buf` is passed by mutable reference, so the function
        //   can append to it without reallocating each time.
        // - This `.await` yields to the Tokio runtime while waiting
        //   for data, allowing other tasks to run.
        let n = stream.read_buf(&mut read_buf).await?;

        if n == 0 {
            // A read of 0 bytes means the peer closed the connection.
            tracing::info!(%peer_addr, "client closed connection");
            return Ok(());
        }

        // Try to parse as many RESP frames as possible from the buffer.
        //
        // The protocol module is responsible for understanding where
        // one message ends and the next begins. It will leave any
        // partial frame bytes in `read_buf` so that future reads can
        // complete the message.
        loop {
            match protocol::try_parse(&mut read_buf) {
                Ok(Some(frame)) => {
                    // Here we would normally convert the parsed frame
                    // into a `Command` enum and execute it against
                    // the shared store.
                    //
                    // For now, we simply log the frame and send back
                    // a fixed response. This keeps the focus on the
                    // async I/O and ownership model.
                    tracing::debug!(?frame, %peer_addr, "received frame");

                    let response = protocol::simple_string("OK");
                    let bytes = protocol::encode(&response);

                    // Writing is also async; we await until the bytes
                    // are written or an error occurs.
                    stream.write_all(&bytes).await?;
                }
                Ok(None) => {
                    // Not enough bytes for a full frame yet. Break out
                    // of the inner loop and go back to reading more
                    // from the socket.
                    break;
                }
                Err(err) => {
                    // A protocol error (malformed message). We log and
                    // close the connection gracefully by returning.
                    tracing::warn!(%peer_addr, error = %err, "protocol error");
                    return Ok(());
                }
            }
        }
    }
}

