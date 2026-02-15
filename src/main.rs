// Declare the `server` module. The Rust compiler will look for
// `src/server/mod.rs` and include it as part of this crate.
mod server;

// `#[tokio::main]` transforms this function into the entry point
// for a Tokio async runtime. It generates a small `main` function
// behind the scenes that starts the runtime, then runs this async fn.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // For now, we hard-code a bind address. Later this will come
    // from configuration (e.g., environment variables or a config file).
    let addr = "127.0.0.1:6379";

    // Delegate to the server module. Keeping `main` thin makes it
    // easy to test and keeps startup concerns in one place.
    server::run(addr).await
}
