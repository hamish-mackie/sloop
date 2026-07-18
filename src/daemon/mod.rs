mod aftercare;
mod commands;
mod dispatcher;
mod recovery;
mod runner;
mod scheduler;
mod server;
mod worker_api;

pub use server::{
    ClientResponse, DaemonError, LockIdentity, read_lock_identity, request, request_running,
    serve_current_repository,
};

pub(crate) const WORKER_BOOTSTRAP_PROMPT: &str =
    include_str!("../worker-instructions.md").trim_ascii();
