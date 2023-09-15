// System
use std::sync::Once;

// Third Party
use tracing_subscriber::prelude::*;

static INIT: Once = Once::new();

/// Create the global tracing subscriber for tests only once so that multiple tests do not
/// conflict.
pub fn init_tracing() {
    INIT.call_once(|| {
        let filter = tracing_subscriber::filter::Targets::new()
            .with_target("kube_state_rs", tracing::Level::DEBUG);
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(filter)
            .init();
    });
}
