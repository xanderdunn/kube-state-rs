// System
use std::panic;
use std::sync::Once;
use std::thread;

// Third Party
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::iterator::Signals;
use tracing::{error, info};
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

/// Set up panic hook to trace errors when panic occurs.
pub fn setup_exit_hooks() {
    info!("Setting up exit hooks...");
    // Set up panic hook
    let default_panic = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        error!("Panic occurred, process is exiting: {}", panic_info);
        default_panic(panic_info);
        std::process::exit(1);
    }));

    // See docs:
    // https://docs.rs/signal-hook/latest/signal_hook/iterator/struct.SignalsInfo.html#examples
    let mut signals = Signals::new(TERM_SIGNALS).unwrap();
    signals.handle();
    // This join handle isn't ever used because the main()
    // thread should never exit.
    thread::spawn(move || {
        for signal in &mut signals {
            error!("Caught signal {signal}, process is exiting");
            std::process::exit(1);
        }
    });
}
