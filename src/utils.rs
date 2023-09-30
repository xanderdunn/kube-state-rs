// System
use std::collections::BTreeMap;
use std::panic;
use std::sync::Once;
use std::thread;

// Third Party
use k8s_openapi::{
    api::core::v1::{ConfigMap, Namespace, Node},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    api::{Api, PostParams},
    Client,
};
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::iterator::Signals;
use tracing::{debug, error, info};
use tracing_subscriber::prelude::*;

/// The namespace where we store transactions to be processed by the transaction processor.
pub const TRANSACTION_NAMESPACE: &str = "node-metadata-transactions";
/// The namespace where we store the latest version of a node's metadata.
/// These ConfigMaps are updated on node deletion.
pub const NODE_METADATA_NAMESPACE: &str = "node-metadata";
/// The key where the Transaction Processor stores the ResourceVersion of the node when it's saving
/// its metadata.
pub const LABEL_STORE_VERSION_KEY: &str = "last_store_resource_version";

static INIT: Once = Once::new();

/// Create the global tracing subscriber for tests only once so that multiple tests do not
/// conflict.
pub fn init_tracing(target: &str, level: tracing::Level) {
    println!("target: {}, level: {}", target, level);
    INIT.call_once(|| {
        let filter = tracing_subscriber::filter::Targets::new().with_target(target, level);
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(filter)
            .init();
    });
}

/// This creatures a namespace if it doesn't exist.
pub async fn create_namespace(client: &Client, namespace: &str) -> Result<(), anyhow::Error> {
    let namespaces: Api<Namespace> = Api::all(client.clone());
    match namespaces.get(namespace).await {
        Ok(_) => {
            debug!("Namespace {} already exists.", namespace);
            Ok(())
        }
        Err(error) => {
            match error {
                // 404 Not found
                kube::Error::Api(kube::error::ErrorResponse { code, .. }) if code == 404 => {
                    info!("Namespace {} does not exist, creating...", namespace);
                    let namespace = Namespace {
                        metadata: ObjectMeta {
                            name: Some(namespace.to_string()),
                            ..Default::default()
                        },
                        ..Default::default()
                    };
                    namespaces
                        .create(&PostParams::default(), &namespace)
                        .await?;
                    Ok(())
                }
                _ => Err(anyhow::Error::new(error)),
            }
        }
    }
}

/// Idempotent replace data on a ConfigMap.
/// This fails if the ConfigMap's ResourceVersion has changed.
pub async fn replace_config_map_data(
    config_maps: &Api<ConfigMap>,
    config_map: &ConfigMap,
    data: &BTreeMap<String, String>,
) -> Result<(), anyhow::Error> {
    let mut new_config_map = config_map.clone();
    new_config_map.data = Some(data.clone());

    config_maps
        .replace(
            config_map.metadata.name.as_ref().unwrap(),
            &PostParams::default(),
            &new_config_map,
        )
        .await?;

    Ok(())
}

/// Idempotent replace labels on a node.
/// The labels in the given map are exactly the labels the node will end up with.
/// This fails if the node's ResourceVersion has changed.
pub async fn replace_node_labels(
    nodes: &Api<Node>,
    node: &Node,
    labels: &BTreeMap<String, String>,
) -> Result<(), anyhow::Error> {
    let mut new_node = node.clone();
    new_node.metadata.labels = Some(labels.clone());

    nodes
        .replace(
            node.metadata.name.as_ref().unwrap(),
            &PostParams::default(),
            &new_node,
        )
        .await?;

    Ok(())
}

/// The special token reserved to encode `/` in label keys so that they can be stored as ConfigMap
/// keys.
// TODO: Prefer to make this private.
pub const SLASH_TOKEN: &str = "---SLASH---";

/// As a workaround for Kubernetes ConfigMap key restrictions, we replace all forward slashes in
/// keys with SLASH_TOKEN.
/// This function either encodes or decodes all keys in a given map.
/// encode: true is to replace `/` with `SLASH_TOKEN`
pub fn code_key_slashes(node_labels: &mut BTreeMap<String, String>, encode: bool) {
    let (from, to) = if encode {
        ("/", SLASH_TOKEN)
    } else {
        (SLASH_TOKEN, "/")
    };
    node_labels
        .keys()
        .cloned()
        .collect::<Vec<String>>()
        .into_iter()
        .for_each(|key| {
            if let Some(value) = node_labels.remove(&key) {
                node_labels.insert(key.replace(from, to), value);
            }
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
