use kube::{error::ErrorResponse, runtime::watcher};

/// Convenience function for generating a kube::runtime::watcher::Error::WatchError.
// An alternative to this approach is to use anyhow to wrap the errors.
pub fn generate_error_response(
    error: Option<kube::Error>,
    node_name: &str,
    reason: &str,
) -> watcher::Error {
    let message = format!("{} for node {}", reason, node_name);
    let status = {
        if let Some(error) = error {
            error.to_string()
        } else {
            message.clone()
        }
    };
    watcher::Error::WatchError(ErrorResponse {
        status,
        message: message.clone(),
        reason: reason.to_string(),
        code: 500,
    })
}
