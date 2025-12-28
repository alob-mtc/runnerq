//! Static HTML content for the RunnerQ Console UI.

/// The RunnerQ Console HTML content.
///
/// This can be served by any HTTP framework to provide the queue monitoring UI.
pub const CONSOLE_HTML: &str = include_str!("../../../ui/runnerq-console.html");
