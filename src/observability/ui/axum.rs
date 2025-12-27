//! Axum-based HTTP endpoints for the RunnerQ Console UI.
//!
//! This module provides Axum routers for serving the observability API
//! and the Console UI.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::get,
    Json, Router,
};
use futures::stream::{Stream, StreamExt};
use serde::Deserialize;

use crate::observability::inspector::QueueInspector;
use crate::observability::models::{ActivityEvent, DeadLetterRecord, QueueStats};

use super::html::CONSOLE_HTML;

#[derive(Clone)]
struct UiState {
    inspector: QueueInspector,
}

#[derive(Deserialize, Default)]
struct Pagination {
    offset: Option<usize>,
    limit: Option<usize>,
}

/// Creates a RunnerQ Console UI router that can be mounted on any path.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use runner_q::{WorkerEngineBuilder, QueueInspector};
/// use runner_q::observability::{runnerq_ui, observability_api};
/// use axum::Router;
///
/// # async fn example() -> anyhow::Result<()> {
/// // Build the worker engine (e.g., with Redis)
/// let engine = WorkerEngineBuilder::new()
///     .redis_url("redis://127.0.0.1:6379")
///     .queue_name("my_app")
///     .max_concurrent_activities(10)
///     .build()
///     .await?;
///
/// // Create inspector from the engine's backend
/// let inspector = engine.inspector().expect("Backend supports inspection");
///
/// let app = Router::new()
///     .nest("/console", runnerq_ui(inspector.clone()))
///     .nest("/api/observability", observability_api(inspector));
///
/// // Now accessible at http://localhost:8081/console
/// # Ok(())
/// # }
/// ```
pub fn runnerq_ui(inspector: QueueInspector) -> Router {
    let state = UiState { inspector };

    Router::new()
        .route("/", get(serve_ui))
        .route("/api/observability/stats", get(get_stats))
        .route(
            "/api/observability/activities/:key",
            get(activity_collection_or_detail),
        )
        .route(
            "/api/observability/activities/:id/events",
            get(activity_events),
        )
        .route(
            "/api/observability/activities/:id/result",
            get(activity_result),
        )
        .route("/api/observability/dead-letter", get(dead_letters))
        .route("/api/observability/stream", get(event_stream))
        .with_state(state)
}

/// Creates just the API routes for observability data.
/// Use this if you want to serve the UI separately or use a custom UI.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use runner_q::{WorkerEngineBuilder, QueueInspector};
/// use runner_q::observability::observability_api;
/// use axum::Router;
///
/// # async fn example() -> anyhow::Result<()> {
/// // Build the worker engine (e.g., with Postgres)
/// let engine = WorkerEngineBuilder::new()
///     .postgres_url("postgres://user:pass@localhost/db")
///     .queue_name("my_app")
///     .max_concurrent_activities(10)
///     .build()
///     .await?;
///
/// // Create inspector from the engine's backend
/// let inspector = engine.inspector().expect("Backend supports inspection");
///
/// let app = Router::new()
///     .nest("/api/observability", observability_api(inspector));
/// # Ok(())
/// # }
/// ```
pub fn observability_api(inspector: QueueInspector) -> Router {
    let state = UiState { inspector };

    Router::new()
        .route("/stats", get(get_stats))
        .route("/activities/:key", get(activity_collection_or_detail))
        .route("/activities/:id/events", get(activity_events))
        .route("/activities/:id/result", get(activity_result))
        .route("/stream", get(event_stream))
        .route("/dead-letter", get(dead_letters))
        .with_state(state)
}

async fn serve_ui() -> Html<&'static str> {
    Html(CONSOLE_HTML)
}

async fn get_stats(State(state): State<UiState>) -> Result<Json<QueueStats>, StatusCode> {
    state
        .inspector
        .stats()
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn activity_collection_or_detail(
    State(state): State<UiState>,
    Path(key): Path<String>,
    Query(pagination): Query<Pagination>,
) -> Result<Response, StatusCode> {
    if let Ok(uuid) = uuid::Uuid::parse_str(&key) {
        // Detail lookup path
        let activity = state
            .inspector
            .get_activity(uuid)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::NOT_FOUND)?;
        return Ok(Json(activity).into_response());
    }

    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(50);

    let activities = match key.as_str() {
        "pending" => state
            .inspector
            .list_pending(offset, limit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        "processing" => state
            .inspector
            .list_processing(offset, limit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        "scheduled" => state
            .inspector
            .list_scheduled(offset, limit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        "completed" => state
            .inspector
            .list_completed(offset, limit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        "dead_letter" => state
            .inspector
            .list_dead_letter(offset, limit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .into_iter()
            .map(|dlr| dlr.activity)
            .collect(),
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    Ok(Json(activities).into_response())
}

async fn activity_events(
    State(state): State<UiState>,
    Path(id): Path<String>,
    Query(pagination): Query<Pagination>,
) -> Result<Json<Vec<ActivityEvent>>, StatusCode> {
    let uuid = uuid::Uuid::parse_str(&id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let limit = pagination.limit.unwrap_or(50);

    state
        .inspector
        .recent_events(uuid, limit)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn activity_result(
    State(state): State<UiState>,
    Path(id): Path<String>,
) -> Result<Json<Option<serde_json::Value>>, StatusCode> {
    let uuid = uuid::Uuid::parse_str(&id).map_err(|_| StatusCode::BAD_REQUEST)?;

    state
        .inspector
        .get_result(uuid)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn dead_letters(
    State(state): State<UiState>,
    Query(pagination): Query<Pagination>,
) -> Result<Json<Vec<DeadLetterRecord>>, StatusCode> {
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(50);

    state
        .inspector
        .list_dead_letter(offset, limit)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn event_stream(
    State(state): State<UiState>,
) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, StatusCode> {
    let stream = state.inspector.event_stream();

    // Convert ActivityEvent stream to SSE Event stream
    let stream = stream.filter_map(|result| async move {
        match result {
            Ok(activity_event) => {
                // Serialize event as JSON
                match serde_json::to_string(&activity_event) {
                    Ok(json) => Some(Ok(Event::default().data(json))),
                    Err(_) => None,
                }
            }
            Err(_) => None, // Skip errors, continue streaming
        }
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

