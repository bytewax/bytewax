use axum::{
    extract::Extension,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use std::{net::SocketAddr, sync::Arc};

use crate::errors::PythonException;

struct State {
    dataflow_json: String,
}

pub(crate) async fn run_webserver(dataflow_json: String) -> PyResult<()> {
    let shared_state = Arc::new(State { dataflow_json });

    let app = Router::new()
        .route("/dataflow", get(get_dataflow))
        .layer(Extension(shared_state));

    let port = std::env::var("BYTEWAX_DATAFLOW_API_PORT")
        .map(|var| {
            var.parse()
                .expect("Unable to parse BYTEWAX_DATAFLOW_API_PORT")
        })
        .unwrap_or(3030);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Starting Dataflow API server on {addr:?}");

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(|err| err.to_string())
        .raise::<PyRuntimeError>(&format!("Unable to create local webserver at port {port}"))
}

async fn get_dataflow(Extension(state): Extension<Arc<State>>) -> impl IntoResponse {
    // We are building a custom response here, as the returned value
    // from our helper function is JSON formatted string.
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(state.dataflow_json.clone())
        .unwrap()
}
