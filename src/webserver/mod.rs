use axum::{
    extract::Extension,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use pyo3::prelude::*;
use std::{net::SocketAddr, sync::Arc};

use crate::dataflow::Dataflow;

struct State {
    dataflow_json: String,
}

pub(crate) async fn run_webserver(dataflow: Dataflow) {
    // Since the dataflow can't change at runtime, we encode it as a string of JSON
    // once, when the webserver starts.
    let dataflow_json: String = Python::with_gil(|py| {
        let encoder_module =
            PyModule::import(py, "bytewax.encoder").expect("Unable to load Bytewax encoder module");
        // For convenience, we are using a helper function supplied in the
        // bytewax.encoder module.
        let encode = encoder_module
            .getattr("encode_dataflow")
            .expect("Unable to load encode_dataflow function");

        encode.call1((dataflow,)).unwrap().to_string()
    });
    let shared_state = Arc::new(State { dataflow_json });

    let app = Router::new()
        .route("/dataflow", get(get_dataflow))
        .layer(Extension(shared_state));

    let addr = match std::env::var("BYTEWAX_PORT") {
        Ok(config) => config.parse().expect("Unable to parse BYTEWAX_PORT"),
        Err(_) => SocketAddr::from(([0, 0, 0, 0], 3030)),
    };
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .expect("Unable to create local webserver at port 3000");
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
