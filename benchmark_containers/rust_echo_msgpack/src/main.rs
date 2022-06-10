#![feature(slice_pattern)]
use axum::{
    routing::{get, post},
    http::StatusCode,
    response::IntoResponse,
    Json, Router, middleware::AddExtension, Extension,
};
use bytes::buf::Reader;
use bytes::Buf;
use rmp;
use serde::{Deserialize};
use std::path::PathBuf;
use core::slice::SlicePattern;

#[derive(Debug,Deserialize)]
pub struct Message<'a>(&'a str, Vec<usize>);


#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/v1/process", post(process))
        .route("/v1/communication_layer",get(communication_layer_msgpack))
        .route("/v1/typesystem",get(typesystem));

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 9714));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn process(body: bytes::Bytes) -> Vec<u8> {
    body.as_slice().to_vec()
} 

async fn typesystem() -> &'static str {
    include_str!("../token_only_types.xml")
}

async fn communication_layer_msgpack() -> &'static str {
    include_str!("../rust_communication_msgpack.lua")
}
