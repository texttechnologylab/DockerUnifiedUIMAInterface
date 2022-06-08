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
    let lua : String = std::fs::read_to_string("rust_communication_msgpack.lua").unwrap();

    let app = Router::new()
        .route("/v1/process", post(process))
        .route("/v1/communication_layer",get(communication_layer));

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 9716));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn process(body: bytes::Bytes) -> Vec<u8> {
    if let Ok(message) = rmp_serde::from_slice::<Message>(body.as_slice()) {
        let mut msg : Vec<u8> = Vec::new();
        rmp::encode::write_str(&mut msg, message.0).unwrap();

        rmp::encode::write_array_len(&mut msg, message.1.len() as u32).unwrap();
        for x in message.1.iter() {
            rmp::encode::write_u32(&mut msg, *x as u32).unwrap();
        }
        msg
    }
    else {
        panic!("this should not be reached!");
    }
} 


async fn communication_layer() -> &'static str {
    include_str!("../rust_communication_msgpack.lua")
}
