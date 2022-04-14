use axum::{
    routing::{get, post},
    http::StatusCode,
    response::IntoResponse,
    Json, Router, middleware::AddExtension, Extension,
};
use rust_bert::pipelines::{sentiment::{SentimentModel, SentimentPolarity}, sequence_classification::SequenceClassificationConfig, common::ModelType};
use rust_bert::pipelines::sentiment::Sentiment;
use bytes::buf::Reader;
use bytes::Buf;
use rmp;
use serde::{Deserialize};
use rust_bert::resources::{Resource,LocalResource, RemoteResource};
use std::path::PathBuf;

#[derive(Debug,Deserialize)]
pub struct Message<'a>(&'a str, Vec<usize>);

struct AppState {
    tx: tokio::sync::mpsc::UnboundedSender<(bytes::Bytes,tokio::sync::oneshot::Sender<Vec<Sentiment>>)>,
}

#[tokio::main]
async fn main() {
    let lua : String = std::fs::read_to_string("rust_communication_msgpack.lua").unwrap();
    let (tx,mut rx) = tokio::sync::mpsc::unbounded_channel::<(bytes::Bytes,tokio::sync::oneshot::Sender<Vec<Sentiment>>)>();
    let thread = std::thread::spawn(move || {
        let sentiment_classifier =  SentimentModel::new(SequenceClassificationConfig::new(ModelType::DistilBert, Resource::Local(LocalResource{local_path: PathBuf::from(r"rust_model.ot")}), Resource::Local(LocalResource{local_path: PathBuf::from(r"config.json")}), Resource::Local(LocalResource{local_path: PathBuf::from(r"vocab.txt")}), None, false, None, None)).unwrap();
        loop {
            let str = rx.blocking_recv();
            if let Some((vals, sender)) = str {
                if let Ok(message) = rmp_serde::from_slice::<Message>(vals.as_ref()) {
                    let mut strvec = Vec::with_capacity(message.1.len());
                    for x in (0..message.1.len()).step_by(2) {
                        strvec.push(&message.0[message.1[x]..message.1[x+1]]);
                    }
                    let res = sentiment_classifier.predict(strvec);
                    let _ = sender.send(res);
                }
                else {
                    let _ = sender.send(Vec::new());
                }
            }
        }
    });

    let app = std::sync::Arc::new(AppState {
        tx
    });

    let app = Router::new()
        .route("/v1/process", post(process))
        .route("/v1/communication_layer",get(communication_layer))
        .layer(Extension(app));

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 9714));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn process(Extension(state): Extension<std::sync::Arc<AppState>>, body: bytes::Bytes) -> Vec<u8> {
    let (rxo,txo) = tokio::sync::oneshot::channel();
    state.tx.send((body,rxo)).unwrap();
    let result = txo.await.unwrap();
    let mut vec = Vec::new();
    rmp::encode::write_array_len(&mut vec, result.len() as u32*2).unwrap();
    for x in result.iter() {
        rmp::encode::write_u32(&mut vec, match x.polarity {
            SentimentPolarity::Negative => 0,
            SentimentPolarity::Positive => 1,
        }).unwrap();
        rmp::encode::write_f64(&mut vec, x.score).unwrap();
    } 
    vec
}

async fn communication_layer() -> &'static str {
    include_str!("../rust_communication_msgpack.lua")
}
