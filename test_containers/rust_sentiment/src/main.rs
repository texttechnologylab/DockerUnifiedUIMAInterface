use axum::{
    routing::{get, post},
    http::StatusCode,
    response::IntoResponse,
    Json, Router, middleware::AddExtension, Extension,
};
use rust_bert::pipelines::sentiment::{SentimentModel, SentimentPolarity};
use rust_bert::pipelines::sentiment::Sentiment;
use bytes::buf::Reader;
use bytes::Buf;
use rmp;

struct AppState {
    tx: tokio::sync::mpsc::UnboundedSender<(bytes::Bytes,tokio::sync::oneshot::Sender<Vec<Sentiment>>)>,
}

#[tokio::main]
async fn main() {
    let lua : String = std::fs::read_to_string("rust_communication_msgpack.lua").unwrap();
    let (tx,mut rx) = tokio::sync::mpsc::unbounded_channel::<(bytes::Bytes,tokio::sync::oneshot::Sender<Vec<Sentiment>>)>();
    let thread = std::thread::spawn(move || {
        let sentiment_classifier =  SentimentModel::new(Default::default()).unwrap();
        loop {
            let str = rx.blocking_recv();
            if let Some((vals, sender)) = str {
                let (st,mut tail) = rmp::decode::read_str_from_slice(vals.as_ref()).unwrap();
                    let arraylen = rmp::decode::read_array_len(&mut tail).unwrap() as usize >> 1;
                    let mut vec = Vec::with_capacity(arraylen);
                    for _ in 0..arraylen {
                        let start = rmp::decode::read_pfix(&mut tail).unwrap() as usize;
                        let end= rmp::decode::read_pfix(&mut tail).unwrap() as usize;
                        vec.push(&st[start..end]);
                    }
                    let res = sentiment_classifier.predict(vec);
                    let _ = sender.send(res);
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
