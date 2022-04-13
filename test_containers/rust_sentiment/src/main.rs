use warp::Filter;

#[tokio::main]
async fn main() {
    let hello = warp::path!("v1"/"process")
        .map(|| {
            "Hello!"
        })
        .or(warp::path!("v1"/"typesystem").map(|| {
            "Hello typesystem"
        }))
        .or(warp::path!("v1"/"communication_layer").map(|| {
            "Hello communication!"
        }));

    warp::serve(hello)
        .run(([0, 0, 0, 0],9714))
        .await;
}
