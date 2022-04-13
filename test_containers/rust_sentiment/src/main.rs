use warp::Filter;

#[tokio::main]
async fn main() {
    let lua : String = std::fs::read_to_string("rust_communication_msgpack.lua").unwrap();
    let hello = warp::path!("v1"/"process")
        .map(|| {
            "Hello!"
        })
        .or(warp::path!("v1"/"typesystem").map(|| {
            "Hello typesystem"
        }))
        .or(warp::path!("v1"/"communication_layer").map(move  || {
            lua.clone()
        }));

    warp::serve(hello)
        .run(([0, 0, 0, 0],9714))
        .await;
}
