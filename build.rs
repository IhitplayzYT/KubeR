// Tonici auto compile of protobuff 
fn main() {
    tonic_build::configure().build_server(true).compile(&["proto/cluster.proto"], &["proto/"]).expect("Failed to compile protos");
}
