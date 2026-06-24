//! Compiles the frozen `.proto` wire contract into tonic/prost Rust code.
//!
//! Uses a vendored `protoc` (via `protoc-bin-vendored`) so the build needs no system
//! protobuf install.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Point prost-build at the vendored protoc. (edition 2021: `set_var` is safe.)
    std::env::set_var("PROTOC", &protoc);

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/kv.proto", "proto/raft.proto", "proto/pd.proto"],
            &["proto"],
        )?;
    Ok(())
}
