fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(&["protos/heath_check.proto"], &["protos"])?;
    Ok(())
}
