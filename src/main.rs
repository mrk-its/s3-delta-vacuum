use clap::Parser;
use maplit::hashmap;
use deltalake::builder::{s3_storage_options, DeltaTableBuilder};
use serde::Serialize;
use serde_json::to_string;
use url::Url;
use std::process::Command;
use rayon::prelude::*;

#[derive(Parser)]
struct Arguments {
    uri: String,
    #[clap(default_value_t=250, short, long)]
    chunk_size: usize,
    #[clap(default_value_t=false, short, long)]
    dry_run: bool,
    #[clap(default_value_t=8, short, long)]
    parallelism: usize,
    #[clap(default_value_t=168, short, long)]
    retention_period_hours: u64,
}

#[allow(non_snake_case)]
#[derive(Serialize)]
struct Key {
    Key: String,
}
#[allow(non_snake_case)]
#[derive(Serialize)]
struct Objects {
    Objects: Vec<Key>,
    Quiet: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> () {
    env_logger::Builder::from_default_env().init();

    let args = Arguments::parse();

    let url = Url::parse(&args.uri).expect("valid delta table url");
    assert!(url.path().chars().last() == Some('/'), "path must end with /");

    let mut table = DeltaTableBuilder::from_uri(url.to_string())
        .with_storage_options(hashmap!{
            // s3_storage_options::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "15".to_string(),
            // s3_storage_options::AWS_S3_LOCKING_PROVIDER.to_string() => "none".to_string(),
        })
        .load().await.unwrap();
    let files_to_delete = table.vacuum(Some(args.retention_period_hours), true, false).await.unwrap();

    log::info!("files to delete: {}", files_to_delete.len());
    
    if args.dry_run {
        println!("{}", files_to_delete.join("\n"));
        return;
    }

    rayon::ThreadPoolBuilder::new().num_threads(args.parallelism).build_global().unwrap();
    files_to_delete.par_chunks(args.chunk_size).for_each(|chunk| {
        let objects = chunk.iter().map(
            |path| Key {
                Key: format!("{}{}", &url.path()[1..], path)
            }
        ).collect::<Vec<_>>();
        let keys_json = to_string(&Objects{Objects: objects, Quiet: true}).unwrap();
        let args = [
            "s3api", "delete-objects",
            "--bucket", url.host_str().unwrap(),
            "--delete", &keys_json
        ];
        log::debug!("aws args: {:?}", args);
        let mut child = Command::new("aws")
            .args(args).spawn().unwrap();
            let exit_code = child.wait();
        log::debug!("exit_code: {:?}", exit_code);
    });
}
