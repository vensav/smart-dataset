use std::{env, time::Duration, path::Path};
use log::{error, debug};
use aws_config::{retry::{RetryConfig}, meta::region::RegionProviderChain};
use aws_sdk_s3::{config, Client, types::{ByteStream}, Credentials, Region};
use aws_smithy_http::body::SdkBody;
use aws_smithy_types::timeout::TimeoutConfig;



pub async fn get_aws_client(
    use_default: Option<bool>,
    region_name: Option<String>
    ) -> Client{

    let max_attempts = option_env!("AWS_METADATA_SERVICE_NUM_ATTEMPTS").unwrap_or("5").to_string().parse::<u32>().unwrap();
    let timeout_secs = option_env!("AWS_METADATA_SERVICE_TIMEOUT").unwrap_or("120").to_string().parse::<u64>().unwrap();

    let final_use_default = if use_default.is_some() {use_default.unwrap()} else {true};
    let final_aws_region = if region_name.is_some() {region_name.unwrap()} else {"us-east-1".to_string()};

    // Start with the shared environment configuration.
    let mut conf = 
        if final_use_default {
            let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
            let shared_config = aws_config::from_env()
                                            .region(region_provider)
                                            .load().await;
            config::Builder::from(&shared_config)
        } else {
            get_aws_credentials(final_aws_region)
        };

    conf.set_sleep_impl(aws_smithy_async::rt::sleep::default_async_sleep());

    // Set timeout interval and max attempts. If tries is 1, there are no retries.
    let conf_with_retry = conf.timeout_config(
                                TimeoutConfig::builder()
                                    .operation_timeout(Duration::from_secs(timeout_secs))
                                    .build()
                            )
                            .retry_config(RetryConfig::standard()
                            .with_max_attempts(max_attempts))
                            .build();
    
    // build aws client
    Client::from_conf(conf_with_retry)

}


fn get_aws_credentials(
    region: String
    ) -> config::Builder{

    // get the id/secret from env
    let key_id = match env::var_os("AWS_ACCESS_KEY_ID") {
        Some(v) => v.into_string().unwrap(),
        None => panic!("$AWS_ACCESS_KEY_ID is not set")
    };
    let key_secret = match env::var_os("AWS_SECRET_ACCESS_KEY") {
        Some(v) => v.into_string().unwrap(),
        None => panic!("$AWS_SECRET_ACCESS_KEY is not set")
    };
    let session_token = match env::var_os("AWS_SESSION_TOKEN") {
        Some(v) => v.into_string().unwrap(),
        None => "".to_string()
    };

	// build the aws cred
	let cred = Credentials::new(key_id, key_secret, 
        Some(session_token), None, 
        "loaded-from-custom-env");

	let region = Region::new(region);
	
	config::Builder::new().region(region).credentials_provider(cred)

}



pub async fn upload_file(
    client: &Client,
    bucket_name: &str,
    file_name: &str,
    key: &str,
    ) -> (String, bool) {

    let path = Path::new(file_name);
    let stream = ByteStream::from_path(&path).await;
    let s3_file_name = format!("s3://{}/{}", bucket_name, key);

    match stream {
        Ok(stream) => {
            let response = put_bytesream(client, bucket_name, key, stream);
            response.await
        },
        Err(e) => {
            error!("Failed to generate bytestream from {}, {}", file_name, e);
            (s3_file_name, false)
        }
    }
}


pub async fn write_to_file(
    client: &Client,
    bucket_name: &str,
    key: &str,
    data: &str,
    ) -> (String, bool) {
    
    let stream = ByteStream::new(SdkBody::from(data));
    let response = put_bytesream(client, bucket_name, key, stream);
    response.await
}

async fn put_bytesream(
    client: &Client,
    bucket_name: &str,
    key: &str,
    stream: ByteStream,
) -> (String, bool) {
    
    let req = client
    .put_object()
    .bucket(bucket_name)
    .key(key)
    .body(stream);

    let response = req.send().await;
    let s3_file_name = format!("s3://{}/{}", bucket_name, key);
    match response {
        Ok(resp) => {
            debug!("{:?}", resp);
            debug!("Success: {}", s3_file_name);
            (s3_file_name, true)
        }
        Err(e) => {
            error!("Failed writing to {} : {}", s3_file_name, e);
            (s3_file_name, false)
        }
    }
}


pub async fn read_from_file(client: &Client, bucket_name: &str, key: &str) -> String {
    let resp = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await;
    
    let s3_file_name = format!("s3://{}/{}", bucket_name, key);
    match resp {
        Ok(resp) => { 
            let result_data = resp.body.collect().await.map(|data| data.into_bytes());
            match result_data {
                Ok(data) => {
                    String::from_utf8(data.to_vec()).expect("Found invalid UTF-8")
                }
                Err(err) => {
                    error!("Error while reading data {}", err);
                    "".to_string()
                }
            }
        }
        Err(e) => {
            error!("Failed reading from {} : {}", s3_file_name, e);
            "".to_string()
        }
    }
}