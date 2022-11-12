use std::{path::Path};
use log::{error, debug};
use aws_sdk_s3::{Client, types::{ByteStream}};
use aws_smithy_http::body::SdkBody;



pub async fn upload_file(
    client: &Option<Client>,
    bucket_name: &str,
    file_name: &str,
    key: &str,
    ) -> (String, bool) {

    let path = Path::new(file_name);
    let stream = ByteStream::from_path(&path).await;
    let s3_file_name = format!("s3://{}/{}", bucket_name, key);

    match stream {
        Ok(stream) => {
            let response = put_bytesream(client.as_ref().unwrap(), bucket_name, stream, key);
            response.await
        },
        Err(e) => {
            error!("Failed to generate bytestream from {}, {}", file_name, e);
            (s3_file_name, false)
        }
    }
}


pub async fn write_to_file(
    client: &Option<Client>,
    data: &str,
    bucket_name: &str,
    key: &str,
    ) -> (String, bool) {
    
    let stream = ByteStream::new(SdkBody::from(data));
    let response = put_bytesream(client.as_ref().unwrap(), bucket_name, stream, key);
    response.await
}

async fn put_bytesream(
    client: &Client,
    bucket_name: &str,
    stream: ByteStream,
    key: &str,
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


pub async fn read_from_file(client: &Option<Client>, bucket_name: &str, key: &str) -> String {
    let resp = client
        .as_ref()
        .unwrap()
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