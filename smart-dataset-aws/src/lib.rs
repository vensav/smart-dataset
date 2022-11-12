use std::{time::Duration, env};
use async_trait::async_trait;
use aws_config::{retry::{RetryConfig}, meta::region::RegionProviderChain};
use aws_sdk_s3::{config, Client, Credentials, Region};
use aws_smithy_types::timeout::TimeoutConfig;
use smart_dataset::{self, SmartDataset};
pub mod s3;


#[derive(Debug, Default)]
pub struct AwsDataset {
    pub file_path: String,
    pub source_location: String,
    pub source_type: String,
    pub aws_region: Option<String>,
    pub aws_use_default_credentials: Option<bool>,
    pub aws_client: Option<Client>,
}



#[async_trait]
impl SmartDataset for AwsDataset {
    
    async fn connect(&mut self) {
        if self.source_location == "aws".to_string() {
            let aws_use_default_credentials: bool = {if self.aws_use_default_credentials.is_some() {self.aws_use_default_credentials.unwrap()} else {true}};
            let aws_region: String = {if self.aws_region.is_some() {self.aws_region.clone().unwrap()} else {"us-east-1".to_string()}};
            self.aws_client = Some(get_aws_client(aws_use_default_credentials, &aws_region).await);
        }
    }

    async fn read_async(&self)  -> String{
        
        if self.source_location == "aws".to_string() {
            let path_objects: Vec<&str> = self.file_path.split("/").collect();
            let bucket_name = path_objects[2];
            let s3_file_key = path_objects[3..].join("/");
    
            s3::read_from_file(&self.aws_client, bucket_name, &s3_file_key).await
        } else {
            smart_dataset::local::read_from_file(&self.file_path)
        }
    }
    
    async fn write_async(&self, data: String) {
        if self.source_location == "aws".to_string() {
            let path_objects: Vec<&str> = self.file_path.split("/").collect();
            let bucket_name = path_objects[2];
            let s3_file_key = path_objects[3..].join("/");
            s3::write_to_file(&self.aws_client, &data, bucket_name, &s3_file_key).await;
        } else {
            smart_dataset::local::write_to_file(&self.file_path, &data);
        }
    }
}



pub async fn get_aws_client(
    use_default: bool,
    region_name: &str
    ) -> Client{

    let max_attempts = option_env!("AWS_METADATA_SERVICE_NUM_ATTEMPTS").unwrap_or("5").to_string().parse::<u32>().unwrap();
    let timeout_secs = option_env!("AWS_METADATA_SERVICE_TIMEOUT").unwrap_or("120").to_string().parse::<u64>().unwrap();

    // Start with the shared environment configuration.
    let mut conf = 
        if use_default {
            let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
            let shared_config = aws_config::from_env()
                                            .region(region_provider)
                                            .load().await;
            config::Builder::from(&shared_config)
        } else {
            get_aws_credentials(region_name)
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
    region: &str
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

	let region = Region::new(region.to_string());
	
	config::Builder::new().region(region).credentials_provider(cred)

}



#[cfg(test)]
mod tests {
    /* 
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
    */
}
