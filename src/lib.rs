pub mod aws_s3;
pub mod local;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use log::{warn};


#[derive(Debug, Default)]
pub struct Dataset {
    file_path: String,
    dataset_type: String,
    aws_region: Option<String>,
    aws_use_default_credentials: Option<bool>,
    aws_client: Option<Client>,
}

#[async_trait]
pub trait SmartDataset {
    fn new(&self, file_path: String) -> Dataset;
    fn read(&self)  -> String;
    fn write(&self, data: String);
    async fn connect(&mut self);
    async fn read_async(&self)  -> String;
    async fn write_async(&self, data: String);
}


#[async_trait]
impl SmartDataset for Dataset {
    fn new(&self, file_path: String) -> Dataset {
        let path_objects: Vec<&str> = self.file_path.split("/").collect();
        let dataset_type = (
            if path_objects[0] == "s3:" {"aws"} 
            else {"local"}
        ).to_string();
        Dataset {
            file_path: file_path,
            dataset_type:  dataset_type,
            ..Default::default() // Replace all other fields with defaults
        }
    }
    
    fn read(&self)  -> String{
        if self.dataset_type == "local".to_string() {
            local::read_from_file(&self.file_path)
        } else {
            warn!("read method not implemented for {}. Consider using read_async. No data being returned", self.dataset_type);
            "".to_string()
        }
    }

    fn write(&self, data: String) {
        if self.dataset_type == "local".to_string() {
            local::write_to_file(&self.file_path, &data)
        } else {
            warn!("write method not implemented for {}. Consider using write_async. No data was written", self.dataset_type);
        }
    }

    async fn connect(&mut self) {
        if self.dataset_type == "aws".to_string() {
            let aws_use_default_credentials: bool = {if self.aws_use_default_credentials.is_some() {self.aws_use_default_credentials.unwrap()} else {true}};
            let aws_region: String = {if self.aws_region.is_some() {self.aws_region.clone().unwrap()} else {"us-east-1".to_string()}};
            self.aws_client = Some(aws_s3::get_aws_client(aws_use_default_credentials, &aws_region).await);
        }
    }

    async fn read_async(&self)  -> String{
        
        if self.dataset_type == "aws".to_string() {
            let path_objects: Vec<&str> = self.file_path.split("/").collect();
            let bucket_name = path_objects[2];
            let s3_file_key = path_objects[3..].join("/");
    
            aws_s3::read_from_file(&self.aws_client, bucket_name, &s3_file_key).await
        } else {
            local::read_from_file(&self.file_path)
        }
    }
    
    async fn write_async(&self, data: String) {
        if self.dataset_type == "aws".to_string() {
            let path_objects: Vec<&str> = self.file_path.split("/").collect();
            let bucket_name = path_objects[2];
            let s3_file_key = path_objects[3..].join("/");
            aws_s3::write_to_file(&self.aws_client, &data, bucket_name, &s3_file_key).await;
        } else {
            local::write_to_file(&self.file_path, &data);
        }
    }
}



#[cfg(test)]
mod tests {

}
