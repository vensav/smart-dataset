pub mod aws_s3;
pub mod local;


pub async fn read_async(file_path: String, 
    aws_use_default: Option<bool>, 
    aws_region: Option<String>)  -> String{
    
    let path_objects: Vec<&str> = file_path.split("/").collect();
    if path_objects[0] == "s3".to_string() {
        let bucket_name = path_objects[2];
        let s3_file_key = path_objects[3..].join("/");
        let aws_client = aws_s3::get_aws_client(aws_use_default, aws_region).await;
        aws_s3::read_from_file(&aws_client, bucket_name, &s3_file_key).await
    } else {
        local::read_from_file(&file_path)
    }
}

pub async fn write_async(file_path: String, data: String,
    aws_use_default: Option<bool>, 
    aws_region: Option<String>) {
    
    let path_objects: Vec<&str> = file_path.split("/").collect();
    if path_objects[0] == "s3".to_string() {
        let bucket_name = path_objects[2];
        let s3_file_key = path_objects[3..].join("/");
        let aws_client = aws_s3::get_aws_client(aws_use_default, aws_region).await;
        aws_s3::write_to_file(&aws_client, &data, bucket_name, &s3_file_key).await;
    } else {
        local::write_to_file(&file_path, &data);
    }
}