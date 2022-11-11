# smart-dataset

A wrapper for reading / writing from either a local or cloud hosted file


## Local Files

### Read
```
    let local_file_name = "my_folder/test.csv".to_string();
    let contents = smart_dataset::read_from_file(&local_file_name);
```

### Write
```
    let data = "some data".to_string();
    let local_file_name = "my_folder/test.csv".to_string();
    let contents = smart_dataset::write_to_file(&local_file_name, &data);
```


## S3 Datasets

### Read from S3
```
    let aws_client = smart_dataset::aws_s3::get_aws_client(false, AWS_S3_REGION).await;
    let s3_file_name = "my_folder/test.csv".to_string();
    let contents = smart_dataset::aws_s3::read_from_file(&aws_client, S3_BUCKET_NAME, &s3_file_name).await;
```

### Write to S3
```
    let aws_client = smart_dataset::aws_s3::get_aws_client(false, AWS_S3_REGION).await;
    let data = "some data".to_string();
    let s3_file_name = "my_folder/test.csv".to_string();;
    let result = smart_dataset::aws_s3::write_to_file(aws_client, S3_BUCKET_NAME, &data, &s3_file_name);
    result.await;
```