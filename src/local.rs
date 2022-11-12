use std::{io::{Write, Read}, fs::File};


pub fn write_to_file(file_name: &str, data: &str){
    let mut file = std::fs::File::create(file_name).unwrap_or_else(|_| panic!("create failed for {file_name}"));
    file.write_all(data.as_bytes()).unwrap_or_else(|_| panic!("write failed for {file_name}"));
}


pub fn read_from_file(file_name: &str) -> String {

    let mut f = File::open(file_name)
    .unwrap_or_else(|_| panic!("file not found: {}", &file_name));
    
    let mut contents = String::new();

    f.read_to_string(&mut contents)
        .unwrap_or_else(|_| panic!("cannot read file {}", &file_name));
    
    contents
}

