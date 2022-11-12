use async_trait::async_trait;
use log::warn;
pub mod local;


#[async_trait]
pub trait SmartDataset {
    fn read(&self)  -> String {"".to_string()}
    fn write(&self, _data: String) {}
    async fn connect(&mut self) {warn!("Not Implemented")}
    async fn read_async(&self)  -> String {warn!("Not Implemented"); "".to_string()}
    async fn write_async(&self, _data: String) {warn!("Not Implemented")}
}
