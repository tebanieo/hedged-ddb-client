use aws_sdk_dynamodb::{Client, types::AttributeValue};
use aws_sdk_dynamodb::operation::get_item::GetItemOutput;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use std::collections::HashMap;
use async_trait::async_trait;
use crate::hedged_client::DynamoOperation;

#[derive(Clone)]
pub struct GetItemOperation {
    pub table_name: String,
    pub key: HashMap<String, AttributeValue>,
}

#[async_trait]
impl DynamoOperation for GetItemOperation {
    type Output = GetItemOutput;
    type Error = SdkError<GetItemError>;

    async fn execute(&self, client: &Client) -> Result<Self::Output, Self::Error> {
        client.get_item()
            .table_name(&self.table_name)
            .set_key(Some(self.key.clone()))
            .send()
            .await
    }
}
