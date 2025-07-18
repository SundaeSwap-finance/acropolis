//! Helper functions for REST handlers

use crate::messages::{Message, RESTResponse};
use anyhow::Result;
use caryatid_sdk::Context;
use futures::future::Future;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Handle a simple REST request with no parameters
pub fn handle_rest<F, Fut>(
    context: Arc<Context<Message>>,
    topic: &str,
    handler: F,
) -> JoinHandle<()>
where
    F: Fn(Option<&HashMap<String, String>>) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<RESTResponse>> + Send + 'static,
{
    context.handle(topic, move |message: Arc<Message>| {
        let handler = handler.clone();
        async move {
            let response = match message.as_ref() {
                Message::RESTRequest(request) => {
                    info!("REST received {} {}", request.method, request.path);

                    let query_params = extract_query_params(&request.path);
                    info!("REST path elements: {:?}", &request.path_elements);
                    info!("REST path: {:?}", &request.path);

                    match handler(query_params.as_ref()).await {
                        Ok(response) => response,
                        Err(error) => {
                            RESTResponse::with_text(500, &format!("{error:?}").to_string())
                        }
                    }
                }
                _ => {
                    error!("Unexpected message type {:?}", message);
                    RESTResponse::with_text(500, "Unexpected message in REST request")
                }
            };

            Arc::new(Message::RESTResponse(response))
        }
    })
}

/// Handle a simple REST request with one path parameter
pub fn handle_rest_with_parameter<F, Fut>(
    context: Arc<Context<Message>>,
    topic: &str,
    handler: F,
) -> JoinHandle<()>
where
    F: Fn(&[&str]) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<RESTResponse>> + Send + 'static,
{
    let topic_owned = topic.to_string();
    context.handle(topic, move |message: Arc<Message>| {
        let handler = handler.clone();
        let topic_owned = topic_owned.clone();
        async move {
            let response = match message.as_ref() {
                Message::RESTRequest(request) => {
                    info!("REST received {} {}", request.method, request.path);
                    let params_vec = extract_path_params(&topic_owned, &request.path_elements);
                    let params_slice: Vec<&str> = params_vec.iter().map(|s| s.as_str()).collect();

                    if params_slice.is_empty() {
                        RESTResponse::with_text(400, "Parameters must be provided")
                    } else {
                        match handler(&params_slice).await {
                            Ok(response) => response,
                            Err(error) => {
                                RESTResponse::with_text(500, &format!("{error:?}").to_string())
                            }
                        }
                    }
                }
                _ => {
                    error!("Unexpected message type {:?}", message);
                    RESTResponse::with_text(500, "Unexpected message in REST request")
                }
            };

            Arc::new(Message::RESTResponse(response))
        }
    })
}

/// Extract parameters from the request path based on the topic pattern.
/// Skips the first 3 parts of the topic as these are never parameters
fn extract_path_params(topic: &str, path_elements: &[String]) -> Vec<String> {
    let topic_parts: Vec<&str> = topic.split('.').collect();

    // Find indexes of '*' in topic
    let param_positions: Vec<usize> = topic_parts
        .iter()
        .enumerate()
        .filter_map(|(i, &part)| if part == "*" { Some(i) } else { None })
        .collect();

    let offset = 2;

    // Map topic '*' positions to corresponding path elements
    param_positions
        .iter()
        .filter_map(|&pos| pos.checked_sub(offset).and_then(|idx| path_elements.get(idx)).cloned())
        .collect()
}

/// Extract query parameters
fn extract_query_params(path: &str) -> Option<HashMap<String, String>> {
    if let Some((_, query)) = path.split_once('?') {
        let mut params = HashMap::new();
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                params.insert(key.to_string(), value.to_string());
            } else {
                params.insert(pair.to_string(), "".to_string());
            }
        }

        if params.is_empty() {
            None
        } else {
            Some(params)
        }
    } else {
        None
    }
}
