use crate::search::EmbeddingClientConfig;
use anyhow::{anyhow, bail, Result as AnyhowResult};
use document::{Document, NormalValue};
use rapidhash::RapidHashSet;
use schema::VectorEmbeddingDescription;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use tracing::warn;

const DEFAULT_OLLAMA_URL: &str = "http://localhost:11434/api";
const DEFAULT_OPENAI_URL: &str = "https://api.openai.com/v1";

#[cfg(not(target_arch = "wasm32"))]
type EmbeddingError = Box<dyn std::error::Error + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type EmbeddingError = Box<dyn std::error::Error>;

/// Generate and set embedding vectors on a document based on the collection's
/// vector embedding configuration.
///
/// Returns the names of fields that were generated (so the caller can add them
/// to modified_fields for block creation).
///
/// On create: generates embeddings for all configured fields unless the user
/// already provided the vector value. Runs BEFORE doc ID generation so the
/// content-addressed ID includes embedding values.
///
/// On update: generates embeddings only if a source field was modified.
/// Skips generation if the user explicitly set the vector field.
pub async fn set_embedding(
    embeddings: &[VectorEmbeddingDescription],
    doc: &mut Document,
    is_create: bool,
    modified_fields: Option<&RapidHashSet<String>>,
    embedding_config: &EmbeddingClientConfig,
) -> Result<Vec<String>, EmbeddingError> {
    let mut generated = Vec::new();

    for embedding in embeddings {
        // Skip if user explicitly provided the embedding vector
        if is_create {
            if let Some(fv) = doc.get_field_value(&embedding.field_name) {
                if fv.is_dirty() {
                    continue;
                }
            }
        } else if let Some(fields) = modified_fields {
            if fields.contains(&embedding.field_name) {
                continue;
            }
        }

        // On update, check if any source field was modified
        if !is_create {
            if let Some(fields) = modified_fields {
                let any_source_modified = embedding.fields.iter().any(|f| fields.contains(f));
                if !any_source_modified {
                    continue;
                }
            }
        }

        let resolved_config = match resolve_embedding_config(embedding, embedding_config) {
            Ok(config) => config,
            // Missing runtime config is non-fatal by design: collection schemas may
            // declare embeddings while node-level defaults are intentionally absent.
            Err(MissingEmbeddingConfig::Url) => {
                warn!(
                    field = %embedding.field_name,
                    "embedding URL is empty, skipping embedding generation"
                );
                continue;
            }
            Err(MissingEmbeddingConfig::Model) => {
                warn!(
                    field = %embedding.field_name,
                    "embedding model is empty, skipping embedding generation"
                );
                continue;
            }
        };

        // Build text from source field values
        let mut text = String::new();
        for field_name in &embedding.fields {
            if let Some(val) = doc.get(field_name) {
                let s = normal_value_to_string(val);
                text.push_str(&s);
                text.push('\n');
            }
        }

        let vec = call_embedding(
            &embedding.provider,
            resolved_config.url,
            resolved_config.model,
            &embedding_config.api_key,
            &text,
        )
        .await?;

        doc.set(&embedding.field_name, NormalValue::Float64Array(vec));
        generated.push(embedding.field_name.clone());
    }

    Ok(generated)
}

/// Exact provider request needed to resolve an embedding after an attempt
/// discovered newer source fields. The API key stays in this internal byte
/// request and is redacted by `PreparedInputRequest::Debug`.
#[derive(Serialize, Deserialize)]
struct PreparedEmbeddingRequest {
    provider: String,
    url: String,
    model: String,
    api_key: String,
    text: String,
}

pub(crate) fn prepared_embedding_request(
    embedding: &VectorEmbeddingDescription,
    doc: &Document,
    config: &EmbeddingClientConfig,
) -> Result<query::prepared::PreparedInputRequest, String> {
    let resolved =
        resolve_embedding_config(embedding, config).map_err(|missing| match missing {
            MissingEmbeddingConfig::Url => "embedding URL is empty".to_string(),
            MissingEmbeddingConfig::Model => "embedding model is empty".to_string(),
        })?;
    let mut text = String::new();
    for field in &embedding.fields {
        if let Some(value) = doc.get(field) {
            text.push_str(&normal_value_to_string(value));
            text.push('\n');
        }
    }
    let request = PreparedEmbeddingRequest {
        provider: embedding.provider.clone(),
        url: resolved.url.to_string(),
        model: resolved.model.to_string(),
        api_key: config.api_key.clone(),
        text,
    };
    let bytes = serde_json::to_vec(&request).map_err(|error| error.to_string())?;
    Ok(query::prepared::PreparedInputRequest::new(
        query::prepared::PreparedInputKind::Embedding,
        bytes,
    ))
}

pub(crate) async fn resolve_prepared_embedding_request(request: &[u8]) -> Result<Vec<u8>, String> {
    let request: PreparedEmbeddingRequest =
        serde_json::from_slice(request).map_err(|error| error.to_string())?;
    let embedding = call_embedding(
        &request.provider,
        &request.url,
        &request.model,
        &request.api_key,
        &request.text,
    )
    .await
    .map_err(|error| error.to_string())?;
    serde_json::to_vec(&embedding).map_err(|error| error.to_string())
}

/// Embed free-form text using DefraDB's v1 embedding contract.
///
/// This expects an OpenAI-compatible `/embeddings` endpoint and sends:
/// `{ "model": "...", "input": "..." }`.
pub async fn embed_text(
    embedding_config: &EmbeddingClientConfig,
    text: &str,
    model: Option<&str>,
) -> AnyhowResult<Vec<f64>> {
    embed_text_with_provider(embedding_config, "openai", text, model).await
}

/// Embed query text using the same provider contract as document embeddings.
pub async fn embed_text_with_provider(
    embedding_config: &EmbeddingClientConfig,
    provider: &str,
    text: &str,
    model: Option<&str>,
) -> AnyhowResult<Vec<f64>> {
    let url = embedding_config.url.trim();
    if url.is_empty() {
        bail!("embedding URL is empty");
    }

    let resolved_model = model
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .or_else(|| {
            let default_model = embedding_config.model.trim();
            (!default_model.is_empty()).then_some(default_model)
        })
        .ok_or_else(|| anyhow!("embedding model is empty"))?;

    let vector = call_embedding(
        provider,
        url,
        resolved_model,
        &embedding_config.api_key,
        text,
    )
    .await
    .map_err(|err| anyhow!(err.to_string()))?;
    Ok(vector)
}

fn normal_value_to_string(val: &NormalValue) -> String {
    match val {
        NormalValue::String(s) => s.clone(),
        NormalValue::Int(i) => i.to_string(),
        NormalValue::Float64(f) => format!("{}", f),
        NormalValue::Float32(f) => format!("{}", f),
        NormalValue::Bool(b) => b.to_string(),
        other => format!("{:?}", other),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum MissingEmbeddingConfig {
    Url,
    Model,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ResolvedEmbeddingConfig<'a> {
    pub url: &'a str,
    pub model: &'a str,
}

pub fn resolve_embedding_config<'a>(
    embedding: &'a VectorEmbeddingDescription,
    embedding_config: &'a EmbeddingClientConfig,
) -> Result<ResolvedEmbeddingConfig<'a>, MissingEmbeddingConfig> {
    let url = if !embedding.url.is_empty() {
        embedding.url.as_str()
    } else if !embedding_config.url.is_empty() {
        embedding_config.url.as_str()
    } else {
        match embedding.provider.as_str() {
            "ollama" => DEFAULT_OLLAMA_URL,
            "openai" => DEFAULT_OPENAI_URL,
            _ => return Err(MissingEmbeddingConfig::Url),
        }
    };

    let model = if embedding.model.is_empty() {
        embedding_config.model.as_str()
    } else {
        embedding.model.as_str()
    };
    if model.is_empty() {
        return Err(MissingEmbeddingConfig::Model);
    }

    Ok(ResolvedEmbeddingConfig { url, model })
}

/// The form every embedding URL is used in: surrounding whitespace gone and
/// trailing slashes dropped, so `https://host/api/` and `https://host/api`
/// are one endpoint everywhere a URL is compared or joined.
pub(crate) fn normalized_embedding_url(url: &str) -> &str {
    url.trim().trim_end_matches('/')
}

async fn call_embedding(
    provider: &str,
    url: &str,
    model: &str,
    api_key: &str,
    text: &str,
) -> Result<Vec<f64>, EmbeddingError> {
    let endpoint = format!("{}/embeddings", normalized_embedding_url(url));
    let (body, response_pointer) = match provider {
        "ollama" => (
            serde_json::json!({ "model": model, "prompt": text }),
            "/embedding",
        ),
        "openai" => (
            serde_json::json!({ "model": model, "input": text }),
            "/data/0/embedding",
        ),
        _ => return Err(format!("unsupported embedding provider: {provider}").into()),
    };

    let mut request = embedding_client().post(&endpoint);
    #[cfg(not(target_arch = "wasm32"))]
    {
        request = request.timeout(std::time::Duration::from_secs(30));
    }
    if provider == "openai" && !api_key.is_empty() {
        request = request.bearer_auth(api_key);
    }

    let resp = request.json(&body).send().await?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("embedding provider returned {}: {}", status, text).into());
    }

    let result: serde_json::Value = resp.json().await?;
    let embedding = result
        .pointer(response_pointer)
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("embedding response missing {response_pointer}"))?;

    let mut vec =
        parse_embedding_vector(embedding).map_err(|err| -> EmbeddingError { err.into() })?;
    if provider == "ollama" {
        normalize_embedding(&mut vec)?;
    }

    Ok(vec)
}

fn normalize_embedding(embedding: &mut [f64]) -> Result<(), EmbeddingError> {
    let magnitude = embedding
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    if magnitude == 0.0 {
        return Err("embedding response contains a zero vector".into());
    }
    if (magnitude - 1.0).abs() >= 1e-6 {
        for value in embedding {
            *value /= magnitude;
        }
    }
    Ok(())
}

fn embedding_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

pub fn parse_embedding_vector(embedding: &[serde_json::Value]) -> Result<Vec<f64>, String> {
    embedding
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .as_f64()
                .ok_or_else(|| format!("embedding value at index {} is not numeric", index))
        })
        .collect()
}

#[cfg(test)]
mod prepared_input_tests {
    use super::*;
    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn retries_reuse_exact_embedding_response_and_changed_source_fetches_again() {
        let provider_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let call_count = std::sync::Arc::clone(&provider_calls);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/embeddings",
                    post(move |Json(_request): Json<Value>| {
                        let calls = std::sync::Arc::clone(&call_count);
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Json(json!({"data": [{"embedding": [1.0, 2.0]}]}))
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });

        let description = VectorEmbeddingDescription {
            field_name: "vector".to_string(),
            fields: vec!["text".to_string()],
            model: "test-model".to_string(),
            provider: "openai".to_string(),
            template: String::new(),
            url: String::new(),
        };
        let config = EmbeddingClientConfig::new()
            .with_url(format!("http://{address}"))
            .with_model("test-model");
        let memo = query::prepared::PreparedInputMemo::default();
        let mut doc = Document::new();
        doc.set("text", "source A");
        let first = prepared_embedding_request(&description, &doc, &config).unwrap();

        for _ in 0..2 {
            if memo.get(&first).is_none() {
                let bytes = resolve_prepared_embedding_request(first.exact_request())
                    .await
                    .unwrap();
                assert!(memo.insert(first.clone(), bytes));
            }
        }
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);

        doc.set("text", "source B");
        let changed = prepared_embedding_request(&description, &doc, &config).unwrap();
        assert!(memo.get(&changed).is_none());
        let bytes = resolve_prepared_embedding_request(changed.exact_request())
            .await
            .unwrap();
        assert!(memo.insert(changed, bytes));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 2);

        server.abort();
    }
}
