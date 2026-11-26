//! Project bounded OTLP JSON into the fields the store is allowed to retain.
use super::{Error, RETENTION_NS, config::Config};
use ring::hmac;
use serde::Deserialize;
use serde_json::{Map, Value, value::RawValue};

/// One model call. A missing metric cannot invalidate a different metric.
pub(super) struct Sample {
    pub harness: &'static str,
    pub time_ns: i64,
    pub digest: [u8; 32],
    pub provider: String,
    pub model: String,
    pub effort: Option<String>,
    pub tokens: [Option<i64>; 6],
    pub reasoning_tokens: Option<i64>,
    pub duration_ms: Option<f64>,
    pub ttft_ms: Option<f64>,
}

/// Normalized samples, Codex pairing events and OTLP record-level drop counts.
#[derive(Default)]
pub(super) struct Batch {
    pub samples: Vec<Sample>,
    pub codex: Vec<super::codex::Event>,
    pub ignored: usize,
    pub invalid: usize,
    pub expired: usize,
}

impl Batch {
    /// Count records eligible for storage, including Codex completions awaiting pairing.
    pub(super) fn sample_count(&self) -> usize {
        self.samples.len()
            + self
                .codex
                .iter()
                .filter(|event| matches!(event.kind, super::codex::Kind::Completed(_)))
                .count()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Export<'a> {
    #[serde(default, borrow)]
    resource_logs: Vec<ResourceLogs<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceLogs<'a> {
    #[serde(default, borrow)]
    scope_logs: Vec<ScopeLogs<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeLogs<'a> {
    #[serde(default, borrow)]
    log_records: Vec<Record<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record<'a> {
    #[serde(default, borrow)]
    body: Option<Body<'a>>,
    #[serde(default)]
    time_unix_nano: Value,
    #[serde(default, borrow)]
    attributes: Option<&'a RawValue>,
}

/// Inspect only the event name in the body.
#[derive(Deserialize)]
struct Body<'a> {
    #[serde(rename = "stringValue", default, borrow)]
    name: Option<&'a RawValue>,
}

/// Borrow values so unrecognized attributes are never decoded as JSON trees.
#[derive(Deserialize)]
struct Attribute<'a> {
    key: String,
    #[serde(borrow)]
    value: &'a RawValue,
}

/// Decode only envelope fields and keep payload values only through the projection.
pub(super) fn normalize(
    input: &[u8],
    config: &Config,
    key: &hmac::Key,
    now: i64,
) -> Result<Batch, Error> {
    let export: Export<'_> = serde_json::from_slice(input).map_err(|_| Error("invalid_json"))?;
    let records = export
        .resource_logs
        .into_iter()
        .flat_map(|resource| resource.scope_logs)
        .flat_map(|scope| scope.log_records);
    let mut batch = Batch::default();
    for (index, record) in records.enumerate() {
        if index >= config.max_records {
            return Err(Error("too_many_records"));
        }
        let event = record
            .body
            .as_ref()
            .and_then(|body| body.name)
            .and_then(|name| serde_json::from_str::<String>(name.get()).ok());
        let attributes = decode_attributes(record.attributes);
        if event.as_deref() != Some("claude_code.api_request") {
            super::codex::project(&attributes, key, now, &mut batch);
            continue;
        }
        match sample(&record.time_unix_nano, &attributes, config, key) {
            Some(sample) if sample.time_ns < now - RETENTION_NS => batch.expired += 1,
            Some(sample) if sample.time_ns <= now => batch.samples.push(sample),
            _ => batch.invalid += 1,
        }
    }
    Ok(batch)
}

/// Decode only allowlisted attributes.
fn decode_attributes(raw: Option<&RawValue>) -> Map<String, Value> {
    let input: Vec<Attribute<'_>> = raw
        .and_then(|value| serde_json::from_str(value.get()).ok())
        .unwrap_or_default();
    input
        .into_iter()
        .filter(|attribute| {
            matches!(
                attribute.key.as_str(),
                "model"
                    | "effort"
                    | "request_id"
                    | "client_request_id"
                    | "session.id"
                    | "event.sequence"
                    | "anthropic.custom_base_url"
                    | "anthropic.base_url"
                    | "duration_ms"
                    | "ttft_ms"
                    | "input_tokens"
                    | "output_tokens"
                    | "cache_read_tokens"
                    | "cache_creation_tokens"
                    | "cache_creation_5m_tokens"
                    | "cache_creation_1h_tokens"
                    | "event.name"
                    | "event.kind"
                    | "event.timestamp"
                    | "conversation.id"
                    | "model_reasoning_effort"
                    | "input_token_count"
                    | "output_token_count"
                    | "cached_token_count"
                    | "cache_write_token_count"
                    | "reasoning_token_count"
            )
        })
        .filter_map(|attribute| {
            Some((
                attribute.key,
                serde_json::from_str(attribute.value.get()).ok()?,
            ))
        })
        .collect()
}

/// Build a Claude sample only when identity and timestamp support deduplication.
fn sample(
    time: &Value,
    attributes: &Map<String, Value>,
    config: &Config,
    key: &hmac::Key,
) -> Option<Sample> {
    let string = |name: &str| attributes.get(name)?.get("stringValue")?.as_str();
    let model = string("model").filter(|value| label(value))?;
    // Custom gateways (OpenCode Go, Z.ai) may omit the provider request id; Claude Code always sets its own
    // client request id, which still identifies one API call for deduplication.
    let usable = |value: &&str| !value.is_empty() && value.len() <= 512;
    // Older Claude Code builds behind gateways send neither id; the per-session event sequence is unique
    // within one session. The session id only enters the HMAC digest and is never stored.
    let sequence = attributes
        .get("event.sequence")
        .and_then(|value| integer(&value["intValue"]))
        .zip(string("session.id").filter(usable))
        .map(|(sequence, session)| format!("seq:{session}/{sequence}"));
    let id = string("request_id")
        .filter(usable)
        .or_else(|| string("client_request_id").filter(usable))
        .map(str::to_owned)
        .or(sequence)?;
    let provider = match attributes
        .get("anthropic.custom_base_url")
        .and_then(|value| value["boolValue"].as_bool())
    {
        Some(false) => "anthropic",
        Some(true) => string("anthropic.base_url")
            .and_then(|url| config.provider_labels.get(url))
            .or_else(|| {
                config
                    .model_providers
                    .iter()
                    .filter(|(prefix, _)| model.starts_with(prefix.as_str()))
                    .max_by_key(|(prefix, _)| prefix.len())
                    .map(|(_, label)| label)
            })
            .map_or("custom", String::as_str),
        None => "unknown",
    };
    let metric = |name: &str| {
        let value = attributes.get(name)?;
        let integer = integer(&value["intValue"]).map(|value| value as f64);
        integer
            .or_else(|| value["doubleValue"].as_f64())
            .filter(|value| value.is_finite() && (0.0..=i64::MAX as f64).contains(value))
    };
    Some(Sample {
        harness: "claude-code",
        time_ns: integer(time).filter(|time| *time > 0)?,
        digest: hmac::sign(key, id.as_bytes()).as_ref().try_into().ok()?,
        provider: provider.to_owned(),
        model: model.to_owned(),
        effort: string("effort")
            .filter(|value| label(value))
            .map(str::to_owned),
        tokens: [
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_creation_tokens",
            "cache_creation_5m_tokens",
            "cache_creation_1h_tokens",
        ]
        .map(|name| integer(&attributes.get(name)?["intValue"]).filter(|value| *value >= 0)),
        reasoning_tokens: None,
        duration_ms: metric("duration_ms"),
        ttft_ms: metric("ttft_ms"),
    })
}

/// Accept protobuf JSON integers in string and numeric encodings.
pub(super) fn integer(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

/// Keep model, effort and configured provider labels bounded and free of URL or email syntax.
pub(super) fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'[' | b']' | b'/')
        })
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "Test setup failures should include their cause"
)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Resolve a custom label only when the event provides an exact configured URL.
    #[test]
    fn custom_provider_mapping() {
        let mut event: Value =
            serde_json::from_str(include_str!("../../tests/claude_api_request.json"))
                .expect("Claude Code fixture");
        let sample = &mut event["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
        let time: i64 = sample["timeUnixNano"]
            .as_str()
            .expect("timestamp")
            .parse()
            .expect("nanos");
        let attributes = sample["attributes"].as_array_mut().expect("attributes");
        let flag = attributes
            .iter_mut()
            .find(|attr| attr["key"] == "anthropic.custom_base_url")
            .expect("base URL flag");
        flag["value"] = json!({"boolValue":true});
        attributes.push(
            json!({"key":"anthropic.base_url","value":{"stringValue":"https://example.invalid"}}),
        );
        let mut config = Config::default();
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"fixture digest key");
        let body = event.to_string();
        let batch = normalize(body.as_bytes(), &config, &key, time).expect("normalize");
        assert_eq!(batch.samples[0].provider, "custom");
        config
            .provider_labels
            .insert("https://example.invalid".into(), "example".into());
        let batch = normalize(body.as_bytes(), &config, &key, time).expect("normalize");
        assert_eq!(batch.samples[0].provider, "example");
        // Without a base URL the model prefix decides; the longest prefix wins.
        config.provider_labels.clear();
        let model = batch.samples[0].model.clone();
        config
            .model_providers
            .insert(model[..1].to_owned(), "short".into());
        config
            .model_providers
            .insert(model[..3].to_owned(), "long".into());
        let batch = normalize(body.as_bytes(), &config, &key, time).expect("normalize");
        assert_eq!(batch.samples[0].provider, "long");
    }

    /// Custom gateways may omit request_id; the client request id still identifies the call, and
    /// bracketed model names such as `glm-5.4[1m]` are kept.
    #[test]
    fn gateway_without_request_id_uses_client_request_id() {
        let mut event: Value =
            serde_json::from_str(include_str!("../../tests/claude_api_request.json"))
                .expect("Claude Code fixture");
        let sample = &mut event["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
        let time: i64 = sample["timeUnixNano"]
            .as_str()
            .expect("timestamp")
            .parse()
            .expect("nanos");
        let attributes = sample["attributes"].as_array_mut().expect("attributes");
        attributes.retain(|attr| {
            ![
                "request_id",
                "client_request_id",
                "session.id",
                "event.sequence",
            ]
            .contains(&attr["key"].as_str().unwrap_or(""))
        });
        for attr in attributes.iter_mut() {
            if attr["key"] == "model" {
                attr["value"] = json!({"stringValue":"glm-5.4[1m]"});
            }
        }
        let config = Config::default();
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"fixture digest key");
        let without = event.to_string();
        assert_eq!(
            normalize(without.as_bytes(), &config, &key, time)
                .expect("normalize")
                .invalid,
            1
        );
        event["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"]
            .as_array_mut()
            .expect("attributes")
            .push(json!({"key":"client_request_id","value":{"stringValue":"client-1"}}));
        let batch =
            normalize(event.to_string().as_bytes(), &config, &key, time).expect("normalize");
        assert_eq!(batch.samples.len(), 1);
        assert_eq!(batch.samples[0].model, "glm-5.4[1m]");
        let mut by_sequence = event.clone();
        let attrs = by_sequence["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"]
            .as_array_mut()
            .expect("attributes");
        attrs.retain(|attr| attr["key"] != "client_request_id");
        attrs.push(json!({"key":"session.id","value":{"stringValue":"session-1"}}));
        attrs.push(json!({"key":"event.sequence","value":{"intValue":"7"}}));
        let batch =
            normalize(by_sequence.to_string().as_bytes(), &config, &key, time).expect("normalize");
        assert_eq!(batch.samples.len(), 1);
    }
}
