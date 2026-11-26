//! Project bounded OTLP JSON into the fields the store is allowed to retain.
use super::{Error, RETENTION_NS, config::Config};
use ring::hmac;
use serde::Deserialize;
use serde_json::{Map, Value, value::RawValue};

/// One model call. A missing metric cannot invalidate a different metric.
pub(super) struct Sample {
    pub time_ns: i64,
    pub digest: [u8; 32],
    pub provider: String,
    pub model: String,
    pub effort: Option<String>,
    pub tokens: [Option<i64>; 6],
    pub duration_ms: Option<f64>,
    pub ttft_ms: Option<f64>,
}

/// Normalized batch and OTLP record-level drop counts.
#[derive(Default)]
pub(super) struct Batch {
    pub samples: Vec<Sample>,
    pub ignored: usize,
    pub invalid: usize,
    pub expired: usize,
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
        if event.as_deref() != Some("claude_code.api_request") {
            batch.ignored += 1;
            continue;
        }
        match sample(record, config, key) {
            Some(sample) if sample.time_ns < now - RETENTION_NS => batch.expired += 1,
            Some(sample) if sample.time_ns <= now => batch.samples.push(sample),
            _ => batch.invalid += 1,
        }
    }
    Ok(batch)
}

/// Build a sample only when identity and timestamp can support storage and deduplication.
fn sample(record: Record<'_>, config: &Config, key: &hmac::Key) -> Option<Sample> {
    let input: Vec<Attribute<'_>> = serde_json::from_str(record.attributes?.get()).ok()?;
    let attributes: Map<String, Value> = input
        .into_iter()
        .filter(|attribute| {
            matches!(
                attribute.key.as_str(),
                "model"
                    | "effort"
                    | "request_id"
                    | "client_request_id"
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
            )
        })
        .filter_map(|attribute| {
            Some((
                attribute.key,
                serde_json::from_str(attribute.value.get()).ok()?,
            ))
        })
        .collect();
    let string = |name: &str| attributes.get(name)?.get("stringValue")?.as_str();
    let model = string("model").filter(|value| label(value))?;
    // Custom gateways (OpenCode Go, Z.ai) may omit the provider request id; Claude Code always sets its own
    // client request id, which still identifies one API call for deduplication.
    let usable = |value: &&str| !value.is_empty() && value.len() <= 512;
    let id = string("request_id")
        .filter(usable)
        .or_else(|| string("client_request_id").filter(usable))?;
    let provider = match attributes
        .get("anthropic.custom_base_url")
        .and_then(|value| value["boolValue"].as_bool())
    {
        Some(false) => "anthropic",
        Some(true) => string("anthropic.base_url")
            .and_then(|url| config.provider_labels.get(url))
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
        time_ns: integer(&record.time_unix_nano).filter(|time| *time > 0)?,
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
        duration_ms: metric("duration_ms"),
        ttft_ms: metric("ttft_ms"),
    })
}

/// Accept protobuf JSON integers in string and numeric encodings.
fn integer(value: &Value) -> Option<i64> {
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
        attributes.retain(|attr| attr["key"] != "request_id" && attr["key"] != "client_request_id");
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
    }
}
