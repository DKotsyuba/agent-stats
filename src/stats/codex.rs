//! Allowlisted Codex telemetry and bounded request pairing owned by the SQLite writer.
use super::{
    RETENTION_NS,
    normalize::{Batch, Sample, integer, label},
};
use ring::hmac;
use serde_json::{Map, Value};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

const TTL: Duration = Duration::from_secs(15 * 60);
const MAX_CONVERSATIONS: usize = 1024;
const MAX_REQUESTS: usize = 16;

type Digest = [u8; 32];

/// A request or completion with its conversation pseudonym and event time.
pub(super) struct Event {
    pub time_ns: i64,
    pub kind: Kind,
    conversation: Digest,
}

/// Only completions can create sample rows.
pub(super) enum Kind {
    Request(String),
    Completed(Box<Sample>),
}

/// Project the Codex event namespace independently of the exporter originator.
pub(super) fn project(
    attributes: &Map<String, Value>,
    key: &hmac::Key,
    now: i64,
    batch: &mut Batch,
) {
    let string = |name: &str| attributes.get(name)?.get("stringValue")?.as_str();
    let completed = match string("event.name") {
        Some("codex.websocket_request") => false,
        Some("codex.sse_event") if string("event.kind") == Some("response.completed") => true,
        _ => {
            batch.ignored += 1;
            return;
        }
    };
    let identity = || {
        let time_ns =
            utc_ns(string("event.timestamp")?).filter(|time| *time > 0 && *time <= now)?;
        let conversation =
            string("conversation.id").filter(|id| !id.is_empty() && id.len() <= 512)?;
        let model = string("model").filter(|value| label(value))?;
        Some((time_ns, conversation, model))
    };
    let Some((time_ns, conversation, model)) = identity() else {
        batch.invalid += 1;
        return;
    };
    if time_ns < now - RETENTION_NS {
        batch.expired += 1;
        return;
    }

    let kind = if completed {
        let count = |name: &str| {
            let value = attributes.get(name)?;
            integer(&value["intValue"])
                .or_else(|| integer(&value["stringValue"]))
                .filter(|value| *value >= 0)
        };
        let ttft_ms = count("ttft_ms")
            .map(|value| value as f64)
            .or_else(|| string("ttft_ms")?.parse().ok())
            .or_else(|| attributes.get("ttft_ms")?["doubleValue"].as_f64())
            .filter(|value| (0.0..=i64::MAX as f64).contains(value));
        Kind::Completed(Box::new(Sample {
            harness: "codex",
            time_ns,
            digest: digest(
                key,
                b"codex completion",
                conversation,
                &[&time_ns.to_be_bytes(), model.as_bytes()],
            ),
            provider: "openai".into(),
            model: model.into(),
            effort: string("model_reasoning_effort")
                .filter(|value| label(value))
                .map(str::to_owned),
            tokens: [
                count("input_token_count"),
                count("output_token_count"),
                count("cached_token_count"),
                count("cache_write_token_count"),
                None,
                None,
            ],
            reasoning_tokens: count("reasoning_token_count"),
            duration_ms: None,
            ttft_ms,
        }))
    } else {
        Kind::Request(model.into())
    };
    batch.codex.push(Event {
        time_ns,
        kind,
        conversation: digest(key, b"codex conversation", conversation, &[]),
    });
}

/// Parse the UTC RFC 3339 form Codex emits (`2026-11-26T22:51:33.061Z`, optional
/// fraction up to nanoseconds, `Z` or `+00:00`) into Unix nanoseconds.
fn utc_ns(text: &str) -> Option<i64> {
    let text = text
        .strip_suffix('Z')
        .or_else(|| text.strip_suffix("+00:00"))?;
    let (date, time) = text.split_once('T')?;
    let field = |value: &str, len: usize| {
        (value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| value.parse::<i64>().ok())
            .flatten()
    };
    let mut date = date.split('-');
    let (year, month, day) = (
        field(date.next()?, 4)?,
        field(date.next()?, 2)?,
        field(date.next()?, 2)?,
    );
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut clock = clock.split(':');
    let (hour, minute, second) = (
        field(clock.next()?, 2)?,
        field(clock.next()?, 2)?,
        field(clock.next()?, 2)?,
    );
    if date.next().is_some()
        || clock.next().is_some()
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
        || fraction.len() > 9
        || (time.contains('.') && fraction.is_empty())
    {
        return None;
    }
    let nanos = if fraction.is_empty() {
        0
    } else {
        field(fraction, fraction.len())? * 10_i64.pow(9 - fraction.len() as u32)
    };
    // Days from civil (Howard Hinnant), proleptic Gregorian calendar.
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

/// Domain-separate identities and length-prefix the variable conversation component.
fn digest(key: &hmac::Key, domain: &[u8], conversation: &str, suffix: &[&[u8]]) -> Digest {
    let mut context = hmac::Context::with_key(key);
    context.update(domain);
    context.update(&(conversation.len() as u64).to_be_bytes());
    context.update(conversation.as_bytes());
    for part in suffix {
        context.update(part);
    }
    let mut digest = [0; 32];
    digest.copy_from_slice(context.sign().as_ref());
    digest
}

/// Pairing state is staged with the write transaction so rejected batches can be replayed.
#[derive(Clone, Default)]
pub(super) struct Pairing {
    conversations: HashMap<Digest, Pending>,
    saturated_until: Option<Instant>,
}

/// Pending requests and the latest committed completion for one conversation.
#[derive(Clone)]
struct Pending {
    touched: Instant,
    completed_at: i64,
    requests: Vec<Request>,
    overflow: Option<(i64, Instant)>,
}

/// One observed WebSocket request, with a monotonic expiry independent of client clocks.
#[derive(Clone)]
struct Request {
    model: String,
    time_ns: i64,
    observed: Instant,
}

impl Pairing {
    /// Drop inactive conversations and expired request observations after 15 minutes.
    pub(super) fn expire(&mut self, now: Instant) {
        self.conversations.retain(|_, pending| {
            pending
                .requests
                .retain(|request| now.duration_since(request.observed) < TTL);
            pending.overflow = pending
                .overflow
                .filter(|(_, observed)| now.duration_since(*observed) < TTL);
            now.duration_since(pending.touched) < TTL
        });
    }

    /// Remember a request, discarding certainty if a memory bound is reached.
    pub(super) fn request(&mut self, event: &Event, model: &str, now: Instant) {
        if let Some(pending) = self.pending(event.conversation, now)
            && event.time_ns > pending.completed_at
        {
            if pending.requests.len() == MAX_REQUESTS {
                let latest = pending
                    .overflow
                    .map_or(event.time_ns, |(time, _)| time.max(event.time_ns));
                pending.overflow = Some((latest, now));
            } else {
                pending.requests.push(Request {
                    model: model.into(),
                    time_ns: event.time_ns,
                    observed: now,
                });
            }
        }
    }

    /// Estimate duration only when exactly one known request matches the completion.
    pub(super) fn duration(&self, event: &Event, model: &str, now: Instant) -> Option<f64> {
        if self.saturated_until.is_some_and(|until| now < until) {
            return None;
        }
        let pending = self.conversations.get(&event.conversation)?;
        if event.time_ns <= pending.completed_at || pending.overflow.is_some() {
            return None;
        }
        let mut matching = pending
            .requests
            .iter()
            .filter(|request| request.model == model && request.time_ns < event.time_ns);
        let elapsed = event.time_ns - matching.next()?.time_ns;
        matching
            .next()
            .is_none()
            .then_some(elapsed as f64 / 1_000_000.0)
    }

    /// Consume the prefix of the conversation only after processing a completion, including replays.
    pub(super) fn complete(&mut self, event: &Event, now: Instant) {
        if let Some(pending) = self.pending(event.conversation, now) {
            pending.completed_at = pending.completed_at.max(event.time_ns);
            pending
                .requests
                .retain(|request| request.time_ns > pending.completed_at);
            pending.overflow = pending
                .overflow
                .filter(|(time, _)| *time > pending.completed_at);
        }
    }

    /// Allocate a conversation slot or suppress pairing while omitted requests may still be live.
    fn pending(&mut self, conversation: Digest, now: Instant) -> Option<&mut Pending> {
        if self.conversations.len() >= MAX_CONVERSATIONS
            && !self.conversations.contains_key(&conversation)
        {
            self.saturated_until = Some(now + TTL);
            return None;
        }
        let pending = self
            .conversations
            .entry(conversation)
            .or_insert_with(|| Pending {
                touched: now,
                completed_at: 0,
                requests: Vec::new(),
                overflow: None,
            });
        pending.touched = now;
        Some(pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// UTC timestamps convert exactly; offsets other than UTC and malformed fields are rejected.
    #[test]
    fn utc_timestamps() {
        assert_eq!(utc_ns("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            utc_ns("2026-11-26T22:51:33.061Z"),
            Some(1_795_733_493_061_000_000)
        );
        assert_eq!(
            utc_ns("2000-02-29T00:00:00.000000001+00:00"),
            Some(951_782_400_000_000_001)
        );
        for bad in [
            "2026-11-26T22:51:33+01:00",
            "2026-13-01T00:00:00Z",
            "2026-11-26T22:51:33.Z",
            "2026-11-26 22:51:33Z",
            "x",
        ] {
            assert_eq!(utc_ns(bad), None, "{bad}");
        }
    }

    /// Exclude other models, equal timestamps, consumed requests and expired observations.
    #[test]
    fn pairing_boundaries() {
        let mut pairing = Pairing::default();
        let now = Instant::now();
        let mut event = Event {
            time_ns: 100_000_000,
            kind: Kind::Request("model".into()),
            conversation: [0; 32],
        };
        pairing.request(&event, "model", now);
        assert_eq!(pairing.duration(&event, "model", now), None);
        event.time_ns = 200_000_000;
        pairing.request(&event, "other-model", now);
        event.time_ns = 300_000_000;
        assert_eq!(pairing.duration(&event, "model", now), Some(200.0));
        pairing.complete(&event, now);
        event.time_ns = 400_000_000;
        assert_eq!(pairing.duration(&event, "model", now), None);
        pairing.request(&event, "model", now);
        event.time_ns = 500_000_000;
        assert_eq!(pairing.duration(&event, "model", now), Some(100.0));
        let expired = now + TTL;
        pairing.expire(expired);
        assert_eq!(pairing.duration(&event, "model", expired), None);
    }
}
