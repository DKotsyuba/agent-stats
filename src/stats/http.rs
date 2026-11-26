//! Authenticated, bounded HTTP/1 OTLP JSON ingestion.
use super::{Error, State, Write, normalize, now_ns};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header,
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use ring::digest;
use std::{convert::Infallible, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::{net::TcpStream, sync::oneshot, time::timeout};

/// Serve one HTTP connection with deadlines for headers and request bodies.
pub(super) async fn connection(stream: TcpStream, state: Arc<State>) {
    let _ = http1::Builder::new()
        .keep_alive(false)
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(2))
        .max_buf_size(16 * 1024)
        .max_headers(32)
        .serve_connection(
            TokioIo::new(stream),
            service_fn(|request| async {
                let result = ingest(request, &state).await;
                let status = match result {
                    Ok(()) => StatusCode::OK,
                    Err(status) => status,
                };
                let mut reply = Response::new(Full::new(if status.is_success() {
                    Bytes::from_static(b"{}")
                } else {
                    Bytes::new()
                }));
                *reply.status_mut() = status;
                reply.headers_mut().insert(
                    header::CONTENT_TYPE,
                    header::HeaderValue::from_static("application/json"),
                );
                Ok::<_, Infallible>(reply)
            }),
        )
        .await;
}

/// Authenticate before decoding and acknowledge only committed writes.
async fn ingest(request: Request<Incoming>, state: &State) -> Result<(), StatusCode> {
    if request.uri().path() != "/v1/logs" {
        return Err(StatusCode::NOT_FOUND);
    }
    if request.method() != hyper::Method::POST {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    let mut tokens = request.headers().get_all("x-stats-token").iter();
    let authorized = tokens.next().is_some_and(|token| {
        let candidate = digest::digest(&digest::SHA256, token.as_bytes());
        bool::from(candidate.as_ref().ct_eq(state.token_hash.as_ref()))
    });
    if !authorized || tokens.next().is_some() {
        state.counters.unauthorized.add(1);
        return Err(StatusCode::UNAUTHORIZED);
    }
    let json = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    let identity = request
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_none_or(|value| value.as_bytes().eq_ignore_ascii_case(b"identity"));
    if !json || !identity {
        state.counters.unsupported.add(1);
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    let _permit = state.requests.try_acquire().map_err(|_| {
        state.counters.overloaded.add(1);
        StatusCode::TOO_MANY_REQUESTS
    })?;
    let limit = state.config.max_body_bytes;
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
        .is_some_and(|length| length > limit as u64)
    {
        state.counters.oversize.add(1);
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let bytes = match timeout(
        Duration::from_secs(2),
        Limited::new(request.into_body(), limit).collect(),
    )
    .await
    {
        Ok(Ok(collected)) => collected.to_bytes(),
        Ok(Err(error)) if error.is::<http_body_util::LengthLimitError>() => {
            state.counters.oversize.add(1);
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        _ => {
            state.counters.malformed.add(1);
            return Err(StatusCode::BAD_REQUEST);
        }
    };
    let batch = normalize::normalize(&bytes, &state.config, &state.digest_key, now_ns()).map_err(
        |error| match error {
            Error("too_many_records") => {
                state.counters.oversize.add(1);
                StatusCode::PAYLOAD_TOO_LARGE
            }
            _ => {
                state.counters.malformed.add(1);
                StatusCode::BAD_REQUEST
            }
        },
    )?;
    state.counters.ignored.add(batch.ignored);
    state.counters.invalid.add(batch.invalid);
    state.counters.expired.add(batch.expired);
    if batch.samples.is_empty() {
        return Ok(());
    }

    let (reply, result) = oneshot::channel();
    state
        .writer
        .try_send(Write::Batch(batch, reply))
        .map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                state.counters.overloaded.add(1);
                StatusCode::TOO_MANY_REQUESTS
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => StatusCode::SERVICE_UNAVAILABLE,
        })?;
    match result.await {
        Ok(Ok(())) => Ok(()),
        _ => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}
