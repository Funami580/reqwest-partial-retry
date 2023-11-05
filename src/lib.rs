//! Wrapper around reqwest to allow for easy partial retries
//!
//! # Example
//!
//! ```
//! use futures_util::StreamExt;
//! use reqwest_partial_retry::ClientExt;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let client = reqwest::Client::new().resumable();
//! let request = client.get("http://httpbin.org/ip").build().unwrap();
//! let mut stream = client
//!     .execute_resumable(request)
//!     .await?
//!     .bytes_stream_resumable();
//!
//! while let Some(item) = stream.next().await {
//!     println!("Bytes: {:?}", item?);
//! }
//! # Ok(())
//! # }
//! ```

#![warn(
    missing_docs,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::missing_const_for_fn,
    clippy::future_not_send,
    clippy::large_futures
)]

use std::fmt::Display;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use futures_core::{ready, Stream};
use reqwest::Request;
use reqwest_retry::{DefaultRetryableStrategy, RetryPolicy, Retryable, RetryableStrategy};
use retry_policies::RetryDecision;

/// Config for the resumable [`Client`]
pub struct Config {
    inner: Arc<ConfigInner>,
}

struct ConfigInner {
    stream_timeout: Option<Duration>,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    retryable_strategy: Box<dyn RetryableStrategy + Send + Sync>,
}

impl Clone for Config {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Default for Config {
    /// The default config has no stream timeout,
    /// uses an exponential backoff with 10 retries,
    /// and uses the [`DefaultRetryableStrategy`]
    #[inline]
    fn default() -> Self {
        Self {
            inner: Arc::new(ConfigInner {
                stream_timeout: None,
                retry_policy: Box::new(
                    retry_policies::policies::ExponentialBackoffBuilder::default().build_with_max_retries(10),
                ),
                retryable_strategy: Box::new(DefaultRetryableStrategy),
            }),
        }
    }
}

impl Config {
    /// Create a `ConfigBuilder`
    #[inline]
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Get the stream timeout
    #[inline]
    pub fn stream_timeout(&self) -> &Option<Duration> {
        &self.inner.stream_timeout
    }

    /// Get the retry policy
    #[inline]
    pub fn retry_policy(&self) -> &(dyn RetryPolicy + Send + Sync) {
        self.inner.retry_policy.as_ref()
    }

    /// Get the retryable strategy
    #[inline]
    pub fn retryable_strategy(&self) -> &(dyn RetryableStrategy + Send + Sync) {
        self.inner.retryable_strategy.as_ref()
    }
}

/// ConfigBuilder for the [`Config`] of the resumable [`Client`]
pub struct ConfigBuilder {
    stream_timeout: Option<Duration>,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    retryable_strategy: Box<dyn RetryableStrategy + Send + Sync>,
}

impl Default for ConfigBuilder {
    /// The default config builder has no stream timeout,
    /// uses an exponential backoff with 10 retries,
    /// and uses the [`DefaultRetryableStrategy`]
    #[inline]
    fn default() -> Self {
        Self {
            stream_timeout: None,
            retry_policy: Box::new(
                retry_policies::policies::ExponentialBackoffBuilder::default().build_with_max_retries(10),
            ),
            retryable_strategy: Box::new(DefaultRetryableStrategy),
        }
    }
}

impl ConfigBuilder {
    /// Set the timeout for the
    /// [`bytes_stream_resumable`](ResumableResponse::bytes_stream_resumable).
    ///
    /// If no new data has been received for the specified duration, it times out,
    /// and depending on the `RetryPolicy` and `RetryableStrategy` a retry is tried.
    ///
    /// If the value is `None`, it will never time out.
    #[inline]
    pub const fn stream_timeout(mut self, stream_timeout: Option<Duration>) -> Self {
        self.stream_timeout = stream_timeout;
        self
    }

    /// Set the retry policy.
    ///
    /// Retries are counted in [`execute_resumable`](Client::execute_resumable)
    /// and [`bytes_stream_resumable`](ResumableResponse::bytes_stream_resumable),
    /// and will not be reset in in between.
    #[inline]
    pub fn retry_policy<P: RetryPolicy + Send + Sync + 'static>(mut self, retry_policy: P) -> Self {
        self.retry_policy = Box::new(retry_policy);
        self
    }

    /// Set the retryable strategy.
    #[inline]
    pub fn retryable_strategy<S: RetryableStrategy + Send + Sync + 'static>(mut self, retryable_strategy: S) -> Self {
        self.retryable_strategy = Box::new(retryable_strategy);
        self
    }

    /// Build a `Config`
    #[inline]
    pub fn build(self) -> Config {
        let Self {
            stream_timeout,
            retry_policy,
            retryable_strategy,
        } = self;

        Config {
            inner: Arc::new(ConfigInner {
                stream_timeout,
                retry_policy,
                retryable_strategy,
            }),
        }
    }
}

/// Extension to [`reqwest::Client`] that provides methods to convert it into a resumable [`Client`]
pub trait ClientExt {
    /// Convert a [`reqwest::Client`] into a
    /// [`reqwest_partial_retry::Client`](Client)
    fn resumable(self) -> Client;

    /// Convert a [`reqwest::Client`] into a
    /// [`reqwest_partial_retry::Client`](Client) with a config
    fn resumable_with_config(self, config: Config) -> Client;
}

impl ClientExt for reqwest::Client {
    #[inline]
    fn resumable(self) -> Client {
        Client {
            client: self,
            config: Config::default(),
        }
    }

    #[inline]
    fn resumable_with_config(self, config: Config) -> Client {
        Client { client: self, config }
    }
}

/// A wrapper for [`reqwest::Client`] to allow for resumable byte streams
pub struct Client {
    client: reqwest::Client,
    config: Config,
}

impl Clone for Client {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
        }
    }
}

impl Deref for Client {
    type Target = reqwest::Client;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Client {
    /// Executes a `Request`.
    ///
    /// A `Request` can be built manually with `Request::new()` or obtained
    /// from a RequestBuilder with `RequestBuilder::build()`.
    ///
    /// # Errors
    ///
    /// This method fails if the `Request` is not cloneable (i.e. if the body is
    /// a stream).
    ///
    /// It also fails if the [`RetryableStrategy`] decides that the error is fatal,
    /// or it is transient and the retry limit has been reached.
    pub async fn execute_resumable(&self, request: Request) -> Result<ResumableResponse, Error> {
        let mut current_retries = 0;

        'outer_loop: loop {
            let Some(cloned_request) = request.try_clone() else {
                return Err(Error::RequestNotCloneable);
            };

            let response = self
                .client
                .execute(cloned_request)
                .await
                .map_err(reqwest_middleware::Error::Reqwest);

            match (self.config.inner.retryable_strategy.handle(&response), response) {
                (Some(Retryable::Fatal), response) => {
                    let reqwest_error = if let Err(reqwest_middleware::Error::Reqwest(err)) = response {
                        Some(err)
                    } else {
                        None
                    };

                    return Err(Error::UnresolvableError(reqwest_error));
                }
                (None, Ok(response)) => {
                    let headers = hyperx::Headers::from(response.headers());
                    let accept_byte_ranges = if let Some(hyperx::header::AcceptRanges(ranges)) = headers.get() {
                        ranges.iter().any(|u| *u == hyperx::header::RangeUnit::Bytes)
                    } else {
                        false
                    };

                    return Ok(ResumableResponse {
                        client: self.clone(),
                        request,
                        response,
                        accept_byte_ranges,
                        current_retries,
                    });
                }
                (Some(Retryable::Transient), response) | (None, response @ Err(_)) => {
                    let retry_decision = self.config.inner.retry_policy.should_retry(current_retries);
                    current_retries += 1;

                    match retry_decision {
                        RetryDecision::Retry { execute_after } => {
                            if let Ok(duration) = (execute_after - Utc::now()).to_std() {
                                tokio::time::sleep(duration).await;
                            }

                            continue 'outer_loop;
                        }
                        RetryDecision::DoNotRetry => {
                            let reqwest_error = if let Err(reqwest_middleware::Error::Reqwest(err)) = response {
                                Some(RetryError::RequestError(err))
                            } else {
                                None
                            };

                            return Err(Error::MaxRetries(reqwest_error));
                        }
                    }
                }
            }
        }
    }
}

/// A wrapper for [`reqwest::Response`] to allow for resumable byte streams
pub struct ResumableResponse {
    client: Client,
    request: reqwest::Request,
    response: reqwest::Response,
    accept_byte_ranges: bool,
    current_retries: u32,
}

impl Deref for ResumableResponse {
    type Target = reqwest::Response;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

impl ResumableResponse {
    /// Get the underlying [`reqwest::Response`]
    #[inline]
    pub fn response(self) -> reqwest::Response {
        self.response
    }

    /// Convert the response into a `Stream` of `Bytes` from the body.
    ///
    /// # Example
    ///
    /// ```
    /// use futures_util::StreamExt;
    /// use reqwest_partial_retry::ClientExt;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = reqwest::Client::new().resumable();
    /// let request = client.get("http://httpbin.org/ip").build().unwrap();
    /// let mut stream = client
    ///     .execute_resumable(request)
    ///     .await?
    ///     .bytes_stream_resumable();
    ///
    /// while let Some(item) = stream.next().await {
    ///     println!("Bytes: {:?}", item?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn bytes_stream_resumable(self) -> impl Stream<Item = Result<Bytes, Error>> + Send + Unpin {
        ResumableBytesStream {
            client: self.client,
            request: self.request,
            accept_byte_ranges: self.accept_byte_ranges,
            pos: 0,
            read_bytes: 0,
            current_retries: self.current_retries,
            stream: Box::pin(self.response.bytes_stream()),
            next_request: None,
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            run_sleep: false,
            stream_sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            tried_stream_poll: false,
        }
    }
}

struct ResumableBytesStream {
    client: Client,
    request: reqwest::Request,
    accept_byte_ranges: bool,
    pos: u64,
    read_bytes: u64,
    current_retries: u32,
    stream: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    next_request: Option<Pin<Box<dyn Future<Output = Result<reqwest::Response, reqwest::Error>> + Send>>>,
    sleep: Pin<Box<tokio::time::Sleep>>,
    run_sleep: bool,
    stream_sleep: Pin<Box<tokio::time::Sleep>>,
    tried_stream_poll: bool,
}

/// The errors that may occur
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// The error was unresolvable
    UnresolvableError(Option<reqwest::Error>),
    /// The maximum retry limit has been reached
    MaxRetries(Option<RetryError>),
    /// The request is not cloneable
    RequestNotCloneable,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::UnresolvableError(err) => {
                if let Some(err) = err {
                    write!(f, "encountered fatal error: {err}")
                } else {
                    write!(f, "encountered fatal error")
                }
            }
            Error::MaxRetries(err) => {
                if let Some(err) = err {
                    write!(f, "maximum amount of retries reached: {err}")
                } else {
                    write!(f, "maximum amount of retries reached")
                }
            }
            Error::RequestNotCloneable => write!(f, "request is not cloneable"),
        }
    }
}

/// The error that possibly caused the retry
#[derive(thiserror::Error, Debug)]
pub enum RetryError {
    /// A request error
    #[error("request error: {0}")]
    RequestError(reqwest::Error),
    /// Requested range is not satisfiable
    #[error("requested range is not satisfiable")]
    RangeNotSatisfiable,
    /// A stream error
    #[error("stream error: {0}")]
    StreamError(reqwest::Error),
    /// The stream timed out
    #[error("stream timed out")]
    StreamTimeout,
}

impl Stream for ResumableBytesStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        macro_rules! prepare_retry {
            ($retry_after:expr $(,)?) => {
                if let Ok(duration) = ($retry_after - Utc::now()).to_std() {
                    self.sleep.as_mut().reset(tokio::time::Instant::now() + duration);
                    self.run_sleep = true;
                }

                let Some(mut cloned_request) = self.request.try_clone() else {
                    return Poll::Ready(Some(Err(Error::RequestNotCloneable)));
                };

                if self.accept_byte_ranges {
                    let range_value = reqwest::header::HeaderValue::from_str(
                        &hyperx::header::Range::Bytes(vec![hyperx::header::ByteRangeSpec::AllFrom(self.pos)])
                            .to_string(),
                    )
                    .unwrap();
                    let _ = cloned_request
                        .headers_mut()
                        .insert(reqwest::header::RANGE, range_value);
                }

                self.next_request = Some(Box::pin(self.client.client.execute(cloned_request)));
            };
        }

        'outer_loop: loop {
            if self.run_sleep {
                ready!(self.sleep.as_mut().poll(cx));
                self.run_sleep = false;
            }

            if let Some(next_request) = &mut self.next_request {
                let response = ready!(next_request.as_mut().poll(cx)).map_err(reqwest_middleware::Error::Reqwest);
                self.next_request = None;

                if let Ok(response) = &response {
                    if matches!(
                        response.status(),
                        reqwest::StatusCode::OK | reqwest::StatusCode::RANGE_NOT_SATISFIABLE
                    ) {
                        self.accept_byte_ranges = false;
                    }
                }

                match (self.client.config.inner.retryable_strategy.handle(&response), response) {
                    (Some(Retryable::Fatal), response) => {
                        let reqwest_error = if let Err(reqwest_middleware::Error::Reqwest(err)) = response {
                            Some(err)
                        } else {
                            None
                        };

                        return Poll::Ready(Some(Err(Error::UnresolvableError(reqwest_error))));
                    }
                    (None, Ok(response)) => {
                        if !self.accept_byte_ranges {
                            self.pos = 0;
                        }

                        if response.status() == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
                            let retry_decision =
                                self.client.config.inner.retry_policy.should_retry(self.current_retries);
                            self.current_retries += 1;

                            match retry_decision {
                                RetryDecision::Retry { execute_after } => {
                                    prepare_retry!(execute_after);
                                    continue 'outer_loop;
                                }
                                RetryDecision::DoNotRetry => {
                                    return Poll::Ready(Some(Err(Error::MaxRetries(Some(
                                        RetryError::RangeNotSatisfiable,
                                    )))));
                                }
                            }
                        }

                        self.stream = Box::pin(response.bytes_stream());

                        if let Some(timeout_duration) = self.client.config.inner.stream_timeout {
                            let stream_sleep_deadline = tokio::time::Instant::now() + timeout_duration;
                            self.stream_sleep.as_mut().reset(stream_sleep_deadline);
                        }
                    }
                    (Some(Retryable::Transient), response) | (None, response @ Err(_)) => {
                        let retry_decision = self.client.config.inner.retry_policy.should_retry(self.current_retries);
                        self.current_retries += 1;

                        match retry_decision {
                            RetryDecision::Retry { execute_after } => {
                                prepare_retry!(execute_after);
                                continue 'outer_loop;
                            }
                            RetryDecision::DoNotRetry => {
                                let reqwest_error = if let Err(reqwest_middleware::Error::Reqwest(err)) = response {
                                    Some(RetryError::RequestError(err))
                                } else {
                                    None
                                };

                                return Poll::Ready(Some(Err(Error::MaxRetries(reqwest_error))));
                            }
                        }
                    }
                }
            }

            if !self.tried_stream_poll {
                self.tried_stream_poll = true;

                if let Some(timeout_duration) = self.client.config.inner.stream_timeout {
                    let stream_sleep_deadline = tokio::time::Instant::now() + timeout_duration;
                    self.stream_sleep.as_mut().reset(stream_sleep_deadline);
                }
            }

            'stream_loop: loop {
                match self.stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Err(err))) => {
                        let retry_decision = self.client.config.inner.retry_policy.should_retry(self.current_retries);
                        self.current_retries += 1;

                        match retry_decision {
                            RetryDecision::Retry { execute_after } => {
                                prepare_retry!(execute_after);
                                continue 'outer_loop;
                            }
                            RetryDecision::DoNotRetry => {
                                return Poll::Ready(Some(Err(Error::MaxRetries(Some(RetryError::StreamError(err))))));
                            }
                        }
                    }
                    Poll::Ready(Some(Ok(bytes))) => {
                        if !bytes.is_empty() {
                            if let Some(timeout_duration) = self.client.config.inner.stream_timeout {
                                let stream_sleep_deadline = tokio::time::Instant::now() + timeout_duration;
                                self.stream_sleep.as_mut().reset(stream_sleep_deadline);
                            }

                            let start_pos = self.pos;
                            self.pos += bytes.len() as u64;

                            if self.pos > self.read_bytes {
                                let diff_read = (self.read_bytes - start_pos) as usize;
                                self.read_bytes = self.pos;
                                return Poll::Ready(Some(Ok(bytes.slice(diff_read..))));
                            }
                        }

                        continue 'stream_loop;
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => {
                        if self.client.config.inner.stream_timeout.is_some() {
                            ready!(self.stream_sleep.as_mut().poll(cx));

                            let retry_decision =
                                self.client.config.inner.retry_policy.should_retry(self.current_retries);
                            self.current_retries += 1;

                            match retry_decision {
                                RetryDecision::Retry { execute_after } => {
                                    prepare_retry!(execute_after);
                                    continue 'outer_loop;
                                }
                                RetryDecision::DoNotRetry => {
                                    return Poll::Ready(Some(Err(Error::MaxRetries(Some(RetryError::StreamTimeout)))));
                                }
                            }
                        }

                        return Poll::Pending;
                    }
                }
            }
        }
    }
}
