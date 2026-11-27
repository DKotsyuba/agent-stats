//! Rust MCP application; protocol, presentation and deployment have separate boundaries.
mod response;
#[cfg(unix)]
mod stats;
mod tools;
use clap::{Parser, Subcommand};
use mcp_presentation::Renderer;
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler, ServiceExt, model::*, service::RequestContext,
};
use std::{path::PathBuf, process::ExitCode, sync::Arc};

/// Private catalog cache lifetime in milliseconds for modern MCP requests.
const TOOLS_LIST_TTL_MS: u64 = 60_000;

#[derive(Parser)]
#[command(version, about = "Agent Stats telemetry service and CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    /// Run the authenticated OTLP receiver and local read socket.
    #[cfg(unix)]
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    /// Read exact rolling model aggregates as versioned JSON.
    #[cfg(unix)]
    Stats {
        #[arg(long, default_value = stats::SOCKET)]
        socket: PathBuf,
        #[arg(long, value_enum, default_value = "24h")]
        window: stats::Window,
        #[arg(long, value_enum, default_value = "model")]
        group_by: stats::Group,
    },
    /// Read UTC-aligned model buckets as versioned JSON.
    #[cfg(unix)]
    Series {
        #[arg(long, default_value = stats::SOCKET)]
        socket: PathBuf,
        #[arg(long)]
        model: String,
        #[arg(long, value_enum, default_value = "hour")]
        bucket: stats::Bucket,
        #[arg(long, value_enum, default_value = "24h")]
        window: stats::Window,
    },
    /// Read service counters and the last sample time as versioned JSON.
    #[cfg(unix)]
    Status {
        #[arg(long, default_value = stats::SOCKET)]
        socket: PathBuf,
    },
    /// Run the protocol-only stdio endpoint.
    Mcp {
        #[arg(long, default_value = DEFAULT_MCP_SOCKET)]
        socket: PathBuf,
    },
    /// Check local resources, without credentials, network or mutation.
    Doctor {
        #[arg(long)]
        json: bool,
    },
    /// Export the actual registry or inspect unimplemented tool stubs.
    Contract {
        #[command(subcommand)]
        command: ContractCommand,
    },
    /// Install a verified single-binary bundle. Does not restart services.
    SelfInstall {
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        bin_dir: PathBuf,
    },
    /// Select a retained compatible installation.
    Releases {
        #[command(subcommand)]
        command: ReleaseCommand,
    },
}
#[derive(Subcommand)]
enum ContractCommand {
    Export,
    Readiness,
}
#[derive(Subcommand)]
enum ReleaseCommand {
    Use {
        version: String,
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        bin_dir: PathBuf,
    },
}
#[derive(Clone)]
struct Handler {
    identity: Arc<Renderer>,
    templates: Arc<response::Templates>,
    catalog: Vec<Tool>,
    socket: PathBuf,
    #[cfg(unix)]
    freshness: Arc<tokio::sync::Mutex<Freshness>>,
}
impl Handler {
    fn new(socket: PathBuf) -> Result<Self, &'static str> {
        let identity = Renderer::new().map_err(|_| "presentation_setup_failed")?;
        let templates = response::Templates::new(&tools::templates())?;
        let catalog = serde_json::from_value(serde_json::Value::Array(tools::definitions()))
            .map_err(|_| "catalog_invalid")?;
        Ok(Self {
            identity: Arc::new(identity),
            templates: Arc::new(templates),
            catalog,
            socket,
            #[cfg(unix)]
            freshness: Arc::default(),
        })
    }
}
/// Default path for the private read socket.
#[cfg(unix)]
const DEFAULT_MCP_SOCKET: &str = stats::SOCKET;
#[cfg(not(unix))]
const DEFAULT_MCP_SOCKET: &str = "/run/agent-stats/read.sock";

/// Per-session version probe and notification state.
#[cfg(unix)]
#[derive(Default)]
struct Freshness {
    checked: Option<std::time::Instant>,
    stale: Option<String>,
    notified: bool,
}

#[cfg(unix)]
impl Handler {
    /// Probe at most once every 30 seconds and retain any confirmed mismatch.
    async fn check_version(&self) -> Option<String> {
        let mut state = self.freshness.lock().await;
        if state.stale.is_none()
            && state
                .checked
                .is_none_or(|checked| checked.elapsed() >= std::time::Duration::from_secs(30))
        {
            state.checked = Some(std::time::Instant::now());
            if let Ok(reply) = stats::call(&self.socket, stats::Operation::Status).await
                && reply["ok"] == true
                && let Some(version) = reply["data"]["service_version"].as_str()
                && !version.is_empty()
                && version.len() <= 64
                && version
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+'))
                && version != env!("CARGO_PKG_VERSION")
            {
                state.stale = Some(format!(
                    "agent-stats service is {version}, this MCP frontend is {}: reconnect the agent-stats MCP server (Claude Code: /mcp → reconnect; Codex: restart the session)",
                    env!("CARGO_PKG_VERSION")
                ));
            }
        }
        state.stale.clone()
    }

    /// Send the catalog invalidation once after the transport is initialized.
    async fn notify_stale(&self, peer: &rmcp::Peer<RoleServer>) {
        let mut state = self.freshness.lock().await;
        if state.stale.is_some() && !state.notified && peer.notify_tool_list_changed().await.is_ok()
        {
            state.notified = true;
        }
    }
}

impl ServerHandler for Handler {
    /// Check the local service before completing the legacy handshake.
    #[cfg(unix)]
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        self.check_version().await;
        context.peer.set_peer_info(request.clone());
        self.negotiate_initialize(&request)
    }

    /// Defer handshake-time invalidation until the client accepts notifications.
    #[cfg(unix)]
    async fn on_initialized(&self, context: rmcp::service::NotificationContext<RoleServer>) {
        self.notify_stale(&context.peer).await;
    }
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .build(),
        )
        .with_server_info(Implementation::new(
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
        ))
        .with_instructions(
            "Use model_stats to compare models and get_status for frontend identity.",
        )
    }
    /// Return the static catalog; modern requests cache it privately for 60 seconds.
    /// Pagination is unused; legacy sessions retain their original wire fields.
    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let mut result = ListToolsResult::with_all_items(self.catalog.clone());
        if context
            .protocol_version()
            .is_some_and(|version| version.as_str() >= ProtocolVersion::V_2026_07_28.as_str())
        {
            result = result
                .with_ttl_ms(TOOLS_LIST_TTL_MS)
                .with_cache_scope(CacheScope::Private);
        }
        Ok(result)
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        #[cfg(unix)]
        if let Some(message) = self.check_version().await {
            self.notify_stale(&context.peer).await;
            return Ok(CallToolResult::error(vec![ContentBlock::text(message)]).into());
        }
        #[cfg(not(unix))]
        let _ = context;
        let args = serde_json::Value::Object(request.arguments.unwrap_or_default());
        let reply = tools::call(
            &request.name,
            args,
            &self.identity,
            &self.templates,
            &self.socket,
        )
        .await
        .ok_or_else(|| McpError::new(ErrorCode::METHOD_NOT_FOUND, "Unknown tool", None))?;
        Ok(reply.into())
    }
}
#[allow(clippy::print_stdout, reason = "Explicit CLI branch, never MCP output")]
fn print_json(value: impl serde::Serialize) -> ExitCode {
    match serde_json::to_string_pretty(&value) {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(_) => {
            eprintln!("CLI serialization failed");
            ExitCode::FAILURE
        }
    }
}
/// Release qualification recorded in the embedded family manifest, so
/// `doctor` and `get_status` report the same value the release tooling reads.
/// An unreadable manifest fails closed to `not_verified`.
fn qualification() -> &'static str {
    static QUALIFICATION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    QUALIFICATION.get_or_init(|| {
        toml::from_str::<toml::Value>(include_str!("../family.toml"))
            .ok()
            .and_then(|family| {
                family
                    .get("qualification")
                    .and_then(toml::Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "not_verified".to_owned())
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    match Cli::parse().command {
        #[cfg(unix)]
        Commands::Serve { config } => match stats::serve(&config).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("serve: {error}");
                ExitCode::FAILURE
            }
        },
        #[cfg(unix)]
        Commands::Stats {
            socket,
            window,
            group_by,
        } => {
            stats_read(
                &socket,
                stats::Operation::ModelStats {
                    window,
                    group_by,
                    model: None,
                },
            )
            .await
        }
        #[cfg(unix)]
        Commands::Series {
            socket,
            model,
            bucket,
            window,
        } => {
            stats_read(
                &socket,
                stats::Operation::ModelSeries {
                    model,
                    bucket,
                    window,
                },
            )
            .await
        }
        #[cfg(unix)]
        Commands::Status { socket } => stats_read(&socket, stats::Operation::Status).await,
        Commands::SelfInstall {
            bundle,
            home,
            bin_dir,
        } => match family_delivery::install(&bundle, &home, &bin_dir) {
            Ok(m) => print_json(m),
            Err(e) => {
                eprintln!("install: {e}");
                ExitCode::FAILURE
            }
        },
        Commands::Releases {
            command:
                ReleaseCommand::Use {
                    version,
                    home,
                    bin_dir,
                },
        } => match family_delivery::use_version(&home, &bin_dir, &version) {
            Ok(m) => print_json(m),
            Err(e) => {
                eprintln!("activate: {e}");
                ExitCode::FAILURE
            }
        },
        Commands::Doctor { json: _ } => {
            let ready = Handler::new(DEFAULT_MCP_SOCKET.into()).is_ok();
            let output = print_json(
                serde_json::json!({"product":env!("CARGO_PKG_NAME"),"version":env!("CARGO_PKG_VERSION"),
                "local_ready":ready,"release_qualification":qualification(),"incomplete_tools":tools::incomplete()}),
            );
            if ready { output } else { ExitCode::from(2) }
        }
        Commands::Contract {
            command: ContractCommand::Export,
        } => match Handler::new(DEFAULT_MCP_SOCKET.into()) {
            Ok(h) => print_json(h.catalog),
            Err(e) => {
                eprintln!("contract: {e}");
                ExitCode::FAILURE
            }
        },
        Commands::Contract {
            command: ContractCommand::Readiness,
        } => print_json(tools::incomplete()),
        Commands::Mcp { socket } => {
            let h = match Handler::new(socket) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("startup: {e}");
                    return ExitCode::FAILURE;
                }
            };
            match h.serve(rmcp::transport::stdio()).await {
                Ok(service) => match service.waiting().await {
                    Ok(_) => ExitCode::SUCCESS,
                    Err(_) => ExitCode::FAILURE,
                },
                // Stateless 2026-07-28 clients never send `initialize`; EOF before it is a clean close.
                Err(rmcp::service::ServerInitializeError::ConnectionClosed(_)) => ExitCode::SUCCESS,
                Err(_) => {
                    eprintln!("MCP transport failed");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

/// Print the versioned socket reply and propagate application errors via exit status.
#[cfg(unix)]
async fn stats_read(socket: &std::path::Path, operation: stats::Operation) -> ExitCode {
    match stats::call(socket, operation).await {
        Ok(reply) => {
            let ok = reply["ok"].as_bool() == Some(true);
            let written = print_json(reply);
            if ok { written } else { ExitCode::FAILURE }
        }
        Err(error) => {
            eprintln!("read: {error}");
            ExitCode::FAILURE
        }
    }
}
