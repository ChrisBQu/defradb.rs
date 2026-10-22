//! The P2P transport a C caller's [`NodeInitOptions`] ask for.

#[cfg(feature = "iroh")]
use crate::target::non_empty;
use crate::target::{go_string, split_comma_separated};
use crate::types::NodeInitOptions;

/// Where a libp2p node listens when the caller names nowhere, as in Go.
#[cfg(feature = "libp2p")]
const DEFAULT_LISTEN_ADDRESS: &str = "/ip4/0.0.0.0/tcp/9171";

/// The transport `options` ask for: none when `disable_p2p` is set,
/// otherwise the one `p2p_transport` names, libp2p by default.
///
/// # Safety
///
/// Every non-null string in `options` must be a valid null-terminated string.
pub(crate) unsafe fn read(options: &NodeInitOptions) -> Result<embedded::TransportConfig, String> {
    if options.disable_p2p != 0 {
        return Ok(embedded::TransportConfig::None);
    }
    let listen_addrs = split_comma_separated(&go_string(options.listening_addresses));
    let peers = split_comma_separated(&go_string(options.peers));

    match go_string(options.p2p_transport).to_lowercase().as_str() {
        "" | "libp2p" => libp2p(options, listen_addrs, peers),
        "iroh" => iroh(options, listen_addrs, peers),
        other => Err(format!(
            "unsupported p2p transport '{}'. Supported: libp2p, iroh",
            other
        )),
    }
}

#[cfg(feature = "libp2p")]
fn libp2p(
    options: &NodeInitOptions,
    listen_addrs: Vec<String>,
    peers: Vec<String>,
) -> Result<embedded::TransportConfig, String> {
    Ok(embedded::TransportConfig::Libp2p(embedded::Libp2pConfig {
        listen_addrs: if listen_addrs.is_empty() {
            vec![DEFAULT_LISTEN_ADDRESS.to_string()]
        } else {
            listen_addrs
        },
        bootstrap_peers: peers,
        enable_pubsub: options.enable_pub_sub != 0,
        enable_relay: options.enable_relay != 0,
        stream_timeout_secs: (options.p2p_block_sync_timeout_ms > 0)
            .then(|| (options.p2p_block_sync_timeout_ms as u64).div_ceil(1000)),
    }))
}

#[cfg(not(feature = "libp2p"))]
fn libp2p(
    _options: &NodeInitOptions,
    _listen_addrs: Vec<String>,
    _peers: Vec<String>,
) -> Result<embedded::TransportConfig, String> {
    Err(
        "this build does not include the libp2p transport; rebuild with the libp2p feature, \
         or set disable_p2p"
            .to_string(),
    )
}

#[cfg(not(feature = "iroh"))]
unsafe fn iroh(
    _options: &NodeInitOptions,
    _listen_addrs: Vec<String>,
    _peers: Vec<String>,
) -> Result<embedded::TransportConfig, String> {
    Err("iroh transport not enabled. Rebuild with --features iroh".to_string())
}

/// # Safety
///
/// As for [`read`].
#[cfg(feature = "iroh")]
unsafe fn iroh(
    options: &NodeInitOptions,
    listen_addrs: Vec<String>,
    peers: Vec<String>,
) -> Result<embedded::TransportConfig, String> {
    if !peers.is_empty() {
        return Err("not supported yet: peers with the iroh transport".to_string());
    }
    let mut config = embedded::IrohConfig {
        secret_key_path: non_empty(options.iroh_key_path).map(std::path::PathBuf::from),
        ..Default::default()
    };

    let relay_url = non_empty(options.iroh_relay_url);
    let mut relay_urls = match non_empty(options.iroh_relay_urls_json) {
        Some(raw) if !raw.trim().is_empty() => serde_json::from_str::<Vec<String>>(&raw)
            .map_err(|error| format!("invalid iroh_relay_urls_json: {}", error))?,
        _ => Vec::new(),
    };
    config.relay_mode = match non_empty(options.iroh_relay_mode).as_deref() {
        Some("disabled") => p2p::iroh::IrohRelayModeConfig::Disabled,
        Some("default") => p2p::iroh::IrohRelayModeConfig::Default,
        Some("custom") => {
            relay_urls.extend(relay_url);
            if relay_urls.is_empty() {
                return Err("iroh_relay_mode=custom requires at least one relay URL".to_string());
            }
            p2p::iroh::IrohRelayModeConfig::Custom(relay_urls)
        }
        Some(other) => {
            return Err(format!(
                "unsupported iroh_relay_mode '{}'. Supported: default, disabled, custom",
                other
            ));
        }
        None => {
            relay_urls.extend(relay_url);
            if relay_urls.is_empty() {
                p2p::iroh::IrohRelayModeConfig::Default
            } else {
                p2p::iroh::IrohRelayModeConfig::Custom(relay_urls)
            }
        }
    };

    config.discovery = match (
        options.iroh_disable_discovery == 0,
        non_empty(options.iroh_discovery_origin_domain),
        non_empty(options.iroh_pkarr_relay_url),
    ) {
        (_, Some(origin_domain), Some(pkarr_relay_url)) => {
            p2p::iroh::IrohDiscoveryConfig::CustomDns {
                origin_domain,
                pkarr_relay_url,
            }
        }
        (_, Some(_), None) | (_, None, Some(_)) => {
            return Err(
                "custom iroh discovery requires both iroh_discovery_origin_domain and iroh_pkarr_relay_url"
                    .to_string(),
            );
        }
        (false, None, None) => p2p::iroh::IrohDiscoveryConfig::Disabled,
        (true, None, None) => p2p::iroh::IrohDiscoveryConfig::N0,
    };

    if let Some(bind_addr) = non_empty(options.iroh_bind_addr) {
        config.bind_addr = Some(
            bind_addr
                .parse()
                .map_err(|error| format!("invalid iroh_bind_addr '{}': {}", bind_addr, error))?,
        );
    }
    if options.iroh_bind_port != 0 {
        config.bind_port = Some(options.iroh_bind_port);
    }

    match listen_addrs.as_slice() {
        [] => {}
        [listen_addr] => {
            let socket_addr: std::net::SocketAddr = listen_addr.parse().map_err(|error| {
                format!("invalid iroh listen address '{}': {}", listen_addr, error)
            })?;
            config.bind_addr = Some(socket_addr.ip());
            config.bind_port = Some(socket_addr.port());
        }
        _ => return Err("the iroh transport listens on one address".to_string()),
    }

    Ok(embedded::TransportConfig::Iroh(config))
}
