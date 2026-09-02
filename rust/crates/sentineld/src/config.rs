//! Configuration, read once at boot.
//!
//! From the environment, because that is what Fly provides and what keeps a
//! secret out of a file that gets committed. Read once and passed down as a
//! value: nothing below this reads the environment, so nothing below this can
//! behave differently depending on when it was called.

use std::time::Duration;

use sentinel_types::{Instrument, Qty};

/// Where the process runs and against what.
#[derive(Debug, Clone)]
pub struct Config {
    /// The journal directory. Must be on a volume that survives a restart.
    pub journal_dir: String,
    /// What to trade.
    pub instruments: Vec<Instrument>,
    /// Venue credentials, absent when running against the simulator.
    pub venue: VenueConfig,
    /// Fat-finger cap on absolute position, per instrument.
    pub max_position: Option<Qty>,
    /// How often to advance the engine's clock.
    pub tick_interval: Duration,
    /// How often to look for orders the venue has stopped talking about.
    pub sweep_interval: Duration,
    /// How long the venue may say nothing before an order counts as abandoned.
    pub stale_after: Duration,
}

/// Which venue, and as whom.
///
/// The simulator is what you get by saying nothing, and it reaches no venue at
/// all. Every other option has to be named, and production has to be named
/// twice — a default that can lose money is a default nobody should have to
/// remember to override.
#[derive(Debug, Clone)]
pub enum VenueConfig {
    /// The deterministic simulator. No keys, no network, no money.
    Simulator,
    /// Binance USD-M futures. What the deployed system actually trades.
    Binance {
        /// The API key.
        api_key: String,
        /// The secret.
        secret: String,
        /// Whether to use Demo Trading rather than production.
        demo: bool,
        /// Per-symbol leverage to set at boot, when asked for.
        leverage: Option<u32>,
    },
    /// Delta Exchange India.
    Delta {
        /// The API key.
        api_key: String,
        /// The secret.
        secret: String,
        /// Whether to use testnet rather than production.
        testnet: bool,
    },
}

/// Why the process will not start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// A required variable is absent.
    Missing(&'static str),
    /// A variable is present and not usable.
    Invalid {
        /// Which one.
        name: &'static str,
        /// What was wrong with it.
        reason: String,
    },
}

impl core::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Missing(name) => write!(f, "{name} is not set"),
            Self::Invalid { name, reason } => write!(f, "{name}: {reason}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Leverage, when asked for.
///
/// Optional because the safe value is the account's own. Setting it is a
/// deliberate act, and a bad one shows up as a margin rejection on a real
/// order — so it is refused at boot rather than at that point.
fn parse_leverage() -> Result<Option<u32>, ConfigError> {
    match std::env::var("SENTINEL_LEVERAGE") {
        Ok(text) => text
            .parse::<u32>()
            .ok()
            .filter(|v| (1..=125).contains(v))
            .map(Some)
            .ok_or(ConfigError::Invalid {
                name: "SENTINEL_LEVERAGE",
                reason: format!("{text:?} is not a leverage between 1 and 125"),
            }),
        Err(_) => Ok(None),
    }
}

impl Config {
    /// Read the environment.
    ///
    /// # Errors
    /// [`ConfigError`] when something required is absent or unusable. Refused
    /// at boot rather than discovered at the first order: a process that starts
    /// with a broken configuration will find out about it while holding a
    /// position.
    pub fn from_env() -> Result<Self, ConfigError> {
        let journal_dir =
            std::env::var("SENTINEL_JOURNAL").unwrap_or_else(|_| "/data/journal".into());

        // `SENTINEL_SYMBOLS` is what the Python deployment already sets, and
        // reading it means a cutover changes the image and not the config.
        // BTCUSDT is the default for the same reason.
        let symbols = std::env::var("SENTINEL_INSTRUMENTS")
            .or_else(|_| std::env::var("SENTINEL_SYMBOLS"))
            .unwrap_or_else(|_| "BTCUSDT".into());
        let mut instruments = Vec::new();
        for symbol in symbols.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            instruments.push(Instrument::new(symbol).map_err(|e| ConfigError::Invalid {
                name: "SENTINEL_INSTRUMENTS",
                reason: format!("{symbol}: {e}"),
            })?);
        }
        if instruments.is_empty() {
            return Err(ConfigError::Missing("SENTINEL_INSTRUMENTS"));
        }

        let venue = match std::env::var("SENTINEL_VENUE").as_deref() {
            // "futures" is the word the Python deployment already uses.
            Ok("binance" | "futures") => VenueConfig::Binance {
                // The names the existing Fly secrets carry, so a cutover does
                // not need the keys re-entered — which is the moment a secret
                // most often ends up somewhere it should not be.
                api_key: std::env::var("BINANCE_FUTURES_KEY")
                    .or_else(|_| std::env::var("BINANCE_API_KEY"))
                    .map_err(|_| ConfigError::Missing("BINANCE_FUTURES_KEY"))?,
                secret: std::env::var("BINANCE_FUTURES_SECRET")
                    .or_else(|_| std::env::var("BINANCE_API_SECRET"))
                    .map_err(|_| ConfigError::Missing("BINANCE_FUTURES_SECRET"))?,
                // Production is opt-in, by an exact word. Anything else —
                // unset, misspelt, "yes" — is Demo Trading, which is also what
                // the Python deployment runs on.
                demo: std::env::var("SENTINEL_BINANCE_ENV").as_deref() != Ok("production"),
                leverage: parse_leverage()?,
            },
            Ok("delta") => VenueConfig::Delta {
                api_key: std::env::var("DELTA_API_KEY")
                    .map_err(|_| ConfigError::Missing("DELTA_API_KEY"))?,
                secret: std::env::var("DELTA_API_SECRET")
                    .map_err(|_| ConfigError::Missing("DELTA_API_SECRET"))?,
                testnet: std::env::var("SENTINEL_DELTA_ENV").as_deref() != Ok("production"),
            },
            _ => VenueConfig::Simulator,
        };

        let max_position = match std::env::var("SENTINEL_MAX_POSITION") {
            Ok(text) => Some(Qty::parse(&text).map_err(|e| ConfigError::Invalid {
                name: "SENTINEL_MAX_POSITION",
                reason: e.to_string(),
            })?),
            Err(_) => None,
        };

        Ok(Self {
            journal_dir,
            instruments,
            venue,
            max_position,
            tick_interval: Duration::from_millis(250),
            sweep_interval: Duration::from_secs(30),
            stale_after: Duration::from_secs(120),
        })
    }

    /// Whether this configuration can place orders that lose real money.
    ///
    /// Printed at boot, in as many words. An operator should never have to
    /// infer it from a URL in a log line.
    #[must_use]
    pub const fn is_live_money(&self) -> bool {
        matches!(
            self.venue,
            VenueConfig::Binance { demo: false, .. } | VenueConfig::Delta { testnet: false, .. }
        )
    }

    /// A description with no secret in it.
    #[must_use]
    pub fn describe(&self) -> String {
        let venue = match &self.venue {
            VenueConfig::Simulator => "simulator",
            VenueConfig::Binance { demo: true, .. } => "binance futures demo",
            VenueConfig::Binance { demo: false, .. } => "binance futures PRODUCTION",
            VenueConfig::Delta { testnet: true, .. } => "delta testnet",
            VenueConfig::Delta { testnet: false, .. } => "delta PRODUCTION",
        };
        let symbols: Vec<&str> = self
            .instruments
            .iter()
            .map(sentinel_types::InlineStr::as_str)
            .collect();
        format!(
            "{venue}, {}, journal {}",
            symbols.join(","),
            self.journal_dir
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(venue: VenueConfig) -> Config {
        Config {
            journal_dir: "/data/journal".into(),
            instruments: vec![Instrument::new("BTCUSDT").unwrap()],
            venue,
            max_position: None,
            tick_interval: Duration::from_millis(250),
            sweep_interval: Duration::from_secs(30),
            stale_after: Duration::from_secs(120),
        }
    }

    fn binance(demo: bool) -> VenueConfig {
        VenueConfig::Binance {
            api_key: "k".into(),
            secret: "s".into(),
            demo,
            leverage: None,
        }
    }

    fn delta(testnet: bool) -> VenueConfig {
        VenueConfig::Delta {
            api_key: "k".into(),
            secret: "s".into(),
            testnet,
        }
    }

    #[test]
    fn only_production_is_live_money() {
        assert!(!config(VenueConfig::Simulator).is_live_money());
        assert!(!config(binance(true)).is_live_money());
        assert!(!config(delta(true)).is_live_money());
        assert!(config(binance(false)).is_live_money());
        assert!(config(delta(false)).is_live_money());
    }

    #[test]
    fn every_venue_says_which_one_it_is_and_says_production_loudly() {
        assert!(
            config(binance(true))
                .describe()
                .contains("binance futures demo")
        );
        assert!(config(binance(false)).describe().contains("PRODUCTION"));
        assert!(config(delta(true)).describe().contains("delta testnet"));
    }

    #[test]
    fn the_description_carries_no_secret() {
        let venues = [
            VenueConfig::Binance {
                api_key: "my-key".into(),
                secret: "my-secret".into(),
                demo: false,
                leverage: Some(25),
            },
            VenueConfig::Delta {
                api_key: "my-key".into(),
                secret: "my-secret".into(),
                testnet: false,
            },
        ];
        for venue in venues {
            let described = config(venue).describe();
            assert!(!described.contains("my-secret"), "{described}");
            assert!(!described.contains("my-key"), "{described}");
            assert!(described.contains("PRODUCTION"), "and says so loudly");
        }
    }

    #[test]
    fn the_simulator_needs_no_keys() {
        assert!(!config(VenueConfig::Simulator).describe().contains("delta"));
    }

    #[test]
    fn a_configuration_error_says_which_variable() {
        assert_eq!(
            ConfigError::Missing("DELTA_API_KEY").to_string(),
            "DELTA_API_KEY is not set"
        );
        assert_eq!(
            ConfigError::Invalid {
                name: "SENTINEL_MAX_POSITION",
                reason: "malformed decimal".into()
            }
            .to_string(),
            "SENTINEL_MAX_POSITION: malformed decimal"
        );
    }
}
