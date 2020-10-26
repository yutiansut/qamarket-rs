use std::{fs, env};
use std::path::Path;
use serde_derive::*;
use toml;
use lazy_static::lazy_static;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Defines and parses CLI argument for this server.
pub fn parse_cli_args<'a>() -> clap::ArgMatches<'a> {
    clap::App::new("qa")
        .version(VERSION)
        .arg(
            clap::Arg::with_name("config")
                .required(false)
                .help("Path to configuration file")
                .index(1),
        )
        .get_matches()
}

/// Parses CLI arguments, finds location of config file, and parses config file into a struct.
pub fn parse_config_from_cli_args(matches: &clap::ArgMatches) -> Config {
    let conf = match matches.value_of("config") {
        Some(config_path) => match Config::from_file(config_path) {
            Ok(config) => config,
            Err(msg) => {
                println!("Failed to parse config file {}: {}", config_path, msg);
                std::process::exit(1);
            }
        },
        None => {
            println!("No config file specified");
            std::process::exit(1);
        }
    };
    conf
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    pub common: Common,
}

impl Config {
    /// Read configuration from a file into a new Config struct.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let path = path.as_ref();
        println!("Reading configuration from {}", path.display());

        let data = match fs::read_to_string(path) {
            Ok(data) => data,
            Err(err) => return Err(err.to_string()),
        };

        let conf: Config = match toml::from_str(&data) {
            Ok(conf) => conf,
            Err(err) => return Err(err.to_string()),
        };

        Ok(conf)
    }
}



#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Common {
    pub subscribe_list: Vec<String>,
}

impl Default for Common {
    fn default() -> Self {
        Self {
            subscribe_list: vec![],
        }
    }
}


pub fn new_config() -> Config {
    let _args: Vec<String> = env::args().collect();
    let cfg: Config = parse_config_from_cli_args(&parse_cli_args());
    cfg
}


lazy_static! {
    pub static ref CONFIG: Config = new_config();
}
