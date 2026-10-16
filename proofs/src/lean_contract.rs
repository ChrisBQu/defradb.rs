//! Lean-generated conformance contract loader.
//!
//! `proofs/lean/Conformance.lean` emits a JSON contract (vocabularies derived
//! from the Lean models) between two sentinel lines. We run it via
//! `lake env lean --run`, slice out the JSON, and deserialize it. Tests then
//! assert the live Rust types still match — so a rename in the code breaks the
//! proof's binding loudly. Mirrors the Gents `lean_vocab_test` pattern.

use serde::de::DeserializeOwned;
use std::path::PathBuf;
use std::process::Command;

/// Why the Lean contract could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("failed to spawn `lake` in {dir}: {source}")]
    Spawn { dir: String, source: std::io::Error },
    #[error("`lake env lean --run Conformance.lean` failed:\n{0}")]
    Generator(String),
    #[error("contract BEGIN marker missing")]
    MissingBegin,
    #[error("contract END marker missing")]
    MissingEnd,
    #[error("contract END marker precedes BEGIN marker")]
    MarkersReversed,
    #[error("parsing contract json:\n{json}\n{source}")]
    Parse {
        json: String,
        source: serde_json::Error,
    },
}

pub type Result<T, E = ContractError> = std::result::Result<T, E>;

pub const BEGIN: &str = "---BEGIN DEFRA LEAN CONTRACT JSON---";
pub const END: &str = "---END DEFRA LEAN CONTRACT JSON---";

pub fn lean_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lean")
}

/// Run the Lean contract generator and return its full stdout.
pub fn run_generator() -> Result<String> {
    let dir = lean_dir();
    let out = Command::new("lake")
        .args(["env", "lean", "--run", "Conformance.lean"])
        .current_dir(&dir)
        .output()
        .map_err(|source| ContractError::Spawn {
            dir: dir.display().to_string(),
            source,
        })?;
    if !out.status.success() {
        return Err(ContractError::Generator(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Slice the contract JSON out of the generator's stdout.
pub fn extract_json(stdout: &str) -> Result<&str> {
    let begin = stdout.find(BEGIN).ok_or(ContractError::MissingBegin)? + BEGIN.len();
    let end = stdout.find(END).ok_or(ContractError::MissingEnd)?;
    if end < begin {
        return Err(ContractError::MarkersReversed);
    }
    Ok(stdout[begin..end].trim())
}

/// Build, run, extract, and deserialize the Lean contract.
pub fn load_contract<T: DeserializeOwned>() -> Result<T> {
    let stdout = run_generator()?;
    let json = extract_json(&stdout)?;
    serde_json::from_str(json).map_err(|source| ContractError::Parse {
        json: json.to_owned(),
        source,
    })
}

#[derive(Debug, serde::Deserialize)]
pub struct ContractSnapshot {
    pub generated_by: String,
    pub vocabularies: Vec<Vocabulary>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Vocabulary {
    pub domain: String,
    pub values: Vec<String>,
}

impl ContractSnapshot {
    pub fn vocab(&self, domain: &str) -> Option<&Vocabulary> {
        self.vocabularies.iter().find(|v| v.domain == domain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_json_slices_between_markers() {
        let stdout = format!("noise\n{BEGIN}\n{{\"k\":1}}\n{END}\ntrailing");
        assert_eq!(extract_json(&stdout).unwrap(), "{\"k\":1}");
    }

    #[test]
    fn extract_json_errors_without_markers() {
        assert!(extract_json("no markers here").is_err());
    }
}
