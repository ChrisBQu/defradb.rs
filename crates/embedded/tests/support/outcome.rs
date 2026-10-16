//! The result type of the integration tests: any error, with `context`, `bail!`
//! and `ensure!` naming what was being done when it failed. Included by each
//! test crate with `#[macro_use] #[path = "support/outcome.rs"] mod outcome;`.
#![allow(dead_code, unused_macros, unused_imports)]

use std::fmt::Display;

pub type Result<T = (), E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;

pub trait Context<T> {
    fn context(self, step: impl Display) -> Result<T>;

    fn with_context<F, C>(self, step: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Display;
}

impl<T, E> Context<T> for std::result::Result<T, E>
where
    E: Display,
{
    fn context(self, step: impl Display) -> Result<T> {
        self.map_err(|error| format!("{step}: {error}").into())
    }

    fn with_context<F, C>(self, step: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Display,
    {
        self.map_err(|error| format!("{}: {error}", step()).into())
    }
}

impl<T> Context<T> for Option<T> {
    fn context(self, step: impl Display) -> Result<T> {
        self.ok_or_else(|| step.to_string().into())
    }

    fn with_context<F, C>(self, step: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Display,
    {
        self.ok_or_else(|| step().to_string().into())
    }
}

macro_rules! bail {
    ($($message:tt)+) => {
        return Err(format!($($message)+).into())
    };
}

macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            bail!($($message)+);
        }
    };
}
