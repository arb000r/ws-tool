#[cfg(feature = "blocking")]
mod blocking;

#[cfg(feature = "blocking")]
pub use blocking::*;

#[cfg(feature = "async")]
mod non_blocking;

#[cfg(feature = "async")]
pub use non_blocking::*;