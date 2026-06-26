#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod handlers;


///Handle the processing of a particular type
pub trait ArbitraryHandler<T, I>{
    type Output;
    ///Handles a [T]
    fn handle(&mut self, message: T, extra_info: I) -> Self::Output;
}

///Checks something periodically (e.g. some parsed packets might want to be applied later)
pub trait PeriodicParsingCheck {
    type CheckOutput;

    /// Returns if [Self::check] needs to be run
    fn needs_check(&self) -> bool { true }
    /// Checks something Periodically
    #[must_use]
    fn check(&mut self) -> Self::CheckOutput;
}

///A Trait, which tries to parse a specific message (in context of this crate to an osc packet).
///Any leftover data is returned and given at the start of the buffer to the next call,
/// with new data being appended after.
pub trait RawPacketHandler<I>{
    type Output;
    ///Handle a buffer of received Bytes, returning any bytes, which were not applied yet.
    ///
    ///If no processing can take place, then it is expected, that the input is just returned as-is.
    fn handle<'a>(&mut self, message: &'a[u8], extra_info: I) -> (&'a [u8], Self::Output);
}