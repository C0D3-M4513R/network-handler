#![cfg(feature = "alloc")]
use crate::{PeriodicParsingCheck, ArbitraryHandler};

///Groups multiple OSC Handlers into one handler.
#[derive(Debug)]
pub struct MultipleHandlers<T> {
    handlers: alloc::boxed::Box<[T]>
}
impl<T> MultipleHandlers<T> {
    /// Create a new instance of an OscHandler, from an array of Handlers
    pub fn new(handlers: alloc::boxed::Box<[T]>) -> Self {
        Self {
            handlers
        }
    }
}
impl<O: Send, M: Clone, I:Clone, T:ArbitraryHandler<M, I, Output = O>> ArbitraryHandler<M, I> for MultipleHandlers<T> {
    type Output = alloc::vec::Vec<O>;
    fn handle(&mut self, message: M, extra_info: I) -> Self::Output {
        self.handlers.iter_mut()
            .map(|handler|handler.handle(message.clone(), extra_info.clone()))
            .collect()
    }
}
impl<T: PeriodicParsingCheck> PeriodicParsingCheck for MultipleHandlers<T> {
    type CheckOutput = alloc::vec::Vec<T::CheckOutput>;
    fn needs_check(&self) -> bool { self.handlers.iter().any(|i|i.needs_check()) }
    fn check(&mut self) -> Self::CheckOutput {
        let mut res = alloc::vec::Vec::new();
        for handler in self.handlers.iter_mut() {
            res.push(handler.check());
        }
        res
    }
}