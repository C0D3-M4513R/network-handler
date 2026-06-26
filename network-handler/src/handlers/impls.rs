use crate::{ArbitraryHandler, PeriodicParsingCheck, RawPacketHandler};

//<editor-fold desc="Implementations for Option">
impl<O, M, I, T:ArbitraryHandler<M, I, Output = O>> ArbitraryHandler<M, I> for Option<T> {
    type Output = Option<O>;
    fn handle(&mut self, message: M, extra_info: I) -> Self::Output {
        self.as_mut().map(|v|v.handle(message, extra_info))
    }
}
impl<O, I, T:RawPacketHandler<I, Output = O>> RawPacketHandler<I> for Option<T> {
    type Output = Option<O>;
    fn handle<'a>(&mut self, message: &'a [u8], extra_info: I) -> (&'a [u8], Self::Output) {
        self.as_mut().map(|v|v.handle(message, extra_info)).map_or((&[], None), |(r, v)|(r, Some(v)))
    }
}
impl<T: PeriodicParsingCheck> PeriodicParsingCheck for Option<T> {
    type CheckOutput = Option<T::CheckOutput>;
    fn needs_check(&self) -> bool { self.as_ref().map(|v|v.needs_check()).unwrap_or(false) }
    fn check(&mut self) -> Self::CheckOutput {
        self.as_mut().map(|v|v.check())
    }
}
//</editor-fold>
//<editor-fold desc="Implementations for Infallible">
impl<T, I> ArbitraryHandler<T, I> for core::convert::Infallible {
    type Output = core::convert::Infallible;
    fn handle(&mut self, _: T, _:I) -> Self::Output { *self }
}
impl<I> crate::RawPacketHandler<I> for core::convert::Infallible {
    type Output = core::convert::Infallible;
    fn handle(&mut self, _: &'_[u8], _:I) -> (&'static[u8], Self::Output) { (&[], *self) }
}
impl PeriodicParsingCheck for core::convert::Infallible {
    type CheckOutput = core::convert::Infallible;
    fn needs_check(&self) -> bool { false }
    fn check(&mut self) -> Self::CheckOutput { *self }
}
//</editor-fold>