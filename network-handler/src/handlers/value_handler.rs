use crate::ArbitraryHandler;

pub struct Value<T>(pub T);
impl<O, M, I, T: for<'a> ArbitraryHandler<&'a M, I, Output = O>> ArbitraryHandler<M, I> for Value<T> {
    type Output = O;
    fn handle(&mut self, message: M, extra_info: I) -> Self::Output {
        T::handle(&mut self.0, &message, extra_info)
    }
}
impl<T: crate::PeriodicParsingCheck> crate::PeriodicParsingCheck for Value<T> {
    type CheckOutput = T::CheckOutput;

    fn needs_check(&self) -> bool {
        self.0.needs_check()
    }

    fn check(&mut self) -> Self::CheckOutput {
        self.0.check()
    }
}