use crate::ArbitraryHandler;

pub struct Value<T>(pub T);
impl<O, I, T: for<'a> ArbitraryHandler<&'a I, Output = O>> ArbitraryHandler<I> for Value<T> {
    type Output = O;
    fn handle(&mut self, message: I) -> Self::Output {
        T::handle(&mut self.0, &message)
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