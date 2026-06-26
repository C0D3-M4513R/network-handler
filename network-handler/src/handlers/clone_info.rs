use crate::{ArbitraryHandler, PeriodicParsingCheck};

pub struct CloneInfo<H>(pub H);
impl<M, I:Clone, H: ArbitraryHandler<M, I>> ArbitraryHandler<M, &I> for CloneInfo<H> {
    type Output = H::Output;
    fn handle(&mut self, message: M, extra_info: &I) -> Self::Output {
        self.0.handle(message, extra_info.clone())
    }
}

impl<H: PeriodicParsingCheck> PeriodicParsingCheck for CloneInfo<H> {
    type CheckOutput = H::CheckOutput;
    #[inline]
    fn needs_check(&self) -> bool {
        self.0.needs_check()
    }
    #[inline]
    fn check(&mut self) -> Self::CheckOutput {
        self.0.check()
    }
}