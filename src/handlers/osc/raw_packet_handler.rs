use crate::ArbitraryHandler;

///Wrap a [crate::PeriodicParsingCheck] into a [crate::RawPacketHandler]
#[non_exhaustive]
#[derive(Debug)]
pub struct RawPacketHandler<H> {
    pub handler: H
}

impl<H> RawPacketHandler<H> {
    /// Create a new [RawPacketHandler]
    pub const fn new(handler: H) -> Self {
        Self { handler }
    }
}

impl<I, H: ArbitraryHandler<rosc::OscPacket, I> + crate::PeriodicParsingCheck> crate::RawPacketHandler<I> for RawPacketHandler<H> {
    type Output = Result<H::Output, rosc::OscError>;

    fn handle<'a>(&mut self, message: &'a [u8], extra_info: I) -> (&'a [u8], Self::Output) {
        #[cfg(feature="debug_log")]
        log::trace!("Received UDP Packet with size {} ",message.len());
        match rosc::decoder::decode_udp(message) {
            Err(e) => {
                log::error!("Error decoding udp packet into an OSC Packet: {}", e);
                #[cfg(feature="debug_log")]
                log::trace!("Packet contents were: {:#X?}",message);
                (message, Err(e))
            }
            Ok((rest, packet)) => {
                let fut = self.handler.handle(packet, extra_info);
                (rest, Ok(fut))
            },
        }
    }
}
impl<H: crate::PeriodicParsingCheck> crate::PeriodicParsingCheck for RawPacketHandler<H> {
    type CheckOutput = H::CheckOutput;
    #[inline]
    fn needs_check(&self) -> bool { self.handler.needs_check() }
    #[inline]
    fn check(&mut self) -> Self::CheckOutput {
        self.handler.check()
    }
}