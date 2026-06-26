use crate::ArbitraryHandler;

pub mod key_value;
type InnerBuf<I> = key_value::KeyValue<time::UtcDateTime,(rosc::OscBundle, I)>;
type Buf<I> = sorted_vec::ReverseSortedVec<InnerBuf<I>>;

///Wrap a [crate::MessageHandler] into a [crate::PeriodicParsingCheck].
///
/// Note, that the [Self::check] function MUST be called regularly, if not-yet-applied [Osc Bundles][rosc::OscBundle] should be applied at all.
/// The polling
#[derive(Debug)]
pub struct PacketHandler<H, I>{
    bundle_buf: Buf<I>,
    pub handler: H,
}

impl<H, I> PacketHandler<H, I> {
    ///Create a new [PacketHandler]
    pub const fn new(handler: H) -> Self {
        Self {
            bundle_buf: Buf::new(),
            handler,
        }
    }
    pub const fn get_buf(&self) -> &Buf<I> {
        &self.bundle_buf
    }
}
impl<O, I: Clone, H: for<'a> ArbitraryHandler<&'a [&'a rosc::OscMessage], I, Output = O>> PacketHandler<H, I> {
    fn apply_bundle(&mut self, bundle: &rosc::OscBundle, extra_info: &I) -> Result<crate::Vec<O>, time::UtcDateTime> {
        let bundle = match self.should_handle_bundle(bundle, &extra_info) {
            Ok(bundle) => bundle,
            Err(date_time) => return Err(date_time),
        };

        let mut msgs = crate::Vec::new();
        let mut bundles = crate::Vec::new();
        let mut content = alloc::vec!(&bundle.content);
        while let Some(bundle) = content.pop() {
            msgs.clear();
            msgs.reserve(bundle.len());
            for message in bundle {
                match message {
                    rosc::OscPacket::Message(msg) => {
                        msgs.push(msg);
                    }
                    rosc::OscPacket::Bundle(bundle) => {
                        let bundle =  match self.should_handle_bundle(bundle, &extra_info) {
                            Ok(bundle) => bundle,
                            Err(_) => continue,
                        };
                        content.push(&bundle.content);
                    }
                }
            }
            bundles.push(self.handler.handle(msgs.as_slice(), extra_info.clone()));
            msgs.clear();
        }

        Ok(bundles)
    }
    fn should_handle_bundle<'a>(&mut self, bundle: &'a rosc::OscBundle, extra_info: &I) -> Result<&'a rosc::OscBundle, time::UtcDateTime> {
        if bundle.timetag.seconds == 0 && bundle.timetag.fractional == 1{
            return Ok(bundle);
        }

        const TWO_POW_32: i64 = (u32::MAX as i64) + 1; // Number of bits in a `u32`
        const NANOS_PER_SECOND: i64 = 1_000_000_000;

        let date_time = time::UtcDateTime::UNIX_EPOCH
            .saturating_add(
                time::Duration::seconds(
                    -2_208_988_800 //From RFC5905
                        + i64::from(bundle.timetag.seconds)
                ).saturating_add(time::Duration::nanoseconds(i64::from(bundle.timetag.fractional) * NANOS_PER_SECOND / TWO_POW_32)) //adopted from rosc crate conversion to SystemTime
            )
            ;
        if time::UtcDateTime::now() > date_time {
            Ok(bundle)
        }else{
            self.bundle_buf.push(core::cmp::Reverse(InnerBuf::new(date_time, (bundle.clone(), extra_info.clone()))));
            Err(date_time)
        }
    }
}

impl<O, I:Clone, H: for<'a> ArbitraryHandler<&'a [&'a rosc::OscMessage], I, Output = O>> ArbitraryHandler<&rosc::OscPacket, I> for PacketHandler<H, I> {
    type Output = Result<crate::Vec<O>, time::UtcDateTime>;
    fn handle(&mut self, message: &rosc::OscPacket, extra_info: I) -> Self::Output {
        match message {
            rosc::OscPacket::Message(msg) => {
                #[cfg(feature = "debug_log")]
                log::trace!("Got a OSC Packet: {}: {:?}", msg.addr, msg.args);
                Ok(alloc::vec![self.handler.handle(&[msg], extra_info)])
            }
            rosc::OscPacket::Bundle(bundle) => {
                self.apply_bundle(bundle, &extra_info)
            }
        }
    }
}
impl<O, I:Clone, H: for<'a> ArbitraryHandler<&'a [&'a rosc::OscMessage], I, Output = O>> crate::PeriodicParsingCheck for PacketHandler<H, I> {
    type CheckOutput = Vec<(Vec<O>, I)>;
    fn needs_check(&self) -> bool { !self.bundle_buf.is_empty() }
    fn check(&mut self) -> Self::CheckOutput {
        let now = time::UtcDateTime::now();
        let to_apply = {
            let partition_point = self.bundle_buf.partition_point(|x| x.0.key > now);
            self.bundle_buf.drain(partition_point..)
                .map(|x| x.0)
                //we consume and create a new iter here to actively consume the drain iter,
                // run the destructor of the drain and to copy the elements we need out
                // (as they could otherwise be overridden I think).
                // Also this scoping allows us to unlock the mutex earlier.
                .collect::<crate::Vec<_>>()
        };

        let mut res = crate::Vec::with_capacity(to_apply.len());
        for i in to_apply {
            match self.apply_bundle(&i.value.0, &i.value.1) {
                Err(_) => continue,
                Ok(v) => res.push((v, i.value.1)),
            }
        }
        res
    }
}