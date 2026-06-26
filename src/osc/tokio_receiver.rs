#![cfg(feature = "tokio")]

use core::convert::Infallible;
use core::net::IpAddr;
use core::time::Duration;
use core::num::NonZeroUsize;
use tokio::net::UdpSocket;
use crate::{ArbitraryHandler, PeriodicParsingCheck};
use crate::handlers::buffered_raw_packet_handler::BufferedRawPacketHandler;
use crate::handlers::combined_handler::{CombinedHandler, CombinedRefHandler};
use crate::handlers::osc::packet_handler::PacketHandler;
use crate::handlers::clone_info::CloneInfo;
use crate::handlers::osc::raw_packet_handler::RawPacketHandler as OscRawPacketHandler;

const DEFAULT_ALLOC:usize = 1024;

///Allows for sending OSC Messages
pub struct OscReceiver<I1, I2, I3> {
    osc_recv:UdpSocket,
    max_message_size: Option<NonZeroUsize>,
    poll_duration: Duration,
    message_handlers: I1,
    packet_handlers: I2,
    raw_packet_handlers: I3,
}
impl<I1, I2, I3> OscReceiver<I1, I2, I3> {
    /// Creates a new OSC Sender.
    /// This will bind a UDP Socket to a random port and connect it to the specified port on the specified ip.
    /// The binding and the connection can both fail, so this function returns a Result.
    pub async fn new(
        ip:IpAddr,
        port:u16,
        max_message_size: Option<NonZeroUsize>,
        poll_duration: Option<Duration>,
        message_handlers: I1,
        packet_handlers: I2,
        raw_packet_handlers: I3,
    ) -> Result<Self, std::io::Error>{
        let osc_recv = match UdpSocket::bind((ip, port)).await {
            Ok(v) => v,
            Err(e) => {
                log::warn!("Failed to Bind and/or connect the OSC UDP receive socket: {}", e);
                Err(e)?
            }
        };
        log::info!("Bound OSC UDP receive Socket.");
        Ok(Self::new_with_socket(
            osc_recv,
            max_message_size,
            poll_duration,
            message_handlers,
            packet_handlers,
            raw_packet_handlers,
        ))
    }

    /// Creates a new OSC Sender from an already bound socket
    pub fn new_with_socket(
        socket:tokio::net::UdpSocket,
        max_message_size: Option<NonZeroUsize>,
        poll_duration: Option<Duration>,
        message_handlers: I1,
        packet_handlers: I2,
        raw_packet_handlers: I3,
    ) -> Self{
        Self{
            osc_recv: socket,
            max_message_size,
            poll_duration: poll_duration.unwrap_or(Duration::from_secs(1)),
            message_handlers,
            packet_handlers,
            raw_packet_handlers,
        }
    }
}

type Handler<'a,H1,H2,H3, I> = CombinedHandler<H3, BufferedRawPacketHandler<OscRawPacketHandler<CombinedRefHandler<CloneInfo<PacketHandler<H1, I>>, H2>>>>;
OscReceiver_listen!(listen, core::net::SocketAddr, tokio::net::UdpSocket::recv_buf_from, core::convert::identity);
OscReceiver_listen!(listen_recv, &'static str, tokio::net::UdpSocket::recv_buf, |v:Result<usize, std::io::Error>|v.map(|v|(v, "")));
macro_rules! OscReceiver_listen {
    ($name: ident, $ty:ty, $listen:expr, $listen_post:expr) => {
impl<
    O1, O3,
    H1:for<'a> ArbitraryHandler<&'a [&'a rosc::OscMessage], $ty, Output =O1> + Sync + Send + 'static,
    H2:ArbitraryHandler<rosc::OscPacket, $ty> + PeriodicParsingCheck + Sync + Send + 'static,
    H3:for<'a> crate::ArbitraryHandler<&'a [u8], $ty, Output = O3> + PeriodicParsingCheck + Sync + Send + 'static,
> OscReceiver<H1, H2, H3> {
    pub fn $name<
        CheckFut:core::future::Future<Output = ()> + Send,
        Fut:core::future::Future<Output = ()> + Send,
        Iter: Iterator<Item = rosc::OscError> + Send,
    >(
        self,
        js: &mut tokio::task::JoinSet<Infallible>,
        mut check_handler: impl FnMut(
            <Handler<'_, H1, H2, H3, $ty> as PeriodicParsingCheck>::CheckOutput,
            &'_ mut Handler<'_,H1, H2, H3, $ty>,
        ) -> CheckFut + Send + 'static,
        mut packet_handler: impl FnMut(
            <Handler<'_, H1, H2, H3, $ty> as ArbitraryHandler<&[u8], $ty>>::Output,
            &'_ mut Handler<'_, H1, H2, H3, $ty>,
            $ty,
        ) -> (Iter, Fut) + Send + 'static,
    ) {
        let Self {
            osc_recv,
            max_message_size,
            poll_duration,
            message_handlers,
            packet_handlers,
            raw_packet_handlers,
        } = self;

        let mut handler =
            CombinedHandler::new(
                raw_packet_handlers,
                BufferedRawPacketHandler::new(
                    OscRawPacketHandler::new(
                        CombinedRefHandler::new(CloneInfo(
                            PacketHandler::new(
                                message_handlers
                            )),
                            packet_handlers,
                        )
                    ),
                    max_message_size
                )
            )
        ;

        js.spawn(async move {
            let mut periodic = tokio::time::interval(poll_duration);
            periodic.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let buf_size = max_message_size.map(NonZeroUsize::get).unwrap_or(DEFAULT_ALLOC);
            let parsing_buf_size = max_message_size.map(NonZeroUsize::get).unwrap_or(usize::MAX);
            let mut buf = Vec::with_capacity(buf_size);

            loop {
                buf.clear(); //This is strictly an Udp byte receive buffer. Additional Packet Parsing buffers exist further down the stack.
                tokio::select! {
                    biased;
                    _ = periodic.tick(), if handler.needs_check() => {
                        check_handler(handler.check(), &mut handler).await;
                    },
                    out = $listen(&osc_recv, &mut buf) => {
                        let out = $listen_post(out);
                        match out {
                            Err(e) => {
                                log::error!("Error receiving udp packet. Discarding receive Buffer. Skipping Packet: {}",e);
                                buf.clear();
                            }
                            Ok((size, addr)) => {
                                #[cfg(feature = "debug_log")]
                                {
                                    log::debug!("Received a Packet with {size} bytes and info: {addr}");
                                }
                                if size != buf.len() {
                                    log::warn!("Read bytes from packet with info {addr} has a mismatched buffer size. {size} bytes were supposedly read, but the buffer contains {} bytes", buf.len());
                                }
                                let (iter, fut) = packet_handler(<Handler<H1, H2, H3, $ty> as ArbitraryHandler<&[u8], $ty>>::handle(&mut handler, buf.as_slice(), addr), &mut handler, addr);
                                fut.await;

                                for e in iter{
                                    match e {
                                        rosc::OscError::BadPacket(reason) => {
                                            log::trace!("OSC packet not decodable yet? Reason: {reason}");
                                            if handler.handler2.get_buffer().len() >= parsing_buf_size {
                                                log::warn!("OSC packet not decodable yet, but the receiving buffer is full? Discarding message buffer. Reason: {reason}");
                                                handler.handler2.clear_buffer();
                                            } else{
                                                continue;
                                            }
                                        },
                                        rosc::OscError::ReadError(nom::error::ErrorKind::Eof) => {
                                            log::trace!("Got EOF Read error when trying to deserialize packet. Waiting for more data");
                                            if handler.handler2.get_buffer().len() >= parsing_buf_size {
                                                log::warn!("Got EOF Read error when trying to deserialize packet, but the receiving buffer is full. Discarding message buffer.");
                                                handler.handler2.clear_buffer();
                                            } else{
                                                continue;
                                            }
                                        },
                                        e => {
                                            log::error!("Error handling raw packet. Clearing internal receive buffer and skipping packet: {e}");
                                            handler.handler2.clear_buffer();
                                        }
                                    }
                                }
                            }
                        };
                    }
                }
            }
        });
    }
}
    };
}
use OscReceiver_listen;