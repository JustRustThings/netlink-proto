// SPDX-License-Identifier: MIT

use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::{Future, Sink, SinkExt, Stream};
use log::{error, warn};
use netlink_packet_core::{
    NetlinkDeserializable, NetlinkMessage, NetlinkPayload, NetlinkSerializable,
};

use crate::{
    Protocol, Request, Response,
    codecs::{NetlinkCodec, NetlinkMessageCodec},
    framed::NetlinkFramed,
    sys::{AsyncSocket, SocketAddr},
};

#[cfg(feature = "tokio_socket")]
use netlink_sys::TokioSocket as DefaultSocket;
#[cfg(not(feature = "tokio_socket"))]
type DefaultSocket = ();

/// Connection to a Netlink socket, running in the background.
///
/// [`ConnectionHandle`](struct.ConnectionHandle.html) are used to pass new
/// requests to the `Connection`, that in turn, sends them through the netlink
/// socket.
pub struct Connection<
    T,
    S = DefaultSocket,
    C = NetlinkCodec,
    U = UnboundedSender<(NetlinkMessage<T>, SocketAddr)>,
> where
    T: Debug + NetlinkSerializable + NetlinkDeserializable,
    U: Sink<(NetlinkMessage<T>, SocketAddr)> + Unpin,
{
    socket: NetlinkFramed<T, S, C>,

    protocol: Protocol<T, UnboundedSender<NetlinkMessage<T>>>,

    /// Channel used by the user to pass requests to the connection.
    requests_rx: Option<UnboundedReceiver<Request<T>>>,

    /// Channel used to transmit to the ConnectionHandle the unsolicited
    /// messages received from the socket (multicast messages for instance).
    unsolicited_messages_tx: Option<U>,

    socket_closed: bool,

    forward_noop: bool,
    forward_done: bool,
    forward_ack: bool,
}

impl<T, S, C, U> Connection<T, S, C, U>
where
    T: Debug + NetlinkSerializable + NetlinkDeserializable + Unpin,
    S: AsyncSocket,
    C: NetlinkMessageCodec,
    U: Sink<(NetlinkMessage<T>, SocketAddr)> + Unpin,
{
    pub(crate) fn new(
        requests_rx: UnboundedReceiver<Request<T>>,
        unsolicited_messages_tx: U,
        protocol: isize,
    ) -> io::Result<Self> {
        let socket = S::new(protocol)?;
        Ok(Connection {
            socket: NetlinkFramed::new(socket),
            protocol: Protocol::new(),
            requests_rx: Some(requests_rx),
            unsolicited_messages_tx: Some(unsolicited_messages_tx),
            socket_closed: false,
            forward_noop: false,
            forward_done: false,
            forward_ack: false,
        })
    }

    /// Whether [NetlinkPayload::Noop] should forwared to handler
    pub fn set_forward_noop(&mut self, value: bool) {
        self.forward_noop = value;
    }

    /// Whether [NetlinkPayload::Done] should forwared to handler
    pub fn set_forward_done(&mut self, value: bool) {
        self.forward_done = value;
    }

    /// Whether [NetlinkPayload::Ack] should forwared to handler
    pub fn set_forward_ack(&mut self, value: bool) {
        self.forward_ack = value;
    }

    pub fn socket_mut(&mut self) -> &mut S {
        self.socket.get_mut()
    }

    pub fn poll_send_messages(&mut self, cx: &mut Context) {
        trace!("poll_send_messages called");
        let Connection {
            socket, protocol, ..
        } = self;
        let mut socket = Pin::new(socket);

        if !protocol.outgoing_messages.is_empty() {
            trace!(
                "found outgoing message to send checking if socket is ready"
            );
            match Pin::as_mut(&mut socket).poll_ready(cx) {
                Poll::Ready(Err(e)) => {
                    // Sink errors are usually not recoverable. The socket
                    // probably shut down.
                    warn!("netlink socket shut down: {:?}", e);
                    self.socket_closed = true;
                    return;
                }
                Poll::Pending => {
                    trace!("poll is not ready, returning");
                    return;
                }
                Poll::Ready(Ok(_)) => {}
            }

            let (messages, addr) =
                protocol.outgoing_messages.pop_front().unwrap();
            for mut message in messages.into_vec().into_iter() {
                message.finalize();

                trace!("sending outgoing message");
                if let Err(e) =
                    Pin::as_mut(&mut socket).start_send((message, addr))
                {
                    error!("failed to send message: {:?}", e);
                    self.socket_closed = true;
                    return;
                }
            }
        }

        trace!("poll_send_messages done");
        self.poll_flush(cx)
    }

    pub fn poll_flush(&mut self, cx: &mut Context) {
        trace!("poll_flush called");
        if let Poll::Ready(Err(e)) = Pin::new(&mut self.socket).poll_flush(cx) {
            warn!("error flushing netlink socket: {:?}", e);
            self.socket_closed = true;
        }
    }

    pub fn poll_read_messages(&mut self, cx: &mut Context) {
        trace!("poll_read_messages called");
        let mut socket = Pin::new(&mut self.socket);

        if !self.protocol.incoming_requests.is_empty() {
            // avoid reading new messages when we're still processing previous
            // ones
            //
            // this will propagate the backpressure from the
            // unsolicited_messages_tx
            return;
        }

        loop {
            trace!("polling socket");
            match socket.as_mut().poll_next(cx) {
                Poll::Ready(Some((message, addr))) => {
                    trace!("read datagram from socket");
                    self.protocol.handle_message(message, addr);
                }
                Poll::Ready(None) => {
                    warn!("netlink socket stream shut down");
                    self.socket_closed = true;
                    return;
                }
                Poll::Pending => {
                    trace!("no datagram read from socket");
                    return;
                }
            }
        }
    }

    pub fn poll_requests(&mut self, cx: &mut Context) {
        trace!("poll_requests called");
        if let Some(mut stream) = self.requests_rx.as_mut() {
            loop {
                match Pin::new(&mut stream).poll_next(cx) {
                    Poll::Ready(Some(request)) => {
                        self.protocol.request(request)
                    }
                    Poll::Ready(None) => break,
                    Poll::Pending => return,
                }
            }
            let _ = self.requests_rx.take();
            trace!("no new requests to handle poll_requests done");
        }
    }

    pub fn forward_unsolicited_messages(&mut self, cx: &mut Context) {
        trace!("forward_unsolicited_messages called");
        let mut error = false;

        let Connection {
            protocol,
            unsolicited_messages_tx,
            ..
        } = self;

        match unsolicited_messages_tx {
            None => {
                while let Some((message, source)) =
                    self.protocol.incoming_requests.pop_front()
                {
                    warn!(
                        "ignoring unsolicited message {:?} from {:?}",
                        message, source
                    );
                }
                return;
            }
            Some(tx) => {
                loop {
                    // first flush tx to make sure that all previous send
                    // completed before attempting to send
                    // any other item
                    match tx.poll_flush_unpin(cx) {
                        Poll::Pending => return, // will retry on the next call
                        Poll::Ready(Err(_e)) => {
                            error = true;
                            break;
                        }
                        Poll::Ready(Ok(())) => (),
                    }
                    // only pop unsolicited message when unsolicited_messages_tx
                    // is ready to receive
                    match tx.poll_ready_unpin(cx) {
                        Poll::Pending => return,
                        Poll::Ready(Err(_e)) => {
                            error = true;
                            break;
                        }
                        Poll::Ready(Ok(())) => {
                            match protocol.incoming_requests.pop_front() {
                                None => break,
                                Some((message, source)) => {
                                    if let Err(_e) =
                                        tx.start_send_unpin((message, source))
                                    {
                                        error = true;
                                        break;
                                    }
                                    // calls poll_flush() at the start of the
                                    // next iteration
                                }
                            }
                        }
                    }
                }
            }
        }
        if error {
            trace!("unsolicited message stream error, closing stream");
            // The channel is closed so we can drop the sender.
            let _ = self.unsolicited_messages_tx.take();
            // purge `protocol.incoming_requests`
            self.forward_unsolicited_messages(cx);
        }

        trace!("forward_unsolicited_messages done");
    }

    pub fn forward_responses(&mut self) {
        trace!("forward_responses called");
        let protocol = &mut self.protocol;

        while let Some(response) = protocol.incoming_responses.pop_front() {
            let Response {
                message,
                done,
                metadata: tx,
            } = response;
            if done {
                use NetlinkPayload::*;
                match &message.payload {
                    Noop => {
                        if !self.forward_noop {
                            trace!("Not forwarding Noop message to the handle");
                            continue;
                        }
                    }
                    // Since `self.protocol` set the `done` flag here,
                    // we know it has already dropped the request and
                    // its associated metadata, ie the UnboundedSender
                    // used to forward messages back to the
                    // ConnectionHandle. By just continuing we're
                    // dropping the last instance of that sender,
                    // hence closing the channel and signaling the
                    // handle that no more messages are expected.
                    Done(_) => {
                        if !self.forward_done {
                            trace!("Not forwarding Done message to the handle");
                            continue;
                        }
                    }
                    Overrun(_) => {
                        // Forward to the handle: the application must
                        // resync its state after message loss.
                    }
                    // We need to forward error messages and messages
                    // that are part of the netlink subprotocol,
                    // because only the user knows how they want to
                    // handle them.
                    Error(err_msg) => {
                        if err_msg.code.is_none() && !self.forward_ack {
                            trace!("Not forwarding Ack message to the handle");
                            continue;
                        }
                    }
                    InnerMessage(_) => {}
                    _ => {}
                }
            }

            trace!("forwarding response to the handle");
            if tx.unbounded_send(message).is_err() {
                // With an unboundedsender, an error can
                // only happen if the receiver is closed.
                warn!("failed to forward response back to the handle");
            }
        }
        trace!("forward_responses done");
    }

    pub fn should_shut_down(&self) -> bool {
        self.socket_closed
            || (self.unsolicited_messages_tx.is_none()
                && self.requests_rx.is_none())
    }
}

impl<T, S, C, U> Connection<T, S, C, U>
where
    T: Debug + NetlinkSerializable + NetlinkDeserializable + Unpin,
    S: AsyncSocket,
    C: NetlinkMessageCodec,
    U: Sink<(NetlinkMessage<T>, SocketAddr)> + Unpin,
{
    pub(crate) fn from_socket(
        requests_rx: UnboundedReceiver<Request<T>>,
        unsolicited_messages_tx: U,
        socket: S,
    ) -> Self {
        Connection {
            socket: NetlinkFramed::new(socket),
            protocol: Protocol::new(),
            requests_rx: Some(requests_rx),
            unsolicited_messages_tx: Some(unsolicited_messages_tx),
            socket_closed: false,
            forward_noop: false,
            forward_done: false,
            forward_ack: false,
        }
    }
}

impl<T, S, C, U> Future for Connection<T, S, C, U>
where
    T: Debug + NetlinkSerializable + NetlinkDeserializable + Unpin,
    S: AsyncSocket,
    C: NetlinkMessageCodec,
    U: Sink<(NetlinkMessage<T>, SocketAddr)> + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        trace!("polling Connection");
        let pinned = self.get_mut();

        trace!("reading incoming messages");
        pinned.poll_read_messages(cx);

        trace!("forwarding unsolicited messages to the connection handle");
        pinned.forward_unsolicited_messages(cx);

        trace!(
            "forwarding responses to previous requests to the connection handle"
        );
        pinned.forward_responses();

        trace!("handling requests");
        pinned.poll_requests(cx);

        trace!("sending messages");
        pinned.poll_send_messages(cx);

        trace!("done polling Connection");

        if pinned.should_shut_down() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[cfg(all(test, feature = "tokio_socket"))]
mod tests {
    use crate::new_connection;
    use crate::sys::protocols::NETLINK_AUDIT;
    use netlink_packet_audit::AuditMessage;
    use tokio::time;

    #[tokio::test]
    async fn connection_is_closed() {
        let (conn, _, _) =
            new_connection::<AuditMessage>(NETLINK_AUDIT).unwrap();
        let join_handle = tokio::spawn(conn);
        time::sleep(time::Duration::from_millis(200)).await;
        assert!(join_handle.is_finished());
    }
}
