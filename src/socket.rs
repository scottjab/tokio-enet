use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use crate::Error;

type SendToFuture<'a> = Pin<Box<dyn Future<Output = std::io::Result<usize>> + Send + 'a>>;
type RecvFromFuture<'a> =
    Pin<Box<dyn Future<Output = std::io::Result<(usize, SocketAddr)>> + Send + 'a>>;

/// A UDP socket usable by an ENet [`Host`](crate::Host).
///
/// Implemented for [`tokio::net::UdpSocket`]. Implement this trait to run ENet
/// over a custom transport, such as a userspace network stack.
pub trait UdpSocketImpl: Send + Sync + 'static {
    /// Send data to the given address.
    fn send_to<'a>(&'a self, buf: &'a [u8], addr: SocketAddr) -> SendToFuture<'a>;

    /// Receive data and the sender's address.
    fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> RecvFromFuture<'a>;

    /// Get the local address this socket is bound to.
    fn local_addr(&self) -> std::io::Result<SocketAddr>;
}

impl UdpSocketImpl for tokio::net::UdpSocket {
    fn send_to<'a>(&'a self, buf: &'a [u8], addr: SocketAddr) -> SendToFuture<'a> {
        Box::pin(tokio::net::UdpSocket::send_to(self, buf, addr))
    }

    fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> RecvFromFuture<'a> {
        Box::pin(tokio::net::UdpSocket::recv_from(self, buf))
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        tokio::net::UdpSocket::local_addr(self)
    }
}

/// Wrapper around a UDP socket, created with SOCK_CLOEXEC and appropriate buffer sizes.
pub struct EnetSocket {
    inner: Arc<dyn UdpSocketImpl>,
}

const RECEIVE_BUFFER_SIZE: usize = 256 * 1024;
const SEND_BUFFER_SIZE: usize = 256 * 1024;

impl EnetSocket {
    /// Create and bind a new UDP socket with SOCK_CLOEXEC and configured buffer sizes.
    pub fn bind(addr: SocketAddr) -> Result<Self, Error> {
        let domain = if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
        socket.set_nonblocking(true)?;
        socket.set_reuse_address(true)?;
        socket.set_recv_buffer_size(RECEIVE_BUFFER_SIZE)?;
        socket.set_send_buffer_size(SEND_BUFFER_SIZE)?;
        if addr.is_ipv4() {
            socket.set_broadcast(true)?;
        }
        socket.bind(&addr.into())?;

        let std_socket: std::net::UdpSocket = socket.into();
        let tokio_socket = tokio::net::UdpSocket::from_std(std_socket)?;
        Ok(Self {
            inner: Arc::new(tokio_socket),
        })
    }

    /// Wrap an existing socket implementation instead of binding a new OS socket.
    pub fn from_socket(socket: Arc<dyn UdpSocketImpl>) -> Self {
        Self { inner: socket }
    }

    /// Send data to the given address.
    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> Result<usize, Error> {
        Ok(self.inner.send_to(buf, addr).await?)
    }

    /// Receive data and the sender's address.
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), Error> {
        Ok(self.inner.recv_from(buf).await?)
    }

    /// Get the local address this socket is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        Ok(self.inner.local_addr()?)
    }
}
