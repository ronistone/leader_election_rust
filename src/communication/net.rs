use core::fmt;
use std::marker::PhantomData;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec;
use futures::{Sink, Stream, StreamExt};
use futures::sink::SinkExt;
use serde::Serialize;

pub struct MessageCodec<T> {
    data: PhantomData<T>,
    data_size: Option<usize>,
}

impl<M> MessageCodec<M> {
    pub fn new() -> Self {
        Self {
            data: PhantomData,
            data_size: None,
        }
    }
}

impl<M: DeserializeOwned> codec::Decoder for MessageCodec<M> {
    type Item = M;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.data_size.is_none() {
            // try read header
            if src.len() < 4 {
                return Ok(None);
            }
            self.data_size = Some(src.get_u32() as usize);
        }

        if let Some(len) = self.data_size {
            if src.len() < len {
                src.reserve(len - src.len());
                return Ok(None);
            }
            self.data_size = None;
            let msg = bincode::deserialize(&src[..len]).map_err(|_| std::io::ErrorKind::InvalidData)?;
            src.advance(len);
            return Ok(Some(msg));
        }
        Ok(None)
    }
}

impl<M: Serialize> codec::Encoder<M> for MessageCodec<M> {
    type Error = std::io::Error;

    fn encode(&mut self, item: M, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let data_len = bincode::serialized_size(&item).unwrap() as usize;
        dst.reserve(4 + data_len);
        dst.put_u32(data_len as u32);
        let mut buf = dst.writer();
        bincode::serialize_into(&mut buf, &item).expect("serialization error");
        Ok(())
    }
}

pub type MessageReader<M, IO> = codec::FramedRead<ReadHalf<IO>, MessageCodec<M>>;
pub type MessageWriter<M, IO> = codec::FramedWrite<WriteHalf<IO>, MessageCodec<M>>;
pub fn message_split<RM, WM, IO>(io: IO) -> (MessageReader<RM, IO>, MessageWriter<WM, IO>)
where
    RM: Serialize + DeserializeOwned,
    WM: Serialize + DeserializeOwned,
    IO: AsyncRead + AsyncWrite,
{
    let (r, w) = tokio::io::split(io);
    (
        codec::FramedRead::new(r, MessageCodec::new()),
        codec::FramedWrite::new(w, MessageCodec::new()),
    )
}


pub struct Connection<M> {
    to: u16,
    rw: MessageReader<M, TcpStream>,
    tx: MessageWriter<M, TcpStream>,
}

impl<M> Connection<M> {
    pub fn new(to: u16, rw: MessageReader<M, TcpStream>, tx: MessageWriter<M, TcpStream>) -> Self {
        Self { to, rw, tx }
    }
}

impl<M> fmt::Debug for Connection<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to)
    }
}

impl<M> Stream for Connection<M> {
    type Item = Result<M, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rw.poll_next_unpin(cx)
    }
}

impl<M> Sink<M> for Connection<M> {
    type Error = std::io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx.poll_ready_unpin(cx)
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, item: M) -> Result<(), Self::Error> {
        self.tx.start_send_unpin(item)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx.poll_flush_unpin(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx.poll_close_unpin(cx)
    }
}