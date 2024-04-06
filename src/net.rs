use core::fmt;
use std::marker::PhantomData;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec;
use futures::{Stream, StreamExt};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Handshake {
        id: u16,
        leader: Option<u16>,
    },
    Propose {
        id: u16,
    },
    Victory {
        id: u16,
    },
    Refused {
        highest: u16,
    },
}

pub struct Connection {
    to: u16,
    rw: MessageReader<Message, TcpStream>,
    tx: MessageWriter<Message, TcpStream>,
}

impl Connection {
    pub fn new(to: u16, rw: MessageReader<Message, TcpStream>, tx: MessageWriter<Message, TcpStream>) -> Self {
        Self { to, rw, tx }
    }
}

pub struct MessageCodec<T> {
    data: PhantomData<T>,
    data_size: usize,
}

impl<M> MessageCodec<M> {
    pub fn new() -> Self {
        Self {
            data: PhantomData,
            data_size: 0,
        }
    }
}

impl<M: DeserializeOwned> codec::Decoder for MessageCodec<M> {
    type Item = M;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.data_size = src.get_u32() as usize;

        let msg = bincode::deserialize(&src[..self.data_size]).map_err(|_| std::io::ErrorKind::InvalidData)?;
        src.advance(self.data_size);
        Ok(Some(msg))
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

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to)
    }
}

impl Stream for Connection {
    type Item = Result<Message, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rw.poll_next_unpin(cx)
    }
}