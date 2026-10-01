//! In-memory [`horton::BlockDevice`] for tests and demos.
//!
//! Mirrors the shape of horton's own test device: a `Vec` of fixed-size
//! blocks, every poll op resolving immediately.

use core::task::{Context, Poll, Waker};

use horton::BlockDevice;

/// In-memory block device. Blocks are zero-filled on first touch.
#[derive(Debug, Clone)]
pub struct MemDevice<const BLOCK: usize> {
    blocks: Vec<[u8; BLOCK]>,
}

impl<const BLOCK: usize> MemDevice<BLOCK> {
    /// Empty device.
    #[must_use]
    pub fn new() -> Self {
        Self { blocks: Vec::new() }
    }

    fn ensure(&mut self, id: u64) {
        let need = id as usize + 1;
        if self.blocks.len() < need {
            self.blocks.resize(need, [0u8; BLOCK]);
        }
    }
}

impl<const BLOCK: usize> Default for MemDevice<BLOCK> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const BLOCK: usize> BlockDevice for MemDevice<BLOCK> {
    type Error = core::convert::Infallible;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        let zero = [0u8; BLOCK];
        let src: &[u8] = self.blocks.get(id as usize).map_or(&zero, |b| &b[..]);
        buf[..BLOCK].copy_from_slice(&src[..BLOCK]);
        Poll::Ready(Ok(()))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.ensure(id);
        self.blocks[id as usize].copy_from_slice(&buf[..BLOCK]);
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// A no-op waker for driving poll-based device calls in tests.
pub fn noop_waker() -> Waker {
    Waker::noop().clone()
}
