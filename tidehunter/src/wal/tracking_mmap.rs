use crate::metrics::MetricIntGauge;
use memmap2::MmapMut;
use minibytes::BytesOwner;

/// Wraps `MmapMut` and tracks total mapped bytes via a Prometheus gauge.
/// Increments the gauge by `len` on creation and decrements it on drop.
pub struct TrackingMMapMut {
    inner: MmapMut,
    gauge: MetricIntGauge,
}

impl TrackingMMapMut {
    pub fn new(inner: MmapMut, gauge: MetricIntGauge) -> Self {
        gauge.add(inner.len() as i64);
        Self { inner, gauge }
    }

    pub fn flush(&self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Drop for TrackingMMapMut {
    fn drop(&mut self) {
        self.gauge.add(-(self.inner.len() as i64));
    }
}

impl AsRef<[u8]> for TrackingMMapMut {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl BytesOwner for TrackingMMapMut {}
