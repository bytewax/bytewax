/// Implements all the recovery traits Bytewax needs, but does not
/// load or backup any data.
pub struct NoopRecovery;

impl NoopRecovery {
    pub fn new() -> Self {
        NoopRecovery {}
    }
}

impl<T> StateWriter<T> for NoopRecovery
where
    T: Debug,
{
    fn write(&mut self, update: &StateUpdate<T>) {
        trace!("Noop wrote state update {update:?}");
    }
}

impl<T> StateCollector<T> for NoopRecovery
where
    T: Debug,
{
    fn delete(&mut self, key: &StateRecoveryKey<T>) {
        trace!("Noop deleted state for {key:?}");
    }
}

impl<T> StateReader<T> for NoopRecovery {
    fn read(&mut self) -> Option<StateUpdate<T>> {
        None
    }
}

impl<T> ProgressWriter<T> for NoopRecovery
where
    T: Debug,
{
    fn write(&mut self, update: &ProgressUpdate<T>) {
        trace!("Noop wrote progress update {update:?}");
    }
}

impl<T> ProgressReader<T> for NoopRecovery {
    fn read(&mut self) -> Option<ProgressUpdate<T>> {
        None
    }
}
