//! Low-level, generic extensions for Timely.

use std::collections::{BTreeSet, HashMap};

use timely::communication::message::RefOrMut;
use timely::dataflow::operators::Capability;
use timely::progress::frontier::MutableAntichain;

/// Helper class for buffering input in a Timely operator.
pub(crate) struct InBuffer<D> {
    tmp: Vec<D>,
    buffer: HashMap<u64, Vec<D>>,
}

impl<D> InBuffer<D>
where
    D: Clone,
{
    pub(crate) fn new() -> Self {
        Self {
            tmp: Vec::new(),
            buffer: HashMap::new(),
        }
    }

    /// Buffer that this input was received on this epoch.
    pub(crate) fn extend(&mut self, epoch: u64, incoming: RefOrMut<Vec<D>>) {
        assert!(self.tmp.is_empty());
        incoming.swap(&mut self.tmp);
        self.buffer
            .entry(epoch)
            .or_insert_with(Vec::new)
            .append(&mut self.tmp);
    }

    /// Get all input received on a given epoch.
    ///
    /// It's your job to decide if this epoch will see more data using
    /// some sort of notificator or checking the frontier.
    pub(crate) fn remove(&mut self, epoch: &u64) -> Option<Vec<D>> {
        self.buffer.remove(epoch)
    }

    /// Return an iterator of all the epochs currently buffered.
    pub(crate) fn epochs(&self) -> impl Iterator<Item = u64> + '_ {
        self.buffer.keys().copied()
    }
}

/// Extension trait for frontiers.
pub(crate) trait FrontierEx {
    /// Collapse a frontier into a single epoch value.
    ///
    /// We can do this because we're using a totally ordered epoch in
    /// our dataflows.
    fn simplify(&self) -> Option<u64>;

    /// Is a given epoch closed based on this frontier?
    fn is_closed(&self, epoch: &u64) -> bool {
        self.simplify().iter().all(|frontier| *epoch < *frontier)
    }

    /// Is this input EOF and will see no more values?
    fn is_eof(&self) -> bool {
        self.simplify().is_none()
    }
}

impl FrontierEx for MutableAntichain<u64> {
    fn simplify(&self) -> Option<u64> {
        self.frontier().iter().min().copied()
    }
}

/// To allow collapsing frontiers of all inputs in an operator.
impl FrontierEx for [MutableAntichain<u64>] {
    fn simplify(&self) -> Option<u64> {
        self.iter().flat_map(FrontierEx::simplify).min()
    }
}

/// Extension trait for vectors of capabilities.
pub(crate) trait CapabilityVecEx {
    /// Run [`Capability::downgrade`] on all capabilities.
    ///
    /// Since capabilities retain their output port, you can do this
    /// on the vec of initial caps.
    fn downgrade_all(&mut self, epoch: &u64);
}

impl CapabilityVecEx for Vec<Capability<u64>> {
    fn downgrade_all(&mut self, epoch: &u64) {
        self.iter_mut().for_each(|cap| cap.downgrade(epoch));
    }
}

/// Manages running logic functions and iteratively downgrading
/// capabilities based on the current operator's input frontiers.
///
/// Any state that you need full ownership on EOF, you can put in the
/// state variable and have full ownership of it in the EOF
/// logic. Otherwise, you can close over it mutably in the epoch
/// logics.
///
/// This is like [`timely::dataflow::operators::Notificator`] but does
/// _not_ wait until the epoch is fully closed to run logic.
pub(crate) struct EagerNotificator<D> {
    /// We have to retain separate capabilities per-output. This seems
    /// to be only documented in
    /// https://github.com/TimelyDataflow/timely-dataflow/pull/187
    caps_state: Option<(Vec<Capability<u64>>, D)>,
    queue: BTreeSet<u64>,
}

impl<D> EagerNotificator<D> {
    pub(crate) fn stateful_new(init_caps: Vec<Capability<u64>>, init_state: D) -> Self {
        Self {
            caps_state: Some((init_caps, init_state)),
            queue: BTreeSet::new(),
        }
    }

    /// Mark this epoch as having seen input items and logic should be
    /// called when the time is right.
    ///
    /// We need to use the "notify at" pattern of Timely's built in
    /// [`timely::dataflow::operators::Notificator`] because otherwise
    /// we don't the largest epoch to process as closed on EOF.
    pub(crate) fn notify_at(&mut self, epoch: u64) {
        self.queue.insert(epoch);
    }

    /// Do some logic in epoch order eagerly.
    ///
    /// Call this on each operator activation with the current input
    /// frontiers.
    ///
    /// Eager logic could be called multiple times for an epoch. Epoch
    /// close logic will be called once when each epoch closes. EOF
    /// logic will be called once when the input is finished and will
    /// drop the state.
    ///
    /// Automatically downgrades capabilities for each output to the
    /// relevant epochs.
    pub(crate) fn stateful_for_each(
        &mut self,
        input_frontiers: &[MutableAntichain<u64>],
        mut epoch_eager_logic: impl FnMut(&[Capability<u64>], &mut D),
        mut epoch_close_logic: impl FnMut(&[Capability<u64>], &mut D),
        eof_logic: impl FnOnce(D),
    ) {
        if let Some(frontier) = input_frontiers.simplify() {
            assert!(self.caps_state.is_some(), "frontier re-opened");
            self.caps_state = self.caps_state.take().map(|(mut caps, mut state)| {
                // Drain off epochs from the queue that are less than
                // the frontier and so we can run both logics on
                // them. Ones in advance of the frontier remain in the
                // queue for later.
                let lt_frontier = {
                    // Do a little swap-a-roo since
                    // [`BTreeSet::split_off`] only calculates >=, but we
                    // want <.
                    let ge_frontier = self.queue.split_off(&frontier);
                    std::mem::replace(&mut self.queue, ge_frontier)
                };

                // Will iterate in epoch order since [`BTreeSet`].
                for epoch in lt_frontier {
                    caps.downgrade_all(&epoch);

                    epoch_eager_logic(&caps, &mut state);
                    epoch_close_logic(&caps, &mut state);
                }

                // Now eagerly execute the frontier. No need to call
                // logic if we haven't any data at the frontier yet.
                caps.downgrade_all(&frontier);
                if self.queue.contains(&frontier) {
                    epoch_eager_logic(&caps, &mut state);
                }

                (caps, state)
            });
        // None means EOF on all inputs.
        } else {
            // This will only be called once, so we can take the state
            // out with ownership.
            if let Some((mut caps, mut state)) = self.caps_state.take() {
                // Since there's no BTreeSet::drain. Will iterate in
                // epoch order since [`BTreeSet`].
                while let Some(epoch) = self.queue.pop_first() {
                    caps.downgrade_all(&epoch);

                    epoch_eager_logic(&caps, &mut state);
                    epoch_close_logic(&caps, &mut state);
                }

                // This drops the state.
                eof_logic(state);
                // Drop caps because there will be no more input.
            }
            // Ignore if we re-activate multiple times after EOF.
        }
    }
}

/// A version where you don't need to close any state.
impl EagerNotificator<()> {
    pub(crate) fn new(init_caps: Vec<Capability<u64>>) -> Self {
        Self::stateful_new(init_caps, ())
    }

    /// Just like [`stateful_for_each`] but hides the state if you're
    /// not using it so there's less symbol line noise.
    pub(crate) fn for_each(
        &mut self,
        input_frontiers: &[MutableAntichain<u64>],
        mut epoch_eager_logic: impl FnMut(&[Capability<u64>]),
        mut epoch_close_logic: impl FnMut(&[Capability<u64>]),
    ) {
        self.stateful_for_each(
            input_frontiers,
            |caps, ()| epoch_eager_logic(caps),
            |caps, ()| epoch_close_logic(caps),
            // Do nothing on frontier close.
            |()| {},
        );
    }
}
