use super::Key;

pub(super) struct Arena<T> {
    runtime: u64,
    slots: Vec<Slot<T>>,
    free: Vec<u32>,
}

struct Slot<T> {
    generation: u64,
    value: Option<T>,
}

impl<T> Arena<T> {
    pub(super) fn new(runtime: u64) -> Self {
        Self {
            runtime,
            slots: Vec::new(),
            free: Vec::new(),
        }
    }

    pub(super) fn insert(&mut self, value: T) -> Key {
        let index = self.free.pop().unwrap_or_else(|| {
            let index = u32::try_from(self.slots.len()).expect("arena index exhausted");
            self.slots.push(Slot {
                generation: 1,
                value: None,
            });
            index
        });
        let slot = &mut self.slots[index as usize];
        slot.value = Some(value);
        Key {
            runtime: self.runtime,
            index,
            generation: slot.generation,
        }
    }

    pub(super) fn get(&self, key: Key) -> Option<&T> {
        let slot = self.slots.get(key.index as usize)?;
        (key.runtime == self.runtime && key.generation == slot.generation)
            .then_some(slot.value.as_ref())
            .flatten()
    }

    pub(super) fn get_mut(&mut self, key: Key) -> Option<&mut T> {
        let slot = self.slots.get_mut(key.index as usize)?;
        (key.runtime == self.runtime && key.generation == slot.generation)
            .then_some(slot.value.as_mut())
            .flatten()
    }

    pub(super) fn remove(&mut self, key: Key) -> Option<T> {
        let slot = self.slots.get_mut(key.index as usize)?;
        if key.runtime != self.runtime || key.generation != slot.generation {
            return None;
        }
        let value = slot.value.take()?;
        if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            self.free.push(key.index);
        }
        Some(value)
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.slots.iter().enumerate().filter_map(|(index, slot)| {
            slot.value.as_ref().map(|_| Key {
                runtime: self.runtime,
                index: index as u32,
                generation: slot.generation,
            })
        })
    }
}
