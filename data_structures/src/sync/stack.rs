use epoch::{self, Atomic, Owned};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ptr;

struct Node<T> {
    value: T,
    next: Atomic<Node<T>>,
}

pub struct Stack<T> {
    head: Atomic<Node<T>>,
    len: AtomicUsize,
}

impl<T> Stack<T> {
    pub fn new() -> Self {
        Stack {
            head: Atomic::null(),
            len: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, value: T) {
        let mut new_node = Owned::new(Node {
            value: value,
            next: Atomic::null(),
        });

        let guard = &epoch::pin();
        loop {
            let head_shared = self.head.load(Ordering::Relaxed, guard);
            new_node.next.store(head_shared, Ordering::Relaxed);
            match self.head.compare_and_set(head_shared, new_node, Ordering::Release, guard) {
                Ok(_) => {
                    self.len.fetch_add(1, Ordering::Release);
                    break;
                },
                Err(e) => new_node = e.new,
            }
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        let guard = &epoch::pin();
        loop {
            let head_shared = self.head.load(Ordering::Acquire, guard);
            match unsafe { head_shared.as_ref() } {
                Some(head) => {
                    let next = head.next.load(Ordering::Relaxed, guard);
                    if self.head.compare_and_set(head_shared, next, Ordering::Release, guard).is_ok() {
                        unsafe {
                            self.len.fetch_sub(1, Ordering::Release);
                            guard.defer(move || head_shared.into_owned());
                            return Some(ptr::read(&(*head).value));
                        }
                    }
                },
                None => return None,
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::Stack;

    #[test]
    fn test_len_empty() {
        let stack: Stack<u32> = Stack::new();
        assert_eq!(stack.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let stack: Stack<u32> = Stack::new();
        assert!(stack.is_empty());
    }

    #[test]
    fn test_push_pop() {
        let stack = Stack::new();
        stack.push(0);
        stack.push(1);

        assert_eq!(stack.len(), 2);
        assert_eq!(stack.try_pop(), Some(1));
        assert_eq!(stack.try_pop(), Some(0));
        assert_eq!(stack.len(), 0);
    }
}
