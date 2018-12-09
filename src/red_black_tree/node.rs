use crate::entry::Entry;
use crate::red_black_tree::tree;
use std::mem;

/// An enum representing the color of a node in a red black tree.
#[derive(Clone, Copy, PartialEq)]
pub enum Color {
    Red,
    Black,
}

impl Color {
    pub fn flip(self) -> Color {
        match self {
            Color::Red => Color::Black,
            Color::Black => Color::Red,
        }
    }
}

/// A struct representing an internal node of a red black tree.
pub struct Node<T, U> {
    pub entry: Entry<T, U>,
    pub color: Color,
    pub left: tree::Tree<T, U>,
    pub right: tree::Tree<T, U>,
}

impl<T, U> Node<T, U> {
    pub fn new(key: T, value: U) -> Self {
        Node {
            entry: Entry { key, value },
            color: Color::Red,
            left: None,
            right: None,
        }
    }

    pub fn flip_colors(&mut self) {
        self.color = self.color.flip();
        if let Some(ref mut child) = self.left {
            child.color = child.color.flip();
        }
        if let Some(ref mut child) = self.right {
            child.color = child.color.flip();
        }
    }

    pub fn rotate_left(&mut self) {
        let mut child = self
            .right
            .take()
            .expect("Expected right child node to be `Some`.");
        self.right = child.left.take();
        mem::swap(&mut *child, self);
        self.color = child.color;
        child.color = Color::Red;
        self.left = Some(child);
    }

    pub fn rotate_right(&mut self) {
        let mut child = self
            .left
            .take()
            .expect("Expected left child node to be `Some`.");
        self.left = child.right.take();
        mem::swap(&mut *child, self);
        self.color = child.color;
        child.color = Color::Red;
        self.right = Some(child);
    }

    pub fn balance(&mut self) {
        if tree::is_red(&self.right) {
            self.rotate_left();
        }

        let should_rotate = {
            if let Some(ref child) = self.left {
                child.color == Color::Red && tree::is_red(&child.left)
            } else {
                false
            }
        };
        if should_rotate {
            self.rotate_right();
        }

        if tree::is_red(&self.left) && tree::is_red(&self.right) {
            self.flip_colors();
        }
    }

    pub fn shift_left(&mut self) {
        self.flip_colors();
        if let Some(mut child) = self.right.take() {
            if tree::is_red(&child.left) {
                child.rotate_right();
                self.right = Some(child);
                self.rotate_left();
                self.flip_colors();
            } else {
                self.right = Some(child);
            }
        }
    }

    pub fn shift_right(&mut self) {
        self.flip_colors();
        if let Some(child) = self.left.take() {
            if tree::is_red(&child.left) {
                self.left = Some(child);
                self.rotate_right();
                self.flip_colors();
            } else {
                self.left = Some(child);
            }
        }
    }
}
