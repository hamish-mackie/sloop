//! Shared mutation helpers.

/// Replaces up to `replaced` bytes of `seed` at `position` with `injected`,
/// snapping both cut points to char boundaries so the result stays valid
/// UTF-8 for any input.
pub fn splice(seed: &str, position: usize, replaced: usize, injected: &str) -> String {
    let start = floor_char_boundary(seed, position.min(seed.len()));
    let end = floor_char_boundary(seed, (start + replaced).min(seed.len()));
    format!("{}{}{}", &seed[..start], injected, &seed[end..])
}

fn floor_char_boundary(text: &str, mut index: usize) -> usize {
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}
