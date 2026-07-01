mod align;
mod compute;
mod resolve;
#[cfg(test)]
mod test_fixtures;

pub use compute::Layout;
pub(crate) use compute::extract_float;
pub use resolve::{RenderSpec, RenderSpecKind, RenderSpecNode, StructChild};
