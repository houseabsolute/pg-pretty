use serde::{Deserialize, Deserializer};

// The libpg_query library doesn't expose these flags. See
// https://github.com/lfittl/libpg_query/issues/77.

bitflags! {
    pub struct FrameOptions: u32 {
        const FRAMEOPTION_NONDEFAULT = 0x00001 ; /* any specified? */
        const FRAMEOPTION_RANGE = 0x00002 ; /* RANGE behavior */
        const FRAMEOPTION_ROWS = 0x00004 ; /* ROWS behavior */
        const FRAMEOPTION_BETWEEN = 0x00008 ; /* BETWEEN given? */
        const FRAMEOPTION_START_UNBOUNDED_PRECEDING = 0x00010 ; /* start is U. P. */
        const FRAMEOPTION_END_UNBOUNDED_PRECEDING = 0x00020 ; /* (disallowed) */
        const FRAMEOPTION_START_UNBOUNDED_FOLLOWING = 0x00040 ; /* (disallowed) */
        const FRAMEOPTION_END_UNBOUNDED_FOLLOWING = 0x00080 ; /* end is U. F. */
        const FRAMEOPTION_START_CURRENT_ROW = 0x00100 ; /* start is C. R. */
        const FRAMEOPTION_END_CURRENT_ROW = 0x00200 ; /* end is C. R. */
        const FRAMEOPTION_START_VALUE_PRECEDING = 0x00400 ; /* start is V. P. */
        const FRAMEOPTION_END_VALUE_PRECEDING = 0x00800 ; /* end is V. P. */
        const FRAMEOPTION_START_VALUE_FOLLOWING = 0x01000 ; /* start is V. F. */
        const FRAMEOPTION_END_VALUE_FOLLOWING = 0x02000 ; /* end is V. F. */

        const FRAMEOPTION_START_VALUE =
            Self::FRAMEOPTION_START_VALUE_PRECEDING.bits | Self::FRAMEOPTION_START_VALUE_FOLLOWING.bits;
        const FRAMEOPTION_END_VALUE =
            Self::FRAMEOPTION_END_VALUE_PRECEDING.bits | Self::FRAMEOPTION_END_VALUE_FOLLOWING.bits;

        const FRAMEOPTION_DEFAULTS =
            Self::FRAMEOPTION_RANGE.bits | Self::FRAMEOPTION_START_UNBOUNDED_PRECEDING.bits |
                Self::FRAMEOPTION_END_CURRENT_ROW.bits;
    }
}

impl Default for FrameOptions {
    fn default() -> FrameOptions {
        FrameOptions::FRAMEOPTION_DEFAULTS
    }
}

impl<'de> Deserialize<'de> for FrameOptions {
    fn deserialize<D>(deserializer: D) -> Result<FrameOptions, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u32(FrameOptionsVisitor)
    }
}

use serde::de::{self, Visitor};
use std::fmt;

struct FrameOptionsVisitor;

impl FrameOptionsVisitor {
    fn new_frame_options<E>(value: u32) -> Result<FrameOptions, E>
    where
        E: de::Error,
    {
        match FrameOptions::from_bits(value) {
            Some(fo) => Ok(fo),
            None => Err(E::custom(format!(
                "invalid bitmask for frame options: {:#034b}",
                value
            ))),
        }
    }
}

impl<'de> Visitor<'de> for FrameOptionsVisitor {
    type Value = FrameOptions;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an unsigned 64-bit integer")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value > u32::MAX.into() {
            Err(E::custom(format!("u32 out of range: {}", value)))
        } else {
            Self::new_frame_options(value as u32)
        }
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value < u32::MIN.into() {
            Err(E::custom(format!("u32 out of range: {}", value)))
        } else {
            Self::new_frame_options(value as u32)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_deserialize() {
        let f: FrameOptions = serde_json::from_str("530").unwrap();
        assert_eq!(f.bits, FrameOptions::FRAMEOPTION_DEFAULTS.bits);
    }
}
