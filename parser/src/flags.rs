// The libpg_query library doesn't expose these flags. See
// https://github.com/lfittl/libpg_query/issues/77.

use serde::Deserialize;
use std::convert::TryFrom;
use thiserror::Error;

// Values come from src/include/nodes/parsenodes.h.
bitflags! {
    #[derive(Deserialize)]
    #[serde(transparent)]
    pub struct FrameOptions: u32 {
        const NONDEFAULT = 0x00001 ; /* any specified? */
        const RANGE = 0x00002 ; /* RANGE behavior */
        const ROWS = 0x00004 ; /* ROWS behavior */
        const BETWEEN = 0x00008 ; /* BETWEEN given? */
        const START_UNBOUNDED_PRECEDING = 0x00010 ; /* start is U. P. */
        const END_UNBOUNDED_PRECEDING = 0x00020 ; /* (disallowed) */
        const START_UNBOUNDED_FOLLOWING = 0x00040 ; /* (disallowed) */
        const END_UNBOUNDED_FOLLOWING = 0x00080 ; /* end is U. F. */
        const START_CURRENT_ROW = 0x00100 ; /* start is C. R. */
        const END_CURRENT_ROW = 0x00200 ; /* end is C. R. */
        const START_VALUE_PRECEDING = 0x00400 ; /* start is V. P. */
        const END_VALUE_PRECEDING = 0x00800 ; /* end is V. P. */
        const START_VALUE_FOLLOWING = 0x01000 ; /* start is V. F. */
        const END_VALUE_FOLLOWING = 0x02000 ; /* end is V. F. */

        const START_VALUE = Self::START_VALUE_PRECEDING.bits | Self::START_VALUE_FOLLOWING.bits;
        const END_VALUE = Self::END_VALUE_PRECEDING.bits | Self::END_VALUE_FOLLOWING.bits;

        const DEFAULTS =
            Self::RANGE.bits | Self::START_UNBOUNDED_PRECEDING.bits | Self::END_CURRENT_ROW.bits;

        const START = Self::START_UNBOUNDED_PRECEDING.bits
            | Self::START_UNBOUNDED_FOLLOWING.bits
            | Self::START_CURRENT_ROW.bits
            | Self::START_VALUE_PRECEDING.bits
            | Self::START_VALUE_FOLLOWING.bits;

        const END = Self::END_UNBOUNDED_PRECEDING.bits
            | Self::END_UNBOUNDED_FOLLOWING.bits
            | Self::END_CURRENT_ROW.bits
            | Self::END_VALUE_PRECEDING.bits
            | Self::END_VALUE_FOLLOWING.bits;
    }
}

impl Default for FrameOptions {
    fn default() -> FrameOptions {
        FrameOptions::DEFAULTS
    }
}

// Values come from src/include/utils/datetime.h.
bitflags! {
    pub struct IntervalMask: u32 {
        const MONTH = 1 << 1;
        const YEAR = 1 << 2;
        const DAY = 1 << 3;
        const HOUR = 1 << 10;
        const MINUTE = 1 << 11;
        const SECOND = 1 << 12;
    }
}

#[derive(Debug, Error)]
pub enum IntervalMaskError {
    #[error("{source:}")]
    CannotConvertI64ToIntervalMask {
        #[from]
        source: std::num::TryFromIntError,
    },
    #[error("invalid Interval Mask value - {val:}")]
    InvalidIntervalMask { val: u32 },
    #[error(
        "IntervalMask resolved to {count:} modifiers ({mods:}), but should not have more than 2"
    )]
    TooManyModifiers { count: usize, mods: String },
}

impl IntervalMask {
    pub fn new_from_i64(i: i64) -> Result<IntervalMask, IntervalMaskError> {
        let u = u32::try_from(i)?;
        Self::from_bits(u).ok_or(IntervalMaskError::InvalidIntervalMask { val: u })
    }

    pub fn type_modifiers(self) -> Result<Vec<&'static str>, IntervalMaskError> {
        let mut mods: Vec<&'static str> = vec![];
        if self.contains(Self::YEAR) {
            mods.push("YEAR");
        }
        if self.contains(Self::MONTH) {
            mods.push("MONTH");
        }
        if self.contains(Self::DAY) {
            mods.push("DAY");
        }
        if self.contains(Self::HOUR) {
            mods.push("HOUR");
        }
        if self.contains(Self::MINUTE) {
            mods.push("MINUTE");
        }
        if self.contains(Self::SECOND) {
            mods.push("SECOND");
        }

        // We don't have to check for length 0 because our constructor rejects
        // a mask which contains bits that don't correspond to a modifier.
        if mods.len() > 2 {
            return Err(IntervalMaskError::TooManyModifiers {
                count: mods.len(),
                mods: mods.join(", "),
            });
        }

        Ok(mods)
    }
}
