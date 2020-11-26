// The libpg_query library doesn't expose these flags. See
// https://github.com/lfittl/libpg_query/issues/77.
use bitflags_serde_int::Deserialize_bitflags_int;

bitflags! {
    #[derive(Deserialize_bitflags_int)]
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
