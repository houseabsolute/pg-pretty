// The libpg_query library doesn't expose these flags. See
// https://github.com/lfittl/libpg_query/issues/77.
use bitflags_serde_int::Deserialize_bitflags_int;

bitflags! {
    #[derive(Deserialize_bitflags_int)]
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
