#[cfg(test)]
#[macro_use]
extern crate bitflags;
use bitflags_serde_int::{Deserialize_bitflags_int, Serialize_bitflags_int};

bitflags! {
    #[derive(Deserialize_bitflags_int, Serialize_bitflags_int)]
    pub struct GenreFlags: u32 {
        const ROCK = 0x01;
        const SYNTH_POP = 0x02;
        const JAZZ = 0x04;
    }
}

#[test]
fn test_deserialize() {
    let rock: GenreFlags = serde_json::from_str("1").unwrap();
    assert_eq!(rock.bits, GenreFlags::ROCK.bits);

    let synth_pop: GenreFlags = serde_json::from_str("2").unwrap();
    assert_eq!(synth_pop.bits, GenreFlags::SYNTH_POP.bits);

    let jazz_rock: GenreFlags = serde_json::from_str("5").unwrap();
    assert_eq!(
        jazz_rock.bits,
        GenreFlags::ROCK.bits | GenreFlags::JAZZ.bits
    );

    let invalid: Result<GenreFlags, serde_json::Error> = serde_json::from_str("9");
    assert!(invalid.is_err());
    let json_err: serde_json::Error = invalid.unwrap_err();
    assert!(format!("{}", json_err)
        .starts_with("invalid bitmask for #name: 0b00000000000000000000000000001001"));
}

#[test]
fn test_serialize() {
    let rock = GenreFlags::ROCK;
    assert_eq!(serde_json::to_string(&rock).unwrap(), "1");

    let jazz_rock = GenreFlags::ROCK | GenreFlags::JAZZ;
    assert_eq!(serde_json::to_string(&jazz_rock).unwrap(), "5");
}
