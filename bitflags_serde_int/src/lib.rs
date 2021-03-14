//! Derive `Serialize` and `Deserialize` that treats a struct created with the
//! [bitflags](https://crates.io/crates/bitflags) crate's `bitflags!` macro as
//! an integer.
//!
//! If you are exchanging data with an external system that represents C-style
//! enum flags as integers, then it's likely that this system will serialize
//! these values as ints. The natural way to work with these flags in Rust is
//! to use the [bitflags](https://crates.io/crates/bitflags) crate to mirror
//! the enum values. But the default serialization for structs created with
//! `bitflags` is as an object with a `bits` field, like `{"bits": 42}`.
//!
//! This crate lets you serialize and deserialize these structs as ints
//! instead.
//!
//! Examples
//!
//! ```
//! #[macro_use]
//! extern crate bitflags;
//! use bitflags_serde_int::{Deserialize_bitflags_int, Serialize_bitflags_int};
//!
//! bitflags! {
//!     #[derive(Deserialize_bitflags_int, Serialize_bitflags_int)]
//!     pub struct GenreFlags: u32 {
//!         const ROCK = 0x01;
//!         const SYNTH_POP = 0x02;
//!         const JAZZ = 0x04;
//!     }
//! }
//!
//! fn main() -> serde_json::Result<()> {
//!     let rock = GenreFlags::ROCK;
//!     let j = serde_json::to_string(&rock)?;
//!     assert_eq!(j, "1");
//!
//!     let jazz_rock = GenreFlags::ROCK | GenreFlags::JAZZ;
//!     let j = serde_json::to_string(&jazz_rock)?;
//!     assert_eq!(j, "5");
//!
//!     let synth_pop: GenreFlags = serde_json::from_str("2")?;
//!     assert_eq!(synth_pop.bits, GenreFlags::SYNTH_POP.bits);
//!
//!     let jazz_rock2: GenreFlags = serde_json::from_str("5")?;
//!     assert_eq!(jazz_rock2.bits, jazz_rock.bits);
//!
//!     Ok(())
//! }
//! ```

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Ident};

#[proc_macro_derive(Serialize_bitflags_int)]
pub fn derive_serialize(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input);
    impl_serialize_macro(&ast)
}

fn impl_serialize_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        #[automatically_derived]
        impl serde::Serialize for #name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_u32(self.bits)
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(Deserialize_bitflags_int)]
pub fn derive_deserialize(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input);
    impl_deserialize_macro(&ast)
}

fn impl_deserialize_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let visitor_name = format_ident!("__BitflagsSerdeInt{}Visitor", name);
    let visitor = visitor_for(&name, &visitor_name);
    let gen = quote! {
        #[automatically_derived]
        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<#name, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_u32(#visitor_name)
            }
        }

        #visitor
    };
    gen.into()
}

fn visitor_for(name: &Ident, visitor_name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        struct #visitor_name;

        #[automatically_derived]
        impl #visitor_name {
            fn new_bitflags_struct<E>(value: u32) -> Result<#name, E>
            where
                E: serde::de::Error,
            {
                match #name::from_bits(value) {
                    Some(fo) => Ok(fo),
                    None => Err(E::custom(format!(
                        "invalid bitmask for #name: {:#034b}",
                        value
                    ))),
                }
            }
        }

        #[automatically_derived]
        impl<'de> serde::de::Visitor<'de> for #visitor_name {
            type Value = #name;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an unsigned 64-bit integer")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value > u32::MAX.into() {
                    Err(E::custom(format!(
                        "value is greater than u32 max value: {} > {}",
                        value,
                        u32::MAX
                    )))
                } else {
                    Self::new_bitflags_struct(value as u32)
                }
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value < u32::MIN.into() {
                    Err(E::custom(format!(
                        "value is less than u32 min value: {} < {}",
                        value,
                        u32::MIN
                    )))
                } else {
                    Self::new_bitflags_struct(value as u32)
                }
            }
        }
    }
}
