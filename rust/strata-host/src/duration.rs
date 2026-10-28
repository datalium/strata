//! Durations in settings: a number of seconds, or a string with a unit
//! (`"250ms"`, `"30s"`, `"5m"`, `"1h"`, `"30d"`).

use std::fmt;
use std::time::Duration;

use serde::de::{self, Deserializer, Visitor};

/// Parses `text` as a duration: a number followed by `ms`, `s`, `m`, `h` or
/// `d`, or a bare number of seconds.
pub(crate) fn parse(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let (number, seconds_per_unit) = [
        ("ms", 0.001),
        ("s", 1.0),
        ("m", 60.0),
        ("h", 3600.0),
        ("d", 86_400.0),
    ]
    .iter()
    .find_map(|(unit, scale)| {
        text.strip_suffix(unit)
            .map(|number| (number.trim_end(), *scale))
    })
    .unwrap_or((text, 1.0));
    match number.parse::<f64>() {
        Ok(value) if value.is_finite() && value >= 0.0 => {
            Ok(Duration::from_secs_f64(value * seconds_per_unit))
        }
        _ => Err(format!(
            "'{text}' is not a duration: use a number of seconds, or a number with ms, s, m, h or d"
        )),
    }
}

struct DurationVisitor;

impl Visitor<'_> for DurationVisitor {
    type Value = Duration;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a number of seconds, or a string such as \"30s\" or \"5m\"")
    }

    fn visit_u64<E: de::Error>(self, seconds: u64) -> Result<Duration, E> {
        Ok(Duration::from_secs(seconds))
    }

    fn visit_i64<E: de::Error>(self, seconds: i64) -> Result<Duration, E> {
        u64::try_from(seconds)
            .map(Duration::from_secs)
            .map_err(|_| E::custom("a duration can't be negative"))
    }

    fn visit_f64<E: de::Error>(self, seconds: f64) -> Result<Duration, E> {
        if seconds.is_finite() && seconds >= 0.0 {
            Ok(Duration::from_secs_f64(seconds))
        } else {
            Err(E::custom("a duration can't be negative"))
        }
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<Duration, E> {
        parse(text).map_err(E::custom)
    }
}

/// `#[serde(deserialize_with = "duration::required")]`
pub(crate) fn required<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    deserializer.deserialize_any(DurationVisitor)
}

/// `#[serde(deserialize_with = "duration::optional")]`: `null` is none.
pub(crate) fn optional<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error> {
    struct OptionalVisitor;

    impl<'de> Visitor<'de> for OptionalVisitor {
        type Value = Option<Duration>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a duration, or null")
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            required(deserializer).map(Some)
        }
    }

    deserializer.deserialize_option(OptionalVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_duration_is_seconds_or_a_number_with_a_unit() {
        assert_eq!(parse("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse("30d").unwrap(), Duration::from_secs(30 * 86_400));
        assert_eq!(parse("1.5 s").unwrap(), Duration::from_millis(1500));
        assert_eq!(parse("45").unwrap(), Duration::from_secs(45));
        assert_eq!(parse(" 0 ").unwrap(), Duration::ZERO);
        for bad in ["", "soon", "-5s", "5 weeks", "nanm"] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }
}
