//! Transform a Home Assistant `state_changed` payload into a ClickHouse row.
//!
//! This module is deliberately free of I/O so it can be unit-tested without a
//! Home Assistant or ClickHouse instance.
//!
//! Timestamps are always normalised to **UTC** before they leave this module.
//! Home Assistant emits ISO-8601/RFC-3339 timestamps with an offset (usually
//! `+00:00`); ClickHouse stores them in `DateTime64(6, 'UTC')` columns.

use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Serialize, Serializer};
use serde_json::Value;
use time::format_description::FormatItem;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

use crate::config::Filters;

/// Format ClickHouse expects for a `DateTime64` value. No offset suffix: the
/// column itself is declared with the `UTC` timezone.
const CH_DATETIME: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6]");

/// Fallback parsers for timestamps without an offset. Home Assistant always
/// sends an offset, but being lenient here costs little and matches Python's
/// `datetime.fromisoformat` behaviour (naive timestamps are assumed UTC).
const NAIVE_NO_FRACTION: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
const NAIVE_FRACTION: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]");

/// One row of `homeassistant.states`.
#[derive(Debug, Clone, Serialize)]
pub struct StateRow {
    /// e.g. `sensor.living_room_temperature`.
    pub entity_id: String,
    /// Raw state string exactly as Home Assistant reported it.
    pub state: String,
    /// Numeric interpretation of `state`, if it parses as a finite `f64`.
    pub state_float: Option<f64>,
    /// JSON-encoded attributes, or an empty string when disabled.
    pub attributes: String,
    /// When the state last changed.
    #[serde(serialize_with = "serialize_datetime")]
    pub last_changed: OffsetDateTime,
    /// When the state was last updated (may be newer than `last_changed`).
    #[serde(serialize_with = "serialize_datetime")]
    pub last_updated: OffsetDateTime,
    /// When Home Assistant fired the event.
    #[serde(serialize_with = "serialize_datetime")]
    pub time_fired: OffsetDateTime,
}

impl StateRow {
    /// Build a row from the `new_state` object of a `state_changed` event.
    ///
    /// Returns `None` when the event should be skipped: no `entity_id`, an
    /// excluded entity, or a missing/invalid `last_updated`.
    #[must_use]
    pub fn from_new_state(
        new_state: &Value,
        time_fired: Option<&str>,
        filters: &Filters,
    ) -> Option<Self> {
        let entity_id = new_state.get("entity_id").and_then(Value::as_str)?;
        if entity_id.is_empty() || filters.exclude.contains(entity_id) {
            return None;
        }

        let last_updated = new_state
            .get("last_updated")
            .and_then(Value::as_str)
            .and_then(parse_ha_timestamp)?;
        let last_changed = new_state
            .get("last_changed")
            .and_then(Value::as_str)
            .and_then(parse_ha_timestamp)
            .unwrap_or(last_updated);
        let time_fired = time_fired
            .and_then(parse_ha_timestamp)
            .unwrap_or(last_updated);

        let state = match new_state.get("state") {
            Some(Value::String(state)) => state.clone(),
            Some(Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        };
        let state_float = state.parse::<f64>().ok().filter(|value| value.is_finite());
        let attributes = attributes_json(new_state, filters);

        Some(Self {
            entity_id: entity_id.to_owned(),
            state,
            state_float,
            attributes,
            last_changed,
            last_updated,
            time_fired,
        })
    }
}

fn attributes_json(new_state: &Value, filters: &Filters) -> String {
    if !filters.include_attributes {
        return String::new();
    }
    let encoded = match new_state.get("attributes") {
        Some(value) if !value.is_null() => {
            serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned())
        }
        _ => "{}".to_owned(),
    };
    match filters.max_attributes_bytes {
        Some(max) if encoded.len() > max => String::new(),
        _ => encoded,
    }
}

static WARNED_BAD_TIMESTAMP: AtomicBool = AtomicBool::new(false);

/// Parse a Home Assistant timestamp and convert it to UTC.
///
/// Accepts RFC-3339 (`2026-10-11T18:10:02.123456+02:00`, `...Z`) and, as a
/// fallback, naive ISO-8601 which is assumed to be UTC. Python's
/// `datetime.fromisoformat` also accepts a space between date and time, so a
/// space is normalised to `T` before parsing.
#[must_use]
pub fn parse_ha_timestamp(raw: &str) -> Option<OffsetDateTime> {
    let normalized = (raw.as_bytes().get(10) == Some(&b' ')).then(|| raw.replacen(' ', "T", 1));
    let candidate = normalized.as_deref().unwrap_or(raw);

    if let Ok(timestamp) = OffsetDateTime::parse(candidate, &Rfc3339) {
        return Some(timestamp.to_offset(UtcOffset::UTC));
    }
    if let Ok(timestamp) = PrimitiveDateTime::parse(candidate, NAIVE_NO_FRACTION) {
        return Some(timestamp.assume_utc());
    }
    if let Ok(timestamp) = PrimitiveDateTime::parse(candidate, NAIVE_FRACTION) {
        return Some(timestamp.assume_utc());
    }

    if !WARNED_BAD_TIMESTAMP.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            value = raw,
            "cannot parse a Home Assistant timestamp; rows with this format will be skipped"
        );
    }
    None
}

fn serialize_datetime<S: Serializer>(
    timestamp: &OffsetDateTime,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let utc = timestamp.to_offset(UtcOffset::UTC);
    let rendered = utc.format(CH_DATETIME).map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&rendered)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn filters() -> Filters {
        Filters {
            include_attributes: true,
            exclude: std::collections::HashSet::new(),
            max_attributes_bytes: None,
        }
    }

    fn state(entity_id: &str, state: Value) -> Value {
        json!({
            "entity_id": entity_id,
            "state": state,
            "attributes": {"unit_of_measurement": "°C", "friendly_name": "Temp"},
            "last_changed": "2026-10-11T18:10:02.123456+02:00",
            "last_updated": "2026-10-11T18:10:02.123456+02:00"
        })
    }

    #[test]
    fn converts_offsets_to_utc_microseconds() {
        let row = StateRow::from_new_state(
            &state("sensor.temperature", json!("21.4")),
            Some("2026-10-11T18:10:03.000001+02:00"),
            &filters(),
        )
        .expect("row");

        assert_eq!(row.last_updated, row.last_changed);
        assert_eq!(
            serde_json::to_value(&row).unwrap()["last_updated"],
            json!("2026-10-11 16:10:02.123456")
        );
        assert_eq!(
            serde_json::to_value(&row).unwrap()["time_fired"],
            json!("2026-10-11 16:10:03.000001")
        );
    }

    #[test]
    fn accepts_zulu_and_naive_timestamps() {
        assert_eq!(
            parse_ha_timestamp("2026-10-11T16:10:02.5Z")
                .unwrap()
                .format(CH_DATETIME)
                .unwrap(),
            "2026-10-11 16:10:02.500000"
        );
        assert_eq!(
            parse_ha_timestamp("2026-10-11T16:10:02")
                .unwrap()
                .format(CH_DATETIME)
                .unwrap(),
            "2026-10-11 16:10:02.000000"
        );
        assert_eq!(
            parse_ha_timestamp("2026-10-11T16:10:02.25")
                .unwrap()
                .format(CH_DATETIME)
                .unwrap(),
            "2026-10-11 16:10:02.250000"
        );
        assert_eq!(
            parse_ha_timestamp("2026-10-11 18:10:02.123456+02:00")
                .unwrap()
                .format(CH_DATETIME)
                .unwrap(),
            "2026-10-11 16:10:02.123456"
        );
        assert_eq!(
            parse_ha_timestamp("2026-10-11 16:10:02")
                .unwrap()
                .format(CH_DATETIME)
                .unwrap(),
            "2026-10-11 16:10:02.000000"
        );
        assert!(parse_ha_timestamp("yesterday").is_none());
    }

    #[test]
    fn parses_numeric_states_only() {
        let numeric = StateRow::from_new_state(
            &state("sensor.temperature", json!("21.4")),
            None,
            &filters(),
        )
        .unwrap();
        assert_eq!(numeric.state_float, Some(21.4));

        let text =
            StateRow::from_new_state(&state("sensor.door", json!("on")), None, &filters()).unwrap();
        assert_eq!(text.state_float, None);

        let infinity =
            StateRow::from_new_state(&state("sensor.bad", json!("inf")), None, &filters()).unwrap();
        assert_eq!(infinity.state_float, None);
    }

    #[test]
    fn missing_state_becomes_empty_string() {
        let mut value = state("binary_sensor.x", Value::Null);
        value["state"] = Value::Null;
        let row = StateRow::from_new_state(&value, None, &filters()).unwrap();
        assert_eq!(row.state, "");
        assert_eq!(row.state_float, None);
    }

    #[test]
    fn excluded_entities_are_skipped() {
        let mut own = filters();
        own.exclude.insert("sensor.temperature".to_owned());
        assert!(
            StateRow::from_new_state(&state("sensor.temperature", json!("21.4")), None, &own)
                .is_none()
        );
    }

    #[test]
    fn attributes_respect_the_toggle_and_the_cap() {
        let mut disabled = filters();
        disabled.include_attributes = false;
        let row =
            StateRow::from_new_state(&state("sensor.temperature", json!("21.4")), None, &disabled)
                .unwrap();
        assert_eq!(row.attributes, "");

        let mut capped = filters();
        capped.max_attributes_bytes = Some(5);
        let row =
            StateRow::from_new_state(&state("sensor.temperature", json!("21.4")), None, &capped)
                .unwrap();
        assert_eq!(row.attributes, "");

        let row = StateRow::from_new_state(
            &state("sensor.temperature", json!("21.4")),
            None,
            &filters(),
        )
        .unwrap();
        let attributes: Value = serde_json::from_str(&row.attributes).unwrap();
        assert_eq!(attributes["unit_of_measurement"], "°C");
    }

    #[test]
    fn missing_attributes_become_an_empty_object() {
        let value = json!({
            "entity_id": "sensor.x",
            "state": "1",
            "last_updated": "2026-10-11T16:10:02Z"
        });
        let row = StateRow::from_new_state(&value, None, &filters()).unwrap();
        assert_eq!(row.attributes, "{}");
        assert_eq!(row.last_changed, row.last_updated);
    }

    #[test]
    fn rows_without_updated_timestamp_are_skipped() {
        let value = json!({"entity_id": "sensor.x", "state": "1"});
        assert!(StateRow::from_new_state(&value, None, &filters()).is_none());
    }
}
