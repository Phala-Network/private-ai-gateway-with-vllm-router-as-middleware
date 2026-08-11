//! Golden parity test: replays response-transform fixtures under
//! `tests/fixtures/` and asserts the Rust transforms produce canonically equal
//! output. Request transforms intentionally do not exist in router middleware:
//! downstream PAG owns request body shaping, while this service routes and
//! forwards the already-shaped bytes unchanged.

use private_ai_gateway::middleware::response_transform::transform_response;
use private_ai_gateway::middleware::types::{Endpoint, ProviderFormat};
use serde_json::Value;

const RESPONSE_FIXTURES: &str = include_str!("fixtures/response_golden.json");

fn parse_format(value: &str) -> ProviderFormat {
    match value {
        "openai" => ProviderFormat::Openai,
        "anthropic" => ProviderFormat::Anthropic,
        other => panic!("unknown format {other}"),
    }
}

fn parse_endpoint(value: &str) -> Endpoint {
    match value {
        "chatComplete" => Endpoint::ChatComplete,
        "complete" => Endpoint::Complete,
        "embed" => Endpoint::Embed,
        "messages" => Endpoint::Messages,
        "createModelResponse" => Endpoint::CreateModelResponse,
        other => panic!("unknown endpoint {other}"),
    }
}

// Structural equality with number comparison by value, so an integer-valued
// float (e.g. Node `2`) matches a Rust integer `2`. Object key order is ignored;
// array order is significant.
fn canonical_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(xf), Some(yf)) => xf == yf,
            _ => x == y,
        },
        (Value::Array(xs), Value::Array(ys)) => {
            xs.len() == ys.len() && xs.iter().zip(ys).all(|(x, y)| canonical_eq(x, y))
        }
        (Value::Object(xo), Value::Object(yo)) => {
            xo.len() == yo.len()
                && xo
                    .iter()
                    .all(|(k, xv)| yo.get(k).map(|yv| canonical_eq(xv, yv)).unwrap_or(false))
        }
        _ => a == b,
    }
}

// Drop the non-deterministic `created` timestamp the response transforms inject.
fn strip_created(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(strip_created).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(k, _)| k.as_str() != "created")
                .map(|(k, v)| (k.clone(), strip_created(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

#[test]
fn rust_response_transforms_match_node_fixtures() {
    let cases: Vec<Value> =
        serde_json::from_str(RESPONSE_FIXTURES).expect("parse response fixtures");
    assert!(!cases.is_empty(), "no response fixtures found");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let format = parse_format(case["format"].as_str().unwrap());
        let endpoint = parse_endpoint(case["fn"].as_str().unwrap());
        let input = case["input"].clone();

        let output = strip_created(&transform_response(format, endpoint, input));
        let expected = &case["output"];
        assert!(
            canonical_eq(&output, expected),
            "case {name}: Rust response output does not match Node fixture\n  rust: {output}\n  node: {expected}"
        );
    }
}
