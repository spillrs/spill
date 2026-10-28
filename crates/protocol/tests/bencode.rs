use protocol::bencode::{ErrorKind, Limits, MAX_DEPTH, Node, Value, decode, decode_prefix, encode};

#[test]
fn byte_strings_are_borrowed_and_binary_safe() {
    let input = b"3:\0\xffe";
    let node = decode(input, Limits::default()).unwrap();
    let Value::Bytes(bytes) = node.value() else {
        panic!("expected bytes")
    };
    assert_eq!(*bytes, &input[2..]);
    assert_eq!(bytes.as_ptr(), input[2..].as_ptr());
}

#[test]
fn input_and_string_budgets_have_exact_boundaries() {
    let limits = Limits {
        max_input_len: 5,
        max_string_len: 3,
        ..Limits::default()
    };
    assert!(decode(b"3:abc", limits).is_ok());
    assert_eq!(
        decode(b"3:abcd", limits).unwrap_err().kind,
        ErrorKind::InputLimit
    );
    assert_eq!(
        decode(b"4:abc", limits).unwrap_err().kind,
        ErrorKind::StringLimit
    );
    assert_eq!(
        decode(b"3:ab", limits).unwrap_err().kind,
        ErrorKind::UnexpectedEnd
    );
    assert_eq!(
        decode_prefix(b"i1eabc", limits).unwrap_err().kind,
        ErrorKind::InputLimit
    );
}

#[test]
fn node_budget_counts_keys_and_overwritten_values() {
    let limits = Limits {
        max_nodes: 5,
        ..Limits::default()
    };
    assert!(decode(b"d1:ai1e1:ai2ee", limits).is_ok());
    let limits = Limits {
        max_nodes: 4,
        ..limits
    };
    assert_eq!(
        decode(b"d1:ai1e1:ai2ee", limits).unwrap_err().kind,
        ErrorKind::NodeLimit
    );
    let limits = Limits {
        max_nodes: 0,
        ..limits
    };
    assert_eq!(
        decode(b"i0e", limits).unwrap_err().kind,
        ErrorKind::NodeLimit
    );
}

#[test]
fn container_depth_budget_and_hard_ceiling() {
    let limits = Limits {
        max_depth: 0,
        ..Limits::default()
    };
    assert!(decode(b"i1e", limits).is_ok());
    assert_eq!(
        decode(b"le", limits).unwrap_err().kind,
        ErrorKind::DepthLimit
    );
    let limits = Limits {
        max_depth: 1,
        ..limits
    };
    assert!(decode(b"li1ee", limits).is_ok());
    assert_eq!(
        decode(b"llee", limits).unwrap_err().kind,
        ErrorKind::DepthLimit
    );
    let at_limit = format!("{}0:{}", "l".repeat(MAX_DEPTH), "e".repeat(MAX_DEPTH));
    let node = decode(at_limit.as_bytes(), Limits::default()).unwrap();
    assert_eq!(
        encode(&node, Limits::default()).unwrap(),
        at_limit.as_bytes()
    );
    let too_deep = format!("l{at_limit}e");
    assert_eq!(
        decode(too_deep.as_bytes(), Limits::default())
            .unwrap_err()
            .kind,
        ErrorKind::DepthLimit
    );
    let bad_config = Limits {
        max_depth: MAX_DEPTH + 1,
        ..limits
    };
    assert_eq!(
        decode(b"i1e", bad_config).unwrap_err().kind,
        ErrorKind::InvalidLimits
    );
    assert_eq!(
        encode(&Node::new(Value::Integer(1)), bad_config)
            .unwrap_err()
            .kind,
        ErrorKind::InvalidLimits
    );
}

#[test]
fn encoder_budgets_apply_to_constructed_values() {
    let node = Node::new(Value::List(vec![Node::new(Value::Bytes(b"abc"))]));
    let limits = Limits {
        max_output_len: 7,
        ..Limits::default()
    };
    assert_eq!(encode(&node, limits).unwrap(), b"l3:abce");
    let limits = Limits {
        max_output_len: 6,
        ..limits
    };
    assert_eq!(
        encode(&node, limits).unwrap_err().kind,
        ErrorKind::OutputLimit
    );
    let limits = Limits {
        max_nodes: 1,
        ..Limits::default()
    };
    assert_eq!(
        encode(&node, limits).unwrap_err().kind,
        ErrorKind::NodeLimit
    );
    let limits = Limits {
        max_depth: 0,
        ..Limits::default()
    };
    assert_eq!(
        encode(&node, limits).unwrap_err().kind,
        ErrorKind::DepthLimit
    );
    let limits = Limits {
        max_string_len: 2,
        ..Limits::default()
    };
    assert_eq!(
        encode(&node, limits).unwrap_err().kind,
        ErrorKind::StringLimit
    );
    let limits = Limits {
        max_output_len: 0,
        ..Limits::default()
    };
    assert_eq!(
        encode(&node, limits).unwrap_err().kind,
        ErrorKind::OutputLimit
    );
}
