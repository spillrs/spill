use spill::protocol::peer::priority::priority;
use std::net::SocketAddr;

#[test]
fn canonical_priority_vectors_and_symmetry() {
    for (a, b, expected) in [
        ("123.213.32.10:0", "98.76.54.32:0", 0xec2d7224),
        ("123.213.32.10:0", "123.213.32.234:0", 0x99568189),
        ("230.12.123.3:1234", "230.12.123.3:300", 0xae8b7034),
        ("230.12.123.1:1234", "230.12.123.3:300", 0x99c54d79),
        ("230.12.23.1:1234", "230.12.123.3:300", 0x21fd5f80),
        ("230.120.23.1:1234", "230.12.123.3:300", 0xfa9a860e),
        ("[::1]:1234", "[::1]:300", 0xae8b7034),
        (
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:1234",
            "[ffff:0fff:ffff:ffff:ffff:ffff:ffff:ffff]:300",
            0x59d71f38,
        ),
        (
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:1234",
            "[ffff:ffff:0fff:ffff:ffff:ffff:ffff:ffff]:300",
            0x081d5282,
        ),
        (
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:1234",
            "[ffff:ffff:ff0f:ffff:ffff:ffff:ffff:ffff]:300",
            0x755f8512,
        ),
        (
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:1234",
            "[ffff:ffff:ffff:0fff:ffff:ffff:ffff:ffff]:300",
            0x27a88382,
        ),
        (
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:1234",
            "[ffff:ffff:ffff:ff0f:ffff:ffff:ffff:ffff]:300",
            0xfb405bc1,
        ),
        (
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:1234",
            "[ffff:ffff:ffff:ffff:0fff:ffff:ffff:ffff]:300",
            0xf69f444e,
        ),
        (
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:1234",
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe]:300",
            0x3d4c771c,
        ),
    ] {
        let a: SocketAddr = a.parse().unwrap();
        let b: SocketAddr = b.parse().unwrap();
        assert_eq!(priority(a, b), expected, "{a} {b}");
        assert_eq!(priority(b, a), expected, "{b} {a}");
    }
}
