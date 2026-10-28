use spill::protocol::upnp;

// Minimal IGD descriptions: an IPv4 connection service nested in embedded
// devices (miniupnpd layout), and a PPP service with a URLBase.
const MINI: &str = "<?xml version=\"1.0\"?><root xmlns=\"urn:schemas-upnp-org:device-1-0\"><device><deviceType>urn:schemas-upnp-org:device:InternetGatewayDevice:1</deviceType><deviceList><device><deviceType>urn:schemas-upnp-org:device:WANDevice:1</deviceType><deviceList><device><deviceType>urn:schemas-upnp-org:device:WANConnectionDevice:1</deviceType><serviceList><service><serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType><serviceId>urn:upnp-org:serviceId:WANIPConn1</serviceId><controlURL>/ctl/IPConn</controlURL></service></serviceList></device></deviceList></device></deviceList></device></root>";
const LINKSYS: &str = "<?xml version=\"1.0\"?><root xmlns=\"urn:schemas-upnp-org:device-1-0\"><URLBase>http://192.168.1.1:49152</URLBase><device><deviceList><device><deviceList><device><serviceList><service><serviceType>urn:schemas-upnp-org:service:WANPPPConnection:1</serviceType><controlURL>/upnp/control/WANPPPConn1</controlURL></service></serviceList></device></deviceList></device></deviceList></device></root>";
const IP: &str = "urn:schemas-upnp-org:service:WANIPConnection:1";

#[test]
fn nested_and_url_base_descriptions_extract_control_and_base() {
    assert_eq!(
        upnp::control_url(MINI.as_bytes()),
        Some((IP.into(), "/ctl/IPConn".into(), None))
    );
    assert_eq!(
        upnp::control_url(LINKSYS.as_bytes()),
        Some((
            "urn:schemas-upnp-org:service:WANPPPConnection:1".into(),
            "/upnp/control/WANPPPConn1".into(),
            Some("http://192.168.1.1:49152".into())
        ))
    );
    let mut prefixed = MINI.replace(
        "<root ",
        "<root xmlns:d=\"urn:schemas-upnp-org:device-1-0\" ",
    );
    for tag in ["service", "serviceType", "controlURL"] {
        prefixed = prefixed
            .replace(&format!("<{tag}>"), &format!("<d:{tag}>\n "))
            .replace(&format!("</{tag}>"), &format!(" \n</d:{tag}>"));
    }
    prefixed = prefixed.replace("/ctl/IPConn", "../control/ip?x=1&amp;y=2");
    assert_eq!(
        upnp::control_url(prefixed.as_bytes()),
        Some((IP.into(), "../control/ip?x=1&y=2".into(), None))
    );
    let absolute = LINKSYS.replace(
        "/upnp/control/WANPPPConn1",
        "http://192.168.1.2:1234/control",
    );
    assert_eq!(
        upnp::control_url(absolute.as_bytes()).unwrap().1,
        "http://192.168.1.2:1234/control"
    );
}

#[test]
fn service_preference_is_independent_of_document_order() {
    let service = |kind, path| {
        format!(
            "<service><serviceType>urn:schemas-upnp-org:service:{kind}</serviceType><controlURL>{path}</controlURL></service>"
        )
    };
    let xml = format!(
        "<root><URLBase>http://router:5000/base/</URLBase>{}{}{}</root>",
        service("WANPPPConnection:1", "ppp"),
        service("WANIPConnection:1", "ip1"),
        service("WANIPConnection:2", "ip2")
    );
    assert_eq!(
        upnp::control_url(xml.as_bytes()),
        Some((
            "urn:schemas-upnp-org:service:WANIPConnection:2".into(),
            "ip2".into(),
            Some("http://router:5000/base/".into())
        ))
    );
}

#[test]
fn ssdp_searches_both_igd_versions_and_accepts_case_insensitive_headers() {
    for (request, version) in upnp::search_request().iter().zip([1, 2]) {
        assert!(request.starts_with("M-SEARCH * HTTP/1.1\r\n"));
        assert!(request.contains("HOST: 239.255.255.250:1900\r\n"));
        assert!(request.contains("MAN: \"ssdp:discover\"\r\nMX: 2\r\n"));
        assert!(request.ends_with(&format!("InternetGatewayDevice:{version}\r\n\r\n")));
    }
    assert_eq!(upnp::search_reply(b"HTTP/1.1 200 OK\r\nCache-Control: max-age=1800\r\nmalformed header without a colon\r\nlOcAtIoN:  http://192.168.1.1:5000/rootDesc.xml \r\nST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\r\n"),
        Some("http://192.168.1.1:5000/rootDesc.xml".into()));
    for bad in [
        b"HTTP/1.1 404 Not Found\r\nLOCATION: http://router/\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nLOCATION: \r\n\r\n",
        b"garbage",
    ] {
        assert!(upnp::search_reply(bad).is_none());
    }
}

#[test]
fn soap_success_faults_and_xml_escaping() {
    let fields = upnp::response(b"<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:GetExternalIPAddressResponse xmlns:u=\"urn:schemas-upnp-org:service:WANIPConnection:1\"><NewExternalIPAddress>1.2.3.4</NewExternalIPAddress></u:GetExternalIPAddressResponse></s:Body></s:Envelope>").unwrap();
    assert_eq!(fields["NewExternalIPAddress"], "1.2.3.4");
    for (code, message) in [
        (725, "OnlyPermanentLeasesSupported"),
        (718, "ConflictInMappingEntry"),
    ] {
        let xml = format!(
            "<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><s:Fault><faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail><UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>{code}</errorCode><errorDescription>{message}</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>"
        );
        assert_eq!(upnp::response(xml.as_bytes()), Err(code));
    }
    let (action, body) = upnp::request(
        IP,
        "AddPortMapping",
        &[
            ("NewRemoteHost", ""),
            ("NewPortMappingDescription", "<&>\"'"),
        ],
    );
    assert_eq!(action, format!("\"{IP}#AddPortMapping\""));
    assert!(body.contains("<NewRemoteHost></NewRemoteHost>"));
    assert!(body.contains(
        "<NewPortMappingDescription>&lt;&amp;&gt;&quot;&apos;</NewPortMappingDescription>"
    ));
    let reply = b"<s:Envelope xmlns:s='http://schemas.xmlsoap.org/soap/envelope/'><s:Body><u:TestResponse xmlns:u='urn:test'><Value>&lt;&amp;&gt;&quot;&apos;&#65;&#x42;</Value><Empty/></u:TestResponse></s:Body></s:Envelope>";
    let fields = upnp::response(reply).unwrap();
    assert_eq!(fields["Value"], "<&>\"'AB");
    assert_eq!(fields["Empty"], "");
    assert!(
        upnp::response(b"<Envelope><Body><AddPortMappingResponse/></Body></Envelope>")
            .unwrap()
            .is_empty()
    );
    for action in ["GetExternalIPAddress", "DeletePortMapping"] {
        let (header, body) = upnp::request(IP, action, &[]);
        assert_eq!(header, format!("\"{IP}#{action}\""));
        assert!(body.contains(&format!("</u:{action}>")));
    }
}

#[test]
fn malformed_xml_entities_and_faults_are_rejected_without_panicking() {
    for bad in [
        "<!DOCTYPE root [<!ENTITY x SYSTEM 'file:///etc/passwd'>]><root/>",
        "<root><service></root>",
        "<root><!-- unfinished",
        "<root x='unterminated>",
    ] {
        assert!(upnp::control_url(bad.as_bytes()).is_none());
    }
    for bad in [
        "<Body><TestResponse><Value>&unknown;</Value></TestResponse></Body>",
        "<Body><Fault/></Body>",
        "<root/>",
        "<Body><TestResponse>",
    ] {
        assert_eq!(upnp::response(bad.as_bytes()), Err(0));
    }
    for n in 0..MINI.len() {
        let _ = upnp::control_url(&MINI.as_bytes()[..n]);
    }
}
