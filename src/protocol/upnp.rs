//! SSDP and the IGD SOAP subset; no network authority.
use std::collections::BTreeMap;

pub fn search_request() -> [String; 2] {
    [1, 2].map(|version| format!(
        "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 2\r\nST: urn:schemas-upnp-org:device:InternetGatewayDevice:{version}\r\n\r\n"
    ))
}
pub fn search_reply(bytes: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.lines();
    let mut status = lines.next()?.split_whitespace();
    if status.next()? != "HTTP/1.1" || status.next()? != "200" {
        return None;
    }
    for line in lines.take_while(|line| !line.is_empty()) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("location") {
            return (!value.trim().is_empty()).then(|| value.trim().to_owned());
        }
    }
    None
}

// IGD needs element text, not a general XML document model.
fn elements(xml: &str) -> Option<Vec<(&str, &str)>> {
    let mut stack: Vec<(&str, usize)> = Vec::new();
    let mut result = Vec::new();
    let mut pos = 0;
    while let Some(start) = xml[pos..].find('<').map(|n| pos + n) {
        let tail = &xml[start..];
        if tail.starts_with("<!--") {
            pos = start + tail.find("-->")? + 3;
            continue;
        }
        if tail.starts_with("<![CDATA[") {
            pos = start + tail.find("]]>")? + 3;
            continue;
        }
        if tail.starts_with("<?") {
            pos = start + tail.find("?>")? + 2;
            continue;
        }
        if tail.starts_with("<!") {
            return None;
        }
        let mut quote = None;
        let end = tail.char_indices().skip(1).find_map(|(n, c)| {
            if Some(c) == quote {
                quote = None;
            } else if quote.is_none() {
                if c == '\'' || c == '"' {
                    quote = Some(c);
                } else if c == '>' {
                    return Some(start + n);
                }
            }
            None
        })?;
        let tag = xml[start + 1..end].trim();
        if let Some(name) = tag.strip_prefix('/') {
            let (open, content) = stack.pop()?;
            if open != name.trim() {
                return None;
            }
            result.push((open.rsplit(':').next()?, &xml[content..start]));
        } else {
            let name = tag.split([' ', '\t', '\r', '\n', '/']).next()?;
            if name.is_empty() {
                return None;
            }
            if tag.ends_with('/') {
                result.push((name.rsplit(':').next()?, ""));
            } else {
                stack.push((name, end + 1));
            }
        }
        pos = end + 1;
    }
    stack.is_empty().then_some(result)
}
fn unescape(value: &str) -> Option<String> {
    if let Some(cdata) = value
        .strip_prefix("<![CDATA[")
        .and_then(|v| v.strip_suffix("]]>"))
    {
        return Some(cdata.to_owned());
    }
    if value.contains('<') {
        return None;
    }
    let mut out = String::new();
    let mut rest = value;
    while let Some(n) = rest.find('&') {
        out.push_str(&rest[..n]);
        let (entity, tail) = rest[n + 1..].split_once(';')?;
        out.push(match entity {
            "amp" => '&',
            "lt" => '<',
            "gt" => '>',
            "quot" => '"',
            "apos" => '\'',
            _ => char::from_u32(if let Some(hex) = entity.strip_prefix("#x") {
                u32::from_str_radix(hex, 16).ok()?
            } else {
                entity.strip_prefix('#')?.parse().ok()?
            })?,
        });
        rest = tail;
    }
    out.push_str(rest);
    Some(out)
}
fn escape(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            '&' => "&amp;".to_owned(),
            '<' => "&lt;".to_owned(),
            '>' => "&gt;".to_owned(),
            '"' => "&quot;".to_owned(),
            '\'' => "&apos;".to_owned(),
            _ => c.to_string(),
        })
        .collect()
}
fn field(xml: &str, name: &str) -> Option<String> {
    let (_, value) = elements(xml)?.into_iter().find(|(tag, _)| *tag == name)?;
    unescape(value.trim())
}
/// Service type, raw controlURL and optional URLBase; URL resolution belongs to the caller.
pub fn control_url(description_xml: &[u8]) -> Option<(String, String, Option<String>)> {
    let xml = std::str::from_utf8(description_xml).ok()?;
    let tags = elements(xml)?;
    let base = match tags.iter().find(|(name, _)| *name == "URLBase") {
        Some((_, value)) if !value.trim().is_empty() => Some(unescape(value.trim())?),
        _ => None,
    };
    for kind in [
        "WANIPConnection:2",
        "WANIPConnection:1",
        "WANPPPConnection:1",
    ] {
        let service_type = format!("urn:schemas-upnp-org:service:{kind}");
        for (_, service) in tags.iter().filter(|(name, _)| *name == "service") {
            if field(service, "serviceType").as_deref() == Some(&service_type) {
                let control = field(service, "controlURL")?;
                if !control.is_empty() {
                    return Some((service_type, control, base));
                }
            }
        }
    }
    None
}
pub fn request(service_type: &str, action: &str, args: &[(&str, &str)]) -> (String, String) {
    let mut body = format!(
        "<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:{action} xmlns:u=\"{}\">",
        escape(service_type)
    );
    for (name, value) in args {
        body.push_str(&format!("<{name}>{}</{name}>", escape(value)));
    }
    body.push_str(&format!("</u:{action}></s:Body></s:Envelope>"));
    (format!("\"{service_type}#{action}\""), body)
}
/// Error 0 denotes malformed XML or a missing SOAP response/fault code.
pub fn response(bytes: &[u8]) -> Result<BTreeMap<String, String>, u16> {
    let xml = std::str::from_utf8(bytes).map_err(|_| 0u16)?;
    let body = elements(xml)
        .ok_or(0u16)?
        .into_iter()
        .find(|(name, _)| *name == "Body")
        .ok_or(0u16)?
        .1;
    let tags = elements(body).ok_or(0u16)?;
    if let Some((_, fault)) = tags.iter().find(|(name, _)| *name == "Fault") {
        return Err(field(fault, "errorCode")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0));
    }
    let (_, contents) = tags
        .iter()
        .find(|(name, _)| name.ends_with("Response"))
        .ok_or(0u16)?;
    elements(contents)
        .ok_or(0u16)?
        .into_iter()
        .map(|(name, value)| Ok((name.to_owned(), unescape(value.trim()).ok_or(0u16)?)))
        .collect()
}
