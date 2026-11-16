use super::*;

fn valid(data: &[u8]) -> Frame {
    let mut checked = Cursor::new(data);
    let mut parsed = Cursor::new(data);
    Frame::check(&mut checked).unwrap();
    let frame = Frame::parse(&mut parsed).unwrap();
    assert_eq!(checked.position(), data.len() as u64);
    assert_eq!(checked.position(), parsed.position());
    frame
}

fn invalid(data: &[u8]) {
    assert!(matches!(
        Frame::check(&mut Cursor::new(data)),
        Err(Error::Other(_))
    ));
    assert!(matches!(
        Frame::parse(&mut Cursor::new(data)),
        Err(Error::Other(_))
    ));
}

#[test]
fn strict_numbers_crlf_and_frame_types() {
    for data in [
        &b"$1\r\naXX"[..],
        b"$1\r\na\n",
        b":12junk\r\n",
        b":\r\n",
        b":+\r\n",
        b": 1\r\n",
        b":1 \r\n",
        b":--1\r\n",
        b":9223372036854775808\r\n",
        b":-9223372036854775809\r\n",
        b"$-2\r\n",
        b"$-01\r\n",
        b"$-0\r\n",
        b"$+1\r\na\r\n",
        b"$1x\r\na\r\n",
        b"$18446744073709551616\r\n",
        b"*-2\r\n",
        b"*+1\r\n",
        b"*1x\r\n",
        b"+a\n",
        b"+a\rb\r\n",
        b"+\xff\r\n",
        b"-\xff\r\n",
        b"?\r\n",
    ] {
        invalid(data);
    }
    for (data, expected) in [
        (&b":+12\r\n"[..], 12),
        (b":-12\r\n", -12),
        (b":9223372036854775807\r\n", i64::MAX),
        (b":-9223372036854775808\r\n", i64::MIN),
    ] {
        assert!(matches!(valid(data), Frame::Integer(n) if n == expected));
    }
    assert!(matches!(valid(b"$-1\r\n"), Frame::Null));
    assert!(matches!(valid(b"*-1\r\n"), Frame::Null));
    assert!(matches!(valid(b"*0\r\n"), Frame::Array(a) if a.is_empty()));
    assert!(matches!(valid(b"$0\r\n\r\n"), Frame::Bulk(b) if b.is_empty()));
    assert!(matches!(valid(b"$3\r\n\0\xff\n\r\n"), Frame::Bulk(b) if b.len() == 3));
}

#[test]
fn every_fragment_and_pipelined_frame_agree() {
    let data = b"*4\r\n+OK\r\n-ERR example\r\n:-42\r\n*2\r\n$3\r\nfoo\r\n$-1\r\n";
    for end in 0..data.len() {
        assert!(matches!(
            Frame::check(&mut Cursor::new(&data[..end])),
            Err(Error::Incomplete)
        ));
        assert!(matches!(
            Frame::parse(&mut Cursor::new(&data[..end])),
            Err(Error::Incomplete)
        ));
    }
    valid(data);
    let pipeline = [data.as_slice(), b":+1\r\n"].concat();
    for build in [false, true] {
        let mut cursor = Cursor::new(pipeline.as_slice());
        for end in [data.len(), pipeline.len()] {
            if build {
                Frame::parse(&mut cursor).unwrap();
            } else {
                Frame::check(&mut cursor).unwrap();
            }
            assert_eq!(cursor.position(), end as u64);
        }
    }
}

#[test]
fn lengths_and_cumulative_elements_are_bounded_before_payloads() {
    invalid(format!("${}\r\n", MAX_BULK_BYTES + 1).as_bytes());
    invalid(format!("*{}\r\n", MAX_ARRAY_ELEMENTS + 1).as_bytes());
    invalid(format!("*2\r\n*{}\r\n", MAX_ARRAY_ELEMENTS - 1).as_bytes());
    let mut bulk = format!("${}\r\n", MAX_BULK_BYTES).into_bytes();
    bulk.resize(bulk.len() + MAX_BULK_BYTES, b'x');
    bulk.extend_from_slice(b"\r\n");
    valid(&bulk);
    // Each bulk is legal individually; the second header already proves that
    // their combined encoding cannot fit in one frame.
    let mut too_large = b"*2\r\n".to_vec();
    too_large.extend_from_slice(&bulk);
    too_large.extend_from_slice(format!("${}\r\n", MAX_BULK_BYTES).as_bytes());
    invalid(&too_large);
    let mut array = format!("*{}\r\n", MAX_ARRAY_ELEMENTS).into_bytes();
    array.extend_from_slice(&b"+\r\n".repeat(MAX_ARRAY_ELEMENTS));
    valid(&array);
}

#[test]
fn nesting_limit_is_shared_by_check_and_parse() {
    let mut data = b"*1\r\n".repeat(MAX_ARRAY_DEPTH);
    data.extend_from_slice(b"+OK\r\n");
    valid(&data);
    invalid(&[&b"*1\r\n"[..], &data].concat());
    // Reject the next array prefix without waiting for its length or children.
    invalid(&[b"*1\r\n".repeat(MAX_ARRAY_DEPTH), b"*".to_vec()].concat());
}

#[test]
fn frame_byte_limit_counts_encoding_and_resets_for_pipeline() {
    let mut data = vec![b'x'; MAX_FRAME_BYTES];
    data[0] = b'+';
    data[MAX_FRAME_BYTES - 2..].copy_from_slice(b"\r\n");
    valid(&data);
    data.extend_from_slice(b":1\r\n");
    let mut cursor = Cursor::new(data.as_slice());
    Frame::check(&mut cursor).unwrap();
    assert_eq!(cursor.position(), MAX_FRAME_BYTES as u64);
    Frame::check(&mut cursor).unwrap();
    assert_eq!(cursor.position(), data.len() as u64);
    data[MAX_FRAME_BYTES - 2] = b'x';
    invalid(&data);
    invalid(&vec![b'+'; MAX_FRAME_BYTES]);
}
