pub struct AttrParseResult<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
}

impl PartialEq for AttrParseResult<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.value == other.value
    }
}

impl std::fmt::Debug for AttrParseResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttrParseResult")
            .field("name", &String::from_utf8_lossy(self.name))
            .field("value", &String::from_utf8_lossy(self.value))
            .finish()
    }
}

pub struct AttrParser<'a> {
    attr_str: &'a [u8],
    cursor: usize,
}

impl<'a> AttrParser<'a> {
    pub fn new(attr_str: &'a [u8]) -> Self {
        Self {
            attr_str,
            cursor: 0,
        }
    }
}

impl<'a> Iterator for AttrParser<'a> {
    type Item = AttrParseResult<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // name 시작지점을 찾음. 공백이 아닌 문자를 찾음
        let (name_start_pos, _) = find_with_start(self.attr_str, self.cursor, |c| {
            !matches!(c, b'\t' | b'\n' | b'\x0C' | b'\r' | b' ')
        })?;

        // name 끝지점을 찾음. 공백 또는 =를 찾음
        let (name_end_pos, _) = find_with_start(self.attr_str, name_start_pos + 1, |c| {
            matches!(c, b'=' | b'\t' | b'\n' | b'\x0C' | b'\r' | b' ')
        })?;

        // =를 찾음
        let mut equal_mark_pos: Option<usize> = None;
        for (i, &c) in self.attr_str.iter().enumerate().skip(name_end_pos) {
            if c == b'=' {
                equal_mark_pos = Some(i);
                break;
            }

            // 여기서 공백 문자 외의 다른 문자는 만나면 안됨
            if !matches!(c, b'\t' | b'\n' | b'\x0C' | b'\r' | b' ') {
                return None;
            }
        }
        let equal_mark_pos = equal_mark_pos?;

        // ' 또는 "가 시작되는 지점을 찾음
        let (mut quot_start_pos, quot) = find_with_start(self.attr_str, equal_mark_pos + 1, |c| {
            matches!(c, b'"' | b'\'')
        })?;
        quot_start_pos += 1;

        // // ' 또는 "가 끝나는 지점을 찾음
        let (quot_end_pos, _) = find_with_start(self.attr_str, quot_start_pos, |c| c == quot)?;

        let name = &self.attr_str[name_start_pos..name_end_pos];
        let value = &self.attr_str[quot_start_pos..quot_end_pos];

        self.cursor = quot_end_pos + 1;

        // println!("name:{}", String::from_utf8_lossy(name));
        // println!("value:{}", String::from_utf8_lossy(value));

        Some(AttrParseResult { name, value })
    }
}

fn find_with_start<F>(arr: &[u8], cursor: usize, f: F) -> Option<(usize, u8)>
where
    F: Fn(u8) -> bool,
{
    for (i, &c) in arr.iter().enumerate().skip(cursor) {
        if f(c) {
            return Some((i, c));
        }
    }

    None
}

#[test]
fn attr_test() {
    let mut test = AttrParser::new(br#"name="id""#);
    let mut test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"name");
    assert_eq!(test_result.value, b"id");
    assert_eq!(test.next(), None);

    // 원래 xml 표준에선 attribute간 공백없이 붙이는 것은 비허용되지만, 여기선 허용함
    test = AttrParser::new(br#"1name1="1id1"2name2="2id2""#);
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"1name1");
    assert_eq!(test_result.value, b"1id1");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"2name2");
    assert_eq!(test_result.value, b"2id2");
    assert_eq!(test.next(), None);

    test = AttrParser::new(br#"a="b"c="d"e='f'g="h""#);
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"a");
    assert_eq!(test_result.value, b"b");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"c");
    assert_eq!(test_result.value, b"d");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"e");
    assert_eq!(test_result.value, b"f");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"g");
    assert_eq!(test_result.value, b"h");
    assert_eq!(test.next(), None);

    test = AttrParser::new(br#"a="b"c=''d="e"f=""g='h'"#);
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"a");
    assert_eq!(test_result.value, b"b");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"c");
    assert_eq!(test_result.value, b"");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"d");
    assert_eq!(test_result.value, b"e");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"f");
    assert_eq!(test_result.value, b"");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"g");
    assert_eq!(test_result.value, b"h");
    assert_eq!(test.next(), None);

    test = AttrParser::new(br#"1name1="1id1" 2name2="2id2" 3name3="3id3""#);
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"1name1");
    assert_eq!(test_result.value, b"1id1");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"2name2");
    assert_eq!(test_result.value, b"2id2");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"3name3");
    assert_eq!(test_result.value, b"3id3");
    assert_eq!(test.next(), None);

    test = AttrParser::new(br#" name1name1  = "id1id1"   name2  =     "id2""#);
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"name1name1");
    assert_eq!(test_result.value, b"id1id1");
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"name2");
    assert_eq!(test_result.value, b"id2");
    assert_eq!(test.next(), None);

    // value가 '로 시작했으면 '로 끝나야 하고, "로 시작하면 "로 끝나야 함
    test = AttrParser::new(br#"  name1  = '"id1"'     name2  =     "id2''""#);
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"name1");
    assert_eq!(test_result.value, br#""id1""#);
    test_result = test.next().unwrap();
    assert_eq!(test_result.name, b"name2");
    assert_eq!(test_result.value, b"id2''");
    assert_eq!(test.next(), None);

    // name사이에 공백이 있으므로 파싱 불가
    test = AttrParser::new(br#"name name="id""#);
    assert_eq!(test.next(), None);

    // "와 짝이 없으므로 파싱 불가
    test = AttrParser::new(br#"name="id'"#);
    assert_eq!(test.next(), None);

    // 빈 문자인 경우에도 패닉이 발생하면 안 됨
    test = AttrParser::new(b"");
    assert_eq!(test.next(), None);
}
