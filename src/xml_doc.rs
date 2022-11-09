use hashbrown::HashMap;
use quick_xml::events::BytesText;
use smallvec::SmallVec;
use std::borrow::Cow;

/// xml의 텍스트 데이터.
/// <br>
/// 기존 데이터에 대한 참조인 경우 Bytes, 값이 추가/변경된 경우 Str
#[derive(Debug, Clone)]
pub enum BytesOrStr<'xml> {
    /// 원문에 대한 참조
    Bytes(BytesText<'xml>),
    /// 값이 변경/추가된 경우. 원문 데이터가 있을 경우 원문 데이터에 대한 참조는 유지함
    Str(Cow<'xml, String>, Option<BytesText<'xml>>),
}

impl<'xml> BytesOrStr<'xml> {
    pub fn to_unescape_str(&self) -> Result<Cow<str>, quick_xml::Error> {
        match self {
            BytesOrStr::Bytes(bytes) => Ok(bytes.unescape()?),
            BytesOrStr::Str(str, _) => Ok(Cow::Borrowed(str)),
        }
    }
}

#[derive(Debug)]
pub struct DocField<'xml> {
    // 데이터가 <field name="seed_id">f371ba73-7e23-11ea-9ea0-fa163e9f6f72</field> 인 경우
    // HashMap<seed_id, SmallVec<f371ba73-7e23-11ea-9ea0-fa163e9f6f72>> 형식으로 데이터가 파싱됨
    field: HashMap<&'xml [u8], SmallVec<[BytesOrStr<'xml>; 1]>>,

    has_changed: bool,
}

impl<'xml> DocField<'xml> {
    pub fn new() -> Self {
        Self {
            field: HashMap::new(),
            has_changed: false,
        }
    }

    pub fn has_changed(&self) -> bool {
        self.has_changed
    }

    pub fn get(&self, key: &[u8]) -> Option<&SmallVec<[BytesOrStr<'xml>; 1]>> {
        self.field.get(key)
    }

    pub fn try_reserve(&mut self, size: usize) -> Result<(), hashbrown::TryReserveError> {
        self.field.try_reserve(size)
    }

    pub fn into_inner(self) -> (HashMap<&'xml [u8], SmallVec<[BytesOrStr<'xml>; 1]>>, bool) {
        (self.field, self.has_changed)
    }

    pub fn push_field_owned(&mut self, name: &'xml [u8], value: String) {
        self.field
            .entry(name)
            .or_insert_with(|| SmallVec::with_capacity(1))
            .push(BytesOrStr::Str(Cow::Owned(value), None));

        self.has_changed = true;
    }

    pub fn push_field_borrowed(&mut self, name: &'xml [u8], bytes: BytesText<'xml>) {
        self.field
            .entry(name)
            .or_insert_with(|| SmallVec::with_capacity(1))
            .push(BytesOrStr::Bytes(bytes));
    }
}

/// Solr에 들어갈 각각의 doc을 파싱한 것.
/// <br>
/// 값복사를 최소화하기 위해 텍스트 데이터에 대해선 &'xml [u8]에 대한 참조를 사용
pub struct Doc<'xml> {
    field: DocField<'xml>,

    /// 원문 doc에 대한 참조. \<doc>으로 시작해서 \</doc>으로 끝남
    ori_str: &'xml [u8],
}

impl<'xml> Doc<'xml> {
    pub fn new(field: DocField<'xml>, ori_str: &'xml [u8]) -> Self {
        Self { field, ori_str }
    }

    pub fn field(&self) -> &DocField<'xml> {
        &self.field
    }

    pub fn ori_str(&self) -> &'xml [u8] {
        self.ori_str
    }

    pub fn into_inner(self) -> (DocField<'xml>, &'xml [u8]) {
        (self.field, self.ori_str)
    }

    pub fn field_as_mut(&mut self) -> &mut DocField<'xml> {
        &mut self.field
    }
}

impl<'xml> std::fmt::Debug for Doc<'xml> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Doc")
            .field("field", &self.field)
            .field(
                "ori_str",
                &String::from_utf8(self.ori_str.to_vec()).unwrap(),
            )
            .finish()
    }
}
