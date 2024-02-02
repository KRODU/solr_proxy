use crate::util::StrError;
use crate::xml_attr_parser::AttrParser;
use crate::xml_doc::*;
use crate::*;
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::name::QName;
use quick_xml::{Reader, Writer};
use sqlx::Row;
use std::borrow::Cow;
use std::io::{Cursor, Write};

pub fn read_xml<'xml>(xml: &'xml [u8]) -> Result<Vec<Doc<'xml>>, BoxedError> {
    let mut ret_docs: Vec<Doc<'xml>> = Vec::new();
    let mut reader = Reader::from_reader(xml);
    reader.trim_text(true);
    let mut field = DocField::new();
    let mut previous_field_name: Option<&'xml [u8]> = None;
    let mut doc_start_position: Option<usize> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let buffer_position = reader.buffer_position();
                let name = e.name().0;

                match name {
                    b"field" => {
                        let raw_attr =
                            &xml[buffer_position - e.len() + name.len()..buffer_position];

                        for attr in AttrParser::new(raw_attr) {
                            if attr.name == b"name" {
                                previous_field_name = Some(attr.value);
                                break;
                            }
                        }
                    }
                    b"doc" => {
                        doc_start_position = Some(buffer_position - e.len() - 2);
                        field.try_reserve(36).map_err(|_| {
                            Box::new(StrError::new("HashMap::try_reserve FAIL".to_string()))
                        })?;
                    }
                    _ => (),
                }
            }
            Ok(Event::Text(e)) => {
                if let Some(pre_name) = previous_field_name {
                    field.push_field_borrowed(pre_name, e);
                }
                previous_field_name = None;
            }
            Ok(Event::End(e)) => {
                // doc의 파싱이 끝난 경우 ret_docs에 추가하여 넣음
                if e.name().0 == b"doc" {
                    let Some(doc_start_position_value) = doc_start_position else {
                        // 이 위치에서 doc_start_position이 None이면 안됨. 에러 반환
                        return Err(Box::new(StrError::new(
                            "DOC_START_POSITION_EMPTY".to_string(),
                        )));
                    };

                    let ori_str = &xml[doc_start_position_value..reader.buffer_position()];

                    // ori_str의 유효성 체크
                    // <doc> 태그로 시작하고 </doc> 태그로 끝나야 함.
                    if !ori_str.starts_with(b"<doc") || !ori_str.ends_with(b"</doc>") {
                        return Err(Box::new(StrError::new(
                            "ORI_STR_VALIDATION_FAIL".to_string(),
                        )));
                    }

                    let doc = Doc::new(field, ori_str);
                    ret_docs.push(doc);
                    field = DocField::new();
                    doc_start_position = None;
                }
                previous_field_name = None;
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(Box::new(e)),
            _ => (),
        }
    }

    Ok(ret_docs)
}

pub async fn proc_xml(docs: &mut Vec<Doc<'_>>) -> Result<(), BoxedError> {
    for doc in docs {
        // seed_id가 없는 경우 넣어야 함
        if doc.field().get(COL_SEED_ID).is_none() {
            let seed_host = seed_host(doc)?;

            let not_found_cache_flag = {
                let mut seed_id_cache_lock = SEED_ID_CACHE.lock().await;
                match seed_id_cache_lock.get(&seed_host) {
                    Some(seed_id) => {
                        doc.field_as_mut()
                            .push_field_owned(COL_SEED_ID, seed_id.to_string());
                        false
                    }
                    None => true,
                }
            };

            {
                let mut cnt_lock = WORKING_CNT.lock().await;
                if not_found_cache_flag {
                    cnt_lock.cache_miss_cnt += 1;
                } else {
                    cnt_lock.cache_hit_cnt += 1;
                }
            }

            // cache에서 seed_id를 찾지 못한 경우
            if not_found_cache_flag {
                // db에서 검색 시도
                let rows = select_seed_id(&seed_host).await?;

                // db에서 찾은 경우
                if let Some(row) = rows {
                    let seed_id = row.try_get::<&str, _>("seed_id")?;

                    doc.field_as_mut()
                        .push_field_owned(COL_SEED_ID, seed_id.to_string());

                    let mut seed_id_cache_lock = SEED_ID_CACHE.lock().await;
                    seed_id_cache_lock.put(seed_host, seed_id.to_string());
                } else {
                    {
                        let mut cnt_lock = WORKING_CNT.lock().await;
                        cnt_lock.seed_id_insert_cnt += 1;
                    }
                    // db에서 찾지 못한 경우 INSERT 후 다시 SELECT
                    let sql = "INSERT IGNORE INTO crawlerdb.t_channel_contents_map
(seed_id, site_name, media_url, media_type_no)
VALUES
(uuid(), '', ?, '0');";
                    sqlx::query(sql).bind(&seed_host).execute(&*CON).await?;
                    let rows = select_seed_id(&seed_host).await?;
                    let Some(row) = rows else {
                        // INSERT 후 다시 SELECT했는데 찾지 못한 경우. 정상적인 경우 발생할 수 없음
                        return Err(Box::new(StrError::new(
                            "SEED_ID_SELECT_AFTER_INSERT_FAIL".to_string(),
                        )));
                    };

                    let seed_id = row.try_get::<&str, _>("seed_id")?;

                    doc.field_as_mut()
                        .push_field_owned(COL_SEED_ID, seed_id.to_string());

                    let mut seed_id_cache_lock = SEED_ID_CACHE.lock().await;
                    seed_id_cache_lock.put(seed_host, seed_id.to_string());
                }
            }
        }
    }

    Ok(())
}

async fn select_seed_id(seed_host: &str) -> Result<Option<sqlx::mysql::MySqlRow>, BoxedError> {
    Ok(
        sqlx::query("SELECT seed_id FROM crawlerdb.t_channel_contents_map WHERE media_url = ?;")
            .bind(seed_host)
            .fetch_optional(&*CON)
            .await?,
    )
}

fn seed_host(doc: &Doc) -> Result<String, BoxedError> {
    let Some(url) = doc.field().get(COL_URL) else {
        return Err(Box::new(StrError::new("NOT_FOUND_URL".to_string())));
    };

    let Some(first) = url.first() else {
        return Err(Box::new(StrError::new("NOT_FOUND_URL".to_string())));
    };

    let url = first.to_unescape_str()?;
    Ok(seed_host_str(&url)?.into_owned())
}

fn seed_host_str(mut url: &str) -> Result<Cow<str>, BoxedError> {
    const HTTPS: &str = "https://";
    const HTTP: &str = "http://";

    // https 및 http를 잘라냄
    if url.starts_with(HTTPS) {
        url = &url[HTTPS.len()..];
    } else if url.starts_with(HTTP) {
        url = &url[HTTP.len()..];
    }

    // www.으로 시작하는 경우 잘라냄
    const WWW: &str = "www.";
    if url.starts_with(WWW) {
        url = &url[WWW.len()..];
    }

    if url.starts_with("cafe.naver.com")
        || url.starts_with("m.cafe.daum.net")
        || url.starts_with("cafe.daum.net")
        || url.starts_with("blog.naver.com")
    {
        match CAFEBLOG_PTRN.captures(url) {
            Some(cap) => {
                let value = cap.get(1).unwrap().as_str();
                Ok(Cow::Owned(value.to_string()))
            }
            None => Err(Box::new(StrError::new(format!(
                "CAFE_PTRN_NOT_MATCH: {}",
                url
            )))),
        }
    } else {
        Ok(Cow::Borrowed(cut_host(url)))
    }
}

fn cut_host(mut url: &str) -> &str {
    let pos = url.find(|c| matches!(c, '/' | '#'));

    if let Some(pos) = pos {
        url = &url[0..pos];
    }
    url
}

/// 변경 사항이 없는 경우 메모리의 기존 데이터를 재사용하며, 변경 사항이 있는 경우에만 메모리 할당 발생
pub enum WriteOk {
    /// 변경사항이 없는 경우 doc 사이즈만 반환. 기존 데이터를 재사용함.
    NoChanged(usize),
    /// 변경 사항이 있는 경우 bytes 배열과 doc 사이즈 반환
    Changed(Vec<u8>, usize),
}

pub fn write_xml(docs: Vec<Doc>) -> Result<WriteOk, BoxedError> {
    let doc_cnt = docs.len();
    let any_changed = docs.iter().any(|doc| doc.field().has_changed());

    // doc 목록중에 하나도 변경사항이 없는 경우 NoChanged return
    // 이렇게 할 경우 전송받은 데이터를 그대로 재사용하게 됨
    if !any_changed {
        return Ok(WriteOk::NoChanged(doc_cnt));
    }

    let xml_cap = docs.iter().fold(0, |sum, doc| sum + doc.ori_str().len());

    // 파싱된 doc이 없는 경우 NoChanged return
    if xml_cap == 0 {
        return Ok(WriteOk::NoChanged(doc_cnt));
    }

    let mut writer = Writer::new(Cursor::new(Vec::with_capacity(xml_cap * 2)));

    writer.write_event(Event::Start(BytesStart::new("add")))?;

    for doc in docs {
        let (doc_field, ori_str) = doc.into_inner();
        let (field, has_changed) = doc_field.into_inner();

        if has_changed {
            // doc에 변경 사항이 있는 경우 field를 순회하며 write
            writer.write_event(Event::Start(BytesStart::new("doc")))?;
            for (field_name, body_list) in field {
                for body in body_list {
                    let mut field_event = BytesStart::new("field");
                    let attr = Attribute {
                        key: QName(b"name"),
                        value: Cow::Borrowed(field_name),
                    };
                    field_event.push_attribute(attr);
                    writer.write_event(Event::Start(field_event))?;

                    match body {
                        BytesOrStr::Bytes(bytes) => writer.write_event(Event::Text(bytes))?,
                        BytesOrStr::Str(str, _) => {
                            writer.write_event(Event::Text(BytesText::new(&str)))?
                        }
                    }

                    writer.write_event(Event::End(BytesEnd::new("field")))?;
                }
            }

            writer.write_event(Event::End(BytesEnd::new("doc")))?;
        } else {
            // doc에 변경사항이 없는 경우 기존 doc 데이터를 그대로 다시 write
            writer.get_mut().write_all(ori_str)?;
        }
    }

    writer.write_event(Event::End(BytesEnd::new("add")))?;

    Ok(WriteOk::Changed(writer.into_inner().into_inner(), doc_cnt))
}

#[test]
fn get_host_test() {
    assert_eq!(
        seed_host_str("http://m.cafe.daum.net/clzkzlck332/5cUp/7606").unwrap(),
        "m.cafe.daum.net/clzkzlck332"
    );
    assert_eq!(
        seed_host_str("http://cafe.daum.net/clzkzlck332/5cUp/7606").unwrap(),
        "cafe.daum.net/clzkzlck332"
    );
    assert_eq!(
        seed_host_str("http://m.cafe.daum.net/clzkzlck332/5cUp").unwrap(),
        "m.cafe.daum.net/clzkzlck332"
    );
    assert_eq!(
        seed_host_str("http://cafe.daum.net/clzkzlck332/5cUp").unwrap(),
        "cafe.daum.net/clzkzlck332"
    );
    assert_eq!(
        seed_host_str("https://m.cafe.daum.net/clzkzlck332/5cUp/7606").unwrap(),
        "m.cafe.daum.net/clzkzlck332"
    );
    assert_eq!(
        seed_host_str("https://cafe.daum.net/clzkzlck332/5cUp/7606").unwrap(),
        "cafe.daum.net/clzkzlck332"
    );
    assert_eq!(
        seed_host_str("https://m.cafe.daum.net/clzkzlck332/5cUp").unwrap(),
        "m.cafe.daum.net/clzkzlck332"
    );
    assert_eq!(
        seed_host_str("https://cafe.daum.net/clzkzlck332/5cUp").unwrap(),
        "cafe.daum.net/clzkzlck332"
    );
    assert_eq!(
        seed_host_str("https://cafe.naver.com/paincare/9741").unwrap(),
        "cafe.naver.com/paincare"
    );
    assert_eq!(
        seed_host_str("https://cafe.naver.com/paincare").unwrap(),
        "cafe.naver.com/paincare"
    );
    assert_eq!(
        seed_host_str("http://cafe.naver.com/paincare/9741").unwrap(),
        "cafe.naver.com/paincare"
    );
    assert_eq!(
        seed_host_str("http://cafe.naver.com/paincare").unwrap(),
        "cafe.naver.com/paincare"
    );
    assert_eq!(
        seed_host_str("https://blog.naver.com/kimeunha99/222856865611").unwrap(),
        "blog.naver.com/kimeunha99"
    );
    assert_eq!(
        seed_host_str("http://blog.naver.com/kimeunha99/222856865611").unwrap(),
        "blog.naver.com/kimeunha99"
    );
    assert_eq!(
        seed_host_str("http://twitter.com/yutaaaaaaaa1103/statuses/1559878365196468224").unwrap(),
        "twitter.com"
    );
    assert_eq!(
        seed_host_str("http://www.fomos.kr/game/news_view?lurl=%2Fgame%2Fnews_list%3Fnews_cate_id%3D2&entry_id=113622#111").unwrap(),
        "fomos.kr"
    );
    assert_eq!(
        seed_host_str("http://www.fomos.kr#111").unwrap(),
        "fomos.kr"
    );
}

#[tokio::test]
async fn doc_read_test() {
    let xml = r#"
<add><doc boost="1.0"><field name="id">a77b3908fb67bd1b</field><field name="crawler_type">crawler</field><field name="crawl_runtime_key">127.0.0.1</field><field name="host">www.lenews.co.kr</field><field name="site">www.lenews.co.kr</field><field name="url">https://cafe.naver.com/moonlightriverside/185</field><field name="title">삼성ENG, 2분기 영업이익 1535억</field><field name="content">[국토경제신 
문 박태선 기자] 삼성엔지니어링이 2분기 영업이익 1535억 원을 달성했다. </field><field name="postdate">2022-07-28T04:48:00.000Z</field><field name="doc_version">10</field><field name="etc_exact1">1</field><field name="tstamp">2022-07-28T06:56:30.487Z</field></doc><doc boost="1.0"><field name="id">c0046e9c36e35a60</field><field name="crawler_type">crawler</field><field name="crawl_runtime_key">127.0.0.1</field><field name="host">www.lenews.co.kr</field><field name="site">www.lenews.co.kr</field><field name="url">http://www.lenews.co.kr/news/articleView.html?idxno=90124</field><field name="title">현대제철, 전기안전공사와 철강부문 전기안전 기술협력</field><field name="content">[국토경제신문 박태선 기자] 현대제철은 27일 한국전기안전공사와 ‘철강부문 전기안전 기술교류 업무 협약’을 체결했다.</field><field name="postdate">2022-07-28T03:54:00.000Z</field><field name="etc_array_text1">https://cdn.lenews.co.kr/news/photo/202207/90124_70053_2859.jpg</field><field name="seed_id">f371ba73-7e23-11ea-9ea0-fa163e9f6f72</field><field name="seed_id">SECOND</field><field name="doc_version">10</field><field name="etc_exact1">1</field><field name="tstamp">2022-07-28T06:56:30.487Z</field></doc></add>
   "#;

    let mut docs = read_xml(xml.as_bytes()).unwrap();

    for doc in &docs {
        let cut_ori_str = String::from_utf8_lossy(&doc.ori_str()[1..]);
        assert!(!cut_ori_str.contains("<doc"));
    }

    let mut doc = &docs[0];
    assert_eq!(
        doc.field().get("id".as_bytes()).unwrap()[0]
            .to_unescape_str()
            .unwrap(),
        "a77b3908fb67bd1b"
    );

    assert_eq!(
        doc.field().get(COL_URL).unwrap()[0]
            .to_unescape_str()
            .unwrap(),
        "https://cafe.naver.com/moonlightriverside/185"
    );

    doc = &docs[1];
    assert_eq!(
        doc.field().get("id".as_bytes()).unwrap()[0]
            .to_unescape_str()
            .unwrap(),
        "c0046e9c36e35a60"
    );

    assert_eq!(
        doc.field().get(COL_URL).unwrap()[0]
            .to_unescape_str()
            .unwrap(),
        "http://www.lenews.co.kr/news/articleView.html?idxno=90124"
    );

    proc_xml(&mut docs).await.unwrap();
    let result = write_xml(docs).unwrap();
    let WriteOk::Changed(final_xml, size) = result else {
        panic!("result is not WriteOk::Changed");
    };

    assert!(final_xml.starts_with(b"<add><doc"));
    assert!(final_xml.ends_with(b"</field></doc></add>"));
    assert_eq!(size, 2);
    let final_read = read_xml(&final_xml).unwrap();
    assert_eq!(final_read.len(), 2);
    assert_eq!(
        final_read[0].field().get(COL_SEED_ID).unwrap()[0]
            .to_unescape_str()
            .unwrap(),
        "e7531c15-2384-11ed-b560-42010a025a43"
    );
}
