mod get_local_ip;
mod proc_xml;
mod setting_log;
mod solr;
mod util;
mod xml_attr_parser;
mod xml_doc;

use crate::util::StrError;
use config::Config;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use log::{error, info, warn};
use lru::LruCache;
use proc_xml::WriteOk;
use regex::Regex;
use solr::Solr;
use sqlx::mysql::MySqlConnectOptions;
use sqlx::pool::PoolOptions;
use sqlx::{ConnectOptions, MySqlPool};
use std::error::Error;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use util::ResponseWithError;

type SyncLazy<T> = once_cell::sync::Lazy<T>;
type BoxedError = Box<dyn Error + Send + Sync>;

/// seed_id 필드명
const COL_SEED_ID: &[u8] = b"seed_id";

/// url 필드명
const COL_URL: &[u8] = b"url";

/// config 전역변수
static CONFIG: SyncLazy<Config> = SyncLazy::new(|| {
    Config::builder()
        .add_source(config::File::with_name("config"))
        .build()
        .expect("CONFIG_READ_FAIL")
});

/// 서버 중단 요청에 대한 Sender
/// <br>
/// panic 발생시 이를 통해 서버 중단을 요청
static STOP_SERVER_SENDER: SyncLazy<Mutex<Option<Sender<()>>>> = SyncLazy::new(|| Mutex::new(None));

/// seed_id 캐시 전역변수
static SEED_ID_CACHE: SyncLazy<Mutex<LruCache<String, String>>> = SyncLazy::new(|| {
    Mutex::new(LruCache::with_hasher(
        std::num::NonZeroUsize::new(10_0000).unwrap(),
        hashbrown::hash_map::DefaultHashBuilder::default(),
    ))
});

/// solr 전역변수
static SOLR: SyncLazy<Solr> = SyncLazy::new(|| {
    let solr_url = CONFIG
        .get_string("solr_kr")
        .expect("FAIL_GET_CONFIG: solr_kr");
    info!("solr client init. solr_kr: {}", solr_url);
    Solr::new(solr_url)
});

/// 카페/블로그인 경우의 패턴 전역변수
static CAFEBLOG_PTRN: SyncLazy<Regex> = SyncLazy::new(|| Regex::new(r#"^([^/]+/[^/]+)"#).unwrap());

/// DB 연결 전역변수
static CON: SyncLazy<MySqlPool> = SyncLazy::new(|| {
    let db_host = CONFIG
        .get_string("db_host")
        .expect("FAIL_GET_CONFIG: db_host");
    let db_user = CONFIG
        .get_string("db_user")
        .expect("FAIL_GET_CONFIG: db_user");
    let db_pwd = CONFIG
        .get_string("db_pwd")
        .expect("FAIL_GET_CONFIG: db_pwd");
    let db_schema = CONFIG
        .get_string("db_schema")
        .expect("FAIL_GET_CONFIG: db_schema");

    info!(
        "DB INIT: host: {}, user: {}, pwd: {}, schema: {}",
        db_host, db_user, db_pwd, db_schema
    );

    let conn = MySqlConnectOptions::new()
        .host(&db_host)
        .username(&db_user)
        .password(&db_pwd)
        .database(&db_schema)
        .statement_cache_capacity(100)
        .log_statements(log::LevelFilter::Debug)
        .log_slow_statements(log::LevelFilter::Info, Duration::from_secs(5));

    PoolOptions::new()
        // 10분동안 미사용시 연결 끊음
        .idle_timeout(Duration::from_secs(60 * 10))
        // 30분 경과시 연결 끊음
        .max_lifetime(Duration::from_secs(60 * 30))
        .min_connections(0)
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(60 * 5))
        .connect_lazy_with(conn)
});

/// 작업횟수 카운트 전역변수
static WORKING_CNT: SyncLazy<Mutex<WorkingCnt>> = SyncLazy::new(|| Mutex::new(WorkingCnt::new()));

/// 작업횟수 카운트
pub struct WorkingCnt {
    pub select_cnt: u32,
    pub add_cnt: u32,
    pub add_doc_cnt: usize,
    pub err_cnt: u32,
    pub add_duration_time_total: Duration,
    pub add_duration_time_min: Duration,
    pub add_duration_time_max: (Duration, usize, usize),
    pub add_bytes_total: usize,
    pub select_duration_time_total: Duration,
    pub select_duration_time_min: Duration,
    pub select_duration_time_max: Duration,
    pub cache_hit_cnt: u32,
    pub cache_miss_cnt: u32,
    pub seed_id_insert_cnt: u32,
}

impl WorkingCnt {
    pub const fn new() -> Self {
        Self {
            select_cnt: 0,
            add_cnt: 0,
            add_doc_cnt: 0,
            err_cnt: 0,
            add_duration_time_total: Duration::ZERO,
            add_duration_time_min: Duration::MAX,
            add_duration_time_max: (Duration::ZERO, 0, 0),
            add_bytes_total: 0,
            select_duration_time_total: Duration::ZERO,
            select_duration_time_min: Duration::MAX,
            select_duration_time_max: Duration::ZERO,
            cache_hit_cnt: 0,
            cache_miss_cnt: 0,
            seed_id_insert_cnt: 0,
        }
    }
}

#[tokio::main]
async fn main() {
    setting_log::setup_logger().expect("Setup Logger Failed");
    info!("server starting...");

    let my_local_ip = get_local_ip::get_local_ip().expect("get_local_ip FAIL");

    // Construct our SocketAddr to listen on...
    let addr = SocketAddr::from((my_local_ip, 3000));
    info!("my IP address: {}", addr);

    // A `MakeService` that produces a `Service` to handle each connection.
    let make_service = make_service_fn(move |c: &AddrStream| {
        let remote_ip = c.remote_addr();

        // Create a `Service` for responding to the request.
        let service = service_fn(move |req| handle(req, remote_ip));

        // Return the service to hyper.
        async move { Ok::<_, BoxedError>(service) }
    });

    // Then bind and serve...
    let server = Server::bind(&addr).serve(make_service);
    let (send, recv) = tokio::sync::oneshot::channel::<()>();
    *STOP_SERVER_SENDER.lock().await = Some(send);
    let graceful = server.with_graceful_shutdown(async move {
        let _ = recv.await;
    });

    tokio::spawn(async move {
        let sleep_duration = std::time::Duration::from_secs(60);
        loop {
            tokio::time::sleep(sleep_duration).await;

            {
                let sender_lock = STOP_SERVER_SENDER.lock().await;
                if sender_lock.is_none() {
                    return;
                }
            }

            let cache_len = {
                let seed_id_cache_lock = SEED_ID_CACHE.lock().await;
                seed_id_cache_lock.len()
            };

            let mut cnt_lock = WORKING_CNT.lock().await;
            info!(
                "SELECT {}, ADD {}[{} doc], ERROR {}",
                cnt_lock.select_cnt, cnt_lock.add_cnt, cnt_lock.add_doc_cnt, cnt_lock.err_cnt
            );
            if cnt_lock.select_cnt > 0 {
                info!(
                    "SELECT: Average {:.2}ms, MIN: {}ms, MAX: {}ms",
                    cnt_lock.select_duration_time_total.as_millis() as f32
                        / cnt_lock.select_cnt as f32,
                    cnt_lock.select_duration_time_min.as_millis(),
                    cnt_lock.select_duration_time_max.as_millis(),
                );
            }
            if cnt_lock.add_cnt > 0 && cnt_lock.add_doc_cnt > 0 {
                info!(
                "ADD: Average {:.2}ms, Average per doc: {:.2}ms, MIN: {}ms, MAX: {}ms[{} doc, {} bytes], Total {} bytes",
                cnt_lock.add_duration_time_total.as_millis() as f32 / cnt_lock.add_cnt as f32,
                cnt_lock.add_duration_time_total.as_millis() as f32 / cnt_lock.add_doc_cnt as f32,
                cnt_lock.add_duration_time_min.as_millis(),
                cnt_lock.add_duration_time_max.0.as_millis(),
                cnt_lock.add_duration_time_max.1,
                cnt_lock.add_duration_time_max.2,
                cnt_lock.add_bytes_total
            );
            }

            if cnt_lock.cache_hit_cnt > 0 || cnt_lock.cache_miss_cnt > 0 {
                let hit_percent: f32;

                if cnt_lock.cache_hit_cnt == 0 {
                    hit_percent = 0f32;
                } else if cnt_lock.cache_miss_cnt == 0 {
                    hit_percent = 100f32;
                } else {
                    hit_percent = cnt_lock.cache_hit_cnt as f32
                        / (cnt_lock.cache_hit_cnt + cnt_lock.cache_miss_cnt) as f32
                        * 100f32;
                }

                info!(
                "seed_id cache: Hit {}, Miss {}, Cache Hit Rate {:.2}%, New seed_id Insert: {}, Cache Len: {}",
                cnt_lock.cache_hit_cnt, cnt_lock.cache_miss_cnt, hit_percent, cnt_lock.seed_id_insert_cnt, cache_len
            );
            }
            info!("DB connection pool cnt: {}", CON.size());
            info!("");

            // working_cnt 초기화
            *cnt_lock = WorkingCnt::new();
        }
    });
    info!("server start.");

    // And run forever...
    if let Err(e) = graceful.await {
        error!("server error: {}", e);
    }
    info!("server shutdown.");
}

async fn handle(req: Request<Body>, remote_ip: SocketAddr) -> Result<Response<Body>, String> {
    match handle_worker(req).await {
        Ok(result) => Ok(result),
        Err(e) => {
            {
                let mut cnt_lock = WORKING_CNT.lock().await;
                cnt_lock.err_cnt += 1;
            }

            let err_str = e.to_string();

            // 에러가 발생했어도 가능한 경우 정상적인 Response를 돌려줌
            if let Ok(error_response) = e.downcast::<ResponseWithError>() {
                warn!("{}", err_str);
                warn!("request from: {}", remote_ip);
                warn!("");
                Ok(error_response.response)
            } else {
                // 정상적인 Response가 불가능한 경우
                warn!("FAIL_RESPONSE... {}", err_str);
                warn!("request from: {}", remote_ip);
                warn!("");
                let mut internal_error_response = Response::new(Body::from(err_str));
                *internal_error_response.status_mut() = hyper::StatusCode::INTERNAL_SERVER_ERROR;
                Ok(internal_error_response)
            }
        }
    }
}

async fn handle_worker(mut req: Request<Body>) -> Result<Response<Body>, BoxedError> {
    let path = req.uri().path().trim();
    let start = Instant::now();

    // select인 경우 받은 그대로 다시 솔라에 날림
    if path.ends_with("/select") {
        let (req_parts, req_body) = req.into_parts();
        let (res_parts, res_body) = SOLR
            .send_request(req_parts.uri, req_parts.method, req_parts.headers, req_body)
            .await?
            .into_parts();
        let response = Response::from_parts(res_parts, res_body);

        let duration = Instant::now() - start;
        let mut cnt_lock = WORKING_CNT.lock().await;
        cnt_lock.select_cnt += 1;
        cnt_lock.select_duration_time_total += duration;
        if cnt_lock.select_duration_time_min > duration {
            cnt_lock.select_duration_time_min = duration;
        }
        if cnt_lock.select_duration_time_max < duration {
            cnt_lock.select_duration_time_max = duration;
        }
        drop(cnt_lock);

        Ok(response)
    } else if path.ends_with("/update") {
        // update 또는 add인 경우
        let bytes = hyper::body::to_bytes(req.body_mut()).await?;
        let bytes_len = bytes.len();

        let doc_cnt: usize;
        let body: Body;
        let parse_error: Option<BoxedError>;
        let (req_parts, _) = req.into_parts();

        match update_xml_parse(&bytes).await {
            Ok(WriteOk::Changed(final_xml, doc_cnt_ok)) => {
                doc_cnt = doc_cnt_ok;
                body = Body::from(final_xml);
                parse_error = None;
            }
            Ok(WriteOk::NoChanged(doc_cnt_ok)) => {
                doc_cnt = doc_cnt_ok;
                // NoChanged인 경우 전송받은 bytes를 그대로 되돌려줌
                body = Body::from(bytes);
                parse_error = None;
            }
            Err(e) => {
                doc_cnt = 0;
                // 파싱 에러가 발생한 경우 전송받은 bytes를 그대로 되돌려줌
                body = Body::from(bytes);
                parse_error = Some(e);
            }
        }

        let (res_parts, res_body) = SOLR
            .send_request(req_parts.uri, req_parts.method, req_parts.headers, body)
            .await?
            .into_parts();
        let response = Response::from_parts(res_parts, res_body);

        let duration = Instant::now() - start;
        let mut cnt_lock = WORKING_CNT.lock().await;
        cnt_lock.add_cnt += 1;
        cnt_lock.add_doc_cnt += doc_cnt;
        cnt_lock.add_duration_time_total += duration;
        cnt_lock.add_bytes_total += bytes_len;
        if cnt_lock.add_duration_time_min > duration {
            cnt_lock.add_duration_time_min = duration;
        }
        if cnt_lock.add_duration_time_max.0 < duration {
            cnt_lock.add_duration_time_max = (duration, doc_cnt, bytes_len);
        }
        drop(cnt_lock);

        match parse_error {
            Some(err) => Err(Box::new(ResponseWithError { err, response })),
            None => Ok(response),
        }
    } else {
        let err_msg = format!("UNKNOWN_PATH {}", path);
        Err(Box::new(StrError::new(err_msg)))
    }
}

async fn update_xml_parse(bytes: &hyper::body::Bytes) -> Result<WriteOk, BoxedError> {
    let mut parse_result = proc_xml::read_xml(bytes)?;
    proc_xml::proc_xml(&mut parse_result).await?;
    proc_xml::write_xml(parse_result)
}
