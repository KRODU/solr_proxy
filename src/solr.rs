use crate::BoxedError;
use hyper::client::HttpConnector;
use hyper::http::HeaderValue;
use hyper::{Body, Client, HeaderMap, Method, Request, Response, Uri};
use std::str::FromStr;

pub struct Solr {
    solr_url: String,
    client: Client<HttpConnector>,
}

impl Solr {
    pub fn new(solr_url: String) -> Solr {
        Solr {
            solr_url,
            client: Client::new(),
        }
    }

    pub async fn send_request(
        &self,
        uri: Uri,
        method: Method,
        header_map: HeaderMap<HeaderValue>,
        body: Body,
    ) -> Result<Response<Body>, BoxedError> {
        // solr_url에 path를 붙여 전체 url을 생성
        let path_and_query = uri.path_and_query().ok_or("Empty PathAndQuery Error")?;
        let mut new_url = self.solr_url.to_string();
        new_url.push_str(path_and_query.as_str());
        let new_url = Uri::from_str(new_url.as_str())?;

        let mut builder = Request::builder().method(method).uri(new_url);

        for (header_name, header_value) in header_map {
            if let Some(name) = header_name {
                builder = builder.header(name, header_value);
            }
        }

        // 솔라에 요청
        let req = builder.body(body)?;
        Ok(self.client.request(req).await?)
    }
}
