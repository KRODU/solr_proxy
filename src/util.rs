use crate::BoxedError;
use hyper::{Body, Response};
use std::error::Error;
use std::fmt::{Debug, Display};

pub struct StrError {
    pub err_msg: String,
}

impl Display for StrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.err_msg)
    }
}

impl Debug for StrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrError")
            .field("err_msg", &self.err_msg)
            .finish()
    }
}

impl Error for StrError {}

impl StrError {
    pub fn new(err_msg: String) -> Self {
        StrError { err_msg }
    }
}

/// 에러는 발생했지만 정상적으로 문서는 주고받기 위한 에러처리
pub struct ResponseWithError {
    pub err: BoxedError,
    pub response: Response<Body>,
}

impl Display for ResponseWithError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl Debug for ResponseWithError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorResponse")
            .field("err", &self.err)
            .finish()
    }
}

impl Error for ResponseWithError {}
