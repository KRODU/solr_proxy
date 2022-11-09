use log::{error, info, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::append::rolling_file::policy::compound::trigger::size::SizeTrigger;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::{Config, Handle};
use std::error::Error;

use crate::STOP_SERVER_SENDER;

pub fn setup_logger() -> Result<Handle, Box<dyn Error + Send + Sync>> {
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "[{d(%Y-%m-%d %H:%M:%S)}] [{l}] {m}{n}",
        )))
        .build();
    let fixed_window_roller = FixedWindowRoller::builder().build("log/solr_proxy.log.{}", 5)?;

    let size_trigger = SizeTrigger::new(500_0000); // 대략 5MB
    let compound_policy =
        CompoundPolicy::new(Box::new(size_trigger), Box::new(fixed_window_roller));
    let file_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "[{d(%Y-%m-%d %H:%M:%S)}] [{l}] {m}{n}",
        )))
        .build("log/solr_proxy.log", Box::new(compound_policy))?;

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("file_appender", Box::new(file_appender)))
        .build(
            Root::builder()
                .appenders(["stdout", "file_appender"])
                .build(LevelFilter::Info),
        )?;

    let handle = log4rs::init_config(config)?;

    std::panic::set_hook(Box::new(|panic_info| {
        if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            error!("panic occurred: {s:?}");
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            error!("panic occurred: {s:?}");
        } else {
            error!("panic occurred");
        }

        if let Some(location) = panic_info.location() {
            error!(
                "panic occurred in file '{}' at line {}",
                location.file(),
                location.line(),
            );
        }

        error!("panic debug info: {:?}", panic_info);

        // panic이 발생한 경우 서버 종료를 요청함
        tokio::spawn(async move {
            let mut server_sender_lock = STOP_SERVER_SENDER.lock().await;
            let sender_option = std::mem::replace(&mut *server_sender_lock, None);

            if let Some(sender) = sender_option {
                if sender.send(()).is_ok() {
                    info!("server shutdown starting...");
                }
            }
        });
    }));

    Ok(handle)
}
