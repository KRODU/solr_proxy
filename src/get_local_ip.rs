use local_ip_address::Error;
use std::net::IpAddr;

pub fn get_local_ip() -> Result<IpAddr, Error> {
    #[cfg(target_os = "linux")]
    {
        local_ip_address::linux::local_ip()
    }

    #[cfg(target_os = "windows")]
    {
        use std::env;

        let ifas = local_ip_address::windows::list_afinet_netifas()?;

        if let Some((_, ipaddr)) = find_ifa(ifas, "Ethernet") {
            return Ok(ipaddr);
        }

        Err(Error::PlatformNotSupported(env::consts::OS.to_string()))
    }
}

#[cfg(target_os = "windows")]
fn find_ifa(ifas: Vec<(String, IpAddr)>, ifa_name: &str) -> Option<(String, IpAddr)> {
    ifas.into_iter()
        .find(|(name, ipaddr)| name.contains(ifa_name) && matches!(ipaddr, IpAddr::V4(_)))
}
