use crate::errors::VCError;
use hmac::{Hmac, Mac};
use lazy_static::lazy_static;
use openssl::ssl::{SslAcceptor, SslAcceptorBuilder, SslFiletype, SslMethod};
use regex::Regex;
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};
use std::{
    collections::HashMap,
    env,
    ffi::{CStr, CString},
    os::raw::c_char,
    str::FromStr,
    sync::{Arc, Weak},
    time::Duration,
};
use uuid::Uuid;
use webrtc::{
    api::{
        API, APIBuilder,
        interceptor_registry::register_default_interceptors,
        media_engine::{
            MIME_TYPE_AV1, MIME_TYPE_H264, MIME_TYPE_OPUS, MIME_TYPE_VP8, MIME_TYPE_VP9,
            MediaEngine,
        },
        setting_engine::SettingEngine,
    },
    ice::udp_network::{EphemeralUDP, UDPNetwork},
    ice_transport::{ice_candidate::RTCIceCandidateInit, ice_candidate_type::RTCIceCandidateType},
    interceptor::registry::Registry,
    peer_connection::{RTCPeerConnection, peer_connection_state::RTCPeerConnectionState},
    rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType},
};

lazy_static! {
    pub static ref EPHEMERAL_UDP_MIN_PORT: u32 = {
        env::var("EPHEMERAL_UDP_MIN_PORT")
            .unwrap_or("49152".to_string())
            .parse::<u32>()
            .unwrap_or(49152)
    };
    pub static ref EPHEMERAL_UDP_MAX_PORT: u32 = {
        env::var("EPHEMERAL_UDP_MAX_PORT")
            .unwrap_or("65535".to_string())
            .parse::<u32>()
            .unwrap_or(65535)
    };
    pub static ref LOG_PATH: String = env::var("LOG_PATH").unwrap_or("./log".to_string());
    pub static ref LOGGER: String = env::var("LOGGER").unwrap_or("log4rs.yaml".to_string());
    pub static ref DEBUG_MODE: bool = env::var("DEBUG_MODE")
        .map(|v| v.parse::<i32>().map(|s| s == 1).unwrap_or(false))
        .unwrap_or(false);
    pub static ref SERVER_PORT: u32 = env::var("SERVER_PORT")
        .map(|v| v.parse::<u32>().unwrap_or(10000))
        .unwrap_or(10000);
    pub static ref SSL_SERVER_PORT: u32 = env::var("SSL_SERVER_PORT")
        .map(|v| v.parse::<u32>().unwrap_or(10043))
        .unwrap_or(10043);
    pub static ref STUN_ADDR: String = env::var("STUN_ADDR").unwrap_or("0.0.0.0:3478".to_string());
    pub static ref PEER_STUN_ADDRS: String =
        env::var("PEER_STUN_ADDRS").unwrap_or("0.0.0.0:3478".to_string());
    pub static ref TURN_ADDR: String = env::var("TURN_ADDR").unwrap_or("0.0.0.0:3479".to_string());
    pub static ref TURN_USERS: String = env::var("TURN_USERS").unwrap_or("".to_string());
    pub static ref HOST_CANDIDATE_IP: String =
        env::var("HOST_CANDIDATE_IP").unwrap_or("".to_owned());
    pub static ref STORAGE_PATH: String =
        std::env::var("STORAGE_PATH").unwrap_or("./storage".to_string());
    pub static ref SSL_KEY: String =
        std::env::var("SSL_KEY").unwrap_or("./cert/cert.key".to_string());
    pub static ref SSL_CERT: String =
        std::env::var("SSL_CERT").unwrap_or("./cert/cert.pem".to_string());
    pub static ref MAX_SESSION_TIME: u64 = env::var("MAX_SESSION_TIME")
        .map(|v| v.parse::<u64>().unwrap_or(604800))
        .unwrap_or(604800);
    pub static ref VIT_URL: String = env::var("VIT_URL")
        .unwrap_or("wss://avatar.cn-huadong-1.xf-yun.com/v1/interact".to_owned());
    pub static ref VIT_APP_ID: String = env::var("VIT_APP_ID").unwrap_or("c210314a".to_owned());
    pub static ref VIT_APP_KEY: String =
        env::var("VIT_APP_KEY").unwrap_or("4ce2bf81d14a01c70809a02cb5fcb5fc".to_owned());
    pub static ref VIT_APP_SECRET: String =
        env::var("VIT_APP_SECRET").unwrap_or("NzBjZDBhZGUzM2UwM2EzNmQ5ZTVhYzIy".to_owned());
    pub static ref VIT_SCENE_ID: String =
        env::var("VIT_SCENE_ID").unwrap_or("243981324682137600".to_owned());
    pub static ref VIT_AVATAR_ID: String =
        env::var("VIT_AVATAR_ID").unwrap_or("111165001".to_owned());
    pub static ref VIT_VCN_ID: String = env::var("VIT_VCN_ID").unwrap_or("x4_yezi".to_owned());
    pub static ref VIT_TTL: u128 = env::var("VIT_TTL")
        .unwrap_or("20000".to_owned())
        .parse::<u128>()
        .unwrap_or(20000);
    pub static ref VIT_OPT_CHUNK: usize = env::var("VIT_OPT_CHUNK")
        .unwrap_or("2730".to_owned())
        .parse::<usize>()
        .unwrap_or(2730);
    pub static ref VIT_CLIENT_IP: Option<String> = env::var("VIT_CLIENT_IP").ok();
    pub static ref VIT_MAX_SESSIONS: usize = env::var("VIT_MAX_SESSIONS")
        .unwrap_or("2".to_owned())
        .parse::<usize>()
        .unwrap_or(2);
    pub static ref USER_RTCP_INTERVAL: u64 = env::var("USER_RTCP_INTERVAL")
        .unwrap_or("10000".to_owned())
        .parse::<u64>()
        .unwrap_or(10000);
    pub static ref USER_RTCP_MAX_HISTORY: usize = env::var("USER_RTCP_MAX_HISTORY")
        .unwrap_or("2000".to_owned())
        .parse::<usize>()
        .unwrap_or(2000);
    pub static ref VAD_PATH: String =
        env::var("VAD_PATH").unwrap_or("./models/silero_vad.onnx".to_string());
    pub static ref RTP_TIMEOUT: u64 = env::var("RTP_TIMEOUT")
        .unwrap_or("10000".to_string())
        .parse::<u64>()
        .unwrap_or(10000);
    pub static ref OPUS_INTERVAL: u64 = env::var("OPUS_INTERVAL")
        .unwrap_or("20".to_string())
        .parse::<u64>()
        .unwrap_or(20);
    pub static ref WATCHDOG_INTERVAL: u64 = env::var("WATCHDOG_INTERVAL")
        .unwrap_or("2000".to_string())
        .parse::<u64>()
        .unwrap_or(2000);
}

type HmacSha256 = Hmac<sha2::Sha256>;

#[derive(Clone)]
pub struct RtcJob {
    // pub token: Option<Uuid>,
    pub target: Option<Uuid>,
    pub candidates: Option<Vec<Option<RTCIceCandidateInit>>>,
    pub prefer_codec: Option<String>,
    pub peer: String,
}

impl<'de> Deserialize<'de> for RtcJob {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // 定义访问者
        struct UserVisitor;

        impl<'de> Visitor<'de> for UserVisitor {
            type Value = RtcJob;
            // 预期解析的数据结构类型（YAML 的 Map）
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a YAML map representing a User")
            }
            // 处理 Map 类型的反序列化
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut target = None;
                let mut prefer_codec = None;
                let mut peer = None;
                let mut candidates = None;
                // 遍历 YAML 的键值对
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "target" => {
                            if let Ok(v) = map.next_value::<String>() {
                                if let Ok(t) = Uuid::from_str(&v) {
                                    target.replace(t);
                                }
                            }
                        }
                        "prefer_codec" => {
                            if let Ok(v) = map.next_value::<String>() {
                                prefer_codec.replace(v);
                            }
                        }
                        "peer" => {
                            if let Ok(v) = map.next_value::<String>() {
                                peer.replace(v);
                            }
                        }
                        "candidates" => {
                            if let Ok(v) = map.next_value::<Vec<Option<RTCIceCandidateInit>>>() {
                                candidates.replace(v);
                            }
                        }
                        // 处理未知字段
                        other => {
                            return Err(serde::de::Error::unknown_field(
                                other,
                                &["target", "prefer_codec", "peer", "candidates"],
                            ));
                        }
                    };
                }
                Ok(RtcJob {
                    target,
                    prefer_codec,
                    candidates,
                    peer: peer.ok_or(serde::de::Error::missing_field("peer field must be set"))?,
                })
            }
        }
        // 触发访问者处理
        deserializer.deserialize_map(UserVisitor)
    }
}

impl RtcJob {
    pub fn parse_codecs(&self) -> Option<(Option<String>, Option<String>)> {
        match &self.prefer_codec {
            Some(codec) => {
                let codecs = codec
                    .split(",")
                    .filter(|v| *v != "")
                    .map(|v| v.trim().to_owned())
                    .collect::<Vec<String>>();
                let codecs_len = codecs.len();
                if codecs_len >= 2 {
                    Some((Some(codecs[0].clone()), Some(codecs[1].clone())))
                } else if codecs_len == 1 {
                    Some((Some(codecs[0].clone()), None))
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

// #[derive(Serialize, Deserialize, Clone)]
// pub struct RtcSubscriber {
//     pub id: u32,
//     pub target: u32,
// }

pub fn gen_xf_webrtc_api() -> API {
    let mut m = MediaEngine::default();
    // m.register_default_codecs()
    //     .expect("Failed to register default codecs");
    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 102,
            ..Default::default()
        },
        RTPCodecType::Video,
    )
    .unwrap();

    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RTPCodecType::Audio,
    )
    .unwrap();
    let mut s = SettingEngine::default();
    s.set_ice_timeouts(
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_millis(200)),
    );
    s.set_dtls_replay_protection_window(256);
    s.set_srtp_replay_protection_window(128);
    s.set_srtcp_replay_protection_window(64);
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)
        .expect("Failed to register default interceptors");

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_setting_engine(s)
        .with_interceptor_registry(registry)
        .build();
    return api;
}

pub fn gen_webrtc_api() -> API {
    let mut m = MediaEngine::default();
    // m.register_default_codecs()
    //     .expect("Failed to register default codecs");
    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 102,
            ..Default::default()
        },
        RTPCodecType::Video,
    )
    .unwrap();

    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RTPCodecType::Audio,
    )
    .unwrap();
    let mut s = SettingEngine::default();
    s.set_ice_timeouts(
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_millis(200)),
    );
    let port_min = *EPHEMERAL_UDP_MIN_PORT as u16;
    let port_max = *EPHEMERAL_UDP_MAX_PORT as u16;
    let host_ips = HOST_CANDIDATE_IP
        .split(",")
        .filter(|v| *v != "")
        .map(|v| v.to_owned())
        .collect::<Vec<String>>();
    let udp_socket = EphemeralUDP::new(port_min, port_max).unwrap();
    s.set_udp_network(UDPNetwork::Ephemeral(udp_socket));
    if host_ips.len() > 0 {
        s.set_nat_1to1_ips(host_ips, RTCIceCandidateType::Host);
    }
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)
        .expect("Failed to register default interceptors");

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_setting_engine(s)
        .with_interceptor_registry(registry)
        .build();
    return api;
}

pub struct WebrtcAPI {
    pub xf_api: API,
    pub user_api: API,
}
impl WebrtcAPI {
    pub fn new() -> Self {
        Self {
            xf_api: gen_xf_webrtc_api(),
            user_api: gen_webrtc_api(),
        }
    }
}

pub fn api_from_codecs(video_codecs: Vec<String>) -> API {
    let mut m = MediaEngine::default();
    for codec in video_codecs {
        if codec == MIME_TYPE_H264 {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_H264.to_owned(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type: 102,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
            .unwrap();
        } else if codec == MIME_TYPE_VP8 {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_VP8.to_owned(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type: 96,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
            .unwrap();
        } else if codec == MIME_TYPE_VP9 {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_VP9.to_owned(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type: 98,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
            .unwrap();
        } else if codec == MIME_TYPE_AV1 {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_AV1.to_owned(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type: 41,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
            .unwrap();
        }
    }
    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RTPCodecType::Audio,
    )
    .unwrap();
    let mut s = SettingEngine::default();
    s.set_ice_timeouts(
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_millis(200)),
    );
    let port_min = *EPHEMERAL_UDP_MIN_PORT as u16;
    let port_max = *EPHEMERAL_UDP_MAX_PORT as u16;

    let host_ips = HOST_CANDIDATE_IP
        .split(",")
        .filter(|v| *v != "")
        .map(|v| v.to_owned())
        .collect::<Vec<String>>();
    let udp_socket = EphemeralUDP::new(port_min, port_max).unwrap();
    s.set_udp_network(UDPNetwork::Ephemeral(udp_socket));
    if host_ips.len() > 0 {
        s.set_nat_1to1_ips(host_ips, RTCIceCandidateType::Host);
    }
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)
        .expect("Failed to register default interceptors");
    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_setting_engine(s)
        .with_interceptor_registry(registry)
        .build();
    return api;
}

// alphabet to base 64
pub fn btoa(b: &str) -> String {
    base64::encode(b)
}
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    // Create HMAC-SHA256 instance
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    // Add data to HMAC calculation
    mac.update(data);
    // Finalize and return result
    mac.finalize().into_bytes().into()
}
// base64 to alphabet
pub fn atob(s: &str) -> Result<String, VCError> {
    let b = base64::decode(s)?;
    let s = String::from_utf8(b)?;
    Ok(s)
}

pub fn check_sdp(sdp_str: &str) -> Result<(), VCError> {
    let b: Vec<u8> = sdp_str.as_bytes().into();
    let mut buff = std::io::Cursor::new(b);
    let sd = sdp::SessionDescription::unmarshal(&mut buff)
        .map_err(|e| VCError::new(&format!("error unmarsalling {}", e.to_string())))?;
    let mut mds = sd.media_descriptions.iter();
    let mv = mds
        .find(|v| (*v).media_name.media == "video")
        .ok_or(VCError::new("No video desc in offer"))?;
    mv.attributes
        .iter()
        .find(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sH264/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .ok_or(VCError::new("No h264 desc in offer"))?;

    let mut mds = sd.media_descriptions.iter();
    let ma = mds
        .find(|v| (*v).media_name.media == "audio")
        .ok_or(VCError::new("No audio desc in offer"))?;
    ma.attributes
        .iter()
        .find(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sopus/48000/2$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .ok_or(VCError::new("No opus desc in offer"))?;
    Ok(())
}

pub fn parse_payloads(sdp_str: &str) -> Result<HashMap<String, Vec<u8>>, VCError> {
    let b: Vec<u8> = sdp_str.as_bytes().into();
    let mut buff = std::io::Cursor::new(b);
    let sd = sdp::SessionDescription::unmarshal(&mut buff)
        .map_err(|e| VCError::new(&format!("error unmarsalling {}", e.to_string())))?;
    let mut mds = sd.media_descriptions.iter();
    let mv = mds
        .find(|v| (*v).media_name.media == "video")
        .ok_or(VCError::new("No video desc in offer"))?;
    let h264_payloads = mv
        .attributes
        .iter()
        .filter(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sH264/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|v| {
            v.value
                .as_ref()
                .map(|s| {
                    Regex::new(r"^(\d+)?\sH264/90000$")
                        .unwrap()
                        .replace_all(s, "$1")
                })
                .unwrap()
                .parse::<u8>()
                .unwrap()
        })
        .collect::<Vec<u8>>();
    let vp8_payloads = mv
        .attributes
        .iter()
        .filter(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sVP8/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|v| {
            v.value
                .as_ref()
                .map(|s| {
                    Regex::new(r"^(\d+)?\sVP8/90000$")
                        .unwrap()
                        .replace_all(s, "$1")
                })
                .unwrap()
                .parse::<u8>()
                .unwrap()
        })
        .collect::<Vec<u8>>();
    let vp9_payloads = mv
        .attributes
        .iter()
        .filter(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sVP9/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|v| {
            v.value
                .as_ref()
                .map(|s| {
                    Regex::new(r"^(\d+)?\sVP9/90000$")
                        .unwrap()
                        .replace_all(s, "$1")
                })
                .unwrap()
                .parse::<u8>()
                .unwrap()
        })
        .collect::<Vec<u8>>();
    let av1_payloads = mv
        .attributes
        .iter()
        .filter(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sAV1/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|v| {
            v.value
                .as_ref()
                .map(|s| {
                    Regex::new(r"^(\d+)?\sAV1/90000$")
                        .unwrap()
                        .replace_all(s, "$1")
                })
                .unwrap()
                .parse::<u8>()
                .unwrap()
        })
        .collect::<Vec<u8>>();
    let mut mds = sd.media_descriptions.iter();
    let ma = mds
        .find(|v| (*v).media_name.media == "audio")
        .ok_or(VCError::new("No audio desc in offer"))?;
    let opus_payloads = ma
        .attributes
        .iter()
        .filter(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sopus/48000/2$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|v| {
            v.value
                .as_ref()
                .map(|s| {
                    Regex::new(r"^(\d+)?\sopus/48000/2$")
                        .unwrap()
                        .replace_all(s, "$1")
                })
                .unwrap()
                .parse::<u8>()
                .unwrap()
        })
        .collect::<Vec<u8>>();
    Ok([
        (MIME_TYPE_H264.to_owned(), h264_payloads),
        (MIME_TYPE_VP8.to_owned(), vp8_payloads),
        (MIME_TYPE_VP9.to_owned(), vp9_payloads),
        (MIME_TYPE_OPUS.to_owned(), opus_payloads),
        (MIME_TYPE_AV1.to_owned(), av1_payloads),
    ]
    .into_iter()
    .collect::<HashMap<String, Vec<u8>>>())
}

pub fn gen_ssl_builder() -> Result<SslAcceptorBuilder, VCError> {
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    builder.set_private_key_file(&*SSL_KEY, SslFiletype::PEM)?;
    builder.set_certificate_chain_file(&*SSL_CERT)?;
    Ok(builder)
}

#[macro_export]
macro_rules! tokio_read_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_millis($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(anyhow!("Being locked"))}
            m = $x.read()=> {Ok(m)}
        }
    }};
}

#[macro_export]
macro_rules! tokio_write_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_millis($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(anyhow!("Being locked"))}
            m = $x.write()=> {Ok(m)}
        }
    }};
}

#[macro_export]
macro_rules! tokio_mutex_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_millis($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(anyhow!("Being locked"))}
            m = $x.lock()=> {Ok(m)}
        }
    }};
}

#[macro_export]
macro_rules! tokio_rcv_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_millis($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(anyhow!("tokio_rcv_lock timeout"))}
            m = $x.recv()=> {Ok(m)}
        }
    }};
}

#[macro_export]
macro_rules! tokio_any_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_millis($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(anyhow!("tokio_any_lock timeout"))}
            m = $x=> {Ok(m)}
        }
    }};
}
#[macro_export]
macro_rules! asign_if_ne {
    ( $x:expr, $y:expr ) => {{
        if $x != $y {
            $x = $y;
        }
    }};
}

// #[macro_export]
// macro_rules! tokio_any_lock2 {
//     ( $x:expr, $y:expr, $z:expr ) => {{
//         let timeout = tokio::time::sleep(Duration::from_millis($z));
//         tokio::pin!(timeout);
//         tokio::select! {
//             _ = timeout.as_mut() => {Err(anyhow!("tokio_any_lock2 timeout"))}
//             m = $x=> {Ok((m, 0))}
//             n = $y=> {Ok((n, 1))}
//         }
//     }};
// }

pub fn peer_closed(conn: &Arc<RTCPeerConnection>) -> bool {
    let state = conn.connection_state();
    state == RTCPeerConnectionState::Closed || state == RTCPeerConnectionState::Failed
}

pub fn weak_peer_closed(conn: &Weak<RTCPeerConnection>) -> bool {
    let mut result = false;
    if let Some(pc3) = conn.upgrade() {
        if peer_closed(&pc3) {
            result = true;
        }
    } else {
        result = true
    }
    result
}

pub fn cleaner() {
    if *DEBUG_MODE {
        if let Ok(results) = std::fs::read_dir(&*LOG_PATH) {
            for entry in results {
                if let Ok(de) = entry {
                    let _ = vclog!(std::fs::remove_file(de.path()));
                }
            }
        }
        if let Ok(results) = std::fs::read_dir(&*STORAGE_PATH) {
            for entry in results {
                if let Ok(de) = entry {
                    let _ = vclog!(std::fs::remove_file(de.path()));
                }
            }
        }
    }
    tokio::spawn(async move {
        log::info!("clearing up log/storage folder");
        log::debug!(target:"debug","clearing up log/storage folder");
        loop {
            if let Ok(results) = std::fs::read_dir(&*LOG_PATH) {
                for entry in results {
                    if let Ok(de) = entry {
                        if let Ok(meta) = de.metadata() {
                            let mut can_delete = false;
                            if let Ok(modified) = meta.modified() {
                                if let Ok(elisped) = modified.elapsed() {
                                    // 默认保留一周
                                    if elisped.as_secs() > 7 * 24 * 3600 {
                                        can_delete = true;
                                    }
                                }
                            }
                            if can_delete {
                                if meta.file_type().is_file() {
                                    let _ = std::fs::remove_file(de.path());
                                } else if meta.file_type().is_dir() {
                                    let _ = std::fs::remove_dir_all(de.path());
                                }
                            }
                        }
                    }
                }
            }
            if let Ok(results) = std::fs::read_dir(&*STORAGE_PATH) {
                for entry in results {
                    if let Ok(de) = entry {
                        if let Ok(meta) = de.metadata() {
                            let mut can_delete = false;
                            if let Ok(modified) = meta.modified() {
                                if let Ok(elisped) = modified.elapsed() {
                                    // 默认保留一周
                                    if elisped.as_secs() > 7 * 24 * 3600 {
                                        can_delete = true;
                                    }
                                }
                            }
                            if can_delete {
                                if meta.file_type().is_file() {
                                    let _ = std::fs::remove_file(de.path());
                                } else if meta.file_type().is_dir() {
                                    let _ = std::fs::remove_dir_all(de.path());
                                }
                            }
                        }
                    }
                }
            }
            // 默认1天检查一次
            tokio::time::sleep(Duration::from_secs(24 * 3600)).await;
        }
    });
}

pub fn chunks(data: &[u8], chunk_size: usize) -> Vec<&[u8]> {
    let list = data.chunks_exact(chunk_size);
    let remains = list.remainder();
    let mut chunks = list.collect::<Vec<&[u8]>>();
    if remains.len() > 0 {
        chunks.push(remains);
    }
    return chunks;
}

pub fn c_str_to_string(c_str: *const c_char) -> String {
    unsafe { CStr::from_ptr(c_str) }
        .to_str()
        .unwrap()
        .to_string()
}

pub fn str_to_c_str(str: &str) -> CString {
    CString::new(str).expect("could not alloc CString")
}

pub fn string_to_c_str(str: String) -> CString {
    CString::new(&str[..]).expect("could not alloc CString")
}

#[test]
fn test_btoa() {
    // let a = vec![
    //     123, 34, 109, 101, 115, 115, 97, 103, 101, 34, 58, 34, 72, 77, 65, 67, 32, 115, 105, 103,
    //     110, 97, 116, 117, 114, 101, 32, 100, 111, 101, 115, 32, 110, 111, 116, 32, 109, 97, 116,
    //     99, 104, 34, 125,
    // ];
    let b = "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";
    let c = base64::decode(b).unwrap();
    println!("c {c:?}, {}", c.len());
}
