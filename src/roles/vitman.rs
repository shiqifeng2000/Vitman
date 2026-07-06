use crate::errors::VCError;
use crate::process::jitter::{JitterBuffer, SequenceNumber};
use crate::roles::user::{
    RtpInfo, UserMessage, UserMessageAudio, UserMessageDispatch, UserMessageInteract,
    UserRegisterEvent,
};
use crate::serde::ser::SerializeStruct;
use crate::utils::{
    self, MAX_SESSION_TIME, RTP_TIMEOUT, VIT_APP_ID, VIT_AVATAR_ID, VIT_CLIENT_IP, VIT_OPT_CHUNK,
    VIT_SCENE_ID, VIT_TTL, VIT_VCN_ID, WATCHDOG_INTERVAL, WebrtcAPI, peer_closed, weak_peer_closed,
};
use crate::{
    tokio_any_lock,
    utils::{VIT_APP_KEY, VIT_APP_SECRET, VIT_URL, hmac_sha256},
};
use crate::{tokio_rcv_lock, tokio_write_lock};
use actix_web::web;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use regex::Regex;
use serde::{Serialize, Serializer};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::time::Instant;
use std::{
    borrow::Cow,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::sync::{Notify, RwLock, broadcast};
use tokio_tungstenite::{connect_async, tungstenite};
use url::Url;
use uuid::Uuid;
use webrtc::api::API;
use webrtc::api::media_engine::{
    MIME_TYPE_AV1, MIME_TYPE_H264, MIME_TYPE_OPUS, MIME_TYPE_VP8, MIME_TYPE_VP9,
};
use webrtc::ice_transport::ice_connection_state::RTCIceConnectionState;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use webrtc::rtcp::transport_feedbacks::transport_layer_nack::TransportLayerNack;
use webrtc::rtp::packet::Packet;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;
use webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver;
use webrtc::rtp_transceiver::rtp_transceiver_direction::RTCRtpTransceiverDirection;
use webrtc::rtp_transceiver::{RTCRtpTransceiver, RTCRtpTransceiverInit};
use webrtc::track::track_remote::TrackRemote;

#[derive(Clone)]
pub struct VitSession {
    // id只有这里是强指针，只要该变量drop其他所有子线程自行终止
    pub vid: Arc<Uuid>,
    // 讯飞通信上行通道
    pub up_messager: broadcast::Sender<UserMessage>,
    // 讯飞通信下行通道
    down_messager: broadcast::Sender<XfDownMessage>,

    // rtc相关
    pub user_notifier: broadcast::Sender<UserRegisterEvent>,
    // 通道的设计属于一层缓冲
    pub rtc_vsdr: broadcast::Sender<Packet>,
    pub rtc_asdr: broadcast::Sender<Packet>,
    pub rtc_conn: Arc<RTCPeerConnection>,

    pub rtc_video_info: Arc<RwLock<Option<RtpInfo>>>,
    pub rtc_audio_info: Arc<RwLock<Option<RtpInfo>>>,

    pub rtc_listeners: Arc<RwLock<Vec<Weak<u32>>>>,
}

impl VitSession {
    pub async fn new(api: &web::Data<WebrtcAPI>, http_client: &reqwest::Client) -> Result<Self> {
        let vid0 = Uuid::new_v4();
        let vid = Arc::new(vid0);
        let (up_messager, up_message_recv) = broadcast::channel::<UserMessage>(100);
        let (down_messager, mut down_messager_recv) = broadcast::channel::<XfDownMessage>(100);
        // 启动虚拟人线程
        let vid1 = Arc::downgrade(&vid);
        let down_messager1 = down_messager.clone();
        tokio::spawn(async move {
            let _ = vclog!(Self::spwn_xf_vitman(vid1, up_message_recv, &down_messager1).await);
        });
        // 等待生成stream_url
        let timeout = Instant::now();
        let stream_url = loop {
            if timeout.elapsed().as_millis() > *RTP_TIMEOUT as u128 {
                return Err(anyhow!("timeout receiving stream_info {vid0}"));
            }
            if let Ok(Ok(v)) = tokio_rcv_lock!(down_messager_recv, 1000) {
                if let XfDownMessage {
                    payload:
                        XfDownMessagePayload::Avatar {
                            stream_url,
                            event_type: XfDownMessagePayloadEventType::StreamInfo,
                            ..
                        },
                    ..
                } = v
                {
                    break stream_url.ok_or(anyhow!("stream_info evt has no url"))?;
                }
            }
        };

        // 根据stream_url生成peer
        let (rtc_vsdr, _) = broadcast::channel::<Packet>(1024 * 1024);
        let (rtc_asdr, _) = broadcast::channel::<Packet>(1024 * 1024);
        let (user_notifier, _) = broadcast::channel::<UserRegisterEvent>(100);
        let rtc_video_info = Arc::new(RwLock::new(None));
        let rtc_audio_info = Arc::new(RwLock::new(None));
        let rtc_listeners = Arc::new(RwLock::new(vec![]));
        let mut rtc_retry = 0;
        let rtc_conn = loop {
            let result = Self::create_rtc_peer(
                &vid,
                &stream_url,
                &api.xf_api,
                &rtc_vsdr,
                &rtc_asdr,
                &user_notifier,
                &rtc_video_info,
                &rtc_audio_info,
                &rtc_listeners,
                http_client,
            )
            .await;
            match result {
                Ok(v) => {
                    break v;
                }
                Err(e) => {
                    if rtc_retry >= 1 {
                        return Err(e);
                    }
                }
            }
            rtc_retry += 1;
        };
        Ok(Self {
            vid,
            up_messager,
            down_messager,
            user_notifier,
            rtc_vsdr,
            rtc_asdr,
            rtc_video_info,
            rtc_audio_info,
            rtc_conn,
            rtc_listeners,
        })
    }
    /// 处理讯飞数字人握手
    fn handshake() -> Result<String> {
        let vit_url = Url::parse(&*VIT_URL)?;
        let (host, date, authorization) = {
            let vit_host = vit_url.host().ok_or(anyhow!("vit_url no host"))?;
            let vit_path = vit_url.path();
            let datetime = chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string();
            let signature_origin =
                format!("host: {vit_host}\ndate: {datetime}\nGET {vit_path} HTTP/1.1");
            let signature_sha = hmac_sha256(VIT_APP_SECRET.as_bytes(), signature_origin.as_bytes());
            let signature_base64 = base64::encode(signature_sha);
            let authorization_origin = format!(
                "api_key=\"{}\", algorithm=\"hmac-sha256\", headers=\"host date request-line\", signature=\"{signature_base64}\"",
                &*VIT_APP_KEY
            );
            let authorization = base64::encode(authorization_origin);
            (
                vit_host.to_string(),
                datetime.replace(" ", "%20"),
                authorization,
            )
        };
        // vit_url
        //     .query_pairs_mut()
        //     .append_pair("host", &host)
        //     .append_pair("date", &date)
        //     .append_pair("authorization", &authorization);
        Ok(format!(
            "{vit_url}?host={host}&date={date}&authorization={authorization}"
        ))
    }

    /// 处理讯飞websocket消息
    async fn spwn_xf_vitman(
        vid1: Weak<Uuid>,
        mut up_message_listener: broadcast::Receiver<UserMessage>,
        down_messager1: &broadcast::Sender<XfDownMessage>,
    ) -> Result<()> {
        let ws_url = Self::handshake()?;
        let (mut ws_stream, ws_res) = connect_async(ws_url).await?;

        log::debug!(target:"debug","ws res: {:?}", ws_res);
        // let (mut ws_sndr, mut ws_rcvr) = ws_stream.split();
        // let (_, mut ws) = awc::Client::new().ws(ws_url).connect().await.unwrap();
        let mut vid2 = Uuid::nil();
        let mut hb = Instant::now();

        // 启动即开始推流
        if let Ok(s) = serde_json::to_string(&XfUpMessage::start(
            &VIT_APP_ID,
            &VIT_SCENE_ID,
            &VIT_AVATAR_ID,
            &VIT_VCN_ID,
        )) {
            log::debug!(target:"debug","ws req: {s}",);
            let _ = tokio_any_lock!(
                ws_stream.send(tungstenite::protocol::Message::Text(s.into())),
                *RTP_TIMEOUT
            );
        }
        let mut ticker = tokio::time::interval(Duration::from_millis(*WATCHDOG_INTERVAL));
        // ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let reason = loop {
            if let Some(id) = vid1.upgrade() {
                if vid2.is_nil() {
                    vid2 = *id;
                }
            } else {
                break "Vid Destroyed";
            }

            // log::debug!(target:"debug", "looping xf message thread");
            tokio::select! {
                _ = ticker.tick() => {
                    if hb.elapsed().as_millis() > *VIT_TTL {
                        break "ws_client timeout";
                    }
                    if let Ok(ping) = serde_json::to_string(&XfUpMessage::ping(&*VIT_APP_ID)) {
                        let _ = ws_stream.send(tungstenite::protocol::Message::Text(ping.into())).await;
                    }
                }
                m = ws_stream.next() => {
                    // log::debug!(target:"debug", "received messge {m:?}", );
                    match m {
                        Some(Ok(tungstenite::protocol::Message::Text(t)))=>{
                            if let Ok(r) = serde_json::from_str::<XfDownMessage>(t.as_str()) {
                                log::debug!(target:"debug", "xf down message {}", t.as_str());
                                let _ = down_messager1.send(r);
                            }
                        }
                        Some(Ok(f))=>{
                            log::debug!("received non-text frame {f:?} for {vid2}");
                        }
                        Some(Err(e))=>{
                            log::error!("error {e:?}");
                        }
                        None=>{
                            break "ws_client dropped";
                        }
                    }
                    hb = Instant::now();
                }
                n = up_message_listener.recv()=>{
                    if let Ok(t) = n {
                        if let Ok(list) = Self::process_user_message(t).await {
                            // log::debug!(target:"debug", "sending user request {}", c);
                            for v in list {
                                let _ = ws_stream.send(v).await;
                            }
                        }
                    }
                    hb = Instant::now();
                }
            }
        };
        // 推出时停止推流
        if let Ok(s) = serde_json::to_string(&XfUpMessage::stop(&VIT_APP_ID)) {
            let _ = tokio_any_lock!(
                ws_stream.send(tungstenite::protocol::Message::Text(s.into())),
                *RTP_TIMEOUT
            );
        }
        log::info!("[XF->S] Quitting Vitman {vid2} ws thread from <{reason}>!");
        Ok(())
    }

    async fn create_rtc_peer(
        vid: &Arc<Uuid>,
        vit_stream_url: &str,
        api: &API,
        rtc_vsdr: &broadcast::Sender<Packet>,
        rtc_asdr: &broadcast::Sender<Packet>,
        user_notifier: &broadcast::Sender<UserRegisterEvent>,
        rtc_video_info: &Arc<RwLock<Option<RtpInfo>>>,
        rtc_audio_info: &Arc<RwLock<Option<RtpInfo>>>,
        rtc_listeners: &Arc<RwLock<Vec<Weak<u32>>>>,
        http_client: &reqwest::Client,
    ) -> Result<Arc<RTCPeerConnection>> {
        let vid0 = *vid.as_ref();
        // let stuns = PEER_STUN_ADDRS
        //     .split(",")
        //     .filter(|v| *v != "")
        //     .map(|v| v.to_owned())
        //     .collect::<Vec<String>>();
        let ice_servers = vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            // urls: vec!["stun:101.200.144.99:31401".to_owned()],
            // urls: stuns
            //     .iter()
            //     .map(|v| format!("stun:{v}"))
            //     .collect::<Vec<String>>(),
            ..Default::default()
        }];
        let conf = RTCConfiguration {
            ice_servers,
            ..Default::default()
        };
        // let api = utils::gen_webrtc_api(false);
        let connection = Arc::new(api.new_peer_connection(conf).await?);

        // 异常退出的通知
        let notify = Arc::new(Notify::new());

        connection
            .add_transceiver_from_kind(
                RTPCodecType::Audio,
                Some(RTCRtpTransceiverInit {
                    direction: RTCRtpTransceiverDirection::Sendrecv,
                    send_encodings: vec![],
                }),
                // None,
            )
            .await?;
        connection
            .add_transceiver_from_kind(
                RTPCodecType::Video,
                Some(RTCRtpTransceiverInit {
                    direction: RTCRtpTransceiverDirection::Sendrecv,
                    send_encodings: vec![],
                }),
            )
            .await?;

        let payload_pool = Arc::new(RwLock::new(None));
        let payload_pool1 = payload_pool.clone();
        let rtc_vsdr1 = rtc_vsdr.clone();
        let rtc_asdr1 = rtc_asdr.clone();
        let notify1 = notify.clone();
        let rtc_video_info1 = rtc_video_info.clone();
        let rtc_audio_info1 = rtc_audio_info.clone();
        let user_notifier1 = user_notifier.clone();
        let rtc_listeners1 = Arc::downgrade(&rtc_listeners);

        let pc1 = Arc::downgrade(&connection);
        let vid1 = Arc::downgrade(&vid);
        let ontrack_notify = Arc::new(Notify::new());
        let ontrack_notify1 = ontrack_notify.clone();
        connection.on_track(Box::new(
            move |track: Arc<TrackRemote>,
                  _receiver: Arc<RTCRtpReceiver>,
                  _tranceiver: Arc<RTCRtpTransceiver>| {
                let media_ssrc = track.ssrc();
                let vid2 = vid1.clone();
                let kind = track.kind();
                let payload_pool2 = payload_pool1.clone();
                let pc2 = pc1.clone();
                let notify2 = notify1.clone();
                match kind {
                    RTPCodecType::Video => {
                        ontrack_notify1.notify_waiters();
                        // 讯飞通信rtcp线程
                        let user_watcher = user_notifier1.subscribe();
                        let pc3 = pc1.clone();
                        let rtc_listeners2 = rtc_listeners1.clone();
                        let (jittr_clear_sndr, jitter_clear_rcvr) =
                            broadcast::channel::<Option<Vec<u16>>>(50);
                        tokio::spawn(async move {
                            Self::process_video_rtcp(
                                vid2,
                                media_ssrc,
                                user_watcher,
                                &notify2,
                                &rtc_listeners2,
                                jitter_clear_rcvr,
                                &pc3,
                            )
                            .await;
                        });
                        let rtc_vsdr2 = rtc_vsdr1.clone();
                        let rtc_video_info2 = rtc_video_info1.clone();
                        let notify2 = notify1.clone();
                        tokio::spawn(async move {
                            Self::process_video_rtp(
                                vid0,
                                media_ssrc,
                                &rtc_vsdr2,
                                &payload_pool2,
                                &notify2,
                                &jittr_clear_sndr,
                                &rtc_video_info2,
                                &track,
                                &pc2,
                            )
                            .await;
                        });
                    }
                    RTPCodecType::Audio => {
                        let rtc_asdr2 = rtc_asdr1.clone();
                        let rtc_audio_info2 = rtc_audio_info1.clone();
                        tokio::spawn(async move {
                            Self::process_audio_rtp(
                                vid0,
                                media_ssrc,
                                &rtc_asdr2,
                                &payload_pool2,
                                &notify2,
                                &rtc_audio_info2,
                                &track,
                                &pc2,
                            )
                            .await;
                        });
                    }
                    _ => {}
                }
                Box::pin(async {})
            },
        ));

        let notify1 = notify.clone();
        connection.on_ice_connection_state_change(Box::new(
            move |connection_state: RTCIceConnectionState| {
                log::info!("[XF->S] Session state changed {connection_state}",);
                if connection_state == RTCIceConnectionState::Closed
                    || connection_state == RTCIceConnectionState::Failed
                {
                    notify1.notify_waiters();
                }
                Box::pin(async {})
            },
        ));

        let connection1 = connection.clone();
        let ontrack_notify1 = ontrack_notify.clone();
        tokio::spawn(async move {
            if tokio_any_lock!(ontrack_notify1.notified(), *RTP_TIMEOUT).is_ok() {
                let timeout = tokio::time::sleep(Duration::from_secs(*MAX_SESSION_TIME));
                tokio::pin!(timeout);
                tokio::select! {
                    _ = timeout.as_mut() => {}
                    _ = notify.notified() => {}
                }
            } else {
                println!("ontrack timeout");
            }
            let closer = connection1.close().await;
            log::info!("[XF->S] Quitting Vitman {vid0} peer {closer:?}");
        });

        let offer = connection.create_offer(None).await?;
        let mut gather_complete = connection.gathering_complete_promise().await;
        connection.set_local_description(offer).await?;
        let _ = gather_complete.recv().await;
        let offer_gathered_sdp = connection
            .local_description()
            .await
            .ok_or(anyhow!("local sdp invalid"))?
            .sdp;
        // log::debug!(target:"debug","offer_gathered_sdp {offer_gathered_sdp}");
        log::info!("Vitman stream_url {vit_stream_url}");
        let whip_payload = Self::whip_payload(vit_stream_url, &offer_gathered_sdp);
        let res_str = http_client
            .post(&whip_payload.api)
            .body(serde_json::to_string(&whip_payload)?)
            .send()
            .await?
            .text()
            .await?;
        log::debug!(target:"debug","whep_payload {res_str}");
        let whep_payload = serde_json::from_str::<VitWhepPayload>(&res_str)?;
        {
            let sdp_hash = utils::parse_payloads(&whep_payload.sdp)?;
            let mut pool = tokio_write_lock!(payload_pool, 10)?;
            pool.replace(sdp_hash);
        }

        connection
            .set_remote_description(RTCSessionDescription::answer(whep_payload.sdp.to_string())?)
            .await?;

        // 如果长时间拉不到流则报错
        tokio_any_lock!(ontrack_notify.notified(), *RTP_TIMEOUT)?;
        Ok(connection)
    }

    async fn process_video_rtcp(
        vid2: Weak<Uuid>,
        media_ssrc: u32,
        mut user_watcher: broadcast::Receiver<UserRegisterEvent>,
        notify2: &Arc<Notify>,
        rtc_listeners: &Weak<RwLock<Vec<Weak<u32>>>>,
        mut jitter_flag: broadcast::Receiver<Option<Vec<u16>>>,
        pc2: &Weak<RTCPeerConnection>,
    ) {
        let kind = RTPCodecType::Video;
        let mut hb = Instant::now();
        let mut vid3 = Uuid::nil();
        let mut ticker = tokio::time::interval(Duration::from_millis(*WATCHDOG_INTERVAL));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // let mut user_watcher = user_notifier2.subscribe();
        let reason = loop {
            // 判断是否uuid还存在
            if let Some(id) = vid2.upgrade() {
                if vid3.is_nil() {
                    vid3 = *id;
                }
            } else {
                notify2.notify_waiters();
                break "Vid Destroyed".to_owned();
            }
            let listeners = if let Some(l) = rtc_listeners.upgrade() {
                l
            } else {
                notify2.notify_waiters();
                break "Vid Destroyed".to_owned();
            };
            // 判断是否有新用户加入
            tokio::select! {
                _ = ticker.tick() => {
                    Self::check_listeners(&listeners);
                    hb = Instant::now();
                }
                uevt = user_watcher.recv() => {
                    let time_past = Instant::now().duration_since(hb).as_millis();
                    if time_past < 400 {
                        tokio::time::sleep(Duration::from_millis(400)).await;
                    }
                    if let Ok(evt) = uevt {
                        if Self::process_register(evt, &listeners).await.unwrap_or(0) == 0{
                            notify2.notify_waiters();
                            break "User Empty".to_owned();
                        }
                    }
                }
                a = jitter_flag.recv() => {
                    log::debug!(target:"debug", "received flag {a:?}");
                    hb = Instant::now();
                }
            };
            if let Some(pc3) = pc2.upgrade() {
                if peer_closed(&pc3) {
                    break "peer closed".to_owned();
                }
                if let Err(e) = pc3
                    .write_rtcp(&[Box::new(PictureLossIndication {
                        sender_ssrc: 0,
                        media_ssrc,
                    })])
                    .await
                {
                    break format!("nack err {}", e.to_string());
                }
                // if let Err(e) = pc3
                //     .write_rtcp(&[Box::new(PictureLossIndication {
                //         sender_ssrc: 0,
                //         media_ssrc,
                //     })])
                //     .await
                // {
                //     break format!("pli err {}", e.to_string());
                // }
                // if let Some(n) = nacks {
                //     log::debug!(target:"debug", "sending nacks {n:?}");
                //     if let Err(e) = pc3
                //         .write_rtcp(&[Box::new(TransportLayerNack {
                //             sender_ssrc: 0,
                //             media_ssrc,
                //             nacks: vec![],
                //         })])
                //         .await
                //     {
                //         break format!("nack err {}", e.to_string());
                //     }
                // } else {

                // }
                // if let Err(e) = pc3
                //     .write_rtcp(&[Box::new(PictureLossIndication {
                //         sender_ssrc: 0,
                //         media_ssrc,
                //     })])
                //     .await
                // {
                //     break format!("err {}", e.to_string());
                // } else {
                //     // info!("=============> pli writed");
                // }
            } else {
                break "peer closed1".to_owned();
            }
        };
        log::info!("[XF->S] Quitting Vitman {vid3}-{kind} rtcp: {reason}");
    }
    async fn process_video_rtp(
        vid2: Uuid,
        media_ssrc: u32,
        rtc_vsdr2: &broadcast::Sender<Packet>,
        payload_pool2: &Arc<RwLock<Option<HashMap<String, Vec<u8>>>>>,
        notify2: &Arc<Notify>,
        jitter_sndr: &broadcast::Sender<Option<Vec<u16>>>,
        rtc_video_info2: &Arc<RwLock<Option<RtpInfo>>>,
        track: &Arc<TrackRemote>,
        pc2: &Weak<RTCPeerConnection>,
    ) {
        let mut hb = Instant::now();
        let mut pool = None;
        let mut init = false;
        let mut jitter = JitterBuffer::<Packet, 255>::new(Some(jitter_sndr));
        // let mut test_cursor = 0u32;
        // let mut ticker = tokio::time::interval(Duration::from_millis(*WATCHDOG_INTERVAL));
        loop {
            if pool.is_none() {
                if Instant::now().duration_since(hb).as_millis() > *RTP_TIMEOUT as u128 {
                    log::info!("[XF->S] {vid2}-video read thread init timeout");
                    break;
                }
                if let Ok(m) = payload_pool2.try_read() {
                    if let Some(v) = &*m {
                        pool.replace(v.clone());
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            } else {
                if Instant::now().duration_since(hb).as_millis() > *RTP_TIMEOUT as u128 {
                    notify2.notify_waiters();
                    log::info!("[XF->S] {vid2}-video read thread wait timeout");
                    break;
                }
            }
            let mypool = pool.as_ref().unwrap();
            let timeout = tokio::time::sleep(Duration::from_millis(*RTP_TIMEOUT));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {
                    log::info!("[XF->S] {vid2}-video read thread read timeout");
                    break;
                }
                // _ = ticker.tick() => {
                //     let lost_pkts = jitter.lost_packets_buffered();
                //     if lost_pkts.len() > 0 {
                //         log::debug!(target:"debug","[XF->S] nacks lost {lost_pkts:?} last {:?} heap {:?}", jitter.last.as_ref().map(|v|v.sequence_number), jitter.heap.clone().into_sorted_vec().iter().map(|v|v.sequence_number.0).collect::<Vec<u16>>());
                //         let _ = jitter_flag_sndr.send(Some(lost_pkts));
                //     }
                // }
                m = track.read_rtp() => {
                    let now = Instant::now();
                    if now.duration_since(hb).as_millis() > *RTP_TIMEOUT as u128 {
                        notify2.notify_waiters();
                        log::info!("[XF->S] {vid2}-video read thread timeout from peer close");
                        break;
                    }
                    if let Ok((rtp, _attr)) = m {
                        if !init {
                            // log::debug!(target:"debug","[XF->S] {vid2}-video read");
                            if Self::set_session_video(media_ssrc,rtp.header.payload_type, mypool, rtc_video_info2).await.is_some() {
                                init = true;
                            }
                        }
                        // log::debug!(target:"debug","[XF->S] {vid2}-video jitter sequence {}", rtp.header.sequence_number);
                        // let _ = rtc_vsdr2.send(rtp);
                        // 模拟丢包 20 %
                        // test_cursor = test_cursor.wrapping_add(1);
                        // if test_cursor % 20 == 3 {
                        //     continue;
                        // }
                        // let in_seq = rtp.header.sequence_number;
                        jitter.push(rtp);
                        'inner: loop {
                            if let Some(_) = jitter.peek(){
                                if let Some(pkt) = jitter.pop() {
                                    let _ = rtc_vsdr2.send(pkt);
                                    continue 'inner;
                                }
                            }
                            break 'inner;
                        }
                        // log::debug!(target:"debug", "[XF->S] video push done");
                        // 如果没有接受者，则不设心跳
                        if rtc_vsdr2.receiver_count() > 0 {
                            hb = now;
                        }
                    } else if weak_peer_closed(&pc2) {
                        log::info!("[XF->S] {vid2}-video read thread end from peer close");
                        break;
                    }
                }
            }
        }
        log::info!("[XF->S] Quitting Vitman {vid2}-video rtp thread");
    }
    async fn process_audio_rtp(
        vid2: Uuid,
        media_ssrc: u32,
        rtc_asdr2: &broadcast::Sender<Packet>,
        payload_pool2: &Arc<RwLock<Option<HashMap<String, Vec<u8>>>>>,
        notify2: &Arc<Notify>,
        rtc_audio_info2: &Arc<RwLock<Option<RtpInfo>>>,
        track: &Arc<TrackRemote>,
        pc2: &Weak<RTCPeerConnection>,
    ) {
        let mut init = false;
        let mut hb = Instant::now();
        let mut pool = None;
        loop {
            if pool.is_none() {
                if Instant::now().duration_since(hb).as_millis() > *RTP_TIMEOUT as u128 {
                    log::debug!("[XF->S] {vid2}-audio read thread init timeout");
                    break;
                }
                if let Ok(m) = payload_pool2.try_read() {
                    if let Some(v) = &*m {
                        pool.replace(v.clone());
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            } else {
                if Instant::now().duration_since(hb).as_millis() > *RTP_TIMEOUT as u128 {
                    notify2.notify_waiters();
                    log::debug!("[XF->S] {vid2}-audio read thread wait timeout");
                    break;
                }
            }
            let mypool = pool.as_ref().unwrap();
            let timeout = tokio::time::sleep(Duration::from_millis(*RTP_TIMEOUT));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {
                    log::debug!("[XF->S] {vid2}-audio read thread read timeout");
                    break;
                }
                m = track.read_rtp() => {
                    let now = Instant::now();
                    if now.duration_since(hb).as_millis() > *RTP_TIMEOUT as u128 {
                        notify2.notify_waiters();
                        log::debug!("[XF->S] {vid2}-audio read thread timeout from peer close");
                        break;
                    }
                    if let Ok((rtp, _n)) = m {
                        if !init {
                            if Self::set_session_audio(media_ssrc, rtp.header.payload_type, mypool, &rtc_audio_info2).await.is_some() {
                                init = true;
                            }
                        }
                        let _ = rtc_asdr2.send(rtp);
                        if rtc_asdr2.receiver_count() > 0 {
                            hb = now;
                        }
                    } else if weak_peer_closed(&pc2) {
                        log::debug!("[XF->S] {vid2}-audio read thread end from peer close");
                        break;
                    }
                }
            }
        }
        log::info!("[XF->S] Quitting Vitman {vid2}-audio rtp thread");
    }
    pub async fn set_session_video(
        media_ssrc: u32,
        p: u8,
        payloads: &HashMap<String, Vec<u8>>,
        rtc_video_info: &Arc<RwLock<Option<RtpInfo>>>,
    ) -> Option<RtpInfo> {
        let video = payloads
            .iter()
            .find(|(k, v)| {
                (*k == MIME_TYPE_H264
                    || *k == MIME_TYPE_AV1
                    || *k == MIME_TYPE_VP8
                    || *k == MIME_TYPE_VP9)
                    && v.contains(&p)
            })
            .map(|(k, _)| RtpInfo::new(k, Some(media_ssrc), p));
        if let Ok(mut rtc_video_info1) = tokio_write_lock!(rtc_video_info, *RTP_TIMEOUT) {
            *rtc_video_info1 = video.clone();
        }
        video
    }
    pub async fn set_session_audio(
        media_ssrc: u32,
        p: u8,
        payloads: &HashMap<String, Vec<u8>>,
        rtc_audio_info: &Arc<RwLock<Option<RtpInfo>>>,
    ) -> Option<RtpInfo> {
        let audio = payloads
            .iter()
            .find(|(k, v)| *k == MIME_TYPE_OPUS && v.contains(&p))
            .map(|(k, _)| RtpInfo::new(k, Some(media_ssrc), p));
        if let Ok(mut rtc_audio_info1) = tokio_write_lock!(rtc_audio_info, *RTP_TIMEOUT) {
            *rtc_audio_info1 = audio.clone();
        }
        audio
    }

    fn whip_payload(vit_stream_url: &str, offer: &str) -> VitWhipPayload {
        // webrtc://srs-stream.cn-huadong-1.xf-yun.com:9850/live/ase0001f2cahu19a771ab2a30442282?stream=ase0001f2cahu19a771ab2a30442282&schema=https
        let re = Regex::new(r"^webrtc://(.+?)/live/\w+?(\?.+?)?$").unwrap();
        let c = re.replace(vit_stream_url, "https://$1/rtc/v1/play/$2");
        VitWhipPayload::new(&c, VIT_CLIENT_IP.clone(), offer, vit_stream_url)
    }

    pub async fn process_user_message(
        data: UserMessage,
    ) -> Result<Vec<tungstenite::protocol::Message>> {
        let UserMessage {
            text,
            audio,
            interact_mode,
            dispatch_mode,
        } = data;
        let msgs = if let Some(content) = text {
            log::debug!(target:"debug", "received text {content}");
            let msg = match interact_mode {
                UserMessageInteract::Interact => XfUpMessage::text_interact(
                    &VIT_APP_ID,
                    &VIT_SCENE_ID,
                    &VIT_VCN_ID,
                    &content,
                    dispatch_mode.map(|v| v.into()),
                ),
                UserMessageInteract::Driver => XfUpMessage::text_driver(
                    &VIT_APP_ID,
                    &VIT_SCENE_ID,
                    &VIT_VCN_ID,
                    &content,
                    dispatch_mode.map(|v| v.into()),
                ),
            };
            vec![msg]
        } else if let Some(UserMessageAudio {
            audio,
            encoding,
            sample_rate,
            channels,
            bit_depth,
        }) = audio
        {
            log::debug!(target:"debug", "received audio {encoding:?} {sample_rate:?} {channels:?} {bit_depth:?} {}", audio.len());
            let a = XfUpMessage::audio_interact(
                &VIT_APP_ID,
                &VIT_SCENE_ID,
                &encoding,
                &sample_rate,
                &channels,
                &bit_depth,
                audio,
            );
            // log::debug!(target:"debug", "a {}", serde_json::to_string(&a).unwrap());
            a
        } else {
            return Err(anyhow!("text or audio must be set"));
        };
        Ok(msgs
            .into_iter()
            .map(|v| serde_json::to_string(&v))
            .filter(|v| v.is_ok())
            .map(|v| tungstenite::protocol::Message::Text(v.unwrap().into()))
            .collect())
        // let msg_str = serde_json::to_string(&msg)?;
        // Ok(tungstenite::protocol::Message::Text(msg_str.into()))
    }

    fn check_listeners(listeners: &Arc<RwLock<Vec<Weak<u32>>>>) {
        let list0_opt = listeners
            .try_read()
            .map(|a| {
                let mut b = a
                    .iter()
                    .map(|v| v.upgrade())
                    .filter(|v| v.is_some())
                    .map(|v| *v.unwrap().as_ref())
                    .collect::<Vec<u32>>();
                b.dedup();
                b
            })
            .ok();
        if let Some(list0) = list0_opt {
            if let Ok(mut list) = listeners.try_write() {
                list.retain(|v| {
                    if let Some(s) = v.upgrade() {
                        list0.contains(s.as_ref())
                    } else {
                        false
                    }
                });
            }
        }
    }

    async fn process_register(
        evt: UserRegisterEvent,
        listeners: &Arc<RwLock<Vec<Weak<u32>>>>,
    ) -> Result<usize> {
        let mut list = tokio_write_lock!(listeners, 2000)?;
        list.retain(|v| v.upgrade().is_some());
        if evt.up {
            list.push(evt.uid);
        } else if let Some(uid) = evt.uid.upgrade() {
            if let Some((i, _)) = list
                .iter_mut()
                .enumerate()
                .find(|(_, v)| v.upgrade().map(|s| *s == *uid).unwrap_or(false))
            {
                list.remove(i);
            }
        }
        Ok(list.len())
    }
}

impl Serialize for VitSession {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("VitSession", 3)?;
        s.serialize_field("vid", &self.vid.as_ref().to_string())?;
        s.serialize_field(
            "rtc_video_info",
            &self
                .rtc_video_info
                .try_read()
                .map(|v| v.clone())
                .unwrap_or(None),
        )?;
        s.serialize_field(
            "rtc_audio_info",
            &self
                .rtc_audio_info
                .try_read()
                .map(|v| v.clone())
                .unwrap_or(None),
        )?;
        s.serialize_field(
            "rtc_listeners",
            &self
                .rtc_listeners
                .try_read()
                .map(|v| {
                    v.iter()
                        .map(|s| s.upgrade())
                        .filter(|s| s.is_some())
                        .map(|s| s.unwrap().as_ref().to_string())
                        .collect::<Vec<String>>()
                })
                .unwrap_or(vec![]),
        )?;
        s.end()
    }
}
#[derive(Serialize, Debug, Default)]
struct VitWhipPayload {
    api: String,
    clientip: Option<String>,
    sdp: String,
    streamurl: String,
}
impl VitWhipPayload {
    pub fn new(api: &str, clientip: Option<String>, sdp: &str, streamurl: &str) -> Self {
        Self {
            api: api.to_owned(),
            clientip,
            sdp: sdp.to_owned(),
            streamurl: streamurl.to_owned(),
        }
    }
}

#[derive(Deserialize, Debug, Default)]
struct VitWhepPayload {
    code: i32,
    server: Cow<'static, str>,
    service: Cow<'static, str>,
    pid: Cow<'static, str>,
    sdp: Cow<'static, str>,
    sessionid: Cow<'static, str>,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct XfUpMessage {
    header: XfUpMessageHeader,
    parameter: Option<XfUpMessageParameter>,
    payload: Option<XfUpMessagePayload>,
}

impl XfUpMessage {
    pub fn ping(app_id: &str) -> Self {
        Self {
            header: XfUpMessageHeader::ping(app_id),
            ..Self::default()
        }
    }

    pub fn start(app_id: &str, scene_id: &str, avatar_id: &str, vcn: &str) -> Self {
        Self {
            header: XfUpMessageHeader::start(app_id, scene_id),
            parameter: Some(XfUpMessageParameter::start(avatar_id, vcn)),
            ..Self::default()
        }
    }

    pub fn stop(app_id: &str) -> Self {
        Self {
            header: XfUpMessageHeader::stop(app_id),
            ..Self::default()
        }
    }
    fn text_interact(
        app_id: &str,
        scene_id: &str,
        vcn: &str,
        content: &str,
        avatar_dispatch: Option<XfUpMessageParameterAvatarDispatch>,
    ) -> Self {
        Self {
            header: XfUpMessageHeader::text_interact(app_id, scene_id),
            parameter: Some(XfUpMessageParameter::text_interact(vcn, avatar_dispatch)),
            payload: Some(XfUpMessagePayload::text(content)),
        }
    }
    fn text_driver(
        app_id: &str,
        scene_id: &str,
        vcn: &str,
        content: &str,
        avatar_dispatch: Option<XfUpMessageParameterAvatarDispatch>,
    ) -> Self {
        Self {
            header: XfUpMessageHeader::text_driver(app_id, scene_id),
            parameter: Some(XfUpMessageParameter::text_driver(vcn, avatar_dispatch)),
            payload: Some(XfUpMessagePayload::text(content)),
        }
    }
    pub fn audio_interact(
        app_id: &str,
        scene_id: &str,
        encoding: &Option<String>,
        sample_rate: &Option<i32>,
        channels: &Option<i32>,
        bit_depth: &Option<i32>,
        data: Bytes,
    ) -> Vec<Self> {
        let request_id = Uuid::new_v4();
        let chunks = utils::chunks(&data, *VIT_OPT_CHUNK);
        let chunk_size = chunks.len();
        chunks
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                let status = if i == 0 {
                    XfUpMessagePayloadAudioStatus::Start
                } else if i + 1 == chunk_size {
                    XfUpMessagePayloadAudioStatus::End
                } else {
                    XfUpMessagePayloadAudioStatus::Work
                };
                Self {
                    header: XfUpMessageHeader::audio_interact(request_id, app_id, scene_id),
                    parameter: Some(XfUpMessageParameter::audio_interact()),
                    payload: Some(XfUpMessagePayload::audio(
                        encoding,
                        sample_rate,
                        channels,
                        bit_depth,
                        status,
                        i as i32,
                        v,
                    )),
                }
            })
            .collect()
    }
}

#[derive(Serialize, Clone, Debug, Default)]
struct XfUpMessageHeader {
    app_id: String,
    request_id: String,
    ctrl: XfUpMessageHeaderCtl,

    scene_id: Option<String>,
    scene_version: Option<String>,

    session: Option<String>,
    uid: Option<String>,
}

impl XfUpMessageHeader {
    pub fn ping(app_id: &str) -> Self {
        Self {
            request_id: Uuid::new_v4().to_string(),
            app_id: app_id.to_owned(),
            ctrl: XfUpMessageHeaderCtl::Ping,
            ..Self::default()
        }
    }

    pub fn start(app_id: &str, scene_id: &str) -> Self {
        Self {
            request_id: Uuid::new_v4().to_string(),
            app_id: app_id.to_owned(),
            ctrl: XfUpMessageHeaderCtl::Start,

            scene_id: Some(scene_id.to_owned()),
            scene_version: Some("".to_owned()),
            ..Self::default()
        }
    }

    pub fn stop(app_id: &str) -> Self {
        Self {
            request_id: Uuid::new_v4().to_string(),
            app_id: app_id.to_owned(),
            ctrl: XfUpMessageHeaderCtl::Stop,

            ..Self::default()
        }
    }

    pub fn text_interact(app_id: &str, scene_id: &str) -> Self {
        Self {
            request_id: Uuid::new_v4().to_string(),
            app_id: app_id.to_owned(),
            ctrl: XfUpMessageHeaderCtl::TextInteract,

            scene_id: Some(scene_id.to_owned()),
            scene_version: Some("".to_owned()),

            session: Some("".to_owned()),
            uid: Some("".to_owned()),
        }
    }

    pub fn text_driver(app_id: &str, scene_id: &str) -> Self {
        Self {
            request_id: Uuid::new_v4().to_string(),
            app_id: app_id.to_owned(),
            ctrl: XfUpMessageHeaderCtl::TextDriver,

            scene_id: Some(scene_id.to_owned()),
            scene_version: Some("".to_owned()),

            session: Some("".to_owned()),
            uid: Some("".to_owned()),
        }
    }

    pub fn audio_interact(request_id: Uuid, app_id: &str, scene_id: &str) -> Self {
        Self {
            request_id: request_id.to_string(),
            app_id: app_id.to_owned(),
            ctrl: XfUpMessageHeaderCtl::AudioInteract,

            scene_id: Some(scene_id.to_owned()),
            scene_version: Some("".to_owned()),

            session: Some("".to_owned()),
            uid: Some("".to_owned()),
        }
    }
}

#[derive(Serialize, Clone, Debug, Default)]
struct XfUpMessageParameter {
    avatar_dispatch: Option<XfUpMessageParameterAvatarDispatch>,
    avatar: Option<XfUpMessageParameterAvatar>,
    tts: Option<XfUpMessageParameterTts>,
    air: Option<XfUpMessageParameterAir>,
    asr: Option<XfUpMessageParameterAsr>,
}

impl XfUpMessageParameter {
    pub fn start(avatar_id: &str, vcn: &str) -> Self {
        Self {
            avatar_dispatch: None,
            avatar: Some(XfUpMessageParameterAvatar::default(avatar_id)),
            tts: Some(XfUpMessageParameterTts::default(vcn)),
            air: Some(XfUpMessageParameterAir::default()),
            asr: None,
        }
    }
    pub fn text_interact(
        vcn: &str,
        avatar_dispatch: Option<XfUpMessageParameterAvatarDispatch>,
    ) -> Self {
        Self {
            avatar_dispatch,
            avatar: None,
            tts: Some(XfUpMessageParameterTts::default(vcn)),
            air: Some(XfUpMessageParameterAir::default()),
            asr: None,
        }
    }
    pub fn text_driver(
        vcn: &str,
        avatar_dispatch: Option<XfUpMessageParameterAvatarDispatch>,
    ) -> Self {
        Self {
            avatar_dispatch,
            avatar: None,
            tts: Some(XfUpMessageParameterTts::default(vcn)),
            air: Some(XfUpMessageParameterAir::default()),
            asr: None,
        }
    }
    pub fn audio_interact() -> Self {
        Self {
            avatar_dispatch: None,
            avatar: None,
            tts: None,
            air: None,
            asr: Some(XfUpMessageParameterAsr::new(0)),
        }
    }
}

#[derive(Serialize, Copy, Clone, Debug, Default)]
enum XfUpMessageParameterAvatarDispatch {
    Interrupt = 0,
    #[default]
    Append = 1,
}
impl From<UserMessageDispatch> for XfUpMessageParameterAvatarDispatch {
    fn from(mode: UserMessageDispatch) -> Self {
        match mode {
            UserMessageDispatch::Interrupt => Self::Interrupt,
            UserMessageDispatch::Append => Self::Append,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
struct XfUpMessageParameterAvatar {
    stream: XfUpMessageParameterAvatarStream,
    avatar_id: String,
    width: Option<i32>,
    height: Option<i32>,
    audio_format: Option<i32>,
    mask_region: Option<Vec<i32>>,
    scale: Option<i32>,
    move_h: Option<i32>,
    move_v: Option<i32>,
}
impl XfUpMessageParameterAvatar {
    pub fn default(avatar_id: &str) -> Self {
        Self {
            stream: XfUpMessageParameterAvatarStream::default(),
            avatar_id: avatar_id.to_owned(),
            width: Some(720),
            height: Some(1280),
            audio_format: Some(1),
            mask_region: Some(vec![0, 0, 1080, 1920]),
            scale: Some(1),
            move_h: Some(0),
            move_v: Some(0),
        }
    }
}

#[derive(Serialize, Debug, Clone)]
struct XfUpMessageParameterAvatarStream {
    protocol: XfUpMessageParameterAvatarStreamProtocol,
    fps: Option<i32>,
    alpha: Option<i32>,
    bitrate: Option<u32>,
}
impl XfUpMessageParameterAvatarStream {
    pub fn default() -> Self {
        Self {
            fps: Some(25),
            alpha: Some(0),
            bitrate: Some(2000),
            protocol: XfUpMessageParameterAvatarStreamProtocol::Webrtc,
        }
    }
}

#[derive(Serialize, Copy, Clone, Debug, Default)]
enum XfUpMessageParameterAvatarStreamProtocol {
    #[serde(rename = "webrtc")]
    #[default]
    Webrtc,
}

#[derive(Serialize, Clone, Debug, Default)]
struct XfUpMessageParameterTts {
    vcn: String,
    speed: i32,
    pitch: i32,
    volume: i32,
    // audio: XfUpMessageParameterTtsSampleRate,
}

impl XfUpMessageParameterTts {
    pub fn default(vcn: &str) -> Self {
        Self {
            vcn: vcn.to_owned(),
            speed: 50,
            pitch: 50,
            volume: 100,
        }
    }
}

// #[derive(Serialize)]
// struct XfUpMessageParameterTtsSampleRate {
//     sample_rate: i32,
// }

#[derive(Serialize, Clone, Debug, Default)]
struct XfUpMessageParameterAir {
    air: i32,
    add_nonsemantic: i32,
}

impl XfUpMessageParameterAir {
    pub fn default() -> Self {
        Self {
            air: 0,
            add_nonsemantic: 0,
        }
    }
}

#[derive(Serialize, Copy, Clone, Debug, Default)]
struct XfUpMessageParameterAsr {
    full_duplex: i32,
}

impl XfUpMessageParameterAsr {
    fn new(full_duplex: i32) -> Self {
        Self { full_duplex }
    }
}

#[derive(Serialize, Copy, Clone, Debug, Default)]
enum XfUpMessageHeaderCtl {
    #[serde(rename = "audio_interact")]
    AudioInteract,
    #[serde(rename = "text_interact")]
    TextInteract,
    #[serde(rename = "text_driver")]
    TextDriver,
    #[serde(rename = "ping")]
    #[default]
    Ping,
    #[serde(rename = "start")]
    Start,
    #[serde(rename = "stop")]
    Stop,
}

#[derive(Serialize, Debug, Clone)]
enum XfUpMessagePayload {
    #[serde(rename = "text")]
    Text { content: String },
    #[serde(rename = "audio")]
    Audio {
        encoding: Option<String>,
        sample_rate: Option<i32>,
        channels: Option<i32>,
        bit_depth: Option<i32>,
        status: XfUpMessagePayloadAudioStatus,
        seq: Option<i32>,
        frame_size: Option<i32>,
        audio: String,
    },
}
impl XfUpMessagePayload {
    pub fn text(text: &str) -> Self {
        Self::Text {
            content: text.to_owned(),
        }
    }
    pub fn audio(
        encoding: &Option<String>,
        sample_rate: &Option<i32>,
        channels: &Option<i32>,
        bit_depth: &Option<i32>,
        status: XfUpMessagePayloadAudioStatus,
        seq: i32,
        data: &[u8],
    ) -> Self {
        Self::Audio {
            encoding: None,
            sample_rate: sample_rate.clone(),
            channels: channels.clone(),
            bit_depth: bit_depth.clone(),
            status,
            seq: Some(seq),
            frame_size: Some(data.len() as i32),
            audio: base64::encode(data),
        }
    }
}

#[derive(Serialize_repr, Deserialize_repr, Clone, Copy, Debug, PartialEq)]
#[repr(i32)]
pub enum XfUpMessagePayloadAudioStatus {
    Start = 0,
    Work = 1,
    End = 2,
}

#[derive(Deserialize, Clone)]
pub struct XfDownMessage {
    header: XfDownMessageHeader,
    payload: XfDownMessagePayload,
}

#[derive(Deserialize, Clone)]
enum XfDownMessagePayload {
    #[serde(rename = "avatar")]
    Avatar {
        error_message: Option<Cow<'static, str>>,
        period: Cow<'static, str>,
        event_type: XfDownMessagePayloadEventType,
        error_code: i32,
        request_id: Cow<'static, str>,
        stream_url: Option<Cow<'static, str>>,
        cid: Option<Cow<'static, str>>,

        frame_number: Option<u32>,

        tts_duration: Option<u32>,

        vmr_status: Option<i32>,
    },
    #[serde(rename = "nlp")]
    Nlp {
        index: u32,
        answer: NlpAnswer,
        service: Cow<'static, str>,
        #[serde(rename = "ttsAnswer")]
        tts_answer: NlpTtsAnswer,

        error_code: i32,
        text: Cow<'static, str>,
        stream_nlp: bool,

        request_id: Cow<'static, str>,
        status: Option<i32>,
        cid: Option<Cow<'static, str>>,
    },
}

#[derive(Deserialize, Clone)]
enum XfDownMessagePayloadEventType {
    #[serde(rename = "stream_info")]
    StreamInfo,
    #[serde(rename = "stream_start")]
    StreamStart,
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "tts_duration")]
    TTSDuration,
    #[serde(rename = "driver_status")]
    DriverStatus,
}
#[derive(Deserialize, Clone)]
struct NlpAnswer {
    text: Cow<'static, str>,
}
#[derive(Deserialize, Clone)]
struct NlpTtsAnswer {
    text: Cow<'static, str>,
}

#[derive(Deserialize, Clone)]
struct XfDownMessageHeader {
    code: i32,
    session: Option<Cow<'static, str>>,
    message: Cow<'static, str>,
    sid: Cow<'static, str>,
    status: i32,
}

#[tokio::test]
async fn test_vitman() {
    dotenv::dotenv().ok();
    // env_logger::init();
    log4rs::init_file(&*utils::LOGGER, Default::default()).unwrap();
    let api_data = web::Data::new(WebrtcAPI::new());
    let http_client_builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(true);
    let http_client = http_client_builder.build().unwrap();
    let vit = VitSession::new(&api_data, &http_client).await.unwrap();
    tokio::time::sleep(Duration::from_secs(20)).await;
    drop(vit);
    tokio::time::sleep(Duration::from_secs(10)).await;
}

#[test]
fn test_signature() {
    let vit_host = "avatar.cn-huadong-1.xf-yun.com";
    let datetime = "Fri, 21 Nov 2025 00:52:02 GMT";
    let vit_path = "/v1/interact";
    let signature_origin = format!("host: {vit_host}\ndate: {datetime}\nGET {vit_path} HTTP/1.1");
    let signature_sha = hmac_sha256(
        "NzBjZDBhZGUzM2UwM2EzNmQ5ZTVhYzIy".as_bytes(),
        signature_origin.as_bytes(),
    );
    let signature_base64 = base64::encode(signature_sha);
    println!("signature base64 {signature_base64:?}");
    println!(
        "correct signature {:?}",
        "xgjEkC7kU/fnDgUlzPYpFbnqQsCOVUoS57UN3pmR/9g="
    );
}
