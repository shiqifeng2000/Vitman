use crate::{
    errors::VCError,
    process::{
        demec::OpusQ,
        jitter::{self, JitterBuffer},
    },
    tokio_read_lock,
    utils::{MAX_SESSION_TIME, RTP_TIMEOUT, USER_RTCP_INTERVAL, WebrtcAPI},
};
use actix_web::web;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::{Serialize, Serializer, ser::SerializeStruct};
use std::{
    borrow::Cow,
    sync::{Arc, Weak},
    time::{Duration, Instant},
    u32,
};
use tokio::sync::{
    Notify, RwLock,
    broadcast::{self, error::RecvError},
    mpsc,
};
use uuid::Uuid;
use webrtc::{
    data_channel::{RTCDataChannel, data_channel_message::DataChannelMessage},
    ice_transport::{
        ice_candidate::RTCIceCandidateInit, ice_candidate_pair::RTCIceCandidatePair,
        ice_connection_state::RTCIceConnectionState, ice_server::RTCIceServer,
    },
    peer_connection::{
        RTCPeerConnection, configuration::RTCConfiguration,
        sdp::session_description::RTCSessionDescription,
    },
    rtcp::transport_feedbacks::transport_layer_nack::TransportLayerNack,
    rtp::packet::Packet,
    rtp_transceiver::{
        RTCRtpTransceiver, rtp_codec::RTCRtpCodecCapability, rtp_receiver::RTCRtpReceiver,
    },
    track::{track_local::track_local_static_rtp::TrackLocalStaticRTP, track_remote::TrackRemote},
};
use webrtc::{
    rtp_transceiver::{
        RTCRtpTransceiverInit, rtp_codec::RTPCodecType,
        rtp_transceiver_direction::RTCRtpTransceiverDirection,
    },
    track::track_local::{TrackLocal, TrackLocalWriter},
};

use crate::{
    roles::vitman::VitSession,
    tokio_rcv_lock,
    utils::{self, PEER_STUN_ADDRS, weak_peer_closed},
};

#[derive(Clone)]
pub struct UserRtcSession {
    pub uid: Arc<u32>,
    pub target: Weak<Uuid>,
    pub connection: Arc<RTCPeerConnection>,
}

impl UserRtcSession {
    pub async fn new(
        offer_str: &str,
        candidates: Option<Vec<Option<RTCIceCandidateInit>>>,
        target: &VitSession,
        api: &web::Data<WebrtcAPI>,
    ) -> Result<(Self, String)> {
        let uid0 = rand::random::<u32>();
        let uid = Arc::new(uid0);
        let offer_sdp = utils::atob(offer_str)?;
        let offer = serde_json::from_str::<RTCSessionDescription>(&offer_sdp)?;
        utils::check_sdp(&offer.sdp)?;
        let stuns = PEER_STUN_ADDRS
            .split(",")
            .filter(|v| *v != "")
            .map(|v| v.to_owned())
            .collect::<Vec<String>>();
        let ice_servers = if stuns.len() > 0 {
            vec![RTCIceServer {
                urls: stuns
                    .iter()
                    .map(|v| format!("stun:{}", v))
                    .collect::<Vec<String>>(),
                ..Default::default()
            }]
        } else {
            vec![]
        };
        let conf = RTCConfiguration {
            ice_servers,
            ..Default::default()
        };
        let connection = Arc::new(api.user_api.new_peer_connection(conf).await?);
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
        // connection
        //     .add_transceiver_from_kind(
        //         RTPCodecType::Video,
        //         Some(RTCRtpTransceiverInit {
        //             direction: RTCRtpTransceiverDirection::Sendonly,
        //             send_encodings: vec![],
        //         }),
        //     )
        //     .await?;
        // video rtcp线程

        let (target_video_info, target_audio_info) = Self::get_target_infos(&target).await?;
        log::debug!(target:"debug", "rtp info {target_video_info:?} {target_video_info:?}");
        let local_video_track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: target_video_info.mime.to_string(),
                ..Default::default()
            },
            "video".to_owned(),
            "webrtc-rs".to_owned(),
        ));

        let video_rtcp_reader = connection
            .add_track(Arc::clone(&local_video_track) as Arc<dyn TrackLocal + Send + Sync>)
            .await?;

        let local_audio_track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: target_audio_info.mime.to_string(),
                ..Default::default()
            },
            "audio".to_owned(),
            "webrtc-rs".to_owned(),
        ));
        let audio_rtcp_reader = connection
            .add_track(Arc::clone(&local_audio_track) as Arc<dyn TrackLocal + Send + Sync>)
            .await?;
        // 这里需要获得端侧的payload type以实现payload代理
        // video rtp线程
        let (vpsdr, mut vprcv) = mpsc::channel(1);
        let local_video_track1 = local_video_track.clone();
        let connection1 = Arc::downgrade(&connection);
        let mut vrcv = target.rtc_vsdr.subscribe();
        tokio::spawn(async move {
            // use webrtc::media::io::Writer;
            let vpayload = if let Ok(Some(v)) = tokio_rcv_lock!(vprcv, *RTP_TIMEOUT) {
                v
            } else {
                log::error!("video payload type error");
                return;
            };
            // let mut h264_writer = H264Writer::new(File::create("./test.264").unwrap());
            let mut check_time = Instant::now();
            // let mut jitter = JitterBuffer::<Packet, 20>::new();

            // let mut last_timestamp = 0;
            'outer: loop {
                let t = tokio::time::sleep(Duration::from_millis(*RTP_TIMEOUT));
                tokio::pin!(t);
                tokio::select! {
                    _ = t.as_mut() => {
                        if weak_peer_closed(&connection1) {
                            log::debug!("[S->C] addr {uid0} video listen break from close");
                            break;
                        }
                    }
                    m = vrcv.recv() => {
                        let now = Instant::now();
                        if now.duration_since(check_time).as_millis() > *RTP_TIMEOUT as u128 {
                            if weak_peer_closed(&connection1) {
                                log::debug!("[S->C] addr {uid0} video listen timeout from close");
                                break;
                            }
                            check_time = now;
                        }
                        if let Ok(mut rtp) = m {
                            rtp.header.payload_type = vpayload;
                            let result = local_video_track1.write_rtp(&rtp).await;
                            // log::debug!(target:"debug", "video {result:?} {vpayload}");
                            if let Err(_err) = &result {
                                if weak_peer_closed(&connection1) {
                                    log::debug!("[S->C] addr {uid0} video listen err from close");
                                    break 'outer;
                                }
                            }
                            // log::debug!(target:"debug", "[S->C] video pushing {}", rtp.header.sequence_number, );
                            // jitter.push(rtp);
                            // 'inner: loop {
                            //     if jitter.peek().is_some() {
                            //         if let Some(pkt) = jitter.pop() {
                            //             // log::debug!(target:"debug", "[S->C] video pop {} {}", pkt.header.sequence_number, pkt.header.sequence_number == last_timestamp);
                            //             // last_timestamp = pkt.header.sequence_number.wrapping_add(1);
                            //             let result = local_video_track1.write_rtp(&pkt).await;
                            //             // log::debug!(target:"debug", "video {result:?} {vpayload}");
                            //             if let Err(_err) = &result {
                            //                 if weak_peer_closed(&connection1) {
                            //                     log::debug!("[S->C] addr {uid0} video listen err from close");
                            //                     break 'outer;
                            //                 }
                            //             }
                            //             continue;
                            //         }
                            //     }
                            //     break 'inner;
                            // }
                        } else if let Err(RecvError::Closed) = m {
                            log::debug!("[S->C] addr {uid0} video listen err");
                            break;
                        }
                    }
                }
            }
            log::info!("[S->C] Quitting user {uid0} video rtp thread");
        });

        let connection1 = Arc::downgrade(&connection);
        tokio::spawn(async move {
            let mut rtcp_buf = vec![0u8; 1500];
            let mut hb = Instant::now();
            loop {
                let timeout = tokio::time::sleep(Duration::from_millis(*RTP_TIMEOUT));
                tokio::pin!(timeout);
                tokio::select! {
                    _ = timeout.as_mut() => {
                        if weak_peer_closed(&connection1){
                            break;
                        }
                        if Instant::now().duration_since(hb).as_millis() > *RTP_TIMEOUT as u128 {
                            break;
                        }
                    }
                    p = video_rtcp_reader.read(&mut rtcp_buf)=> {
                        if p.is_ok() {
                            hb = Instant::now();
                        } else {
                            if weak_peer_closed(&connection1){
                                break;
                            }
                            if Instant::now().duration_since(hb).as_millis() > *RTP_TIMEOUT as u128 {
                                log::debug!("[S->C] addr {uid0} video timeout");
                                break;
                            }
                        }
                    }
                }
            }
            // if let Some(conn) = connection1.upgrade() {
            //     let _ = conn.close().await;
            // }
            log::info!("[S->C] Quitting user {uid0} video rtcp");
        });

        // audio rtp线程
        let local_audio_track1 = local_audio_track.clone();
        let connection1 = Arc::downgrade(&connection);
        let mut arcv = target.rtc_asdr.subscribe();
        let (apsdr, mut aprcv) = mpsc::channel(1);
        tokio::spawn(async move {
            // use webrtc::media::io::Writer;
            let apayload = if let Ok(Some(v)) = tokio_rcv_lock!(aprcv, *RTP_TIMEOUT) {
                v
            } else {
                log::error!("audio payload type error");
                return;
            };
            // let mut expected_seq = 0;
            // let mut writer =
            //     OggWriter::new(std::fs::File::create("./test.ogg").unwrap(), 48000, 2).unwrap();
            // let result = writer.write_rtp(&valid_packet);
            let mut check_time = Instant::now();
            // let mut jitter = JitterBuffer::<Packet, 10>::new();
            'outer: loop {
                let t = tokio::time::sleep(Duration::from_millis(*RTP_TIMEOUT));
                tokio::pin!(t);
                tokio::select! {
                    _ = t.as_mut() => {
                        if weak_peer_closed(&connection1) {
                            log::debug!("[S->C] addr {uid0} audio listen break from close");
                            break;
                        }
                    }
                    m = arcv.recv() => {
                        let now = Instant::now();
                        if now.duration_since(check_time).as_millis() > *RTP_TIMEOUT as u128 {
                            if weak_peer_closed(&connection1) {
                                log::debug!("[S->C] addr {uid0} audio listen timeout from close");
                                break;
                            }
                            check_time = now;
                        }
                        if let Ok(mut rtp) = m {
                            rtp.header.payload_type = apayload;
                            let result = local_audio_track1.write_rtp(&rtp).await;
                            if let Err(_err) = &result {
                                if weak_peer_closed(&connection1) {
                                    log::debug!("[S->C] addr {uid0} audio listen err from close");
                                    break 'outer;
                                }
                            }
                            // log::debug!(target:"debug", "audio push {}", rtp.header.sequence_number);
                            // jitter.push(rtp);
                            // 'inner: loop {
                            //     if jitter.peek().is_some() {
                            //         if let Some(pkt) = jitter.pop() {
                            //             // writer.write_rtp(&pkt);
                            //             // log::debug!(target:"debug", "audio pop {} expected_seq {expected_seq}", pkt.header.sequence_number);
                            //             // expected_seq = pkt.header.sequence_number.wrapping_add(1);
                            //             let result = local_audio_track1.write_rtp(&pkt).await;
                            //             if let Err(_err) = &result {
                            //                 if weak_peer_closed(&connection1) {
                            //                     log::debug!("[S->C] addr {uid0} audio listen err from close");
                            //                     break 'outer;
                            //                 }
                            //             }
                            //             continue;
                            //         }
                            //     }
                            //     break 'inner;
                            // }
                        } else if let Err(RecvError::Closed) = m {
                            log::debug!("[S->C] addr {uid0} audio listen err");
                            break;
                        }
                    }
                }
            }
            log::info!("[S->C] Quitting user {uid0} audio rtp thread");
        });
        // audio rtcp线程
        let connection1 = Arc::downgrade(&connection);
        tokio::spawn(async move {
            let mut rtcp_buf = vec![0u8; 1500];
            let mut hb = Instant::now();
            loop {
                let timeout = tokio::time::sleep(Duration::from_millis(*RTP_TIMEOUT));
                tokio::pin!(timeout);
                tokio::select! {
                    _ = timeout.as_mut() => {
                        if weak_peer_closed(&connection1){
                            break;
                        }
                        if Instant::now().duration_since(hb).as_millis() > *RTP_TIMEOUT as u128 {
                            break;
                        }
                    }
                    p = audio_rtcp_reader.read(&mut rtcp_buf)=> {
                        if p.is_ok() {
                            hb = Instant::now();
                        } else {
                            if weak_peer_closed(&connection1){
                                break;
                            }
                            if Instant::now().duration_since(hb).as_millis() > *RTP_TIMEOUT as u128{
                                break;
                            }
                        }
                    }
                }
            }
            log::info!("[S->C] Quitting user {uid0} audio rtcp");
        });

        let apid: Arc<RwLock<u8>> = Arc::new(RwLock::new(0));
        let up_messager = target.up_messager.downgrade();
        let uid1 = Arc::downgrade(&uid);
        let connection1 = Arc::downgrade(&connection);
        let apid1 = apid.clone();
        let mut apid0 = apid.write().await;
        connection.on_track(Box::new(
            move |track: Arc<TrackRemote>,
                  _receiver: Arc<RTCRtpReceiver>,
                  _tranceiver: Arc<RTCRtpTransceiver>| {
                let media_ssrc = track.ssrc();
                let kind = track.kind();
                let up_messager1 = up_messager.clone();
                let uid2 = uid1.clone();
                let apid2 = apid1.clone();
                if kind == RTPCodecType::Audio {
                    // TODO 音频可能不需要显示的统计数据，待定
                    // let connection2 = connection1.clone();
                    // tokio::spawn(async move {
                    //     let mut status = OpusQStatstics::new(uid0, media_ssrc);
                    //     status
                    //         .process_loop(uid2, rtp_entry_recv, &connection2)
                    //         .await;
                    // });
                    let connection3 = connection1.clone();
                    tokio::spawn(async move {
                        Self::process_user_audio_rtp(
                            uid2,
                            media_ssrc,
                            track,
                            connection3,
                            apid2,
                            &up_messager1,
                        )
                        .await;
                    });
                }
                Box::pin(async {})
            },
        ));

        let vit_up_messager = target.up_messager.downgrade();
        connection.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            log::info!("[S->C] Session {uid0} data_channel open");
            let vit_up_messager1 = vit_up_messager.clone();
            dc.on_message(Box::new(move |msg: DataChannelMessage| {
                let data = String::from_utf8_lossy(&msg.data).to_string();
                log::debug!(target:"debug","[S->C] recevied {data}");
                if let Some(m) = vit_up_messager1.upgrade() {
                    let _ = m.send(UserMessage::text(&data));
                }
                Box::pin(async {})
            }));
            Box::pin(async move {})
        }));

        let user_notifier = target.user_notifier.downgrade();
        let notify1 = notify.clone();
        let uid1 = Arc::downgrade(&uid);
        connection.on_ice_connection_state_change(Box::new(
            move |connection_state: RTCIceConnectionState| {
                log::info!("[S->C] Session {uid0} state changed {connection_state}",);
                if connection_state == RTCIceConnectionState::Connected {
                    let user_notifier1 = user_notifier.clone();
                    let uid2 = uid1.clone();
                    Box::pin(async move {
                        if let Some(un) = user_notifier1.upgrade() {
                            let _ = un.send(UserRegisterEvent::new(uid2, true));
                        }
                    })
                } else if connection_state == RTCIceConnectionState::Disconnected {
                    Box::pin(async {})
                } else if connection_state == RTCIceConnectionState::Closed
                    || connection_state == RTCIceConnectionState::Failed
                {
                    let user_notifier1 = user_notifier.clone();
                    notify1.notify_waiters();
                    let uid2 = uid1.clone();
                    Box::pin(async move {
                        if let Some(un) = user_notifier1.upgrade() {
                            let _ = un.send(UserRegisterEvent::new(uid2, false));
                        }
                    })
                } else {
                    Box::pin(async move {})
                }
            },
        ));
        let connection1 = connection.clone();
        tokio::spawn(async move {
            let timeout = tokio::time::sleep(Duration::from_secs(*MAX_SESSION_TIME));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {}
                _ = notify.notified() => {}
            }
            let closer = connection1.close().await;
            log::info!("[S->C] Quitting user {uid0} peer {closer:?}");
        });

        connection.set_remote_description(offer).await?;
        if let Some(list) = candidates {
            for candidate in list
                .iter()
                .filter(|v| v.is_some())
                .map(|v| v.as_ref().unwrap())
            {
                let _ = connection.add_ice_candidate(candidate.clone()).await;
            }
        }

        let answer = connection.create_answer(None).await?;
        let payload = utils::parse_payloads(&answer.sdp)?;
        let vpayloads = payload
            .get(&target_video_info.mime.to_string())
            .ok_or(vcerr!("video codec mismatch with remote"))?;
        let _ = vpsdr.send(vpayloads[0]).await;
        let apayloads = payload
            .get(&target_audio_info.mime.to_string())
            .ok_or(vcerr!("audio codec mismatch with remote"))?;
        let _ = apsdr.send(apayloads[0]).await;

        // 释放apid琐
        *apid0 = apayloads[0];
        drop(apid0);

        let mut gather_complete = connection.gathering_complete_promise().await;
        connection.set_local_description(answer).await?;
        let _ = gather_complete.recv().await;
        let result = connection
            .local_description()
            .await
            .map(|v| serde_json::to_string(&v).map_err(|e| e.into()))
            .unwrap_or(Err(VCError::new("Error getting server sdp description")))?;

        connection
            .sctp()
            .transport()
            .ice_transport()
            .on_selected_candidate_pair_change(Box::new(move |p: RTCIceCandidatePair| {
                log::info!("[S->C] candidate pair {p}");
                Box::pin(async {})
            }));

        let data = utils::btoa(&result);
        Ok((
            Self {
                uid,
                target: Arc::downgrade(&target.vid),
                connection,
            },
            data,
        ))
    }

    pub async fn get_target_infos(target: &VitSession) -> Result<(RtpInfo, RtpInfo)> {
        let timeout = Instant::now();
        let mut video_info = None;
        let mut audio_info = None;
        loop {
            if timeout.elapsed().as_millis() > *RTP_TIMEOUT as u128 {
                return Err(anyhow!("timeout awaiting target infos"));
            }
            if video_info.is_none() {
                if let Ok(v) = target.rtc_video_info.try_read() {
                    if let Some(s) = v.as_ref() {
                        video_info.replace(s.clone());
                    }
                }
            }
            if audio_info.is_none() {
                if let Ok(v) = target.rtc_audio_info.try_read() {
                    if let Some(s) = v.as_ref() {
                        audio_info.replace(s.clone());
                    }
                }
            }
            if video_info.is_some() && audio_info.is_some() {
                return Ok((video_info.unwrap(), audio_info.unwrap()));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn process_user_audio_rtp(
        uid: Weak<u32>,
        media_ssrc: u32,
        track: Arc<TrackRemote>,
        connection: Weak<RTCPeerConnection>,
        apid: Arc<RwLock<u8>>,
        up_messager: &broadcast::WeakSender<UserMessage>,
    ) {
        // use webrtc::media::io::Writer;
        let uid0 = if let Some(id) = uid.upgrade() {
            *id
        } else {
            log::debug!("[C->S] uid drop for audio read thread");
            return;
        };
        let payload_type = if let Ok(payload_type) = tokio_read_lock!(apid, *RTP_TIMEOUT) {
            *payload_type
        } else {
            log::debug!("[C->S] no apid for {uid0}-audio read thread");
            return;
        };
        let opusq = if let Ok(q) = OpusQ::new(&uid, media_ssrc, 48000, 2, payload_type, up_messager)
        {
            q
        } else {
            log::debug!("[C->S] opusq fail for {uid0}-audio read thread");
            return;
        };
        // let mut force_hb = Instant::now();
        // let mut jitter = JitterBuffer::<Packet, 255>::new();
        // let mut writer =
        //     OggWriter::new(std::fs::File::create("./test.ogg").unwrap(), 48000, 2).unwrap();
        let mut ticker = tokio::time::interval(Duration::from_millis(*USER_RTCP_INTERVAL));
        let reason = loop {
            if uid.upgrade().is_none() {
                break "uid drop".to_owned();
            }
            tokio::select! {
                _ = ticker.tick() => {
                    if let Some(conn) = connection.upgrade() {
                        // // 每隔2秒强制出一个report用于各种情况下保活
                        // let force_report = force_hb.elapsed().as_secs() >= 1;
                        // if let Some(pkt) = self.build_nack_report(false) {
                        //     let _ = conn.write_rtcp(&[Box::new(pkt)]).await;
                        //     if force_report {
                        //         force_hb = Instant::now();
                        //     }
                        // }
                        let _ = conn.write_rtcp(&[Box::new(TransportLayerNack {
                            sender_ssrc: uid0,
                            media_ssrc,
                            nacks: vec![],
                        })])
                        .await;
                    } else {
                        break "rtp close".to_owned();
                    }
                }
                readed = track.read_rtp() => {
                    if let Ok((pkt, _attr)) = readed {
                        if pkt.header.payload_type != payload_type {
                            break "rtp mismatch".to_owned();
                        }
                        // writer.write_rtp(&pkt);
                        opusq.send_pkt(pkt);
                        // jitter.push(pkt);
                        // loop {
                        //     if jitter.peek().is_some() {
                        //         if let Some(pkt) = jitter.pop() {
                        //             opusq.send_pkt(pkt);
                        //         }
                        //     }
                        //     break;
                        // }
                    }
                }
            }
        };
        // opusq
        //     .process_loop(uid, payload_type, &track, &connection)
        //     .await;
        log::info!("[C->S] Quitting user {uid0} audio rtp thread from {reason}");
    }
}

impl Serialize for UserRtcSession {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("UserRtcSession", 2)?;
        s.serialize_field("uid", &self.uid.as_ref().to_string())?;
        s.serialize_field("target", &self.target.upgrade().map(|v| v.to_string()))?;
        s.end()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RtpInfo {
    pub mime: Cow<'static, str>,
    pub media_ssrc: Option<u32>,
    pub payload_type: u8,
}
impl RtpInfo {
    pub fn new(mime: &str, media_ssrc: Option<u32>, payload_type: u8) -> Self {
        Self {
            mime: mime.to_string().into(),
            media_ssrc,
            payload_type,
        }
    }
}
#[derive(Deserialize, Clone)]
pub struct UserMessage {
    pub text: Option<String>,
    pub audio: Option<UserMessageAudio>,
    pub interact_mode: UserMessageInteract,
    pub dispatch_mode: Option<UserMessageDispatch>,
}

impl UserMessage {
    pub fn audio(
        encoding: Option<String>,
        sample_rate: Option<i32>,
        channels: Option<i32>,
        bit_depth: Option<i32>,
        data: Bytes,
    ) -> Self {
        Self {
            text: None,
            audio: Some(UserMessageAudio {
                encoding,
                sample_rate,
                channels,
                bit_depth,
                audio: data,
            }),
            interact_mode: UserMessageInteract::Interact,
            dispatch_mode: Some(UserMessageDispatch::Interrupt),
        }
    }
    pub fn text(text: &str) -> Self {
        Self {
            text: Some(text.to_owned()),
            audio: None,
            interact_mode: UserMessageInteract::Interact,
            dispatch_mode: Some(UserMessageDispatch::Interrupt),
        }
    }
}
#[derive(Deserialize, Copy, Clone)]
pub enum UserMessageInteract {
    Interact = 0,
    Driver = 1,
}
#[derive(Deserialize, Copy, Clone)]
pub enum UserMessageDispatch {
    Interrupt = 0,
    Append = 1,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct UserMessageAudio {
    pub encoding: Option<String>,
    pub sample_rate: Option<i32>,
    pub channels: Option<i32>,
    pub bit_depth: Option<i32>,
    #[serde(skip)]
    pub audio: Bytes,
}

impl UserMessageAudio {
    pub fn new(
        encoding: Option<String>,
        sample_rate: Option<i32>,
        channels: Option<i32>,
        bit_depth: Option<i32>,
        audio: Bytes,
    ) -> Self {
        Self {
            encoding,
            sample_rate,
            channels,
            bit_depth,
            audio,
        }
    }
}
#[derive(Clone)]
pub struct UserRegisterEvent {
    pub uid: Weak<u32>,
    pub up: bool,
}
impl UserRegisterEvent {
    pub fn new(uid: Weak<u32>, up: bool) -> Self {
        Self { uid, up }
    }
}

impl jitter::Packet for Packet {
    #[inline]
    fn sequence_number(&self) -> u16 {
        self.header.sequence_number
    }
}
