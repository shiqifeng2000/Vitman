use crate::errors::VCError;
use crate::process::jitter::JitterBuffer;
use crate::process::vad::{Vad, VadConfig};
use crate::roles::user::UserMessage;
use crate::utils::{OPUS_INTERVAL, VAD_PATH};
use anyhow::Result;
use bytes::Bytes;
// use neteq::{AudioPacket, NetEq, NetEqConfig, RtpHeader, codec::OpusDecoder, neteq::SpeechType};
// use audiopus::MutSignals;
// use audiopus::coder::Decoder as OpusDecoder;
use opus::{
    Application as OpusApp, Channels as OpusChannels, Decoder as OpusDecoder,
    Encoder as OpusEncoder,
};
use rubato::{FastFixedIn, PolynomialDegree, Resampler};
use std::{sync::Weak, time::Duration, u32};
use tokio::sync::{broadcast, mpsc};
use webrtc::rtp::codecs::opus::OpusPacket;
use webrtc::rtp::packet::Packet;
use webrtc::rtp::packetizer::Depacketizer;

// const SAMPLE_RATE_IN: u64 = 48000;
const SAMPLE_RATE_OUT: u32 = 16000;
const CHANNELS_OUT: usize = 1;
const FRAME_SIZE_OUT: usize = 1024;
const BIT_DEPTH_OUT: u8 = 16;
const VAD_WINDOW: i32 = 512;
const VAD_BUF_SECS: f32 = 60f32;

pub struct OpusQ {
    sender_ssrc: Weak<u32>,
    media_ssrc: u32,
    // history: BTreeMap<u16, Packet>,
    // missing: HashSet<u16>,
    // highest_seq: Option<u16>,
    // highest_seq_timestamp: Option<u32>,
    sample_rate: u32,
    channels: u8,
    payload_type: u8,

    // neteq: neteq::NetEq,
    // resampler: SincFixedIn<f32>,
    // encoder: OpusEncoder,

    // out_status: XfUpMessagePayloadAudioStatus,
    // out_sequence: u32,
    // messager: broadcast::WeakSender<UserMessage>,
    pkt_sndr: mpsc::Sender<Packet>,
}
impl OpusQ {
    pub fn new(
        sender_ssrc: &Weak<u32>,
        media_ssrc: u32,
        sample_rate: u32,
        channels: u8,
        payload_type: u8,
        messager: &broadcast::WeakSender<UserMessage>,
    ) -> Result<Self> {
        // let mut neteq = NetEq::new(NetEqConfig {
        //     sample_rate,
        //     channels,
        //     ..Default::default()
        // })?;
        // neteq.register_decoder(
        //     payload_type,
        //     Box::new(OpusDecoder::new(sample_rate, channels)?),
        // );
        // let params = SincInterpolationParameters {
        //     sinc_len: 256,
        //     f_cutoff: 0.95,
        //     interpolation: SincInterpolationType::Linear,
        //     oversampling_factor: 2048,
        //     window: WindowFunction::BlackmanHarris2,
        // };
        let mut jitter = JitterBuffer::<Packet, 20>::new(None);
        let mut depacketier = OpusPacket::default();
        let mut decoder = OpusDecoder::new(
            sample_rate,
            if channels == 2 {
                OpusChannels::Stereo
            } else {
                OpusChannels::Mono
            },
        )?;
        // let mut decoder = OpusDecoder::new(
        //     audiopus::SampleRate::try_from(sample_rate as i32)?,
        //     audiopus::Channels::try_from(channels as i32)?,
        // )?;
        let mut resampler = FastFixedIn::<f32>::new(
            SAMPLE_RATE_OUT as f64 / sample_rate as f64,
            100.0,
            PolynomialDegree::Cubic,
            FRAME_SIZE_OUT,
            1,
        )?;
        let vad_config = VadConfig::new(
            VAD_PATH.clone(),
            0.5,
            0.5,
            0.5,
            SAMPLE_RATE_OUT as i32,
            VAD_WINDOW,
            Some("cpu".to_owned()),
            None,
            None,
        );
        let mut vad = Vad::new_from_config(vad_config, VAD_BUF_SECS)?;
        let mut encoder = OpusEncoder::new(SAMPLE_RATE_OUT, OpusChannels::Mono, OpusApp::Audio)?;
        encoder.set_inband_fec(true)?;

        let sender_ssrc1 = sender_ssrc.clone();
        let (pkt_sndr, mut pkt_rcvr) = mpsc::channel::<Packet>(1000);
        let messager = messager.clone();
        // let connection = connection.clone();

        // std::fs::File::create("./test.pcm").ok();
        std::thread::spawn(move || {
            let mut sender_ssrc0 = u32::MAX;
            // let mut test_pcm = std::fs::File::options()
            //     .append(true)
            //     .open("./test.pcm")
            //     .ok();
            let mut resample_cache = vec![];
            // let mut vad_cache = vec![];
            let reason = loop {
                match sender_ssrc1.upgrade() {
                    Some(id) => {
                        sender_ssrc0 = *id;
                    }
                    None => {
                        break "uid drop".to_owned();
                    }
                }
                match pkt_rcvr.try_recv() {
                    Ok(pkt) => {
                        // log::debug!(target:"debug", "q buffer {}", pkt_rcvr.len());
                        jitter.push(pkt);
                        'inner: loop {
                            if jitter.peek().is_some() {
                                if let Some(package) = jitter.pop() {
                                    // log::debug!(target:"debug", "[S->C] pop package {}", package.header.sequence_number);
                                    let _ = vclog!(Self::process_pkt(
                                        channels,
                                        &mut depacketier,
                                        &mut decoder,
                                        &mut resampler,
                                        &mut resample_cache,
                                        &mut vad,
                                        // &mut vad_cache,
                                        &mut encoder,
                                        package,
                                        &messager,
                                        // &mut test_pcm,
                                    ));
                                    continue 'inner;
                                }
                            }
                            break 'inner;
                        }
                    }
                    Err(e) => {
                        if mpsc::error::TryRecvError::Disconnected == e {
                            break "peer closed".to_owned();
                        } else {
                            std::thread::sleep(Duration::from_millis(*OPUS_INTERVAL));
                        }
                    }
                }
            };
            // if let Some(m) = messager.upgrade() {
            //     let _ = m.send(UserMessage::audio(
            //         Some("raw".to_owned()),
            //         Some(SAMPLE_RATE_OUT as i32),
            //         Some(CHANNELS_OUT as i32),
            //         Some(BIT_DEPTH_OUT as i32),
            //         XfUpMessagePayloadAudioStatus::End,
            //         Some(sequence as i32),
            //         Some(0),
            //         Bytes::new(),
            //     ));
            // }
            log::info!("[C->S] closing {sender_ssrc0}-audio codec thread from {reason}");
        });
        Ok(Self {
            sender_ssrc: sender_ssrc.clone(),
            media_ssrc,
            // history: BTreeMap::new(),
            // missing: HashSet::new(),
            // highest_seq: None,
            // highest_seq_timestamp: None,
            sample_rate,
            channels,
            payload_type,
            // neteq,
            // resampler,
            // encoder,
            // out_status: XfUpMessagePayloadAudioStatus::Start,
            // out_sequence: 0,
            // messager,
            pkt_sndr,
        })
    }
    pub fn send_pkt(&self, pkt: Packet) {
        let _ = vclog!(self.pkt_sndr.try_send(pkt));
    }
    pub fn process_pkt(
        channels: u8,
        depacketier: &mut OpusPacket,
        decoder: &mut OpusDecoder,
        resampler: &mut FastFixedIn<f32>,
        resample_cache: &mut Vec<f32>,
        vad: &mut Vad,
        encoder: &mut OpusEncoder,
        pkt: Packet,
        messager: &broadcast::WeakSender<UserMessage>,
        // test_pcm: &mut Option<std::fs::File>,
    ) -> Result<()> {
        // log::debug!(target:"debug", "[C->S] up_audio seq {} timestamp {}", pkt.header.sequence_number, pkt.header.timestamp);
        let payload = match depacketier.depacketize(&pkt.payload) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        // let input_packet = audiopus::packet::Packet::try_from(payload.iter().as_slice())?;
        let mut pcm_output: Vec<i16> = vec![0; FRAME_SIZE_OUT * channels as usize];
        // let mut_signals = MutSignals::try_from(&mut pcm_output)?;
        // let pcm_output_len = decoder.decode(Some(input_packet), mut_signals, false)?;
        let pcm_output_len = decoder.decode(&payload, &mut pcm_output, false)?;
        // log::debug!(target:"debug", "[C->S] up_audio pcm_output_len {pcm_output_len} cache {}", resample_cache.len());
        let wav_in = pcm_output[..pcm_output_len * channels as usize]
            .chunks_exact(2)
            .map(|frame| frame[0] as f32 / 0x8000 as f32)
            .collect::<Vec<f32>>();
        resample_cache.extend_from_slice(&wav_in);
        // log::debug!(target:"debug", "[C->S] up_audio new cache {}", resample_cache.len());
        while resample_cache.len() >= FRAME_SIZE_OUT {
            let new_cache = resample_cache.split_off(FRAME_SIZE_OUT);
            let mut waves_out: Vec<Vec<f32>> = resampler.process(&vec![&resample_cache], None)?;
            if let Some(wav) = waves_out.pop() {
                vad.accept_waveform(wav);
            }
            if !vad.is_empty() {
                let mut cache = vec![];
                while !vad.is_empty() {
                    let seg = vad.front();
                    cache.extend_from_slice(
                        &seg.samples
                            .iter()
                            .map(|v| (*v * 0x8000 as f32) as i16)
                            .collect::<Vec<i16>>(),
                    );
                    vad.pop();
                }
                // 过滤掉0.5秒钟以下的声音
                if cache.len() >= SAMPLE_RATE_OUT as usize / 2 {
                    if let Some(m) = messager.upgrade() {
                        let cache_data = bytemuck::cast_slice::<i16, u8>(&cache);
                        let _ = m.send(UserMessage::audio(
                            Some("opus-wb".to_owned()),
                            Some(SAMPLE_RATE_OUT as i32),
                            Some(CHANNELS_OUT as i32),
                            Some(BIT_DEPTH_OUT as i32),
                            Bytes::copy_from_slice(cache_data),
                        ));

                        // let timestamp = chrono::Local::now()
                        //     .naive_local()
                        //     .format("%Y%m%d%H%M%S")
                        //     .to_string();
                        // let mut f = std::fs::File::create(format!("./test{timestamp}.pcm",)).unwrap();
                        // f.write_all(cache_data);
                    }
                }
            }
            *resample_cache = new_cache;
        }
        Ok(())
    }
    // pub fn process_decode(decoder: &mut audiopus::coder::Decoder, pkt: Packet) -> Result<()> {}
    // fn build_nack_report(&self, force: bool) -> Option<TransportLayerNack> {
    //     if self.missing.is_empty() && !force {
    //         return None;
    //     }
    //     let mut seqs: Vec<u16> = self.missing.iter().copied().collect();
    //     seqs.sort_unstable();

    //     let mut pairs = Vec::new();

    //     let mut i = 0usize;
    //     while i < seqs.len() {
    //         let pid = seqs[i];
    //         let mut blp: u16 = 0;
    //         let mut j = i + 1;
    //         while j < seqs.len() {
    //             let diff = seqs[j].wrapping_sub(pid) as i32;
    //             if diff >= 1 && diff <= 16 {
    //                 blp |= 1u16 << (diff - 1);
    //                 j += 1;
    //             } else {
    //                 break;
    //             }
    //         }
    //         pairs.push(NackPair {
    //             packet_id: pid,
    //             lost_packets: blp,
    //         });
    //         i = j;
    //     }

    //     Some(TransportLayerNack {
    //         sender_ssrc: self.sender_ssrc,
    //         media_ssrc: self.media_ssrc,
    //         nacks: pairs,
    //     })
    // }

    // fn process_rtp1(&mut self, pkt: Packet) {
    //     // log::debug!(target:"debug","user rtp {:?}", pkt.header);
    //     // let sender_ssrc = self.sender_ssrc;
    //     let seq = pkt.header.sequence_number;
    //     let timestamp = pkt.header.timestamp;
    //     self.history.insert(seq, pkt);

    //     let mut output = None;
    //     if let Some(high) = self.highest_seq {
    //         let next_expected = high.wrapping_add(1);
    //         if seq == next_expected {
    //             if let Some(previous_timestamp) = self.highest_seq_timestamp {
    //                 if let Some(pkt1) = self.history.remove(&high) {
    //                     output.replace(AudioPacket::new(
    //                         to_rtp_header(&pkt1.header),
    //                         pkt1.payload.to_vec(),
    //                         48000,
    //                         2,
    //                         timestamp - previous_timestamp,
    //                     ));
    //                 }
    //             }
    //             self.highest_seq = Some(seq);
    //             self.highest_seq_timestamp = Some(timestamp);
    //             let mut cur = seq;
    //             loop {
    //                 let nxt = cur.wrapping_add(1);
    //                 if self.history.contains_key(&nxt) {
    //                     self.highest_seq = Some(nxt);
    //                     self.missing.remove(&nxt);
    //                     cur = nxt;
    //                 } else {
    //                     break;
    //                 }
    //             }
    //         } else if seq.wrapping_sub(next_expected) < 0x8000 {
    //             let mut s = next_expected;
    //             while s != seq {
    //                 self.missing.insert(s);
    //                 s = s.wrapping_add(1);
    //             }
    //             self.highest_seq = Some(seq);
    //         } else {
    //             self.missing.remove(&seq);
    //         }
    //     } else {
    //         self.highest_seq = Some(seq);
    //     }

    //     while self.history.len() > *USER_RTCP_MAX_HISTORY {
    //         if self.history.pop_first().is_none() {
    //             break;
    //         }
    //     }

    //     // 如果有输出则处理音频
    //     if let Some(pkt) = output {
    //         // 放入jitter队列
    //         let _ = self.neteq.insert_packet(pkt);
    //         // 尝试输出
    //         while let Ok(audio_frame) = self.neteq.get_audio() {
    //             if audio_frame.samples.is_empty()
    //                 || audio_frame.speech_type == SpeechType::Expand
    //                 || !audio_frame.vad_activity
    //             {
    //                 break;
    //             }
    //             if audio_frame
    //                 .samples
    //                 .iter()
    //                 .find(|&s| s.abs() > 0.001)
    //                 .is_none()
    //             {
    //                 continue;
    //             }

    //             let mut waves_in: Vec<Vec<f32>> =
    //                 vec![vec![0f32; FRAME_SIZE_OUT]; self.channels as usize];
    //             for (i, frame) in audio_frame.samples.chunks_exact(2).enumerate() {
    //                 waves_in[0][i] = frame[0];
    //                 waves_in[1][i] = frame[1];
    //             }

    //             // log::debug!(target:"debug", "audio resample in {}", waves_in[0].len());
    //             let a = self.resampler.process(&waves_in, None).map_err(|e| {
    //                 log::debug!(target:"debug", "error {}", e.to_string());
    //                 e
    //             });
    //             if let Ok(wave_out) = a {
    //                 // log::debug!(target:"debug", "audio resample out {}", wave_out[0].len());
    //                 if wave_out.len() >= 1 {
    //                     let wave_out_s16 = wave_out[0]
    //                         .iter()
    //                         .map(|v| (v * 0x8000 as f32) as i16)
    //                         .collect::<Vec<i16>>();
    //                     let wave_out_data = bytemuck::cast_slice::<i16, u8>(&wave_out_s16);
    //                     if let Some(m) = self.messager.upgrade() {
    //                         // let _ = m.send(UserMessage::audio(
    //                         //     Some("raw".to_owned()),
    //                         //     Some(SAMPLE_RATE_OUT as i32),
    //                         //     Some(CHANNELS_OUT as i32),
    //                         //     Some(BIT_DEPTH_OUT as i32),
    //                         //     self.out_status,
    //                         //     Some(self.out_sequence as i32),
    //                         //     Some(wave_out_s16.len() as i32),
    //                         //     Bytes::copy_from_slice(wave_out_data),
    //                         // ));
    //                         self.out_sequence += 1;
    //                         if self.out_status == XfUpMessagePayloadAudioStatus::Start {
    //                             self.out_status = XfUpMessagePayloadAudioStatus::Work;
    //                         }
    //                     }
    //                     // let mut output = vec![0u8; 4000];
    //                     // if let Ok(n) = self.encoder.encode(&wave_out_s16, &mut output) {
    //                     // }
    //                 }
    //             }
    //         }
    //     }
    //     // log::info!("Quitting {sender_ssrc}-Rtcp thread");
    // }
    // pub async fn process_loop(
    //     &mut self,
    //     uid: Weak<u32>,
    //     payload_type: u8,
    //     track: &Arc<TrackRemote>,
    //     connection: &Weak<RTCPeerConnection>,
    // ) -> String {
    //     let mut force_hb = Instant::now();
    //     let mut ticker = tokio::time::interval(Duration::from_secs(*USER_RTCP_INTERVAL));
    //     loop {
    //         if uid.upgrade().is_none() {
    //             break "uid drop".to_owned();
    //         }
    //         tokio::select! {
    //             _ = ticker.tick() => {
    //                 if let Some(conn) = connection.upgrade() {
    //                     // 每隔2秒强制出一个report用于各种情况下保活
    //                     // let force_report = force_hb.elapsed().as_secs() >= 1;
    //                     // if let Some(pkt) = self.build_nack_report(false) {
    //                     //     let _ = conn.write_rtcp(&[Box::new(pkt)]).await;
    //                         // if force_report {
    //                         //     force_hb = Instant::now();
    //                         // }
    //                     // }
    //                     let _ = conn.write_rtcp(&[Box::new(TransportLayerNack {
    //                         sender_ssrc: self.sender_ssrc,
    //                         media_ssrc: self.media_ssrc,
    //                         nacks: vec![],
    //                     })])
    //                     .await;
    //                 } else {
    //                     break "rtp close".to_owned();
    //                 }
    //             }
    //             readed = track.read_rtp() => {
    //                 if let Ok((pkt, _attr)) = readed {
    //                     if pkt.header.payload_type != payload_type {
    //                         break "rtp mismatch".to_owned();
    //                     }
    //                     self.process_rtp(pkt);
    //                 }
    //             }
    //         }
    //     }
    //     // log::info!("[C->S] {uid1}-Rtp thread end from {reason}");
    // }
}

// impl Drop for OpusQ {
//     fn drop(&mut self) {
//         if let Some(m) = self.messager.upgrade() {
//             let _ = m.send(UserMessage::audio(
//                 Some("raw".to_owned()),
//                 Some(SAMPLE_RATE_OUT as i32),
//                 Some(CHANNELS_OUT as i32),
//                 Some(BIT_DEPTH_OUT as i32),
//                 XfUpMessagePayloadAudioStatus::End,
//                 Some(self.out_sequence as i32),
//                 Some(0),
//                 Bytes::new(),
//             ));
//         }
//     }
// }
// fn to_rtp_header(webrtc_rtp_header: &webrtc::rtp::header::Header) -> RtpHeader {
//     RtpHeader {
//         sequence_number: webrtc_rtp_header.sequence_number,
//         timestamp: webrtc_rtp_header.timestamp,
//         ssrc: webrtc_rtp_header.ssrc,
//         payload_type: webrtc_rtp_header.payload_type,
//         marker: webrtc_rtp_header.marker,
//     }
// }
