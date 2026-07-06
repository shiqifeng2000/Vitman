use crate::serde::ser::SerializeStruct;
use crate::utils::{RTP_TIMEOUT, WATCHDOG_INTERVAL};
use crate::{
    roles::{user::UserRtcSession, vitman::VitSession},
    tokio_read_lock, tokio_write_lock,
    utils::{VIT_MAX_SESSIONS, WebrtcAPI, peer_closed},
};
use actix_web::web;
use anyhow::{Result, anyhow};
use serde::{Serialize, Serializer};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use uuid::Uuid;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;

#[derive(Clone)]
pub struct TalkManager {
    pub users: Arc<RwLock<HashMap<u32, UserRtcSession>>>,
    pub vitmans: Arc<RwLock<HashMap<Uuid, VitSession>>>,
    http_client: reqwest::Client,
}

impl TalkManager {
    pub fn new() -> Result<Self> {
        let http_client_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .danger_accept_invalid_certs(true);
        // default_headers()
        let http_client = http_client_builder.build()?;
        let users: Arc<RwLock<HashMap<u32, UserRtcSession>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let vitmans: Arc<RwLock<HashMap<Uuid, VitSession>>> = Arc::new(RwLock::new(HashMap::new()));
        let users1 = users.clone();
        let vitmans1 = vitmans.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(*WATCHDOG_INTERVAL));
            loop {
                if let Ok(mut vitmans) = vitmans1.try_write() {
                    vitmans.retain(|_, v| !peer_closed(&v.rtc_conn));
                }
                if let Ok(mut list) = users1.try_write() {
                    list.retain(|_, v| !peer_closed(&v.connection));
                }
                let _ = ticker.tick().await;
            }
        });
        Ok(Self {
            users,
            vitmans,
            http_client,
        })
    }

    pub async fn start(
        &self,
        offer_sdp: &str,
        candidates: Option<Vec<Option<RTCIceCandidateInit>>>,
        target: Option<Uuid>,
        api: &web::Data<WebrtcAPI>,
    ) -> Result<(String, String)> {
        let vit_session = if let Some(t) = target {
            let vitmans = tokio_read_lock!(self.vitmans, *RTP_TIMEOUT)?;
            vitmans.get(&t).ok_or(anyhow!("no such vit"))?.clone()
        } else {
            let vit_session = VitSession::new(api, &self.http_client).await?;
            let mut vitmans = tokio_write_lock!(self.vitmans, *RTP_TIMEOUT)?;
            if vitmans.len() >= *VIT_MAX_SESSIONS {
                return Err(anyhow!("vit session cap reached"));
            }
            let vid = *vit_session.vid;
            vitmans.insert(vid, vit_session.clone());
            vit_session
        };

        let (user_session, answer) =
            UserRtcSession::new(offer_sdp, candidates, &vit_session, api).await?;
        {
            let uid = *user_session.uid;
            let mut users = tokio_write_lock!(self.users, *RTP_TIMEOUT)?;
            users.insert(uid, user_session);
        }
        Ok((vit_session.vid.to_string(), answer))
    }

    pub async fn end(&self, uid: u32) -> Result<()> {
        let target = {
            let mut users = tokio_write_lock!(self.users, *RTP_TIMEOUT)?;
            users
                .remove(&uid)
                .map(|v| v.target.upgrade().map(|s| *s))
                .unwrap_or(None)
        };
        if let Some(vid) = target {
            let mut vitmans = tokio_write_lock!(self.vitmans, *RTP_TIMEOUT)?;
            vitmans.remove(&vid);
        }
        Ok(())
    }
}

impl Serialize for TalkManager {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("TalkManager", 2)?;
        // s.serialize_field("users", &self.users.try_read().map(|v| v.clone()).ok())?;
        s.serialize_field(
            "vitmans",
            &self
                .vitmans
                .try_read()
                .map(|v| {
                    v.iter()
                        .map(|(s, t)| (s.to_string(), t.clone()))
                        .collect::<HashMap<String, VitSession>>()
                })
                .ok(),
        )?;
        s.end()
    }
}
