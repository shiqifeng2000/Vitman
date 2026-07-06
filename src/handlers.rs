use crate::{
    errors::VCError,
    roles::server::TalkManager,
    utils::{RtcJob, WebrtcAPI},
};
use actix_files::{Files, NamedFile};
use actix_web::{HttpRequest, HttpResponse, Result, get, http, post, web};
use serde_json::json;

#[post("/start")]
pub async fn start_session(
    peer_job: web::Json<RtcJob>,
    manager: web::Data<TalkManager>,
    api: web::Data<WebrtcAPI>,
) -> Result<HttpResponse, VCError> {
    let RtcJob {
        peer,
        candidates,
        target,
        ..
    } = peer_job.0;
    let (vid, answer) = manager.start(&peer, candidates, target, &api).await?;
    Ok(HttpResponse::Ok().json(json!({"success": true, "vid": vid, "data": answer})))
}

#[post("/stop/{uid}")]
pub async fn stop_session(
    uid: web::Path<u32>,
    manager: web::Data<TalkManager>,
) -> Result<HttpResponse, VCError> {
    manager.end(*uid).await?;
    Ok(HttpResponse::Ok().json(json!({"success": true})))
}

#[get("/")]
pub async fn idx_file(req: HttpRequest) -> Result<HttpResponse> {
    let mut res: HttpResponse = NamedFile::open("./static/index.html")?.into_response(&req);
    (&mut res).head_mut().headers_mut().insert(
        http::header::HeaderName::from_static("cache-control"),
        http::header::HeaderValue::from_static("private, no-cache, no-store, must-revalidate"),
    );
    Ok(res)
}

#[get("/sessions")]
pub async fn get_sessions(manager: web::Data<TalkManager>) -> Result<HttpResponse, VCError> {
    Ok(HttpResponse::Ok().json(json!({"success": true, "sessions":manager})))
}

pub fn static_file() -> Files {
    Files::new("/", "./static").use_last_modified(true)
}

pub fn storage_file() -> Files {
    Files::new("/storage", "./storage").use_last_modified(true)
}

pub fn doc_file() -> Files {
    Files::new("/document", "./target/doc").use_last_modified(true)
}
