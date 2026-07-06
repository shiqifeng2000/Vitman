use actix_web::{HttpRequest, HttpResponse, get, rt, web};
use actix_ws::{AggregatedMessage, Session};
use anyhow::{Result, anyhow};
use futures::StreamExt;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Weak;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock, broadcast};
use uuid::Uuid;
// : rand::random::<u32>()

// #[derive(Clone, Default)]
// pub struct JTaskManager {
//     pub tasks: Vec<JTask>,
// }
#[derive(Clone)]
pub struct JSessions {
    pub sessions: Arc<Mutex<HashMap<Uuid, WsSession>>>,
}
impl JSessions {
    pub fn new() -> Self {
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let session1: Arc<Mutex<HashMap<Uuid, WsSession>>> = sessions.clone();
        tokio::spawn(async move {
            let mut ticker: tokio::time::Interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                if let Ok(mut s) = session1.try_lock() {
                    s.retain(|_, v| v.sid.upgrade().is_some());
                }
                ticker.tick().await;
            }
            log::debug!("Quitting session watchdog green thread");
        });
        Self { sessions }
    }
}

// #[derive(Clone)]
// pub struct XTask {
//     pub id: u32,
//     pub starter: WeakAddr<WsSession>,
//     pub manager: WorkManager,
// }

#[derive(Debug, Clone)]
pub struct SessionMessage {
    pub id: Option<Uuid>,
    // pub session_id: Option<Uuid>,
    pub type_: String,
    pub content: Option<SessionMessageContent>,
}

impl Serialize for SessionMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("SessionMessage", 6)?;
        s.serialize_field("id", &self.id.map(|v| v.to_string()))?;
        s.serialize_field("type", &self.type_)?;
        s.serialize_field("content", &self.content)?;
        s.end()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum SessionMessageContent {
    StartTask {
        session_id: Option<String>,
        request: String,
    },
    EndTask {
        session_id: String,
        reason: Option<String>,
    },
    FollowUpRequest {
        followup: String,
    },
    Reconfig {
        workspace: Option<String>,
        lang: Option<String>,
        memory_file: Option<String>,
        reset: Option<bool>,
    },
    ToolApproval {
        tools: Vec<String>,
        approved: bool,
        remember: bool,
        feedback: Option<String>,
    },
    ToolEnable {
        tools: Vec<String>,
        enabled: bool,
    },
    ToolSetState {
        states: Vec<ToolState>,
    },
    PredefinePrompts {
        prompts: Vec<PredefinePrompt>,
    },
    McpGetPrompt {
        mcp_args: String,
    },
}

impl<'de> Deserialize<'de> for SessionMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // 定义访问者
        struct UserVisitor;

        impl<'de> Visitor<'de> for UserVisitor {
            type Value = SessionMessage;
            // 预期解析的数据结构类型（YAML 的 Map）
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a YAML map representing a User")
            }
            // 处理 Map 类型的反序列化
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                // 初始化字段（可设置默认值）
                let mut id = None;
                // let mut session_id = None;
                let mut type_ = None;
                let mut content = None;
                // 遍历 YAML 的键值对
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "id" => {
                            if let Ok(v) = map.next_value::<Option<String>>() {
                                id = v;
                            }
                        }
                        // "sessionId" => {
                        //     if let Ok(v) = map.next_value::<Option<String>>() {
                        //         session_id = v;
                        //     }
                        // }
                        "type" => {
                            if let Ok(v) = map.next_value::<String>() {
                                type_.replace(v);
                            }
                        }
                        "content" => {
                            if let Ok(v) = map.next_value::<SessionMessageContent>() {
                                content.replace(v);
                            }
                        }
                        // 处理未知字段
                        other => {
                            return Err(serde::de::Error::unknown_field(
                                other,
                                &["id", "sessionId", "type", "content"],
                            ));
                        }
                    };
                }
                Ok(SessionMessage {
                    id: id.map(|v| Uuid::parse_str(&v).ok()).unwrap_or(None),
                    // session_id: session_id.map(|v| Uuid::parse_str(&v).ok()).unwrap_or(None),
                    type_: type_.ok_or(serde::de::Error::missing_field("type"))?,
                    content,
                })
            }
        }
        // 触发访问者处理
        deserializer.deserialize_map(UserVisitor)
    }
}

pub async fn handle_ws(
    req: HttpRequest,
    stream: web::Payload,
    query: web::Query<WorkArgs>,
    sessions_data: web::Data<JSessions>,
) -> Result<HttpResponse, JError> {
    let ip = req
        .peer_addr()
        .as_ref()
        .map(|v| v.ip().to_string())
        .unwrap_or("".to_owned());
    let (res, mut context, stream) = actix_ws::handle(&req, stream)?;

    let mut stream = stream
        .aggregate_continuations()
        // aggregate continuation frames up to 1MiB
        .max_continuation_size(2_usize.pow(20));

    let mut client_config = HashMap::new();
    if let Some(t) = &query.token {
        client_config.insert("token".to_owned(), t.clone());
    }
    let mut workspace_path =
        PathBuf::from_str(&query.workspace).unwrap_or(PathBuf::from_str("./").unwrap());
    if workspace_path.is_relative() {
        workspace_path = workspace_path.canonicalize()?;
    }
    let (ws_session, sid) = WsSession::new(
        query
            .id
            .as_ref()
            .map(|v| Uuid::parse_str(v).ok())
            .unwrap_or(None),
        &ip,
        workspace_path.to_str().unwrap_or("./"),
        &query.lang,
        &query.memory,
        client_config,
        query
            .client_meta
            .as_ref()
            .map(|v| serde_json::from_str::<MetaConfig>(v).ok())
            .unwrap_or(None),
        &query.mcp_config,
        &context,
    )?;
    let sid1 = *sid.as_ref();
    let sessions = sessions_data.sessions.clone();
    let mut context_arc = context.clone();
    let ws_session1 = ws_session.clone();
    rt::spawn(async move {
        // 以stream loop为准，如果去掉sid则drop
        let sid2 = *sid.as_ref();
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(AggregatedMessage::Text(text)) => {
                    match serde_json::from_str::<SessionMessage>(&text) {
                        Ok(SessionMessage { id, type_, content }) => {
                            match process_request(&type_, content, &ws_session1).await {
                                Ok(Some(v)) => {
                                    let _ = context_arc
                                        .text(to_text!(SessionEvent::message_response(
                                            id, &type_, v
                                        )))
                                        .await;
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    let _ = context_arc
                                        .text(to_text!(SessionEvent::error(
                                            None,
                                            e.to_string(),
                                            None
                                        )))
                                        .await;
                                }
                            }
                        }
                        Err(_) => {
                            let _ = context_arc.text("SessionMessage parse error").await;
                        }
                    }
                }

                Ok(AggregatedMessage::Binary(_bin)) => {
                    // echo binary message
                    // session.binary(bin).await.unwrap()
                    let _ = context_arc
                        .text(to_text!(SessionEvent::error(
                            None,
                            "binary is yet forbidden".to_owned(),
                            None
                        )))
                        .await;
                }

                Ok(AggregatedMessage::Ping(msg)) => {
                    // respond to PING frame with PONG frame
                    let _ = context_arc.pong(&msg).await;
                }

                _ => {}
            }
        }
        if let Ok(mut ss) = tokio_mutex_lock!(sessions, 10000) {
            ss.remove(&sid2);
        }
        log::debug!("Quitting websocket session {sid2}");
    });

    {
        let mut ss = tokio_mutex_lock!(sessions_data.sessions, 10000)?;
        ss.insert(sid1, ws_session);
    }

    let _ = context
        .text(to_text!(SessionEvent::ready(None, sid1)))
        .await;
    // respond immediately with response connected to WS session
    Ok(res)
}

async fn process_request(
    msg_type: &str,
    content: Option<SessionMessageContent>,
    ws_session: &WsSession,
) -> Result<Option<SessionEventContent>> {
    // let session = tokio_mutex_lock!(session_arc, 1000)?;
    // let session_message = serde_json::from_str::<SessionMessage>(text)?;
    // let msg_type = session_message.type_.as_str();
    match msg_type {
        "start_task" => {
            handle_start_task(content, ws_session).await?;
            return Ok(None);
        }
        "toggle_tool_approval" => {
            handle_approve_tool(content, ws_session).await?;
            return Ok(None);
        }
        "toggle_tool_enable" => {
            handle_enable_tool(content, ws_session).await?;
            return Ok(None);
        }
        "end_task" => {
            handle_end_task(content, ws_session).await?;
            return Ok(None);
        }
        "followup" => {
            handle_followup_task(content, ws_session).await?;
            return Ok(None);
        }
        "setup_predefined_prompts" => {
            handle_setup_predefined_prompts(content, ws_session).await?;
            return Ok(None);
        }
        "reconfig" => {
            handle_reconfig(content, ws_session).await?;
            return Ok(None);
        }
        "list_inner_tools" => {
            let content = handle_list_inner_tools(ws_session).await;
            return Ok(Some(content));
        }
        "mcp_list_tools" => {
            let content = handle_list_mcp_tools(ws_session).await;
            return Ok(Some(content));
        }
        "mcp_list_prompts" => {
            let content = handle_list_mcp_prompts(ws_session).await;
            return Ok(Some(content));
        }
        "mcp_get_prompts" => {
            let content = handle_get_mcp_prompt(content, ws_session).await?;
            return Ok(Some(content));
        }
        "list_tool_states" => {
            let content = handle_list_tool_states(ws_session).await;
            return Ok(Some(content));
        }
        "list_predefined_prompts" => {
            let content = handle_list_predefined_prompts(ws_session).await;
            return Ok(Some(content));
        }
        _ => {}
    }
    Err(anyhow!("No such upstream message type:<{msg_type}> ",))
}

async fn handle_approve_tool(
    content: Option<SessionMessageContent>,
    ws_session: &WsSession,
) -> Result<()> {
    // let SessionMessage { content, .. } = upstream;
    let (tools, approved, remember, feedback) = if let Some(SessionMessageContent::ToolApproval {
        tools,
        approved,
        remember,
        feedback,
    }) = content
    {
        (tools, approved, remember, feedback)
    } else {
        return Err(anyhow!("handle_approve_tool content mismatch"));
    };
    let _ = ws_session
        .approval_notifier
        .send((tools.clone(), approved, feedback));
    if remember {
        ws_session
            .worker
            .toggle_auto_approval_tools(approved, &tools)
            .await;
    }
    Ok(())
}
async fn handle_enable_tool(
    content: Option<SessionMessageContent>,
    ws_session: &WsSession,
) -> Result<()> {
    // let SessionMessage { content, .. } = upstream;
    let (tools, enabled) =
        if let Some(SessionMessageContent::ToolEnable { tools, enabled }) = content {
            (tools, enabled)
        } else {
            return Err(anyhow!("handle_approve_tool content mismatch"));
        };
    ws_session
        .worker
        .toggle_disable_tools(!enabled, &tools)
        .await;
    Ok(())
}
async fn handle_start_task(
    content: Option<SessionMessageContent>,
    ws_session: &WsSession,
) -> Result<()> {
    // let SessionMessage { content, .. } = upstream;
    let (request, session_id) = if let Some(SessionMessageContent::StartTask {
        session_id,
        request,
    }) = content
    {
        (request, session_id)
    } else {
        return Err(anyhow!("start_task content mismatch"));
    };
    let id: Arc<Uuid> = Arc::new(
        session_id
            .map(|v| Uuid::from_str(v.as_str()).unwrap_or(Uuid::new_v4()))
            .unwrap_or(Uuid::new_v4()),
    );
    let worker = ws_session.worker.clone();
    {
        let mut tid = tokio_write_lock!(ws_session.tid, 10000)?;
        tid.replace(id.clone());
    }
    let tid1 = ws_session.tid.clone();
    tokio::task::spawn(async move {
        elogger!(worker.start_task(&Arc::downgrade(&id), &request).await);
        if let Ok(mut tid) = tokio_write_lock!(tid1, 10000) {
            tid.take();
        }
    });
    Ok(())
}
async fn handle_end_task(
    content: Option<SessionMessageContent>,
    ws_session: &WsSession,
) -> Result<()> {
    // let SessionMessage { content, .. } = upstream;
    if let Some(SessionMessageContent::EndTask { session_id, .. }) = content {
        if ws_session
            .sid
            .upgrade()
            .map(|v| v.to_string() == session_id)
            .unwrap_or(false)
        {
            let mut tid = tokio_write_lock!(ws_session.tid, 10000)?;
            tid.take();
        }
    } else {
        return Err(anyhow!("end_task content mismatch"));
    };
    Ok(())
}
async fn handle_followup_task(
    content: Option<SessionMessageContent>,
    ws_session: &WsSession,
) -> Result<()> {
    // let SessionMessage { content, .. } = upstream;
    let followup = if let Some(SessionMessageContent::FollowUpRequest { followup }) = content {
        let mut tid = tokio_write_lock!(ws_session.tid, 10000)?;
        tid.take();
        followup
    } else {
        return Err(anyhow!("handle_followup_task content mismatch"));
    };
    let id: Arc<Uuid> = Arc::new(Uuid::new_v4());
    let worker = ws_session.worker.clone();
    {
        let mut tid = tokio_write_lock!(ws_session.tid, 10000)?;
        tid.replace(id.clone());
    }
    tokio::task::spawn(async move {
        elogger!(worker.start_task(&Arc::downgrade(&id), &followup).await);
    });
    Ok(())
}
async fn handle_reconfig(
    content: Option<SessionMessageContent>,
    ws_session: &WsSession,
) -> Result<()> {
    // let SessionMessage { content, .. } = &upstream;
    if let Some(SessionMessageContent::Reconfig {
        workspace,
        lang,
        memory_file,
        reset,
    }) = &content
    {
        ws_session
            .worker
            .reconfig(workspace, lang, memory_file, reset.unwrap_or(false))
            .await;
    } else {
        return Err(anyhow!("handle_reconfig content mismatch"));
    };
    handle_start_task(content, ws_session).await
}
async fn handle_list_inner_tools(ws_session: &WsSession) -> SessionEventContent {
    SessionEventContent::InnerAgentTools(ws_session.worker.list_inner_tools())
}
async fn handle_list_mcp_tools(ws_session: &WsSession) -> SessionEventContent {
    SessionEventContent::McpAgentTools(ws_session.worker.list_mcp_tools().await)
}
async fn handle_list_mcp_prompts(ws_session: &WsSession) -> SessionEventContent {
    SessionEventContent::McpPrompts(ws_session.worker.list_mcp_prompts().await)
}
async fn handle_get_mcp_prompt(
    content: Option<SessionMessageContent>,
    ws_session: &WsSession,
) -> Result<SessionEventContent> {
    // let SessionMessage { content, .. } = upstream;
    let mcp_args = if let Some(SessionMessageContent::McpGetPrompt { mcp_args, .. }) = content {
        mcp_args
    } else {
        return Err(anyhow!("handle_get_mcp_prompt content mismatch"));
    };

    ws_session
        .worker
        .get_mcp_prompt(&None, &mcp_args)
        .await
        .map(|v| SessionEventContent::McpPromptGot(v))
        .ok_or(anyhow!("No such mcp prompt"))
}
async fn handle_list_tool_states(ws_session: &WsSession) -> SessionEventContent {
    SessionEventContent::ToolStates(ws_session.worker.list_tool_states().await)
}
async fn handle_list_predefined_prompts(ws_session: &WsSession) -> SessionEventContent {
    SessionEventContent::PredefinedPrompts(ws_session.worker.list_predefined_prompts().await)
}
async fn handle_setup_predefined_prompts(
    content: Option<SessionMessageContent>,
    ws_session: &WsSession,
) -> Result<()> {
    // let SessionMessage { content, .. } = upstream;
    if let Some(SessionMessageContent::PredefinePrompts { prompts }) = content {
        ws_session.worker.setup_predefined_prompts(&prompts).await;
    } else {
        return Err(anyhow!("handle_set_tool_states content mismatch"));
    };
    Ok(())
}

#[derive(Clone)]
pub struct WsSession {
    pub sid: Weak<Uuid>,
    pub ip: String,
    pub hb: Instant,
}

impl WsSession {
    fn new(
        sid: Option<Uuid>,
        ip: &str,
        workspace: &str,
        lang: &Option<String>,
        memory_file: &Option<String>,
        client_config: HashMap<String, String>,
        metadata: Option<MetaConfig>,
        mcp_config: &Option<String>,
        context: &Session,
    ) -> Result<(Self, Arc<Uuid>)> {
        let worker = WorkManager::new(
            workspace,
            lang,
            memory_file,
            client_config,
            metadata,
            mcp_config,
        )?;
        let (approval_notifier, mut approval_rcv) =
            broadcast::channel::<(Vec<String>, bool, Option<String>)>(100);
        let strong_sid = Arc::new(sid.unwrap_or(Uuid::new_v4()));
        let weak_sid = Arc::downgrade(&strong_sid);
        let auto_approve_tools = Arc::new(RwLock::new(vec![]));

        let mut evt_rcv = worker.evt_sender.subscribe();
        let weak_sid1 = weak_sid.clone();
        let sid1 = *strong_sid.as_ref();
        let auto_approve_tools1 = auto_approve_tools.clone();
        let mut context1 = context.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(Ok(evt)) = tokio_rcv_lock!(evt_rcv, 10000) {
                    match evt {
                        WorkEvent::Stop(tid) => {
                            let _ = context1.text(to_text!(SessionEvent::stop(None, tid))).await;
                        }
                        WorkEvent::Content { tid, id, content } => {
                            let _ = context1
                                .text(to_text!(SessionEvent::stream_content(
                                    None, tid, id, content
                                )))
                                .await;
                        }
                        WorkEvent::Toolcall {
                            tid,
                            id,
                            toolcall_id,
                            toolcall_name,
                            toolcall_arg,
                        } => {
                            let _ = context1
                                .text(to_text!(SessionEvent::stream_toolcall(
                                    None,
                                    tid,
                                    id,
                                    toolcall_id,
                                    toolcall_name,
                                    toolcall_arg
                                )))
                                .await;
                        }
                        WorkEvent::ToolcallConfirm {
                            tid,
                            id,
                            toolcall_id,
                            toolcall_name,
                            toolcall_arg,
                            toolcall_response,
                        } => {
                            let mut approval = false;
                            if let Ok(list) = tokio_read_lock!(auto_approve_tools1, 10000) {
                                approval = list.contains(&toolcall_name);
                            }
                            // 告知业务端toolcall流结束
                            let _ = context1
                                .text(to_text!(SessionEvent::toolcall_stream_end(
                                    None,
                                    tid,
                                    id.clone(),
                                    toolcall_id.clone(),
                                )))
                                .await;
                            if approval {
                                let _ = toolcall_response.send(None).await;
                            } else {
                                // 告知业务端toolcall需点击同意
                                let _ = context1
                                    .text(to_text!(SessionEvent::toolcall_confirm(
                                        None,
                                        tid,
                                        id,
                                        toolcall_id,
                                        toolcall_name.clone(),
                                    )))
                                    .await;
                                if let Ok(Ok((confirmed_list, is_approve, feedback))) =
                                    tokio_rcv_lock!(approval_rcv, 10000)
                                {
                                    if is_approve {
                                        if confirmed_list.contains(&toolcall_name) {
                                            let _ = toolcall_response.send(None).await;
                                        } else {
                                            let _ = toolcall_response
                                                .send(Some((-1, "User forbids".to_owned())))
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                        WorkEvent::ToolcallOutput {
                            tid,
                            id,
                            toolcall_id,
                            output,
                        } => {
                            let _ = context1
                                .text(to_text!(SessionEvent::stream_toolcall_output(
                                    None,
                                    tid,
                                    id,
                                    toolcall_id,
                                    output
                                )))
                                .await;
                        }
                        WorkEvent::ToolcallError {
                            tid,
                            id,
                            toolcall_id,
                            error,
                            ..
                        } => {
                            let _ = context1
                                .text(to_text!(SessionEvent::stream_toolcall_output(
                                    None,
                                    tid,
                                    id,
                                    toolcall_id,
                                    error
                                )))
                                .await;
                        }
                        WorkEvent::Usage { tid, usage } => {
                            let _ = context1
                                .text(to_text!(SessionEvent::usage(None, tid, usage)))
                                .await;
                        }
                        WorkEvent::Error { reason, retrying } => {
                            let _ = context1
                                .text(to_text!(SessionEvent::error(None, reason, retrying)))
                                .await;
                        }
                        _ => {}
                    }
                } else if weak_sid1.upgrade().is_none() {
                    break;
                }
            }
            // while let Ok(evt) = evt_rcv.recv().await {}
            log::info!("Quitting session {sid1} listening green thread");
        });
        Ok((
            Self {
                ip: ip.to_owned(),
                sid: weak_sid,
                worker,
                context: context.clone(),
                // auto_approve_tools: Arc::new(RwLock::new(vec![])),
                approval_notifier,
                hb: Instant::now(),
                tid: Arc::new(RwLock::new(None)),
            },
            strong_sid,
        ))
    }
}
impl Serialize for WsSession {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("WsSession", 3)?;
        s.serialize_field("sid", &self.sid.upgrade().map(|v| v.to_string()))?;
        s.serialize_field("ip", &self.ip)?;
        s.serialize_field("hb", &self.hb.elapsed().as_secs())?;
        s.end()
    }
}

#[derive(Debug, Clone)]
pub struct SessionEvent {
    pub id: Option<Uuid>,
    // pub session_id: Uuid,
    pub type_: String,
    pub content: SessionEventContent,
}
impl Serialize for SessionEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("SessionEvent", 3)?;
        // s.serialize_field("session_id", &self.session_id.to_string())?;
        s.serialize_field("id", &self.id.map(|v| v.to_string()))?;
        s.serialize_field("type", &self.type_)?;
        s.serialize_field("content", &self.content)?;
        s.end()
    }
}
#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum SessionEventContent {
    SessionReady(String), // session_id
    Error {
        reason: String,
        retrying: Option<bool>,
    },
    StreamContent {
        id: String,
        tid: String,
        text: String,
    },
    StreamTool {
        id: String,
        tid: String,
        tool_id: String,
        name: String,
        args: String,
    },
    StreamToolEnd {
        id: String,
        tid: String,
        tool_id: String,
    },
    StreamToolConfirm {
        id: String,
        tid: String,
        tool_id: String,
        name: String,
    },
    StreamToolOutput {
        id: String,
        tid: String,
        tool_id: String,
        text: String,
    },
    StreamToolError {
        id: String,
        tid: String,
        tool_id: String,
        error: String,
    },
    Usage {
        tid: String,
        usage: CompletionUsage,
    },
    Stop(String), // tid

    InnerAgentTools(Vec<AgentTool>),
    McpAgentTools(HashMap<String, Vec<AgentTool>>),
    McpPrompts(HashMap<String, Vec<Prompt>>),
    McpPromptGot(GetPromptResult),
    ToolStates(HashSet<ToolState>),
    PredefinedPrompts(Vec<PredefinePrompt>),
}

impl SessionEvent {
    pub fn ready(id: Option<Uuid>, sid: Uuid) -> Self {
        Self {
            id: id.clone(),
            type_: "ready".to_owned(),
            content: SessionEventContent::SessionReady(sid.to_string()),
        }
    }
    pub fn error(id: Option<Uuid>, reason: String, retrying: Option<bool>) -> Self {
        Self {
            id: id.clone(),
            type_: "error".to_owned(),
            content: SessionEventContent::Error { reason, retrying },
        }
    }
    pub fn stop(id: Option<Uuid>, tid: Uuid) -> Self {
        Self {
            id: id.clone(),
            type_: "task_stop".to_owned(),
            content: SessionEventContent::Stop(tid.to_string()),
        }
    }
    pub fn stream_content(id: Option<Uuid>, tid: Uuid, chat_id: String, text: String) -> Self {
        Self {
            id: id.clone(),
            type_: "task_stream_content".to_owned(),
            content: SessionEventContent::StreamContent {
                text,
                id: chat_id,
                tid: tid.to_string(),
            },
        }
    }
    pub fn stream_toolcall(
        id: Option<Uuid>,
        tid: Uuid,
        chat_id: String,
        tool_id: String,
        name: String,
        args: String,
    ) -> Self {
        Self {
            id: id.clone(),
            type_: "task_stream_toolcall".to_owned(),
            content: SessionEventContent::StreamTool {
                id: chat_id,
                tid: tid.to_string(),
                tool_id,
                name,
                args,
            },
        }
    }

    pub fn toolcall_stream_end(
        id: Option<Uuid>,
        tid: Uuid,
        chat_id: String,
        tool_id: String,
    ) -> Self {
        Self {
            id: id.clone(),
            type_: "task_toolcall_stream_end".to_owned(),
            content: SessionEventContent::StreamToolEnd {
                id: chat_id,
                tid: tid.to_string(),
                tool_id,
            },
        }
    }

    pub fn toolcall_confirm(
        id: Option<Uuid>,
        tid: Uuid,
        chat_id: String,
        tool_id: String,
        name: String,
    ) -> Self {
        Self {
            id: id.clone(),
            type_: "task_toolcall_confirm".to_owned(),
            content: SessionEventContent::StreamToolConfirm {
                id: chat_id,
                tid: tid.to_string(),
                tool_id,
                name,
            },
        }
    }

    pub fn stream_toolcall_output(
        id: Option<Uuid>,
        tid: Uuid,
        chat_id: String,
        tool_id: String,
        text: String,
    ) -> Self {
        Self {
            id: id.clone(),
            type_: "stream_toolcall_output".to_owned(),
            content: SessionEventContent::StreamToolOutput {
                id: chat_id,
                tid: tid.to_string(),
                tool_id,
                text,
            },
        }
    }
    pub fn stream_toolcall_error(
        id: Option<Uuid>,
        tid: Uuid,
        chat_id: String,
        tool_id: String,
        error: String,
    ) -> Self {
        Self {
            id: id.clone(),
            type_: "stream_toolcall_error".to_owned(),
            content: SessionEventContent::StreamToolError {
                id: chat_id,
                tid: tid.to_string(),
                tool_id,
                error,
            },
        }
    }
    pub fn usage(id: Option<Uuid>, tid: Uuid, usage: CompletionUsage) -> Self {
        Self {
            id: id.clone(),
            type_: "usage".to_owned(),
            content: SessionEventContent::Usage {
                tid: tid.to_string(),
                usage,
            },
        }
    }
    pub fn message_response(id: Option<Uuid>, type_: &str, content: SessionEventContent) -> Self {
        Self {
            id: id.clone(),
            type_: format!("msg_{type_}"),
            content,
        }
    }
}
