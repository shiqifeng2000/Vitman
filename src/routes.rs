//! `routes` 为路由模块，负责工程中的路由匹配，路由保护等规则
//!
use actix_web::web;

use crate::handlers;

pub fn routes(cfg: &mut web::ServiceConfig) {
    let factory = web::scope("")
        .service(handlers::get_sessions)
        .service(handlers::start_session)
        .service(handlers::stop_session)
        .service(handlers::idx_file)
        .service(handlers::static_file());
    cfg.service(factory);
}

// pub fn static_routes(cfg: &mut web::ServiceConfig) {
//     cfg.service(
//         web::scope("")
//             .guard(guard::Get())
//             .service(handlerss::files::main)
//             .service(handlerss::files::static_file()),
//     );
// }
