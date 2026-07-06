//! `midware` 中间件模块，用于进行鉴权或单点登陆，跨域，第三方storage集成等
use actix_cors::Cors;
use actix_web::http;

// use std::{
//     future::{Ready, ready},
//     time::SystemTime,
// };

// use actix_web::{
//     Error,
//     dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
// };

// pub struct SecureCheck;

// // Middleware factory is `Transform` trait
// // `S` - type of the next service
// // `B` - type of response's body
// impl<S, B> Transform<S, ServiceRequest> for SecureCheck
// where
//     S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
//     S::Future: 'static,
//     B: 'static,
// {
//     type Response = ServiceResponse<B>;
//     type Error = Error;
//     type InitError = ();
//     type Transform = SecureCheckMiddleware<S>;
//     type Future = Ready<Result<Self::Transform, Self::InitError>>;

//     fn new_transform(&self, service: S) -> Self::Future {
//         ready(Ok(SecureCheckMiddleware { service }))
//     }
// }

// pub struct SecureCheckMiddleware<S> {
//     service: S,
// }

// impl<S, B> Service<ServiceRequest> for SecureCheckMiddleware<S>
// where
//     S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
//     S::Future: 'static,
//     B: 'static,
// {
//     type Response = ServiceResponse<B>;
//     type Error = Error;
//     type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

//     forward_ready!(service);

//     fn call(&self, req: ServiceRequest) -> Self::Future {
//         // println!("Hi from start. You requested: {}", req.path());
//         let mut valid = false;
//         let headers = req.headers();
//         if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
//             if content_type.to_str().unwrap_or("").to_lowercase().trim() == "application/json" {
//                 let signature = headers
//                     .get("signature")
//                     .map(|v| v.to_str().unwrap_or(""))
//                     .unwrap_or("");
//                 let timestamp_str = headers
//                     .get("timestamp")
//                     .map(|v| v.to_str().unwrap_or(""))
//                     .unwrap_or("");
//                 let timestamp = timestamp_str.parse::<u128>().unwrap_or(0);
//                 let local_time = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_millis();
//                 // 2分以内认为有效
//                 if timestamp.abs_diff(local_time) < 2 * 60 * 1000 {
//                     if utils::md5(&format!("{timestamp_str}&{}", &*TOKEN)) == signature {
//                         valid = true;
//                     }
//                 }
//             }
//         }

//         if !valid {
//             let req_path = req.path();
//             if req_path.ends_with(".html") || req_path.ends_with(".js") {
//                 if let Some(query) = req.query_string().split(",").find(|v| v.contains("TOKEN")) {
//                     let kv = query.split("=").collect::<Vec<&str>>();
//                     if kv.len() == 2 && kv[1] == &*TOKEN {
//                         valid = true;
//                     }
//                 }
//             }
//         }

//         if valid {
//             let fut = self.service.call(req);
//             Box::pin(async move {
//                 let res = fut.await?;
//                 Ok(res)
//             })
//         } else {
//             let error = Err(actix_web::error::ErrorForbidden("Op not allowed"));
//             return Box::pin(async move { error });
//         }
//     }
// }

pub fn default() -> Cors {
    Cors::permissive()
        // .allowed_origin("All")
        // .send_wildcard()
        // .allowed_origin("http://10.10.86.54:8081/")
        .allowed_methods(vec!["GET", "POST", "OPTIONS"])
        .allowed_headers(vec![
            http::header::ACCEPT,
            http::header::AUTHORIZATION,
            http::header::CONTENT_TYPE,
        ])
        .max_age(3600)
}
