use DigtalTalk::{
    midware,
    roles::server::TalkManager,
    routes::routes,
    stun,
    utils::{self, LOGGER, SERVER_PORT, SSL_SERVER_PORT, WebrtcAPI},
};
use actix_web::{App, HttpServer, middleware, web};
use dotenv::dotenv;
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    utils::cleaner();
    // env_logger::init();
    log4rs::init_file(&*LOGGER, Default::default()).unwrap();
    let api_data = web::Data::new(WebrtcAPI::new());

    let manager = web::Data::new(TalkManager::new().expect("Manager init failed"));
    stun::start_udp().expect("Stun server error!");

    let ssl_api_data = api_data.clone();
    let ssl_manager = manager.clone();
    let builder = utils::gen_ssl_builder().expect("Ssl builder error!");

    let result = futures::join!(
        HttpServer::new(move || {
            App::new()
                .wrap(middleware::Logger::default())
                .wrap(midware::default())
                // .wrap(SecureCheck)
                .app_data(api_data.clone())
                .app_data(manager.clone())
                // .app_data(sockets_pool.clone())
                .configure(routes)
        })
        .bind(format!("0.0.0.0:{}", &*SERVER_PORT))?
        .run(),
        HttpServer::new(move || {
            App::new()
                .wrap(middleware::Logger::default())
                .wrap(midware::default())
                // .wrap(SecureCheck)
                .app_data(ssl_api_data.clone())
                .app_data(ssl_manager.clone())
                // .app_data(ssl_sockets_pool.clone())
                .configure(routes)
        })
        .bind_openssl(format!("0.0.0.0:{}", &*SSL_SERVER_PORT), builder)?
        .run()
    )
    .0;
    result
}
