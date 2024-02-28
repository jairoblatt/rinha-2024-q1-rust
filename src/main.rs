use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use std::{
    env,
    time:: SystemTime
};
use chrono::{DateTime, Utc};
use deadpool::Runtime;
use deadpool_postgres::GenericClient;
use postgres_types::Json;
use serde_json::{from_value, to_string, Value};
use std::{error::Error, net::SocketAddr};
use tokio_postgres::NoTls;

//⠀⠀⠀⠀⠀⠀⠀⠀ ⢀⣠⣤⣶⣶⣶⣶⣶⣤⣄⡀
//       ⣠⣴⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣄⡀
//    ⣠⣴⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣮⣵⣄
//   ⢾⣻⣿⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⢿⣿⣿⡀
//   ⣽⣻⠃⣿⡿⠋⣉⠛⣿⣿⣿⣿⣿⣿⣿⣿⣏⡟⠉⡉⢻⣿⡌⣿⣳⡥
//  ⢜⣳⡟⢸⣿⣷⣄⣠⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⣤⣠⣼⣿⣇⢸⢧⢣
//  ⠨⢳⠇⣸⣿⣿⢿⣿⣿⣿⣿⡿⠿⠿⠿⢿⣿⣿⣿⣿⣿⣿⣿⣿⠀⡟⢆
//     ⣾⣿⣿⣼⣿⣿⣿⣿⡀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣽⣿⣿⠐⠈⠀
//  ⢀⣀⣼⣷⣭⣛⣯⡝⠿⢿⣛⣋⣤⣤⣀⣉⣛⣻⡿⢟⣵⣟⣯⣶⣿⣄⡀
// ⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣶⣶⣶⣾⣶⣶⣴⣾⣿⣿⣿⣿⣿⣿⢿⣿⣿⣧
// ⣿⣿⣿⠿⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠿⠿⣿⡿

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    const POOL_SIZE:usize = 101;

    let mut conf: deadpool_postgres::Config = deadpool_postgres::Config::new();

    conf.user = Some("admin".to_string());
    conf.password = Some("123".to_string());
    
    conf.dbname = Some("rinha".to_string());
    conf.host = Some("localhost".to_string());
    conf.port = Some(5432);

    conf.pool = deadpool_postgres::PoolConfig::new(POOL_SIZE).into();

    let pg: deadpool::managed::Pool<deadpool_postgres::Manager> = conf
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("error: create pool");


    let http_port: u16 = env::var("HTTP_PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("error: parsing HTTP_PORT env var");

    let addr = SocketAddr::from(([127, 0, 0, 1], http_port));

    let app: Router = Router::new()
        .route("/clientes/:id/transacoes", post(create_transaction))
        .route("/clientes/:id/extrato", get(get_extract))
        .with_state::<()>(pg);

    println!("Ready at: {addr}");

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

async fn create_transaction(
    Path(client_id): Path<i32>,
    State(pg_pool): State<deadpool_postgres::Pool>,
    payload: Bytes,
) -> impl IntoResponse {
    if !is_valid_client(client_id) {
        return (StatusCode::NOT_FOUND, String::new());
    }

    let payload = match serde_json::from_slice::<TransactionDTO>(&payload) {
        Ok(payload) => payload,
        Err(_) => return (StatusCode::UNPROCESSABLE_ENTITY, String::new()),
    };
    
    if  payload.descricao.trim().is_empty() || payload.descricao.len() > 10 || (payload.tipo != "d" && payload.tipo != "c") {
        return (StatusCode::UNPROCESSABLE_ENTITY, String::new());
    }

    let client: deadpool::managed::Object<deadpool_postgres::Manager> =
        pg_pool.get().await.expect("error: getting db client at create_transaction");

    if payload.tipo == "c" {
        let credit = client
            .query_one(
                "SELECT creditar($1, $2, $3);",
                &[&client_id, &payload.valor, &payload.descricao],
            )
            .await
            .expect("error: executing query at create_transaction");

        let credit_json: Json<Value> = credit.get(0);
        let credit: TransactionResp = from_value(credit_json.0)
                                          .expect("error: parsing json at create_transaction");

        return (StatusCode::OK, to_string(&credit).unwrap());
    }

    let debit = client
        .query_one(
            "SELECT debitar($1, $2, $3);",
            &[&client_id, &payload.valor, &payload.descricao],
        )
        .await
        .expect("error: executing query at create_transaction");

    let debit_json: Json<Value> = debit.get(0);
    let debit: TransactionResp = from_value(debit_json.0)
                                    .expect("error: parsing json at create_transaction");

    if debit.limite == -9 {
        return (StatusCode::UNPROCESSABLE_ENTITY, String::new());
    }

    return (StatusCode::OK, to_string(&debit).unwrap());
}

async fn get_extract(
    Path(client_id): Path<i32>,
    State(pg_pool): State<deadpool_postgres::Pool>,
) -> impl IntoResponse {

    if !is_valid_client(client_id) {
        return (StatusCode::NOT_FOUND, String::new());
    }

    let client: deadpool::managed::Object<deadpool_postgres::Manager> =
        pg_pool.get().await.expect("error: getting db client at get_extract");

    client
        .batch_execute("BEGIN;")
        .await
        .expect("error: executing query at get_extract");

    let balance = client
        .query_one(
            "SELECT saldo, limite FROM clientes WHERE id = $1;",
            &[&client_id],
        )
        .await
        .expect("error: executing query at get_extract");

    let extracts = client
        .query("SELECT valor, tipo, descricao, realizada_em FROM transacoes WHERE id_cliente = $1 ORDER BY id DESC LIMIT 10;",
        &[&client_id]).await.expect("error: executing query at get_extract");

    client
        .batch_execute("COMMIT;")
        .await
        .expect("error: executing query at get_extract");

    let ultimas_transacoes: Vec<Extract> = extracts
        .iter()
        .map(|row| {
            let realizada_em_time: SystemTime = row.get("realizada_em");

            Extract {
                realizada_em: DateTime::from(realizada_em_time),
                valor: row.get("valor"),
                tipo: row.get("tipo"),
                descricao: row.get("descricao"),
            }
        } )
        .collect();

    return (
        StatusCode::OK,
        to_string(&ExtractResp {
            saldo: ExtractBalance {
                total: balance.get("saldo"),
                limite: balance.get("limite"),
                data_extrato: Utc::now(),
            },
            ultimas_transacoes,
        })
        .unwrap(),
    );
}

fn is_valid_client(client_id: i32) -> bool {
    client_id > 0 && client_id < 6
}


#[derive(Debug, serde::Serialize)]
struct ExtractBalance {
    total: i32,
    limite: i32,
    data_extrato: DateTime<Utc>,
}

#[derive(Debug, serde::Serialize)]
struct Extract {
    valor: i32,
    tipo: String,
    descricao: String,
    realizada_em: DateTime<Utc>,
}

#[derive(Debug, serde::Serialize)]
struct ExtractResp {
    saldo: ExtractBalance,
    ultimas_transacoes: Vec<Extract>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct TransactionResp {
    saldo: i32,
    limite: i32,
}

#[derive(serde::Deserialize)]
struct TransactionDTO {
    tipo: String,
    descricao: String,
    valor: i32,
}
