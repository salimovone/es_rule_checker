use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
use warp::Filter;

//
// ------ GLOBAL SOZLAMALAR ------
//
const ES_URL: &str =
    "https://search-siemlog-rddlwektckldlou57enditbsqa.eu-north-1.es.amazonaws.com/_search";
const CALLBACK_URL: &str = "http://113.60.91.11:8000/api/v1/elastic/post-json/";

const ES_USER: &str = "sardor";
const ES_PASS: &str = "Aws0000$";

type SharedDb = Arc<Db>;

//
// ------ MA'LUMOT TUZILMALARI ------
//
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Rule {
    id: String,
    query: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct NewRule {
    query: serde_json::Value,
}

//
// ------ ENTRY POINT ------
//
#[tokio::main]
async fn main() {
    let db = Arc::new(sled::open("rules_db").expect("[x] DB ochilmadi"));
    let db_filter = warp::any().map({
        let db = db.clone();
        move || db.clone()
    });

    // REST endpointlar
    let add_rule = warp::path("add-rule")
        .and(warp::post())
        .and(warp::body::json())
        .and(db_filter.clone())
        .and_then(add_rule_handler);

    let get_rules = warp::path("rules")
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(get_rules_handler);

    let routes = add_rule.or(get_rules);

    println!("[+] Server http://localhost:3030 da ishga tushdi");

    // Worker taskni ishga tushiramiz
    spawn_worker_loop(db.clone());

    // HTTP server
    warp::serve(routes).run(([0, 0, 0, 0], 3030)).await;
}

//
// ------ REST HANDLERLAR ------
//
async fn add_rule_handler(
    new_rule: NewRule,
    db: SharedDb,
) -> Result<impl warp::Reply, warp::Rejection> {
    let rule = Rule {
        id: Uuid::new_v4().to_string(),
        query: new_rule.query,
    };

    db.insert(&rule.id, serde_json::to_vec(&rule).unwrap())
        .expect("[-] DB insert error");

    println!("[+] Qoida qo‘shildi: {}", rule.id);
    Ok(warp::reply::json(&rule))
}

async fn get_rules_handler(db: SharedDb) -> Result<impl warp::Reply, warp::Rejection> {
    let rules: Vec<Rule> = db
        .iter()
        .values()
        .filter_map(Result::ok)
        .filter_map(|v| serde_json::from_slice::<Rule>(&v).ok())
        .collect();

    Ok(warp::reply::json(&rules))
}

//
// ------ WORKER ISHI ------
//
fn spawn_worker_loop(db: SharedDb) {
    let semaphore = Arc::new(Semaphore::new(100));
    tokio::spawn(run_worker_loop(db, semaphore));
}

async fn run_worker_loop(db: SharedDb, semaphore: Arc<Semaphore>) {
    let client = Client::builder()
        .pool_max_idle_per_host(100)
        .build()
        .unwrap();

    loop {
        let mut tasks = FuturesUnordered::new();

        for result in db.iter().values() {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let client = client.clone();

            if let Ok(bytes) = result {
                if let Ok(rule) = serde_json::from_slice::<Rule>(&bytes) {
                    let rule_clone = rule.clone();
                    tasks.push(tokio::spawn(handle_rule(rule_clone, client, permit)));
                }
            }
        }

        while let Some(_res) = tasks.next().await {}

        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

async fn handle_rule(rule: Rule, client: Client, _permit: OwnedSemaphorePermit) {
    match client
        .post(ES_URL)
        .basic_auth(ES_USER, Some(ES_PASS))
        .json(&rule.query)
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            println!(
                "[!] recived: [{}] rule_id={} status={}",
                chrono::Utc::now(),
                rule.id,
                status
            );

            if status.is_success() {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    process_es_response(&rule, &client, json).await;
                }
            } else {
                eprintln!("[x] ES query failed rule_id={} status={}", rule.id, status);
            }
        }
        Err(e) => eprintln!("[x] ES error rule_id={}: {}", rule.id, e),
    }
}

// Yangi funksiya: Elasticsearch javobini qayta ishlash va callback yuborish
async fn process_es_response(rule: &Rule, client: &Client, json: serde_json::Value) {
    let hits = json["hits"]["total"]
        .get("value")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| json["hits"]["total"].as_i64().unwrap_or(0));

    // println!(
    //     "[!] [{}] rule_id={} hits={}",
    //     chrono::Utc::now(),
    //     rule.id,
    //     hits
    // );

    if hits > 0 {
        match client
            .post(CALLBACK_URL)
            .json(&serde_json::json!({
                "data": &json["hits"]["hits"]
            }))
            .send()
            .await
        {
            Ok(cb_resp) => println!(
                "[!] sent: [{}] callback rule_id={} status={}",
                chrono::Utc::now(),
                rule.id,
                cb_resp.status()
            ),
            Err(e) => eprintln!("[-] callback error rule_id={}: {}", rule.id, e),
        }
    }
}
