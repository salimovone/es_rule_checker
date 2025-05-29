use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
use warp::Filter;

//
// ------ GLOBAL SOZLAMALAR ------
//
const ES_URL: &str =
    "https://search-siemlog-rddlwektckldlou57enditbsqa.eu-north-1.es.amazonaws.com/_search";
const CALLBACK_URL: &str = "http://13.60.91.11:8000/api/v1/elastic/post-json/";

const ES_USER: &str = "sardor";
const ES_PASS: &str = "Aws0000$";

type SharedDb = Arc<Db>;
type ResultStore = Arc<Mutex<Vec<RuleHit>>>;

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

#[derive(Debug, Clone)]
struct RuleHit {
    rule_id: String,
    hits: serde_json::Value, // yoki Vec<serde_json::Value>
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

    let result_store: ResultStore = Arc::new(Mutex::new(Vec::new()));

    // Worker task va kerakli handlerlarga uzating
    spawn_worker_loop(db.clone(), result_store.clone());

    // Agar kerak bo‘lsa, boshqa handlerlarga ham uzating
    // Masalan:
    // warp::any().map(move || result_store.clone())

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
fn spawn_worker_loop(db: SharedDb, result_store: ResultStore) {
    let semaphore = Arc::new(Semaphore::new(100));
    tokio::spawn(run_worker_loop(db, semaphore, result_store));
}

async fn run_worker_loop(db: SharedDb, semaphore: Arc<Semaphore>, result_store: ResultStore) {
    let client = Client::builder()
        .pool_max_idle_per_host(100)
        .build()
        .unwrap();

    loop {
        let mut tasks = FuturesUnordered::new();

        for result in db.iter().values() {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let client = client.clone();
            let result_store = result_store.clone();

            if let Ok(bytes) = result {
                if let Ok(rule) = serde_json::from_slice::<Rule>(&bytes) {
                    let rule_clone = rule.clone();
                    tasks.push(tokio::spawn(handle_rule(
                        rule_clone,
                        client,
                        permit,
                        result_store.clone(),
                    )));
                }
            }
        }

        while let Some(_res) = tasks.next().await {}

        println!("[!] Barcha qoida ishlov berildi: [{}]", chrono::Utc::now());

        // === YANGI QISM: result_store dagi har bir rule uchun callback va ESdan o'chirish ===
        {
            let mut store = result_store.lock().unwrap();
            for rule_hit in store.iter() {
                // 1. Callbackga yuborish
                if rule_hit.hits.as_array().map_or(false, |arr| !arr.is_empty()) {
                    let callback_body = serde_json::json!({
                        "id": rule_hit.rule_id,
                        "data": rule_hit.hits
                    });
                    // Asinxron yuborish uchun tokio::spawn ishlatamiz
                    let client = client.clone();
                    let hits = rule_hit.hits.clone();
                    println!(
                        "[!] sent: [{}] rule_id={}",
                        chrono::Utc::now(),
                        rule_hit.rule_id
                    );
                    tokio::spawn(async move {
                        let _ = client.post(CALLBACK_URL).json(&callback_body).send().await;
                        
                        // 2. ESdan o'chirish (masalan, har bir hit uchun _id bo'lsa)
                        if let Some(hits_arr) = hits.as_array() {
                            for hit in hits_arr {
                                if let Some(id) = hit["_id"].as_str() {
                                    let index = hit["_index"].as_str().unwrap_or("default-index");
                                    let url = format!(
                                        "https://search-siemlog-rddlwektckldlou57enditbsqa.eu-north-1.es.amazonaws.com/{}/_doc/{}",
                                        index, id
                                    );
                                    let _ = client
                                        .delete(&url)
                                        .basic_auth(ES_USER, Some(ES_PASS))
                                        .send()
                                        .await;
                                }
                            }
                        }
                    });
                }
            }
            // 3. result_store ni tozalash
            store.clear();
        }

        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

async fn handle_rule(
    rule: Rule,
    client: Client,
    _permit: OwnedSemaphorePermit,
    result_store: ResultStore,
) {
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
                    process_es_response(
                        &rule,
                        // &client,
                        json,
                        result_store,
                    )
                    .await;
                }
            } else {
                eprintln!("[x] ES query failed rule_id={} status={}", rule.id, status);
            }
        }
        Err(e) => eprintln!("[x] ES error rule_id={}: {}", rule.id, e),
    }
}

// Yangi funksiya: Elasticsearch javobini qayta ishlash va callback yuborish
async fn process_es_response(
    rule: &Rule,
    // client: &Client,
    json: serde_json::Value,
    result_store: ResultStore,
) {
    // Natijani arrayga saqlash
    {
        let mut store = result_store.lock().unwrap();
        store.push(RuleHit {
            rule_id: rule.id.clone(),
            hits: json["hits"]["hits"].clone(),
        });
    }
}
