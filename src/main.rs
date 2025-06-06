use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
use warp::Filter;
use warp::Reply;

//
// ------ GLOBAL SOZLAMALAR ------
//
const ES_URL: &str =
    "https://search-siemlog-rddlwektckldlou57enditbsqa.eu-north-1.es.amazonaws.com";
const CALLBACK_URL: &str = "http://localhost:8000/api/v1/elastic/post-json/";

const ES_USER: &str = "sardor";
const ES_PASS: &str = "Aws0000$";

type SharedDb = Arc<Db>;
type ResultStore = Arc<Mutex<Vec<RuleHit>>>;

//
// ------ MA'LUMOT TUZILMALARI ------
//

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuleData {
    id: String,
    title: String,
    description: Option<String>,
    author: Option<String>,
    references: Option<Vec<String>>,
    logsource: Option<serde_json::Value>,
    status: Option<String>,
    date: Option<String>,
    level: Option<String>,
    falsepositives: Option<Vec<String>>,
    tags: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Rule {
    id: String,
    rule_query: serde_json::Value,
    rule_data: RuleData,
}

#[derive(Debug, Clone, Deserialize)]
struct NewRule {
    rule_query: serde_json::Value,
    rule_data: RuleData,
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

    let get_rule_by_id = warp::path!("rule" / String)
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(get_rule_by_id_handler);

    let delete_rules = warp::path("delete-rules")
        .and(warp::delete())
        .and(db_filter.clone())
        .and_then(delete_rules_handler);

    let delete_rule_by_id = warp::path!("delete-rule" / String)
        .and(warp::delete())
        .and(db_filter.clone())
        .and_then(delete_rule_by_id_handler);

    let routes = add_rule
        .or(get_rules)
        .or(get_rule_by_id)
        .or(delete_rules)
        .or(delete_rule_by_id);

    println!("[+] Server 3030 portda ishga tushdi");

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
        rule_query: new_rule.rule_query,
        rule_data: RuleData {
            id: new_rule.rule_data.id,
            title: new_rule.rule_data.title,
            description: new_rule.rule_data.description,
            author: new_rule.rule_data.author,
            references: new_rule.rule_data.references,
            logsource: new_rule.rule_data.logsource,
            status: new_rule.rule_data.status,
            date: new_rule.rule_data.date,
            level: new_rule.rule_data.level,
            falsepositives: new_rule.rule_data.falsepositives,
            tags: new_rule.rule_data.tags,
        },
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

async fn get_rule_by_id_handler(
    id: String,
    db: SharedDb,
) -> Result<warp::reply::Response, warp::Rejection> {
    let response = match db.get(id.as_bytes()) {
        Ok(Some(bytes)) => {
            if let Ok(rule) = serde_json::from_slice::<Rule>(&bytes) {
                println!("[+] Rule topildi: {}", id);
                warp::reply::with_status(warp::reply::json(&rule), warp::http::StatusCode::OK)
                    .into_response()
            } else {
                eprintln!("[-] JSON parse error for rule: {}", id);
                let error_resp = serde_json::json!({
                    "error": "Rule ni JSON ga parse qilib bo'lmadi"
                });
                warp::reply::with_status(
                    warp::reply::json(&error_resp),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                )
                .into_response()
            }
        }
        Ok(None) => {
            println!("[-] Rule topilmadi: {}", id);
            let error_resp = serde_json::json!({
                "error": format!("Rule topilmadi: {}", id)
            });
            warp::reply::with_status(
                warp::reply::json(&error_resp),
                warp::http::StatusCode::NOT_FOUND,
            )
            .into_response()
        }
        Err(e) => {
            eprintln!("[-] DB error: {}", e);
            let error_resp = serde_json::json!({
                "error": "DB xatolik"
            });
            warp::reply::with_status(
                warp::reply::json(&error_resp),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response()
        }
    };

    Ok(response)
}

async fn delete_rules_handler(db: SharedDb) -> Result<impl warp::Reply, warp::Rejection> {
    db.clear().expect("[-] DB clear error");
    println!("[+] Barcha qoida o'chirildi");
    Ok(warp::reply::with_status(
        "Barcha qoida o'chirildi",
        warp::http::StatusCode::OK,
    ))
}

async fn delete_rule_by_id_handler(
    id: String,
    db: SharedDb,
) -> Result<impl warp::Reply, warp::Rejection> {
    match db.remove(id.as_bytes()) {
        Ok(Some(_)) => {
            println!("[+] Qoida o'chirildi: {}", id);
            Ok(warp::reply::with_status(
                format!("Qoida o'chirildi: {}", id),
                warp::http::StatusCode::OK,
            ))
        }
        Ok(None) => Ok(warp::reply::with_status(
            format!("Qoida topilmadi: {}", id),
            warp::http::StatusCode::NOT_FOUND,
        )),
        Err(e) => {
            eprintln!("[-] DB error: {}", e);
            Ok(warp::reply::with_status(
                "DB xatolik".to_string(),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
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

        println!("[+] Barcha qoida ishlov berildi: [{}]", chrono::Utc::now());

        // === result_store dagi har bir rule uchun callback va ESdan o'chirish ===
        {
            let mut store = result_store.lock().unwrap();
            for rule_hit in store.iter() {
                // 1. Callbackga yuborish
                if rule_hit
                    .hits
                    .as_array()
                    .map_or(false, |arr| !arr.is_empty())
                {
                    let callback_body = serde_json::json!({
                        "rule_id": rule_hit.rule_id,
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
                                    if index != "sigma_rules" {
                                        let url = format!(
                                            "{}/{}/_doc/{}",
                                            ES_URL,
                                            index, id
                                        );
                                        let res = client
                                            .delete(&url)
                                            .basic_auth(ES_USER, Some(ES_PASS))
                                            .send()
                                            .await;
                                        match res {
                                            Ok(resp) => {
                                                println!(
                                                    "[+] Deleted: status={} message={}",
                                                    resp.status(),
                                                    resp.text().await.unwrap_or_default()
                                                );
                                            }
                                            Err(e) => {
                                                eprintln!(
                                                    "[-] ESdan o'chirishda xatolik: index={} id={} error={}",
                                                    index, id, e
                                                );
                                            }
                                        }
                                    }
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
        .post( format!("{}/_search", ES_URL))
        .basic_auth(ES_USER, Some(ES_PASS))
        .json(&rule.rule_query)
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
        let hits = json["hits"]["hits"].clone();
        if hits.as_array().map_or(false, |arr| !arr.is_empty()) {
            let mut store = result_store.lock().unwrap();
            store.push(RuleHit {
                rule_id: rule.id.clone(),
                hits,
            });
        }
    }
}
