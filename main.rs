use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use warp::Filter;
use warp::ws::Message;
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use futures::{StreamExt, SinkExt};


type Users = Arc<Mutex<HashSet<String>>>;

#[tokio::main]
async fn main() {
    let users: Users = Arc::new(Mutex::new(HashSet::new()));
    let (tx, _rx) = broadcast::channel(100);

    let chat_route = warp::path("chat")
        .and(warp::ws())
        .and(with_users(users.clone()))
        .and(with_broadcast(tx.clone()))
        .map(|ws: warp::ws::Ws, users, tx| {
            ws.on_upgrade(move |socket| handle_connection(socket, users, tx))
        });

    let routes = chat_route.with(warp::cors().allow_any_origin());

    println!("Chat server running on ws://127.0.0.1:3030/chat");
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

fn with_users(users: Users) -> impl Filter<Extract = (Users,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || users.clone())
}

fn with_broadcast(tx: broadcast::Sender<String>) -> impl Filter<Extract = (broadcast::Sender<String>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || tx.clone())
}

async fn handle_connection(
    ws: warp::ws::WebSocket,
    users: Users,
    tx: broadcast::Sender<String>,
) {
    let (mut user_ws_tx, mut user_ws_rx) = ws.split();
    let (user_tx, mut user_rx) = mpsc::unbounded_channel();

    let user_id = format!("User-{}", rand::random::<u16>());
    {
        let mut users_guard = users.lock().unwrap();
        if users_guard.len() >= 10 {
            drop(users_guard); // Release the lock before sending the message
            let _ = user_ws_tx.send(warp::ws::Message::text("Chatroom is full!")).await;
            return;
        }
        users_guard.insert(user_id.clone());
    }

    let _tx_clone = tx.clone();
    let mut rx = tx.subscribe();

    tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            let _ = user_tx.send(msg);
        }
    });

    tokio::spawn(async move {
        while let Some(result) = user_ws_rx.next().await {
            match result {
                Ok(msg) => {
                    if msg.is_text() {
                        let text = msg.to_str().unwrap();
                        println!("Received text: {}", text);
                        // Your chat logic here
                    }
                }
                Err(e) => eprintln!("websocket error: {}", e),
            }
        }
        
    });

    while let Some(msg) = user_rx.recv().await {
        let _ = user_ws_tx.send(Message::text(msg)).await;
    }

    {
        let mut users_guard = users.lock().unwrap();
        users_guard.remove(&user_id);
    }
}