use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{process, thread};
use clap::Parser;
use sha256::digest;
use std::time::Duration;
use chrono::Utc;
use crossbeam_channel::{Sender, unbounded};

/// Аргументы командной строки
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// сколькими N-символами нуля оканчивается дайджест хеша (символьное представление хеша)
    #[arg(short = 'N', long)]
    N: usize,
    /// Параметр F определяет сколько значений хеша следует найти команде.
    #[arg(short = 'F', long)]
    F: usize,
}

/// Распределяет данные для задач
struct Producer {
    current_number: Arc<AtomicUsize>, // Arc<usize>
    senders: Arc<Vec<Sender<usize>>>,
}

impl Producer {

    fn new(sync_senders: Arc<Vec<Sender<usize>>>) -> Self {
        Self {
            current_number: Arc::new(AtomicUsize::new(1)), // TODO AtomicU128
            senders: sync_senders
        }
    }

    /// Отправляет каждой задаче свой набор чисел начиная с 1,
    /// при 8 ядрах процессора (16 потоков)
    /// 1 -> task1, 2-> task2, 3 -> task3 ... 17 -> task1
    fn start_sending(&self) {
        let len = self.senders.len();
        let mut current = 0;
        let senders = self.senders.clone();
        let current_number = self.current_number.clone();

        thread::spawn(move || {
            loop {
                let senders = senders.clone();

                while current < len {
                    let tx = senders.get(current.clone()).unwrap();
                    let number = current_number.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.send(number.clone());
                    current += 1;
                }

                current = 0;
            }
        });
    }
}

fn main() {
    // Парсим аргументы командной строки
    let args = Args::parse();
    let mut N =  args.N;
    let F = args.F;
    // Получаем количество ядер процессора
    let num_cpus = thread::available_parallelism().unwrap();
    // Количество нулей (N), которыми должен оканчиваться дайджест хэша
    let mut pattern = String::new();
    let pat = 
    while N > 0 {
        pattern += "0";
        N -= 1;
    }

    // Массив (исходное число, хеш) , размером F
    let hashes: Arc<Mutex<Vec<(usize, String)>>> = Arc::new(Mutex::new(Vec::new()));
    // Каждая задача будет обрабатывать свое число, producer распределяет данные
    let mut senders = Vec::with_capacity(num_cpus.get());
    let mut receivers = Vec::with_capacity(num_cpus.get());
    for _ in 0..num_cpus.get() {
        let (tx, rx) = unbounded(); //  bounded(1000000);
        senders.push(tx);
        receivers.push(rx);
    }
    let senders = Arc::new(senders);
    let recv_len = receivers.len();
    let receivers = Arc::new(Mutex::new(receivers));

    let producer = Producer::new(senders);
    producer.start_sending();

    let start = Utc::now();
    for _ in 0..recv_len {
        let receivers = receivers.clone();
        let hashes = hashes.clone();
        let pattern = pattern.clone();

        thread::spawn( move || {
            let mut receivers = receivers.lock().unwrap();
            let len = receivers.len();
            let rx = receivers.remove(len - 1);
            drop(receivers);

            loop {
                let received = rx.recv();
                if let Ok(number) = received {
                    let hash = digest(&number.to_string());

                    if hash.ends_with(pattern.as_str()) {
                        let mut hashes = hashes.lock().unwrap();
                        println!(" {}, \"{}\"", &number, &hash);
                        hashes.push((number, hash));

                        if hashes.len() >= F {
                            let finish = Utc::now();
                            println!("total time: {}", finish - start);
                            process::exit(0)
                        }
                    }
                }
            }
        });
    }

    thread::sleep(Duration::from_secs(u64::MAX)); // TODO
}
