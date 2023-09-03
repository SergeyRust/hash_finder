use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{process, thread};
use chrono::Utc;
use clap::Parser;
use sha256::digest;
use crossbeam_channel::{bounded, Sender, unbounded};

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
    current_number: Arc<AtomicUsize>,
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
    fn run(&self) {
        let len = self.senders.len();
        let mut current = 0;
        let senders = self.senders.clone();
        let current_number = self.current_number.clone();

        thread::spawn( move || {
            loop {
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
    // Аргументы командной строки
    let args = Args::parse();
    let mut N =  args.N;
    let F = args.F;
    // Оптимальное количество потоков при текущей архитектуре процессора
    let num_cpus = thread::available_parallelism().unwrap();
    // Количество нулей (N), которыми должен оканчиваться дайджест хэша
    let mut pattern = String::new();
    while N > 0 {
        pattern += "0";
        N -= 1;
    }

    // Количество посчитанных хешей с необходимым количеством нулей
    let hashes: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    // Каждая задача обрабатывает свои числа, которые распределяет producer
    let mut receivers = Vec::with_capacity(num_cpus.get());
    let mut senders = Vec::with_capacity(num_cpus.get());

    for _ in 0..num_cpus.get() {
        // в зависимости от имеющегося свободного объема памяти можно регулировать объем буффера
        let (tx, rx) = bounded(1000000); // unbounded()
        senders.push(tx);
        receivers.push(rx);
    }

    let senders = Arc::new(senders);
    let receivers = Arc::new(Mutex::new(receivers));
    let producer = Producer::new(senders);
    producer.run();

    // Запускаем столько потоков, сколько позволяет процессор
    for _ in 0..num_cpus.get() {
        let receivers = receivers.clone();
        let hashes = hashes.clone();
        let pattern = pattern.clone();

        let start = Utc::now();
        // Каждый поток получает свое число для рассчета хеша
        thread::spawn( move || {
            let mut receivers = receivers.lock().unwrap();
            let len = receivers.len();
            let rx = receivers.remove(len - 1);
            drop(receivers);

            loop {
                let number = rx.recv().unwrap();
                let hash = digest(&number.to_string());

                // Когда хеш с N нулями найден, увеличиваем счетчик
                if hash.ends_with(pattern.as_str()) {
                    println!(" {}, \"{}\"", &number, &hash);
                    hashes.fetch_add(1, Ordering::SeqCst);
                    // Когда количество посчитанных хешей достигает F - завершаем программу.
                    // Так как никаких внешних ресурсов не задействовано, это безопасно.
                    if hashes.load(Ordering::SeqCst) >= F {
                        let finish = Utc::now();
                        println!("total time: {}", finish - start);
                        process::exit(0)
                    }
                }
            }
        });
    }

    thread::park()
}
