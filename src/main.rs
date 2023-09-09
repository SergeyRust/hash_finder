use std::sync::{
    {Arc, Mutex},
    atomic::{AtomicUsize, Ordering}
};
use std::{process, thread};
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use clap::{Error as CommandError, Parser};
use clap::error::{ContextKind, ContextValue, ErrorKind};
use sha256::digest;
use crossbeam_channel::{Receiver, Sender};

const CHANNEL_CAPACITY: usize = 100000;

fn main() {
    let args = Args::parse();
    let null_amount =  args.null_amount;
    let hashes_amount = args.hashes_amount;

    if let Ok(hashes) = find_hashes(null_amount, hashes_amount) {
        for (number, hash) in hashes.iter() {
            println!(" {}, \"{}\"", number, hash);
        }
    } else {
        // Аргументы запуска программы неверны
        process::exit(0)
    }
}

/// Аргументы командной строки
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// сколькими N-символами нуля оканчивается дайджест хеша (символьное представление хеша)
    #[arg(short = 'N', long)]
    null_amount: usize,
    /// Параметр F определяет сколько значений хеша следует найти команде.
    #[arg(short = 'F', long)]
    hashes_amount: usize,
}

/// Распределяет данные для задач
struct Producer {
    /// возможно тут нужно было бы сделать Arc<AtomicPtr<BigUint>>,
    /// но вряд ли кто-то будет проверять это все на суперкомпьютерах или ASIC
    current_number: Arc<AtomicUsize>,
    channel_senders: Arc<Vec<Sender<usize>>>,
}

impl Producer {

    fn new(txs: Arc<Vec<Sender<usize>>>) -> Self {
        Self {
            current_number: Arc::new(AtomicUsize::new(1)),
            channel_senders: txs
        }
    }

    /// Отправляет каждой задаче свой набор чисел начиная с 1,
    /// при 8 ядрах процессора (16 потоков)
    /// 1 -> task1, 2-> task2, 3 -> task3 ... 17 -> task1
    fn start(&self) {
        let len = self.channel_senders.len();
        let mut current = 0;
        let senders = self.channel_senders.clone();
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

struct Worker {
    receiver: Arc<Mutex<Receiver<usize>>>,
}

impl Worker {

    fn new(receiver: Arc<Mutex<Receiver<usize>>>) -> Self {
        Self {
            receiver
        }
    }

    fn start(&self, pattern: String, worker_hash_tx: Sender<(usize,String)>) {
        let receiver = self.receiver.clone();
        // Каждый worker получает свое число для рассчета хеша
        thread::spawn( move || {
            let receiver = receiver.lock().unwrap();

            loop {
                let number = receiver.recv().unwrap();
                let hash = digest(&number.to_string());

                // Когда хеш с N нулями найден, увеличиваем счетчик
                if hash.ends_with(pattern.as_str()) {
                    let _ = worker_hash_tx.send((number, hash));
                }
            }
        });
    }
}

/// Распределяет задачи Producer для Worker-ов и возвращает массив (номер, хеш)
fn find_hashes(null_amount: usize, hashes_amount: usize) -> Result<Vec<(usize, String)>, CommandError> {
    // Оптимальное количество потоков при текущей архитектуре процессора
    let num_cpus = thread::available_parallelism().unwrap();
    // Количество нулей (N), которыми должен оканчиваться дайджест хэша
    let pattern = parse_null_arg(null_amount);

    match pattern {
        Ok(pattern) => {
            // Каждая задача обрабатывает свои числа, которые создает producer
            let mut producer_senders = Vec::with_capacity(num_cpus.get());
            let (worker_hash_tx, worker_hash_rx) = crossbeam_channel::bounded(hashes_amount);
            for _ in 0..num_cpus.get() {
                // в зависимости от имеющегося свободного объема памяти можно регулировать объем буффера
                let (tx, rx) = crossbeam_channel::bounded(CHANNEL_CAPACITY);
                producer_senders.push(tx);
                let worker = Worker::new( Arc::new(Mutex::new(rx)));
                worker.start(pattern.clone(), worker_hash_tx.clone());
            }

            let producer = Producer::new(Arc::new(producer_senders));
            producer.start();

            // Когда количество посчитанных хешей достигает F - завершаем программу.
            let mut hashes = Vec::with_capacity(hashes_amount);
            let wait_for_other_threads = Arc::new(AtomicBool::new(true));
            while let Ok(hash) = worker_hash_rx.recv() {
                let wait_for_other_threads = wait_for_other_threads.clone();
                hashes.push(hash);
                // Ждем пока другие потоки досчитают свой хэш (для небольших чисел)
                // Так как много потоков считают хеши быстрее одного, а нам нужно выводить
                // числа и хеши в порядке возрастания
                if (hashes.len() == hashes_amount) && wait_for_other_threads.load(Ordering::Relaxed) {
                    thread::spawn( move || {
                        thread::sleep(Duration::from_millis(1500));
                        wait_for_other_threads.store(false, Ordering::Relaxed);
                    });
                } else if !wait_for_other_threads.load(Ordering::Relaxed) {
                    hashes.sort_by_key(|(number, _)| number.clone());
                    return Ok(hashes)
                }
            }
        }
        Err(e) => {
            let _ = e.print();
            return Err(CommandError::new(ErrorKind::InvalidValue))
        }
    }

    unreachable!()
}

fn parse_null_arg(mut null_amount: usize) -> Result<String, CommandError> {
    if null_amount > 6 {
        println!("hash finder: \"it may take time...\"")
    }

    if null_amount < 1 {
        let mut err = CommandError::new(ErrorKind::ValueValidation);
        err.insert(ContextKind::InvalidArg, ContextValue::String("-N".to_owned()));
        err.insert(ContextKind::InvalidValue, ContextValue::String("0".to_owned()));

        return Err(err)
    }

    let mut pattern = String::new();

    while null_amount > 0 {
        pattern += "0";
        null_amount -= 1;
    };

    Ok(pattern)
}

/// На моей машине максимально обозримое количество нулей в хеше для рассчета - 6.
/// Основная проблема в моей реализации была в том, что при небольших (1-2) значениях аргумента N
/// вывод программы содержал не последовательно рассчитаные значения от 1 до N, некоторые значения
/// не выводились из-за отставания скорости вычисления в разных потоках, в результате чего пришлось
/// дожидаться всех потоков и сортировать значения
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use super::*;

    #[test]
    fn with_null_amount_1_in_range_of_1_to_10() {
        const NULL_AMOUNT: usize = 1;

        for hashes_amount in 1..10 {
            let single_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes_single_threaded(NULL_AMOUNT, hashes_amount)
            );
            let multi_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes(NULL_AMOUNT, hashes_amount).unwrap()
            );
            for val in single_threaded_hashes.iter() {
                assert!(multi_threaded_hashes.contains(val))
            }
        }
        println!("hashes_amount in 1..10 finished");
    }

    #[test]
    fn with_null_amount_1_in_range_of_10_to_20() {
        const NULL_AMOUNT: usize = 1;

        for hashes_amount in 10..20 {
            let single_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes_single_threaded(NULL_AMOUNT, hashes_amount)
            );
            let multi_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes(NULL_AMOUNT, hashes_amount).unwrap()
            );
            for val in single_threaded_hashes.iter() {
                assert!(multi_threaded_hashes.contains(val))
            }
        }
    }

    #[test]
    fn with_null_amount_1_in_range_from_20_to_30() {
        const NULL_AMOUNT: usize = 1;

        for hashes_amount in 20..30 {
            let single_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes_single_threaded(NULL_AMOUNT, hashes_amount)
            );
            let multi_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes(NULL_AMOUNT, hashes_amount).unwrap()
            );
            for val in single_threaded_hashes.iter() {
                assert!(multi_threaded_hashes.contains(val))
            }
        }
    }

    #[test]
    fn with_null_amount_2_in_range_from_1_to_10() {
        const NULL_AMOUNT: usize = 2;

        for hashes_amount in 1..10 {
            let single_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes_single_threaded(NULL_AMOUNT, hashes_amount)
            );
            let multi_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes(NULL_AMOUNT, hashes_amount).unwrap()
            );
            for val in single_threaded_hashes.iter() {
                assert!(multi_threaded_hashes.contains(val))
            }
        }
    }

    #[test]
    fn with_null_amount_2_in_range_from_10_to_20() {
        const NULL_AMOUNT: usize = 2;

        for hashes_amount in 10..20 {
            let single_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes_single_threaded(NULL_AMOUNT, hashes_amount)
            );
            let multi_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes(NULL_AMOUNT, hashes_amount).unwrap()
            );
            for val in single_threaded_hashes.iter() {
                assert!(multi_threaded_hashes.contains(val))
            }
        }
    }

    #[test]
    fn with_null_amount_3_in_range_from_1_to_10() {
        const NULL_AMOUNT: usize = 3;

        for hashes_amount in 1..10 {
            let single_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes_single_threaded(NULL_AMOUNT, hashes_amount)
            );
            let multi_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes(NULL_AMOUNT, hashes_amount).unwrap()
            );
            for val in single_threaded_hashes.iter() {
                assert!(multi_threaded_hashes.contains(val))
            }
        }
    }

    #[test]
    fn with_null_amount_4_in_range_from_1_to_10() {
        const NULL_AMOUNT: usize = 4;

        for hashes_amount in 1..10 {
            let single_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes_single_threaded(NULL_AMOUNT, hashes_amount)
            );
            let multi_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes(NULL_AMOUNT, hashes_amount).unwrap()
            );
            for val in single_threaded_hashes.iter() {
                assert!(multi_threaded_hashes.contains(val))
            }
        }
    }

    #[test]
    fn with_null_amount_5_in_range_from_1_to_10() {
        const NULL_AMOUNT: usize = 5;

        for hashes_amount in 1..10 {
            let single_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes_single_threaded(NULL_AMOUNT, hashes_amount)
            );
            let multi_threaded_hashes: HashSet<(usize, String)> = HashSet::from_iter(
                find_hashes(NULL_AMOUNT, hashes_amount).unwrap()
            );
            for val in single_threaded_hashes.iter() {
                assert!(multi_threaded_hashes.contains(val))
            }
        }
    }

    #[test]
    fn null_amount_null_argument_would_return_error() {
        let error_kind = find_hashes(0, 5).unwrap_err().kind();
        assert_eq!(error_kind, ErrorKind::InvalidValue);
    }

    /// Для безошибочного рассчета хешей в одном потоке
    fn find_hashes_single_threaded(null_amount: usize, hashes_amount: usize) -> Vec<(usize, String)> {
        let pattern = parse_null_arg(null_amount).unwrap();

        let mut hashes = Vec::with_capacity(hashes_amount);
        let mut number = 1usize;

        loop {
            let hash = digest(&number.to_string());
            if hash.ends_with(pattern.as_str()) {
                hashes.push((number, hash))
            };

            if  hashes.len() == hashes_amount {
                return hashes
            }

            number += 1;
        }
    }
}

