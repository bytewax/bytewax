use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion};

use pyo3::{PyResult, Python};

fn bench_python_code(b: &mut Bencher, source: &str) {
    let gil = Python::acquire_gil();
    let python = gil.python();

    b.iter(|| {
        let res: PyResult<()> = python.run(source, None, None);
        if let Err(e) = res {
            e.print(python);
            panic!("Error running source")
        }
    });
}

pub fn criterion_benchmark(c: &mut Criterion) {
    pyo3::prepare_freethreaded_python();

    let wordcount_python: &str = include_str!("./benchmarks/wordcount_python.py");
    let wordcount_bytewax: &str = include_str!("./benchmarks/wordcount_bytewax.py");

    let mut group = c.benchmark_group("wordcount");
    group.bench_function(BenchmarkId::new("wordcount_python.py", "cpython"), |b| {
        bench_python_code(b, wordcount_python);
    });
    group.bench_function(BenchmarkId::new("wordcount_bytewax.py", "bytewax"), |b| {
        bench_python_code(b, wordcount_bytewax);
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
