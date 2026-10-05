#![no_main]

use libfuzzer_sys::fuzz_target;
use telegenic::genicam::evaluator::{Expr, Value};

fuzz_target!(|data: &[u8]| {
    let Some((&seed, text)) = data.split_first() else {
        return;
    };
    if let Ok(expr) = Expr::parse(&String::from_utf8_lossy(text)) {
        let n = expr.variables().len();
        let ints: Vec<Value> = (0..n)
            .map(|i| Value::I(i64::from(seed) - i as i64 * 1000))
            .collect();
        let floats: Vec<Value> = (0..n)
            .map(|i| Value::F(f64::from(seed) / (i as f64 + 0.5)))
            .collect();
        let _ = expr.eval(&ints);
        let _ = expr.eval(&floats);
    }
});
