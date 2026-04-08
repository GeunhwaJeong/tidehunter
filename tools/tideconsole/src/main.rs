use anyhow::Result;
use clap::Parser;
use parking_lot::Mutex;
use rhai::{Dynamic, Engine, Scope};
use rustyline::error::ReadlineError;
use rustyline::{Config, DefaultEditor};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "tideconsole",
    about = "Interactive Rhai shell for TideHunter databases"
)]
struct Cli {
    /// Path to a TideHunter database directory to open on startup
    #[arg(short, long)]
    db: Option<PathBuf>,

    /// Rhai snippet to evaluate non-interactively, then exit
    #[arg(short, long)]
    exec: Option<String>,

    /// Path to a Rhai script file to evaluate non-interactively, then exit
    #[arg(short, long)]
    script: Option<PathBuf>,
}

use tideconsole::engine::{ConsoleContext, create_engine, init_scope_with_db, is_complete};

// ---------------------------------------------------------------------------
// REPL
// ---------------------------------------------------------------------------

fn repl(engine: &Engine, scope: &mut Scope) -> Result<()> {
    let rl_config = Config::builder()
        .history_ignore_space(true)
        .max_history_size(1000)
        .unwrap()
        .build();
    let mut rl = DefaultEditor::with_config(rl_config)?;

    let history_path = std::env::var("HOME")
        .ok()
        .map(|h| std::path::PathBuf::from(h).join(".tideconsole_history"));

    if let Some(ref p) = history_path {
        let _ = rl.load_history(p);
    }

    println!("TideConsole — TideHunter Interactive Shell");
    println!("Type help() for available functions, Ctrl+D to exit.\n");

    let mut buf = String::new();

    loop {
        let prompt = if buf.is_empty() { ">> " } else { ".. " };

        match rl.readline(prompt) {
            Ok(line) => {
                // Add non-empty lines to history only at the start of a new expr
                if buf.is_empty() && !line.trim().is_empty() {
                    let _ = rl.add_history_entry(line.as_str());
                }

                if !buf.is_empty() {
                    buf.push('\n');
                }
                buf.push_str(&line);

                if !is_complete(&buf) {
                    continue; // wait for more input
                }

                let input = buf.trim().to_string();
                buf.clear();

                if input.is_empty() {
                    continue;
                }

                match engine.eval_with_scope::<Dynamic>(scope, &input) {
                    Ok(v) if !v.is_unit() => println!("{v}"),
                    Ok(_) => {}
                    Err(e) => eprintln!("Error: {e}"),
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl+C clears current incomplete input
                if !buf.is_empty() {
                    buf.clear();
                    println!("(input cleared)");
                }
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(e) => {
                eprintln!("Readline error: {e}");
                break;
            }
        }
    }

    if let Some(ref p) = history_path {
        let _ = rl.save_history(p);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let cli = Cli::parse();

    let ctx = Arc::new(Mutex::new(ConsoleContext::default()));
    let engine = create_engine(ctx.clone());
    let mut scope = Scope::new();

    if let Some(path) = cli.db {
        init_scope_with_db(&engine, &mut scope, &ctx, &path.display().to_string())?;
    }

    if let Some(code) = cli.exec {
        let result = engine
            .eval_with_scope::<Dynamic>(&mut scope, &code)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if !result.is_unit() {
            println!("{result}");
        }
        return Ok(());
    }

    if let Some(path) = cli.script {
        let code = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("Failed to read {}: {e}", path.display()))?;
        let result = engine
            .eval_with_scope::<Dynamic>(&mut scope, &code)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if !result.is_unit() {
            println!("{result}");
        }
        return Ok(());
    }

    repl(&engine, &mut scope)
}
