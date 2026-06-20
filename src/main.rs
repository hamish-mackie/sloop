use std::io;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();
    sloop::cli::run(std::env::args_os(), &mut stdout, &mut stderr)
}
