pub mod common;

extern crate toml;
extern crate clap;

use clap::{arg, ArgAction, Command};

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};


fn main(){
    // Initialize command parser
    let mut command = Command::new("orcajob").version("0.1.0")
        .subcommand(Command::new("job").about("Schedules a job for execution")
            .args(&[
                arg!(path: [path] "The path of the job folder").default_value(".").required(false)
                ])) 
        .subcommand(Command::new("stop").about("Stops a scheduled command")
            .args(&[
                arg!(id: <id> "The id of the command")
                ]))
        .subcommand(Command::new("status").about("Returns the status of the commands")
            .args(&[
                arg!(completed: -c --completed "Lists all jobs").action(ArgAction::SetTrue),
                arg!(all: -a --all "Lists jobs launched by any user").action(ArgAction::SetTrue),
                arg!(running: -r --running "Lists currently running jobs").action(ArgAction::SetTrue)
                ]));

    // Parse args
    let matches = command.get_matches_mut();
    
    match matcher(matches) {
        Err(_) => {command.print_help().unwrap();}
        Ok(msg) => println!("{}", msg),
    }
}

fn matcher(matches: clap::ArgMatches) -> Result<String, clap::Error> {
    match matches.subcommand_name() {
        None => Err(clap::Error::new(clap::error::ErrorKind::MissingSubcommand)),
        Some(subcommand) => {
            match matches.subcommand_matches(subcommand) {
                None => Err(clap::Error::new(clap::error::ErrorKind::InvalidSubcommand)),
                Some(submatches) => {
                    match subcommand {
                        "job" => {
                            if let Some(path) = submatches.get_one::<String>("path") {
                                Ok(schedule_job(path))
                            } else { Err(clap::Error::new(clap::error::ErrorKind::MissingRequiredArgument))}
                        },
                        "stop" => {
                            if let Some(id) = submatches.get_one::<String>("id") {
                                Ok(stop_job(id))
                            } else { Err(clap::Error::new(clap::error::ErrorKind::MissingRequiredArgument)) }
                        },
                        "status" => {
                            let completed = submatches.get_flag("completed");
                            let all = submatches.get_flag("all");
                            let running = submatches.get_flag("running");
                            Ok(get_status(completed, all, running))
                        },
                        _ => Err(clap::Error::new(clap::error::ErrorKind::InvalidSubcommand))
                    }
                }
            }
        }
    }
}

fn generate_random_id() -> String {
    thread_rng().sample_iter(&Alphanumeric).take(16).map(|c| c).map(char::from).collect::<String>()
}

fn schedule_job(path: &String) -> String {
    let jobid = generate_random_id();
    
    
    
    jobid
}

fn stop_job(id: &String) -> String {
    id.to_string()
}

fn get_status(completed:bool, all: bool, running: bool) -> String {
    "status".to_string()
}