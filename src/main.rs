pub mod common;

extern crate toml;
extern crate clap;

use clap::{arg, ArgAction, Command};


fn main(){
    let command = Command::new("orcajob").version("0.1.0")
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
                arg!(active: -a --active "Lists all jobs").action(ArgAction::SetTrue),
                arg!(all: -A --all "Lists jobs launched by any user").action(ArgAction::SetTrue),
                arg!(running: -r --running "Lists currently running jobs").action(ArgAction::SetTrue)
                ]));
    let matches = command.get_matches();

    match matches.subcommand_name() {
        None => {println!("Error"); std::process::exit(-1)},
        Some(subcommand) => {
            let _sub_matches = matches.subcommand_matches(subcommand);
            match subcommand {
                "job" => {},
                "stop" => {},
                "status" => {},
                _ => {println!("Error"); std::process::exit(-1)}
            }
            println!("{}", subcommand);
        }
    }


}