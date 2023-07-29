#![allow(unused_imports)]

pub mod common;


extern crate toml;
extern crate clap;
use whoami;

use clap::{arg, ArgAction, Command};

use common::{JOBS_FILE,CONF_FILE, merge_toml, JOBS_FOLD, acquire_lock, JOBS_LOCK, release_lock};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::hash_map::Entry;
use std::io::{BufReader, Read, Write, Seek};
use std::os::windows::prelude::FileExt;
use std::{io, path};
use std::fs::{self, File};
use std::time::{SystemTime,UNIX_EPOCH};


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
    let matches = command.try_get_matches_from_mut(["orcajob","job",".\\env\\work"]).unwrap();
    
    match matcher(matches) {
        Err(_) => {command.print_help().unwrap();}
        Ok(_) => (),
    }
}

fn matcher(matches: clap::ArgMatches) -> Result<(), clap::Error> {
    match matches.subcommand_name() {
        None => Err(clap::Error::new(clap::error::ErrorKind::MissingSubcommand)),
        Some(subcommand) => {
            match matches.subcommand_matches(subcommand) {
                None => Err(clap::Error::new(clap::error::ErrorKind::InvalidSubcommand)),
                Some(submatches) => {
                    match subcommand {
                        "job" => {
                            if let Some(path) = submatches.get_one::<String>("path") {
                                let path = path::PathBuf::from(path);
                                match schedule_job(path) {
                                    Ok(resp) => {println!("{}", resp); Ok(())},
                                    Err(err) => {eprintln!("{}", err); Ok(())}
                                }
                            } else { Err(clap::Error::new(clap::error::ErrorKind::MissingRequiredArgument))}
                        },
                        "stop" => {
                            if let Some(id) = submatches.get_one::<String>("id") {
                                match stop_job(id) {
                                    Ok(resp) => {println!("{}", resp); Ok(())},
                                    Err(err) => {eprintln!("{}", err); Ok(())}
                                }
                            } else { Err(clap::Error::new(clap::error::ErrorKind::MissingRequiredArgument)) }
                        },
                        "status" => {
                            let completed = submatches.get_flag("completed");
                            let all = submatches.get_flag("all");
                            let running = submatches.get_flag("running");
                            match get_status(completed, all, running) {
                                Ok(resp) => {println!("{}", resp); Ok(())},
                                Err(err) => {eprintln!("{}", err); Ok(())}
                                }
                        },
                        _ => Err(clap::Error::new(clap::error::ErrorKind::InvalidSubcommand))
                    }
                }
            }
        }
    }
}

fn generate_random_id() -> String {
    thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(|c| c.to_ascii_lowercase())
            .map(char::from)
            .collect::<String>()
}

fn findfile(path: path::PathBuf, ext:String) -> Option<path::PathBuf> {
    match fs::read_dir(path.clone()) {
        Err(_) => None,
        Ok(entries) => {
            for entry in entries {
                if let Ok(entry) = entry {
                    let fname = entry.file_name();
                    if let Some(fname) = fname.to_str() {
                        if fname.ends_with(&ext) {
                            return Some(path.join(path::PathBuf::from(fname)));
                        }
                    }
                }
            } None
        }
    }
}

fn parse_nprocs(path: path::PathBuf) -> io::Result<Option<i64>> {
    let mut in_pal_block = false;
    for rawline in fs::read_to_string(path)?.lines() {
        let line = rawline.to_lowercase();
        let line = line.trim();

        if line.starts_with("%") && line.contains("pal") {in_pal_block = true; continue;}
        else if in_pal_block && line.contains("end") {in_pal_block = false; continue;}

        if in_pal_block && line.contains("nprocs") {
            if let Some(value) = line.split_whitespace().skip(1).next() {
                if let Ok(value) = value.parse::<i64>() {
                    return Ok(Some(value))
                }
            }
        }

        if line.starts_with("!") && line.contains("pal") {
            for block in line.split_whitespace() {
                if block.contains("pal") {
                    let value = block
                            .chars()
                            .skip_while(|c| !c.is_digit(10))
                            .take_while(|c| c.is_digit(10))
                            .collect::<String>();
                    if let Ok(value) = value.parse::<i64>() {
                        return Ok(Some(value))
                    }
                }
            }
        }
    };

    Ok(None)
}

fn compile_job(jobtoml: &mut toml::Value, path: path::PathBuf, jobid: String, timestamp: u64, orcatoml: toml::Value) -> io::Result<()>{
    let defaultjob = match orcatoml.get("defaultjob") {
        None => {return Err(io::Error::new(io::ErrorKind::InvalidData, "Error in parsing TOML orcarc: missing defaultjob"))}
        Some(defaultjob) => defaultjob.to_owned()
    };
    let inputfile = match findfile(path.clone(), ".inp".to_string()) {
        None => {return Err(io::Error::new(io::ErrorKind::NotFound, "Inputfile not found"))},
        Some(inputfile) => inputfile,
    };
    // Should not panic, the inputfile exists and ends in .inp
    let outputfile = inputfile.to_string_lossy().to_string().strip_suffix(".inp").unwrap().to_string() + ".out";

    merge_toml(jobtoml, &defaultjob);

    let nprocs_inp = parse_nprocs(inputfile.clone())?;
    let nprocs_job = match jobtoml.get("scheduling") {
        None => None,
        Some(toml::Value::Table(schedule)) => {
            schedule.get("nprocs").and_then(|v| v.as_integer())
        },
        Some(_) => None
    }; 
    match (nprocs_inp, nprocs_job) {
        (Some(n1), Some(n2)) => {
            if n1 != n2 {return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("nprocs collision: {} != {}", n1,n2)))}
        },
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Set nprocs in both inp and job file"))
    }

    if let Some(jobtable) = jobtoml.as_table_mut() {
        let mut restable = toml::Table::new();
        restable.insert("path".to_string(), toml::Value::String(path.to_string_lossy().to_string()));
        restable.insert("id".to_string(), toml::Value::String(jobid));
        restable.insert("scheduled".to_string(), toml::Value::Integer(timestamp as i64));

        let mut launchtable = toml::Table::new();
        launchtable.insert("input".to_string(), toml::Value::String(inputfile.to_string_lossy().to_string()));
        launchtable.insert("output".to_string(), toml::Value::String(outputfile));
        launchtable.insert("username".to_string(), toml::Value::String(whoami::username()));


        jobtable.insert("result".to_string(), toml::Value::Table(restable));
        jobtable.insert("launch".to_string(), toml::Value::Table(launchtable));
    }

    Ok(())
}

fn schedule_job(path: path::PathBuf) -> io::Result<String> {
    let jobid = generate_random_id();
    let queuetimestamp = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time error").as_secs();
    let fullpath = match fs::canonicalize(path.clone()) {
        Err(_) => {return Err(io::Error::new(io::ErrorKind::Unsupported, "Cannot resolve path"))},
        Ok(fullpath) => fullpath
    };
    let mut jobtoml = match findfile(path.clone(), ".job".to_string()) {
        None => {
            eprintln!("The directory is missing a .job file. Create one before proceeding");
            return Err(io::Error::new(io::ErrorKind::NotFound, "Jobfile not found"))
        },
        Some(jobpath) => {
            match fs::read_to_string(jobpath) {
                Err(_) => return Err(io::Error::new(io::ErrorKind::NotFound, "Cannot read jobfile")),
                Ok(cont) => {
                    match cont.parse::<toml::Value>() {
                        Err(e) => {eprintln!("{:?}", e);return Err(io::Error::new(io::ErrorKind::InvalidData, "Error in parsing TOML jobfile"))},
                        Ok(config) => config
                    }
                }
            }
        }
    };
    let orcatoml = {
        let orcarcpath = path::PathBuf::from(CONF_FILE);
        match fs::read_to_string(orcarcpath) {
            Err(_) => return Err(io::Error::new(io::ErrorKind::NotFound, "Cannot read orcarc")),
            Ok(cont) => {
                match cont.parse::<toml::Value>() {
                    Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "Error in parsing TOML orcarc")),
                    Ok(config) => config
                }
            }
        }
    };

    compile_job(&mut jobtoml, fullpath, jobid.clone(), queuetimestamp, orcatoml)?;

    let jobfolder = path::PathBuf::from(JOBS_FOLD).join(jobid.clone());
    fs::create_dir_all(jobfolder.clone())?;
    
    for entry in fs::read_dir(path.clone())?{
        if let Ok(entry) = entry {
            let entrypath = entry.path();
            let destination = jobfolder.clone().join(entry.file_name().to_string_lossy().to_string());
            if entry.file_name().to_string_lossy().to_string().ends_with(".job") {
                let mut jfile = fs::File::create(destination)?;
                let tomlstr = toml::to_string_pretty(&jobtoml).unwrap();
                let tomlcontent = tomlstr.as_bytes();
                jfile.write_all(tomlcontent)?;
            } else {
                if entrypath.is_file() {
                    fs::copy(entrypath, destination)?;
                }
            }
        }
    }
    let (mut file,lock) = acquire_lock(JOBS_FILE, JOBS_LOCK)?;

    match file.seek(io::SeekFrom::End(0)) {
        Err(e) => {
            release_lock(&lock)?;
            return Err(e)},
        Ok(_) => {
            let line = jobid.clone() + "\n";
            match file.write(line.as_bytes()) {
                Err(e) => {
                    release_lock(&lock)?;
                    return Err(e)
                },
                Ok(_) => {release_lock(&lock)?;},
            }
        }
    };

    Ok(jobid)
}

fn stop_job(id: &String) -> io::Result<String> {
    Ok(id.to_string())
}

fn get_status(completed:bool, all: bool, running: bool) -> io::Result<String> {
    Ok("status".to_string())
}