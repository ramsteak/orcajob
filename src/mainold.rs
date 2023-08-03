pub mod common;
pub mod orcarc;


extern crate toml;
extern crate clap;
use whoami;

use clap::{arg, ArgAction, Command};

use common::{JOBS_FILE,CONF_FILE, merge_toml, JOBS_FOLD, JOBS_LOCK, release_lock, acquire_lock_wait, WORK_FILE, DONE_FILE};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::io::{Write, Seek, Read};
use std::{io, path};
use std::fs;
use std::time::{SystemTime,UNIX_EPOCH};
use prettytable::{row, Table};

// fn main(){
//     println!("{:?}", readjob(&"141nb1giytlos64t".to_string()).unwrap());
// }
fn main(){
    // Initialize command parser
    let mut command = Command::new("orcajob").version("0.1.0")
        .subcommand(Command::new("job").about("Schedules a job for execution")
            .args(&[
                arg!(path: [path] "The path of the job folder").default_value(".").required(false)
                ])) 
        .subcommand(Command::new("stop").about("Stops a scheduled command")
            .args(&[
                arg!(id: <id> "The job id to stop, returned by orcajob status")
                ]))
        .subcommand(Command::new("status").about("Returns the status of the commands")
            .args(&[
                arg!(old: -o --old "Lists jobs older than 2 days").action(ArgAction::SetTrue),
                arg!(running: -r --running "Lists currently running jobs").action(ArgAction::SetTrue),
                arg!(completed: -c --completed "Lists all completed jobs").action(ArgAction::SetTrue),
                arg!(active: -a --active "Lists all active jobs").action(ArgAction::SetTrue),
                arg!(all: -A --all "Lists jobs launched by any user").action(ArgAction::SetTrue),
                arg!(user: -U --user "Add the user column").action(ArgAction::SetTrue),
                arg!(id: [id] "The job id")
                ]));

    // Parse args
    // let matches = command.try_get_matches_from_mut(["orcajob","status"]).unwrap();
    let matches = command.get_matches_mut();
    
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
                                match schedule_job(&path) {
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
                            let old = submatches.get_flag("old");
                            let running = submatches.get_flag("running");
                            let completed = submatches.get_flag("completed");
                            let active = submatches.get_flag("active");
                            let all = submatches.get_flag("all");
                            let user = submatches.get_flag("user");
                            let id = submatches.get_one::<String>("id");
                            // TODO: bundle all settings into a single struct

                            // Disallow some flag combinations:
                            // orca status        -> orca status -rca
                            // orca status -o     -> orca status -oc
                            // orca status -a     -> orca status -ra
                            // The output flags are not modified (UA...)
                            let (old,running,completed,active) = match (old,running,completed,active) {
                                (false,false,false,false) => (false,true,true,true),
                                (true,false,false,false) => (true,false,true,false),
                                (false,false,false,true) => (false,true,false,true),
                                _ => (old,running,completed,active)
                            };


                            match get_status(&old, &running, &completed, &active, &all, &user, id) {
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

fn findfile(path: &path::PathBuf, ext:&String) -> Option<path::PathBuf> {
    match fs::read_dir(&path) {
        Err(_) => None,
        Ok(entries) => {
            for entry in entries {
                if let Ok(entry) = entry {
                    let fname = entry.file_name();
                    if let Some(fname) = fname.to_str() {
                        if fname.ends_with(ext) {
                            return Some(path.join(path::PathBuf::from(fname)));
                        }
                    }
                }
            } None
        }
    }
}

fn parse_nprocs(path: &path::PathBuf) -> io::Result<Option<i64>> {
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

fn compile_job(jobtoml: &mut toml::Value, path: &path::PathBuf, jobid: &String, timestamp: u64, orcatoml: toml::Value) -> io::Result<()>{
    let defaultjob = match orcatoml.get("defaultjob") {
        None => {return Err(io::Error::new(io::ErrorKind::InvalidData, "Error in parsing TOML orcarc: missing defaultjob"))}
        Some(defaultjob) => defaultjob.to_owned()
    };
    let inputfile = match findfile(path, &".inp".to_string()) {
        None => {return Err(io::Error::new(io::ErrorKind::NotFound, "Inputfile not found"))},
        Some(inputfile) => inputfile,
    };
    // Should not panic, the inputfile exists and ends in .inp
    let outputfile = inputfile.to_string_lossy().to_string().strip_suffix(".inp").unwrap().to_string() + ".out";

    merge_toml(jobtoml, &defaultjob);

    let nprocs_inp = parse_nprocs(&inputfile)?;
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
        restable.insert("id".to_string(), toml::Value::String(jobid.to_string()));
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

fn schedule_job(path: &path::PathBuf) -> io::Result<String> {
    let jobid = generate_random_id();
    let queuetimestamp = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time error").as_secs();
    let fullpath = match fs::canonicalize(path) {
        Err(_) => {return Err(io::Error::new(io::ErrorKind::Unsupported, "Cannot resolve path"))},
        Ok(fullpath) => fullpath
    };
    let mut jobtoml = match findfile(path, &".job".to_string()) {
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

    compile_job(&mut jobtoml, &fullpath, &jobid, queuetimestamp, orcatoml)?;

    let jobfolder = path::PathBuf::from(JOBS_FOLD).join(&jobid);
    fs::create_dir_all(&jobfolder)?;
    
    for entry in fs::read_dir(path)?{
        if let Ok(entry) = entry {
            let entrypath = entry.path();
            let destination = jobfolder.join(entry.file_name().to_string_lossy().to_string());
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
    
    let lock = acquire_lock_wait(JOBS_LOCK)?;
    
    match fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(JOBS_FILE) {
        Err(e) => {
            release_lock(&lock)?;
            return Err(e)
        },
        Ok(mut file) => {
            match writeln!(file, "{}", jobid) {
                Ok(()) => {release_lock(&lock)?;}
                Err(e) => {release_lock(&lock)?; return Err(e)}
            };
        }
    }

    Ok(jobid)
}

fn stop_job(id: &String) -> io::Result<String> {
    Ok(id.to_string())
}
#[derive(Debug)]
struct JobData {
    id: String,
    scheduled: u64,
    launched: u64,
    ended: u64,
    status: Status,
    user: String,
}
#[derive(Debug)]
enum Status {
    FAILED,
    QUEUED,
    ACTIVE,
    DONE,
    ERROR,
}
fn readjob(job:&String) -> io::Result<JobData> {
    let jobdir = path::PathBuf::from(JOBS_FOLD.to_string()).join(job);
    if let Some(jobpath) = findfile(&jobdir, &".job".to_string()) {
        if let Ok(content) = fs::read_to_string(jobpath) {
            if let Ok(config) = content.parse::<toml::Value>() {
                let resulttable = config.get("result").unwrap();
                let launchtable = config.get("launch").unwrap();

                let user = launchtable.get("username").and_then(|v| v.as_str()).unwrap_or("notset").to_string();

                let id = resulttable.get("id").and_then(|v| v.as_str()).unwrap_or("0000000000000000").to_string();
                let scheduled = resulttable.get("scheduled").and_then(|v| v.as_integer()).unwrap_or_default() as u64;
                let launched = resulttable.get("launched").and_then(|v| v.as_integer()).unwrap_or_default() as u64;
                let ended = resulttable.get("ended").and_then(|v| v.as_integer()).unwrap_or_default() as u64;

                let status = match (scheduled,launched,ended) {
                    (0,0,0) => Status::FAILED,
                    (_,0,0) => Status::QUEUED,
                    (_,_,0) => Status::ACTIVE,
                    (_,_,_) => {
                        match findfile(&jobdir, &".out".to_string()) {
                            None => Status::ERROR,
                            Some(outpath) => {
                                let mut outfile = fs::File::open(outpath)?;
                                let fsize = outfile.seek(io::SeekFrom::End(0))?;
                                // Here use the min value because fsize-1024 can wrap around
                                let pos = std::cmp::min(fsize - 1024, 0);
                                outfile.seek(io::SeekFrom::Start(pos))?;
                                let mut buffer = String::new();
                                outfile.read_to_string(&mut buffer)?;

                                if buffer.contains("****ORCA TERMINATED NORMALLY****") {Status::DONE}
                                else {Status::ERROR}
                            }
                        }
                    },
                };
                return Ok(JobData { id, scheduled, launched, ended, status, user })
            }
        }
    }
    Err(io::Error::new(io::ErrorKind::NotFound, format!("Cannot check on job status {}", job)))
}
fn readjobs(path:&path::PathBuf) -> io::Result<Vec<JobData>> {
    match fs::read_to_string(path) {
        Err(e) => {return Err(e);},
        Ok(jobs) => {
            Ok(jobs.lines()
                .map(|l| l.trim().to_string())
                .map(|id| readjob(&id))
                .filter_map(Result::ok)
                .collect::<Vec<JobData>>())
        }
    }
}

fn is_selected(jd: &JobData, old: &bool, running: &bool, completed: &bool, active: &bool, all: &bool, user: &bool, currentuser: &String) -> bool {
    let select_flag = match jd.status {
        Status::ACTIVE => running.clone(),
        Status::DONE => completed.clone(),
        Status::QUEUED => active.clone(),
        Status::ERROR => completed.clone(),
        Status::FAILED => completed.clone(),
    };
    let select_uname = if jd.user == currentuser.clone() {true} else {user.clone()};
    // TODO: select based off of oldness
    select_flag && select_uname
}

fn get_status(old: &bool, running: &bool, completed: &bool, active: &bool, all: &bool, user: &bool, id: Option<&String>) -> io::Result<String> {
    let mut alljobs: Vec<JobData> = vec![];
    alljobs.extend(readjobs(&path::PathBuf::from(JOBS_FILE))?);
    alljobs.extend(readjobs(&path::PathBuf::from(WORK_FILE))?);
    alljobs.extend(readjobs(&path::PathBuf::from(DONE_FILE))?);

    match id {
        Some(id) => {
            match alljobs.iter().filter(|j| j.id.starts_with(id)).next() {
                None => {return Err(io::Error::new(io::ErrorKind::InvalidInput, "No job with specified id"));},
                Some(job) => {
                    // TODO: pretty print a single line
                    println!("{:?}", job);
                }
            }
        }
        None => {
            let mut table = Table::new();
            if *user {table.add_row(row!["ID", "START", "STATUS", "USER", "TIME"]);}
            else {table.add_row(row!["ID", "START", "STATUS", "TIME"]);}
            
            let currentuser = whoami::username();
            
            for jd in alljobs.iter()
                                    .filter(|j| is_selected(j, old, running, completed, active, all, user, &currentuser))
            {
                let timestr = match jd.ended {
                    0 => (jd.ended - jd.launched).to_string(),
                    _ => "-".to_string(),
                };
                let status = match jd.status{
                    Status::FAILED => "FAILED",
                    Status::QUEUED => "QUEUED",
                    Status::ACTIVE => "ACTIVE",
                    Status::DONE => "DONE",
                    Status::ERROR => "ERROR",
                }.to_string();
                if *user {table.add_row(row![jd.id, jd.scheduled, status, jd.user, timestr]);}
                else {table.add_row(row![jd.id, jd.scheduled, status, timestr]);}
            }
            let mut format = prettytable::format::TableFormat::new();
            format.padding(0, 3);
            table.set_format(format);
            table.printstd();
        },
    }


    let empty = "".to_string();
    Ok(empty)
}