use regex::Regex;
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use toml;

#[derive(Debug)]
pub struct Config {
    pub maxprocs: usize,
    pub checkinterval: Duration,
    pub deleteafter: Duration,
    pub jobsfolder: PathBuf,
    pub defaultjob: Job,
}
#[derive(Debug)]
pub struct ValueError {
    pub field: String,
    pub message: String,
}
impl ValueError {
    pub fn new<F, M>(field: F, message: M) -> Self
    where
        F: ToString,
        M: ToString,
    {
        ValueError {
            field: field.to_string(),
            message: message.to_string(),
        }
    }
}
#[derive(Debug)]
pub enum ConfigError {
    IO(io::Error),
    Toml(toml::de::Error),
    Val(ValueError),
}
impl From<io::Error> for ConfigError {
    fn from(error: io::Error) -> Self {
        ConfigError::IO(error)
    }
}
impl From<toml::de::Error> for ConfigError {
    fn from(error: toml::de::Error) -> Self {
        ConfigError::Toml(error)
    }
}
fn split_duration(timestr: String) -> Vec<String> {
    let re = Regex::new(r"(\d+)[A-Za-z]+").unwrap();
    re.find_iter(&timestr)
        .map(|m| m.as_str().to_string())
        .collect()
}
fn get_duration(num: String, unit: String) -> Option<Duration> {
    let num = num.parse::<u64>().unwrap_or_default();
    let mult = match unit.as_str() {
        "" => 1,
        "s" => 1,
        "sec" => 1,
        "second" => 1,
        "seconds" => 1,
        "m" => 60,
        "min" => 60,
        "minute" => 60,
        "minutes" => 60,
        "h" => 3600,
        "hr" => 3600,
        "hour" => 3600,
        "hours" => 3600,
        "d" => 86400,
        "day" => 86400,
        "days" => 86400,
        "mo" => 2592000,
        "mon" => 2592000,
        "month" => 2592000,
        _ => 0,
    };
    Some(Duration::from_secs(num * mult))
}
fn str_to_duration(timestr: String) -> Option<Duration> {
    let times = split_duration(timestr);
    let mut totaltime = Duration::from_secs(0);

    for timestr in times {
        let (num, unit): (String, String) = timestr.chars().partition(|c| c.is_digit(10));
        if let Some(time) = get_duration(num, unit) {
            totaltime += time
        }
    }
    Some(totaltime)
}

pub fn load_config() -> Result<Config, ConfigError> {
    // As current_exe is always a file (not root) this will not panic
    let currentexe = env::current_exe()?;
    let exedir = match currentexe.parent() {
        Some(exedir) => exedir,
        None => {
            return Err(ConfigError::IO(io::Error::new(
                io::ErrorKind::NotFound,
                "Parent directory not found",
            )))
        }
    };
    let orcapath = exedir.join("orcarc");
    let content = fs::read_to_string(orcapath)?;

    let orcarc = content.parse::<toml::Table>()?;

    let maxprocs = orcarc
        .get("maxprocs")
        .and_then(|v| v.as_integer())
        .unwrap_or(1) as usize;

    let checkinterval = str_to_duration(
        orcarc
            .get("checkinterval")
            .and_then(|v| v.as_str())
            .unwrap_or("0s")
            .to_string(),
    )
    .unwrap_or_default();

    let deleteafter = str_to_duration(
        orcarc
            .get("deleteafter")
            .and_then(|v| v.as_str())
            .unwrap_or("10d")
            .to_string(),
    )
    .unwrap_or_default();

    let jobsdir = orcarc
        .get("jobsfolder")
        .and_then(|v| v.as_str())
        .and_then(|v| Some(PathBuf::from(v)));
    if jobsdir == None {
        return Err(ConfigError::Val(ValueError::new(
            "jobsfolder",
            "No jobs path specified",
        )));
    };
    let jobsdir = jobsdir.unwrap();
    if !jobsdir.exists() {
        return Err(ConfigError::IO(io::Error::new(
            io::ErrorKind::NotFound,
            "Job path not specified",
        )));
    }

    let defaultjob = orcarc.get("defaultjob").and_then(|v| v.as_table());
    if defaultjob == None {
        return Err(ConfigError::Val(ValueError::new(
            "defaultjob",
            "No default job specification",
        )));
    };
    let defaultjob = defaultjob.unwrap().clone();
    let defaultjob = load_job_mut(defaultjob);

    Ok(Config {
        maxprocs,
        checkinterval,
        deleteafter,
        jobsfolder: jobsdir,
        defaultjob,
    })
}

#[derive(Debug, Default)]
pub struct After {
    pub copyfiles: Vec<String>,
}

#[derive(Debug, Default)]
pub struct Launch {
    pub input: PathBuf,
    pub output: PathBuf,
    pub username: String,
}

#[derive(Debug, Default)]
pub enum Status {
    #[default]
    NONE,
    INPROGRESS,
    QUEUED,
    ERROR,
    DONE,
}

#[derive(Debug)]
pub struct JobResult {
    pub id: String,
    pub path: PathBuf,
    pub scheduled: SystemTime,
    pub launched: SystemTime,
    pub ended: SystemTime,
    pub status: Status,
}
impl Default for JobResult {
    fn default() -> Self {
        JobResult {
            id: String::default(),
            path: PathBuf::default(),
            scheduled: UNIX_EPOCH,
            launched: UNIX_EPOCH,
            ended: UNIX_EPOCH,
            status: Status::default(),
        }
    }
}

#[derive(Debug)]
pub enum Notification {
    EMAIL(String),
}
fn tabtonotify(tab: &toml::map::Map<String, toml::Value>) -> Option<Notification> {
    if let Some(addr) = tab.get("email") {
        let addr = addr.as_str()?.to_string();
        return Some(Notification::EMAIL(addr));
    };
    None
}

#[derive(Debug, Default)]
pub enum RestartPolicy {
    #[default]
    NONE,
    RESTART(usize),
    CONTINUE(usize),
}

#[derive(Debug, Default)]
pub struct Scheduling {
    pub priority: usize,
    pub maxtime: Duration,
    pub nprocs: usize,
    pub restartpolicy: RestartPolicy,
}

#[derive(Debug, Default)]
pub struct Job {
    pub name: String,
    pub author: String,
    pub after: After,
    pub launch: Launch,
    pub notify: Vec<Notification>,
    pub result: JobResult,
    pub scheduling: Scheduling,
}

fn load_job_mut(job: toml::map::Map<String, toml::Value>) -> Job {
    let name = job
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let author = job
        .get("author")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let after = match job.get("after").and_then(|v| v.as_table()) {
        None => After::default(),
        Some(aftertable) => {
            let copyfiles = aftertable
                .get("copyfiles")
                .and_then(|v| v.as_array())
                .map(|v| {
                    v.iter()
                        .filter_map(|i| i.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            After { copyfiles }
        }
    };
    let launch = match job.get("launch").and_then(|v| v.as_table()) {
        None => Launch::default(),
        Some(launchtable) => {
            let input = launchtable
                .get("input")
                .and_then(|v| v.as_str())
                .and_then(|v| Some(PathBuf::from(v)))
                .unwrap_or_default();
            let output = launchtable
                .get("output")
                .and_then(|v| v.as_str())
                .and_then(|v| Some(PathBuf::from(v)))
                .unwrap_or_default();
            let uname = launchtable
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            Launch {
                input: input,
                output: output,
                username: uname,
            }
        }
    };

    let notify = match job.get("notify").and_then(|v| v.as_array()) {
        None => Vec::<Notification>::default(),
        Some(notifylist) => notifylist
            .iter()
            .filter_map(|v| v.as_table())
            .filter_map(|v| tabtonotify(v))
            .collect::<Vec<Notification>>(),
    };

    let result = match job.get("result").and_then(|v| v.as_table()) {
        None => JobResult::default(),
        Some(resulttable) => {
            let id = resulttable
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let path = resulttable
                .get("path")
                .and_then(|v| v.as_str())
                .and_then(|v| Some(PathBuf::from(v)))
                .unwrap_or_default();
            let sched = resulttable
                .get("scheduled")
                .and_then(|v| v.as_integer())
                .and_then(|v| Some(UNIX_EPOCH + Duration::from_secs(v as u64)))
                .unwrap_or(UNIX_EPOCH);
            let launch = resulttable
                .get("launched")
                .and_then(|v| v.as_integer())
                .and_then(|v| Some(UNIX_EPOCH + Duration::from_secs(v as u64)))
                .unwrap_or(UNIX_EPOCH);
            let ended = resulttable
                .get("ended")
                .and_then(|v| v.as_integer())
                .and_then(|v| Some(UNIX_EPOCH + Duration::from_secs(v as u64)))
                .unwrap_or(UNIX_EPOCH);
            let status = match resulttable.get("status").and_then(|v| v.as_str()) {
                None => Status::NONE,
                Some("QUEUED") => Status::QUEUED,
                Some("DONE") => Status::DONE,
                Some("ERROR") => Status::ERROR,
                Some("INPROGRESS") => Status::INPROGRESS,
                Some(_) => Status::NONE,
            };

            JobResult {
                id: id,
                path: path,
                scheduled: sched,
                launched: launch,
                ended: ended,
                status: status,
            }
        }
    };

    let scheduling = match job.get("scheduling").and_then(|v| v.as_table()) {
        None => Scheduling::default(),
        Some(scheduletable) => {
            let maxrestart = scheduletable
                .get("maxrestart")
                .and_then(|v| v.as_integer())
                .unwrap_or_default() as usize;
            let maxtime = str_to_duration(
                scheduletable
                    .get("maxtime")
                    .and_then(|v| v.as_str())
                    .unwrap_or("1h")
                    .to_string(),
            )
            .unwrap_or_default();
            let nprocs = scheduletable
                .get("nprocs")
                .and_then(|v| v.as_integer())
                .unwrap_or(1) as usize;
            let priority = scheduletable
                .get("priority")
                .and_then(|v| v.as_integer())
                .unwrap_or(1) as usize;
            let restartpolicy = match scheduletable.get("restartpolicy").and_then(|v| v.as_str()) {
                None => RestartPolicy::NONE,
                Some("RESTART") => RestartPolicy::RESTART(maxrestart),
                Some("CONTINUE") => RestartPolicy::CONTINUE(maxrestart),
                _ => RestartPolicy::NONE,
            };

            Scheduling {
                maxtime,
                nprocs,
                priority,
                restartpolicy,
            }
        }
    };

    let job = Job {
        name: name,
        author: author,
        after: after,
        launch: launch,
        notify: notify,
        result: result,
        scheduling: scheduling,
    };
    job
}
