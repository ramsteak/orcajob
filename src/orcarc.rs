use std::env;
use std::fs;
use std::io;
use std::path;
use std::time::Duration;
use toml;
use regex::Regex;

#[derive(Debug)]
pub struct Config {
    maxprocs: usize,
    checkinterval: Duration,
    deleteafter: Duration,
    jobsfolder: path::PathBuf,
    defaultjob: toml::Table,
}
#[allow(dead_code)]
#[derive(Debug)]
pub struct ValueError {
    field: String,
    message: String,
}
impl ValueError{
    pub fn new<F,M>(field: F, message: M) -> Self
    where
        F: ToString,
        M: ToString,
    { 
        ValueError { field: field.to_string(), message: message.to_string() }
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
fn split_duration(timestr: String) -> Vec<String>{
    let re = Regex::new(r"(\d+)[A-Za-z]+").unwrap();
    re.find_iter(&timestr)
        .map(|m|m.as_str().to_string())
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
        let (num,unit): (String,String) =
                        timestr.chars()
                        .partition(|c|c.is_digit(10));
        if let Some(time) = get_duration(num, unit){totaltime += time}
    }
    Some(totaltime)
}

pub fn load_config() -> Result<Config, ConfigError> {
    // As current_exe is always a file (not root) this will not panic
    let currentexe = env::current_exe()?;
    let exedir = match currentexe.parent(){
        Some(exedir) => exedir,
        None => {return Err(ConfigError::IO(io::Error::new(io::ErrorKind::NotFound, "Parent directory not found")))}
    };
    let orcapath = exedir.join("orcarc");
    let content = fs::read_to_string(orcapath)?;

    let orcarc = content.parse::<toml::Table>()?;

    let maxprocs = orcarc
        .get("maxprocs")
        .and_then(|v| v.as_integer())
        .unwrap_or(1) as usize;

    let checkinterval = str_to_duration(orcarc
        .get("checkinterval")
        .and_then(|v|v.as_str())
        .unwrap_or("0s").to_string())
        .unwrap_or_default();

    let deleteafter = str_to_duration(orcarc
        .get("deleteafter")
        .and_then(|v|v.as_str())
        .unwrap_or("10d").to_string())
        .unwrap_or_default();
    
    let jobsdir = orcarc
        .get("jobsfolder")
        .and_then(|v|v.as_str())
        .and_then(|v|Some(path::PathBuf::from(v)));
    if jobsdir == None {return Err(ConfigError::Val(ValueError::new("jobsfolder", "No jobs path specified")))};
    let jobsdir = jobsdir.unwrap();
    if !jobsdir.exists() {return Err(ConfigError::IO(io::Error::new(io::ErrorKind::NotFound, "Job path not specified")))}

    let defaultjob = orcarc
        .get("defaultjob")
        .and_then(|v|v.as_table());
    if defaultjob == None {return Err(ConfigError::Val(ValueError::new("defaultjob", "No default job specification")))};
    let defaultjob = defaultjob.unwrap().clone();
    
    Ok(Config {
        maxprocs: maxprocs,
        checkinterval: checkinterval,
        deleteafter: deleteafter,
        jobsfolder: jobsdir,
        defaultjob: defaultjob })
}

fn main(){
    let conf = load_config();
    println!("{:?}", conf)
}