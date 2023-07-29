#![allow(unused_imports)]
pub mod common;


use common::{JOBS_FILE, JOBS_LOCK, DONE_FILE, DONE_LOCK, WORK_FILE, WORK_LOCK, CONF_FILE, ORCARC_DEFAULT, JOBS_FOLD};
use common::{acquire_lock,release_lock,merge_toml};
use std::fs;
use std::io::{self,BufReader, Seek, SeekFrom, Read, BufRead};
use std::path;
use std::time::Duration;
use toml;
use std::thread;
use std::fs::File;

fn read_config(path: &str) -> Config {
    let confstr = match fs::read_to_string(path::Path::new(path)) {
        Ok(content) => content,
        Err(_) => {
            // File does not exist
            ORCARC_DEFAULT.to_string()
        }
    };
    let default = ORCARC_DEFAULT.to_string().parse::<toml::Value>().unwrap();

    let mut conf = match confstr.parse::<toml::Value>() {
        Ok(conf) => conf,
        // This line should never result in panic, the fixed values for orcarc
        // should always work
        Err(_) => ORCARC_DEFAULT.to_string().parse::<toml::Value>().unwrap(),
    };
    merge_toml(&mut conf, &default);

    Config {
        maxproc: conf.get("maxproc").and_then(|v| v.as_integer()).unwrap_or(1) as usize,
        checkinterval: conf.get("checkinterval").and_then(|v| v.as_integer()).unwrap_or(30) as u64,
        copyfiles: conf.get("copyfiles")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter().filter_map(|i| i.as_str().map(|s| s.to_string())).collect::<Vec<String>>()
                    }).unwrap_or_default(),
    }
}

struct Config {
    maxproc: usize,
    copyfiles: Vec<String>,
    checkinterval: u64,
}
fn main() {
    // Load configuration
    let config = read_config(CONF_FILE);
    let mut cores = 0;

    println!("{}", read_second_to_last_line(".\\src\\daemon.rs").unwrap());
    
    // Check for interrupted jobs
    // TODO

    // Begin main loop
    loop {
        // Check for job completeness
        completedjobs();

        // Check for available cores
        cores = getusedcores();

        // Check for available jobs
        let availablecores = config.maxproc - cores;
        let job = get_new_job(availablecores);
        
        // Start new jobs
        start_new_job(job);

        // Sleep
        thread::sleep(Duration::from_secs(config.checkinterval));
    }
}



fn read_second_to_last_line(file_path: &str) -> io::Result<String> {
    // ChatGPT TODO
    // This code is not that good, it appears that for very long lines it might 
    // encounter errors. As the orca files do not have very long lines I will ignore it
    // for the time being. -_-
    const BUFFER_SIZE: usize = 1024;

    // Open the file for reading and create a buffered reader
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);

    // Get the file size and calculate the starting position for reverse reading
    let file_size = reader.seek(SeekFrom::End(0))?;

    if file_size == 0 {
        // Empty file, no lines to read
        return Err(io::Error::new(io::ErrorKind::NotFound, "File is empty"));
    }

    let mut pos = if file_size < BUFFER_SIZE as u64 {0}
    else {file_size - BUFFER_SIZE as u64};

    let mut buffer = [0; BUFFER_SIZE];

    loop {
        // Seek to the calculated position and read a chunk of data into the buffer
        reader.seek(SeekFrom::Start(pos))?;
        let bytes_read = reader.read(&mut buffer)?;

        // Iterate through the buffer in reverse, looking for two newline characters
        for i in (0..bytes_read).rev() {
            if buffer[i] == b'\n' {
                // We found the last newline character, now look for the second one
                for j in (0..i).rev() {
                    if buffer[j] == b'\n' {
                        // We found the second-to-last newline character
                        // Extract the line between the two newline characters and return it
                        let mut line = Vec::with_capacity(i - j + 1);
                        line.extend_from_slice(&buffer[j + 1..i + 1]);
                        return String::from_utf8(line).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e));
                    }
                }
            }
        }

        if pos == 0 {
            // We reached the beginning of the file, break the loop
            break;
        }

        // Move the position backwards to read the next chunk
        pos = if pos < BUFFER_SIZE as u64 {
            0
        } else {
            pos - BUFFER_SIZE as u64
        };
    }

    // If we reach this point, we didn't find two newline characters in the entire file
    Err(io::Error::new(io::ErrorKind::NotFound, "Two newline characters not found"))
}
fn check_job_complete(jobpath: path::PathBuf) -> io::Result<bool> {
    Ok(false)
}

fn completedjobs() -> io::Result<()>{
    let (work, lock) = acquire_lock(WORK_FILE, WORK_LOCK)?;
    let inprogress = BufReader::new(work);

    for job in inprogress.lines() {
        if let Ok(job) = job {
            let jobdir = path::Path::new(JOBS_FOLD).join(job);
        }
    }
    release_lock(&lock)?;
    Ok(())
}

fn getusedcores() -> usize {0}

fn get_new_job(ncores: usize) -> String {"a".to_string()}

fn start_new_job(job: String) {}