pub const JOBS_FILE: &str = "./env/jobs.txt";
pub const JOBS_LOCK: &str = "./env/jobs.txt.lock";

pub const DONE_FILE: &str = "./env/done.txt";
pub const DONE_LOCK: &str = "./env/done.txt.lock";

pub const WORK_FILE: &str = "./env/work.txt";
pub const WORK_LOCK: &str = "./env/work.txt.lock";

pub const CONF_FILE: &str = "./env/orcarc";

pub const JOBS_FOLD: &str = "./env/jobs";

use fs2::FileExt;
use std::io;
use std::{fs::File, path::Path};
use toml::Value;

pub fn acquire_lock(pfile: &str, plock: &str) -> io::Result<(File, File)> {
    let jobs_lock_path = Path::new(plock);
    let jobs_lock = File::create(jobs_lock_path)?;
    jobs_lock.try_lock_exclusive()?;

    let jobs_file = File::open(Path::new(pfile))?;
    return Ok((jobs_file, jobs_lock));
}
pub fn release_lock(lock: &File) -> io::Result<()> {
    lock.unlock()?;
    Ok(())
}
pub fn merge_toml(base: &mut Value, default: &Value) {
    match (base,default) {
        (Value::Table(basetable), Value::Table(deftable)) => {
            for (key, defvalue) in deftable.iter() {
                if ! basetable.contains_key(key) {basetable.insert(key.clone(), defvalue.clone());}
                else {if let Some(baseval) = basetable.get_mut(key) {
                    merge_toml(baseval, defvalue)
                }}
            }
        },
        _ => (),
    }
}


pub const ORCARC_DEFAULT : &str = "\
maxproc = 4
checkinterval = 10
copyfiles = [
    \".densities\",
    \".engrad\",
    \".gbw\",
    \".hess\",
    \".inp\",
    \".opt\",
    \".out\",
    \"_property.txt\",
    \"_trj.xyz\",
    \".xyz\"
]";

