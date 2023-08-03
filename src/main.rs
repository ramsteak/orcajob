mod orcarc;

use orcarc::load_config;


fn main(){
    let conf = load_config().unwrap();
    let dfj = conf.defaultjob;
    println!("{:?}", dfj);
}