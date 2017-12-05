extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate futures;
extern crate futures_fs;
extern crate hyper;
extern crate tokio_core;
extern crate zip;

use std::io::{self, Write};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use futures::{Future, Stream};
use hyper::Client;
use hyper::client::{HttpConnector, FutureResponse};
use tokio_core::reactor::Core;


#[derive(Debug)]
#[derive(Serialize, Deserialize)]
struct MesosTaskInfo {
	sandbox_directory: String,
	items: Vec<MesosFetcherItem>
}

#[derive(Serialize, Deserialize)]
#[derive(Debug)]
struct MesosFetcherItem {
	uri: URI,
	action: String
}

#[derive(Serialize, Deserialize)]
#[derive(Debug)]
struct URI {
	value: String
}

fn parse(data: &String) -> Result<MesosTaskInfo, serde_json::Error> {
	let info: MesosTaskInfo = serde_json::from_str(data)?;
	Ok(info)
}

fn process(info: &MesosTaskInfo) -> () {
	let mut core = Core::new().unwrap();
	let client = Client::new(&core.handle());

	info.items.iter().for_each(|item| {
		println!("{:?}", item);
		let path = fetch(&mut core, &client, &item.uri, &info.sandbox_directory).unwrap();
		match path.extension() {
			Some(ext) if ext == "zip" => {
				println!("Unzipping {}", path.display());
				unzip(&path);
			},
			Some(_) => (),
			None => ()			
		}
	})
}

fn build_path(parsed: &hyper::Uri, dest: &String) -> PathBuf {
	let parsed_path = parsed.path().to_string();
	let file_name = Path::new(&parsed_path).file_name().unwrap();
	let mut my_dest = dest.to_string();
	my_dest.push_str("/destfile");
	Path::new(&my_dest).with_file_name(file_name)
}

fn unzip(path: &PathBuf) -> () {
	let foo = path.as_path();
	let zipfile = File::open(path).unwrap();
	let mut archive = zip::ZipArchive::new(zipfile).unwrap();
	
	for i in 0..archive.len() {
		let mut file = archive.by_index(i).unwrap();
		let mut outpath = foo.with_file_name(file.name());
		println!("{} -> {:?}", file.name(), outpath);
		
		if (&*file.name()).ends_with('/') {
			fs::create_dir_all(&outpath).unwrap();
		} else {
			if let Some(p) = outpath.parent() {
				if !p.exists() {
					fs::create_dir_all(&outpath).unwrap();
				}
			}

			let mut outfile = File::create(outpath).unwrap();
			io::copy(&mut file, &mut outfile).unwrap();
		}
	}
}

fn fetch(core: &mut Core, client: &Client<HttpConnector>, uri: &URI, dest: &String) -> Result<PathBuf, hyper::Error> {
	println!("Fetching {:?}", uri);
	let parsed: hyper::Uri = uri.value.parse().unwrap();
	let dest_path = build_path(&parsed, dest);
	println!("Fetching {} to {:?}", uri.value, dest_path);
	let outfile = &std::fs::File::create(&dest_path)?;
	let mut outwriter = io::BufWriter::new(outfile);

	let work = client.get(parsed).and_then(|res| {
		println!("Response {}", res.status());
		res.body().for_each(|chunk| {
			outwriter
				.write_all(&chunk)
				.map(|_| ())
				.map_err(From::from)
		})
	});

	core.run(work)?;
	outfile.sync_all()?;
	Ok(dest_path)
}

fn main() {
	match parse(&"{\"sandbox_directory\": \"C:\\\\temp\\\\fetchtest\", \"items\": [{\"uri\": {\"value\":\"http://some/zip/url\"}, \"action\": \"fetch\"}]}".to_string()) {
		Ok(data) => process(&data),
		Err(err) => println!("{}", err)
	};
}
