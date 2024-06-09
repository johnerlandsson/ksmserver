use dashmap::DashMap;
use ksmparser::ParseError;
use polars::prelude::*;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;
use tide::log;
use std::env;

pub struct Environment {
    pub bind_addr: String,
    pub art_path: String,
    pub dat_path: String,
}

impl Environment {
    pub fn new() -> Environment {
        Environment {
            bind_addr: env::var("BIND_ADDRESS").unwrap_or(String::from("127.0.0.1:8080")),
            art_path: env::var("KSM_ART_PATH").unwrap_or(String::from(".")).to_owned(),
            dat_path: env::var("KSM_DAT_PATH").unwrap_or(String::from(".")).to_owned(),
        }
    }
}

/// Represents the state of the server application, holding shared resources.
#[derive(Clone)]
pub struct AppState<'a> {
    pub measurement_data: Arc<KSMData<'a>>,
    pub parameter_data: Arc<KSMData<'a>>,
}

/// Represents a structure for storing the contents of a KSMFile and its modification time
pub struct KSMFile {
    pub lazyframe: LazyFrame,
    modified: SystemTime,
}
/// Represents a structure that holds and manages lazy-loaded data frames loaded from files in the KSM system.
pub struct KSMData<'a> {
    pub data: DashMap<String, KSMFile>,
    dir_path: String,
    file_extension: &'a str,
    parse_function: fn(file_path: PathBuf) -> Result<DataFrame, ParseError>,
}
impl<'a> KSMData<'a> {
    /// Creates a new instance of KSMData.
    pub fn new(
        dir_path: String,
        file_extension: &'a str,
        parse_function: fn(file_path: PathBuf) -> Result<DataFrame, ParseError>,
    ) -> Self {
        KSMData {
            data: DashMap::new(),
            dir_path,
            file_extension,
            parse_function,
        }
    }

    /// Loads data frames from files in the specified directory and stores them in the concurrent map.
    ///
    /// This function reads the directory specified by `dir_path`, checks each file for the specified `file_extension`,
    /// and parses the file if it is modified more recently than the stored version. The parsed data frame is stored
    /// in a concurrent map with the file name as the key.
    ///
    /// # Returns
    /// A `Result` which is `Ok(())` if all files are processed successfully, or a `ParseError` if any error occurs.
    pub async fn sync_data(&self, stop: Arc<AtomicBool>) -> Result<(), ParseError> {
        for entry in fs::read_dir(&self.dir_path).map_err(|_| ParseError::ReadFolderError)? {
            if stop.load(Ordering::Relaxed) {
                break;
            }

            let entry = entry.map_err(|_| ParseError::ReadFolderError)?;
            let current_entry_modified = entry
                .metadata()
                .map_err(|_| ParseError::ReadMetadataError)?
                .modified()
                .map_err(|_| ParseError::ReadMetadataError)?;

            let path = entry.path();

            // Check if the file has the correct extension
            if let Some(extension) = path.extension().and_then(|ext| ext.to_str()) {
                if extension == self.file_extension {
                    if let Some(file_name) = path.file_stem().and_then(|name| name.to_str()) {
                        let stored_entry_modified = match self.data.get(file_name) {
                            Some(ksmfile) => ksmfile.modified,
                            None => SystemTime::UNIX_EPOCH,
                        };

                        // Parse and store the file if it is modified more recently
                        if current_entry_modified > stored_entry_modified {
                            log::info!("Loading {}...", file_name);
                            let parse_function = self.parse_function;
                            let data_frame = parse_function(path.clone())?;
                            let ksm_file_entry = KSMFile {
                                lazyframe: data_frame.lazy(),
                                modified: current_entry_modified,
                            };
                            self.data.insert(file_name.to_owned(), ksm_file_entry);
                        }
                    } else {
                        return Err(ParseError::FileNameExtractionError);
                    }
                }
            }
        }
        Ok(())
    }
}
