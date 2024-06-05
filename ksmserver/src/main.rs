use async_std::task;
use chrono::NaiveDate;
use dashmap::DashMap;
use ksmparser::article::parse_art_file;
use ksmparser::measurement::parse_dat_file;
use ksmparser::ParseError;
use polars::prelude::*;
use polars_io::json::JsonWriter;
use serde::Deserialize;
use signal_hook;
use std::fmt;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{self, SystemTime};
use tide::{log, Request, Response, StatusCode};

/// Represents a structure for storing the contents of a KSMFile and its modification time
struct KSMFile {
    lazyframe: LazyFrame,
    modified: SystemTime,
}
/// Represents a structure that holds and manages lazy-loaded data frames loaded from files in the KSM system.
struct KSMData<'a> {
    data: DashMap<String, KSMFile>,
    dir_path: &'a str,
    file_extension: &'a str,
    parse_function: fn(file_path: PathBuf) -> Result<DataFrame, ParseError>,
}
impl<'a> KSMData<'a> {
    /// Creates a new instance of KSMData.
    pub fn new(
        dir_path: &'a str,
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
        for entry in fs::read_dir(self.dir_path).map_err(|_| ParseError::ReadFolderError)? {
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

async fn sync_task<'a>(
    stop: Arc<AtomicBool>,
    measurement_data: Arc<KSMData<'a>>,
    parameter_data: Arc<KSMData<'a>>,
) {
    log::info!("Startup: Entering sync task");
    while !stop.load(Ordering::Relaxed) {
        if let Err(e) = measurement_data.sync_data(stop.clone()).await {
            log::error!("Error when syncing measurement data: {}", e);
        }
        if let Err(e) = parameter_data.sync_data(stop.clone()).await {
            log::error!("Error when syncing parameter data: {}", e);
        }
        task::sleep(time::Duration::from_secs(2)).await;
    }
    log::info!("Sync task finished");
}

/// Represents the state of the server application, holding shared resources.
#[derive(Clone)]
struct AppState<'a> {
    measurement_data: Arc<KSMData<'a>>,
    parameter_data: Arc<KSMData<'a>>,
}

#[async_std::main]
async fn main() -> tide::Result<()> {
    tide::log::start();

    // Setup signal hook for SIGINT (2)
    let sigint = Arc::new(AtomicBool::new(false));
    if signal_hook::flag::register(2, sigint.clone()).is_err() {
        log::error!("Failed to register SIGINT signal hook");
        return Ok(());
    }

    // Initialize article parameters struct
    // TODO use environment variables for path
    log::info!("Startup: Reading article parameters");
    let art_data = Arc::new(KSMData::new("./testdata/art", "art", parse_art_file));
    if let Err(e) = art_data.sync_data(sigint.clone()).await {
        log::error!("Error when loading article parameters: {}", e);
        return Ok(());
    }

    // Initialize measurement data struct
    // TODO use environment variables for path
    log::info!("Startup: Reading measurement data");
    let meas_data = Arc::new(KSMData::new("./testdata/dat", "dat", parse_dat_file));
    if let Err(e) = meas_data.sync_data(sigint.clone()).await {
        log::error!("Error when loading measurement data: {}", e);
        return Ok(());
    }

    log::info!("Done loading data...");


    //Start data sync task
    let sync_task_handle = task::spawn(sync_task(
        sigint.clone(),
        meas_data.clone(),
        art_data.clone(),
    ));

    //Setup shared resources
    let state = AppState {
        measurement_data: meas_data.clone(),
        parameter_data: art_data.clone(),
    };

    //Create server object
    let mut server = tide::with_state(state);

    //Setup endpoints
    server.at("/measurement/:name").get(measurement);
    server.at("/parameters/:name").get(parameters);

    //Start server
    server.listen("127.0.0.1:8080").await?;

    //Wait for sync task to finish for graceful exit
    sync_task_handle.await;

    Ok(())
}

fn dataframe_to_json_response(dataframe: &mut DataFrame) -> tide::Response {
    // Create a buffer using a cursor over a new, empty vector to temporarily store the JSON data.
    let mut buf = Cursor::new(Vec::new());
    //
    // Attempt to write the DataFrame to the buffer as JSON. If this fails,
    // return a 500 Internal Server Error response.
    if JsonWriter::new(&mut buf).finish(dataframe).is_err() {
        return plain_response(StatusCode::InternalServerError, "Failed to write JSON");
    }

    // Convert the buffer into a String. This is done by first obtaining the Vec<u8>
    // (byte vector) inside the buffer, then attempting to create a UTF-8 string from it.
    let json = match String::from_utf8(buf.into_inner()) {
        Ok(data) => data,
        Err(_) => {
            return plain_response(StatusCode::InternalServerError, "Found invalid UTF-8");
        }
    };

    // If everything was successful and the JSON data is valid, return a 200 OK response
    // with the JSON data as the body, and set the content type to application/json.
    Response::builder(StatusCode::Ok)
        .body(json)
        .content_type(tide::http::mime::JSON)
        .build()
}

fn plain_response(code: StatusCode, msg: &str) -> tide::Response {
    Response::builder(code)
        .body(msg)
        .content_type(tide::http::mime::PLAIN)
        .build()
}

enum KSMError {
    DateCreationError { date: String, reason: String },
}

impl fmt::Display for KSMError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            KSMError::DateCreationError {
                ref date,
                ref reason,
            } => {
                write!(f, "Error creating date '{}': {}", date, reason)
            }
        }
    }
}

fn filter_dataframe_by_measure_time(
    lazyframe: LazyFrame,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<LazyFrame, KSMError> {
    // Convert the start date to a timestamp at the beginning of the day (midnight)
    let start = start_date
        .and_hms_opt(0, 0, 0)
        .map(|dt| dt.and_utc().timestamp())
        .ok_or_else(|| KSMError::DateCreationError {
            date: start_date.to_string(),
            reason: "Invalid start date time combination".to_string(),
        })?;

    // Convert the end date to a timestamp at the end of the day (one second before midnight)
    let end = end_date
        .and_hms_opt(23, 59, 59)
        .map(|dt| dt.and_utc().timestamp())
        .ok_or_else(|| KSMError::DateCreationError {
            date: start_date.to_string(),
            reason: "Invalid end date time combination".to_string(),
        })?;

    // Create filter expressions for data after the start date and before the end date
    let start = col("measure_time1970").gt_eq(start);
    let end = col("measure_time1970").lt_eq(end);

    // Apply filter and lazily load results
    Ok(lazyframe.filter(start.and(end)))
}

fn select_dataframe_columns(lazyframe: LazyFrame, columns: &str) -> Result<DataFrame, PolarsError> {
    //Assume all columns if columns string is empty
    if columns.is_empty() {
        return lazyframe.collect();
    }

    // Split the input string by commas and collect into a vector of column names
    let column_names: Vec<&str> = columns.split(',').collect::<Vec<&str>>();

    // Create a vector of column selection expressions based on column names
    let column_expressions: Vec<Expr> = column_names.iter().map(|&name| col(name)).collect();

    // Use the column expressions to select specified columns from the LazyFrame and collect the result into a DataFrame
    lazyframe.select(column_expressions).collect()
}

// Defines a structure to parse query parameters from a request.
#[derive(Deserialize, Debug)]
struct MeasurementQuery {
    start_date: Option<NaiveDate>, // Optional start date for filtering dataframe
    end_date: Option<NaiveDate>,   // Optional end date for filtering dataframe
    columns: Option<String>,       // Optional comma-separated string of columns to select
}
async fn measurement(req: Request<AppState<'_>>) -> tide::Result {
    // Deserialize the query parameters into the MeasurementQuery struct
    let query: MeasurementQuery = req.query()?;
    let data = &req.state().measurement_data;

    let key = match req.param("name") {
        Ok(file) => file,
        Err(_) => {
            log::error!("Invalid key for measurement request");
            // Return a BadRequest response if filename parameter is missing or incorrect
            return Ok(plain_response(StatusCode::BadRequest, "Invalid key"));
        }
    };

    let lazyframe = match data.data.get(key) {
        Some(ksmfile) => ksmfile.lazyframe.clone(),
        None => {
            log::error!("Invalid parameter entry requested: {}", key);
            let response_string = format!("Parameter entry not found: {}", key);
            return Ok(plain_response(
                StatusCode::InternalServerError,
                response_string.as_str(),
            ));
        }
    };

    // Filter the dataframe by measure time using provided start and end dates
    let lazyframe = match filter_dataframe_by_measure_time(
        lazyframe,
        query.start_date.unwrap_or(NaiveDate::MIN),
        query.end_date.unwrap_or(NaiveDate::MAX),
    ) {
        Ok(df) => df,
        Err(e) => {
            // Return InternalServerError if there is an error in filtering
            return Ok(plain_response(
                StatusCode::InternalServerError,
                e.to_string().as_str(),
            ));
        }
    };

    // Process the optional column filtering
    let column_string = query.columns.unwrap_or_default();
    let mut dataframe = match select_dataframe_columns(lazyframe, column_string.as_str()) {
        Ok(df) => df,
        Err(e) => match e {
            PolarsError::ColumnNotFound(..) => {
                // Return BadRequest if specified column doesn't exist
                return Ok(plain_response(StatusCode::BadRequest, "Column not found"));
            }
            _ => {
                // Return InternalServerError for other column-related errors
                return Ok(plain_response(
                    StatusCode::InternalServerError,
                    format!("Column errror {:?}", e.to_string()).as_str(),
                ));
            }
        },
    };

    // Convert the final dataframe to JSON and use it as the response
    Ok(dataframe_to_json_response(&mut dataframe))
}

#[derive(Deserialize, Debug)]
struct ParameterQuery {
    columns: Option<String>,
}
async fn parameters(req: Request<AppState<'_>>) -> tide::Result {
    let query: ParameterQuery = req.query()?;
    let data = &req.state().parameter_data;

    let key = match req.param("name") {
        Ok(file) => file, //format!("{}.art", file),
        Err(_) => {
            log::error!("Invalid key for parameters request");
            // Return a BadRequest response if filename parameter is missing or incorrect
            return Ok(plain_response(StatusCode::BadRequest, "Invalid parameter"));
        }
    };

    let lazyframe = match data.data.get(key) {
        Some(ksmfile) => ksmfile.lazyframe.clone(),
        None => {
            log::error!("Invalid parameter entry requested: {}", key);
            let response_string = format!("Parameter entry not found: {}", key);
            return Ok(plain_response(
                StatusCode::InternalServerError,
                response_string.as_str(),
            ));
        }
    };

    let column_string = query.columns.unwrap_or_default();
    let mut dataframe = match select_dataframe_columns(lazyframe, &column_string) {
        Ok(df) => df,
        Err(e) => match e {
            PolarsError::ColumnNotFound(..) => {
                log::error!(
                    "Parameters request with invalid column names: {}",
                    column_string
                );
                // Return BadRequest if specified column doesn't exist
                return Ok(plain_response(StatusCode::BadRequest, "Column not found"));
            }
            _ => {
                log::error!(
                    "Error when selecting parameters column. Column string: {}. Error: {}",
                    column_string,
                    e.to_string()
                );
                // Return InternalServerError for other column-related errors
                return Ok(plain_response(
                    StatusCode::InternalServerError,
                    format!("Column errror {:?}", e.to_string()).as_str(),
                ));
            }
        },
    };

    Ok(dataframe_to_json_response(&mut dataframe))
}
