use chrono::NaiveDate;
use dashmap::DashMap;
use ksmparser::article::parse_art_file;
use ksmparser::measurement::parse_dat_file;
use ksmparser::ParseError;
use polars::prelude::*;
use polars_io::json::JsonWriter;
use serde::Deserialize;
use std::fmt;
use std::io::Cursor;
use std::path::PathBuf;
use tide::{Request, Response, StatusCode, log};
use std::fs;

struct KSMData<'a> {
    data: DashMap<String, DataFrame>,
    dir_path: &'a str,
    file_extension: &'a str,
    parse_function: fn(file_path: PathBuf) -> Result<DataFrame, ParseError>,
}
impl<'a> KSMData<'a> {
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

    pub async fn load_data(&self) -> Result<(), ParseError> {
        for entry in fs::read_dir(self.dir_path).map_err(|_| ParseError::ReadFolderError)? {
            let path = entry.map_err(|_| ParseError::ReadFolderError)?.path();

            if let Some(extension) = path.extension().and_then(|ext| ext.to_str()) {
                if extension == self.file_extension {
                    if let Some(file_name) = path.file_stem().and_then(|name| name.to_str()) {
                        let parse_function = self.parse_function;
                        let data_frame = parse_function(path.clone())?;
                        self.data.insert(file_name.to_owned(), data_frame);
                    } else {
                        return Err(ParseError::FileNameExtractionError);
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_std::main]
async fn main() -> tide::Result<()> {
    tide::log::start();

    log::info!("Startup: Reading article parameters");
    let art_data = KSMData::new("./testdata/art", "art", parse_art_file);
    if let Err(e) = art_data.load_data().await {
        log::error!("{}", e);
        return Ok(());
    }

    let mut server = tide::new();

    server.at("/measurement/:name").get(measurement);
    server.at("/parameters/:name").get(parameters);

    server.listen("127.0.0.1:8080").await?;

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
    dataframe: DataFrame,
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
    Ok(dataframe.lazy().filter(start.and(end)))
}

fn select_dataframe_columns(dataframe: LazyFrame, columns: &str) -> Result<DataFrame, PolarsError> {
    //Assume all columns if columns string is empty
    if columns.is_empty() {
        return dataframe.collect();
    }

    // Split the input string by commas and collect into a vector of column names
    let column_names: Vec<&str> = columns.split(',').collect::<Vec<&str>>();

    // Create a vector of column selection expressions based on column names
    let column_expressions: Vec<Expr> = column_names.iter().map(|&name| col(name)).collect();

    // Use the column expressions to select specified columns from the LazyFrame and collect the result into a DataFrame
    dataframe.select(column_expressions).collect()
}

// Defines a structure to parse query parameters from a request.
#[derive(Deserialize, Debug)]
struct MeasurementQuery {
    start_date: Option<NaiveDate>, // Optional start date for filtering dataframe
    end_date: Option<NaiveDate>,   // Optional end date for filtering dataframe
    columns: Option<String>,       // Optional comma-separated string of columns to select
}
async fn measurement(req: Request<()>) -> tide::Result {
    // Deserialize the query parameters into the MeasurementQuery struct
    let query: MeasurementQuery = req.query()?;

    // Extract the filename parameter from the request path and format it with directory structure
    let filename: String = match req.param("name") {
        Ok(file) => format!("testdata/dat/{}.dat", file),
        Err(_) => {
            // Return a BadRequest response if filename parameter is missing or incorrect
            return Ok(plain_response(StatusCode::BadRequest, "Invalid parameter"));
        }
    };

    // Attempt to parse the .dat file based on the given filename
    let dataframe = match parse_dat_file(filename.as_str()) {
        Ok(df) => df,
        Err(e) => {
            let body = format!("Error message: {e}.\nFile path: {filename}");
            return Ok(plain_response(StatusCode::NotFound, body.as_str()));
        }
    };

    // Filter the dataframe by measure time using provided start and end dates
    let dataframe = match filter_dataframe_by_measure_time(
        dataframe,
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
    let mut dataframe = match select_dataframe_columns(dataframe, column_string.as_str()) {
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
async fn parameters(req: Request<()>) -> tide::Result {
    let query: ParameterQuery = req.query()?;

    // Extract the filename parameter from the request path and format it with directory structure
    let filename: String = match req.param("name") {
        Ok(file) => format!("testdata/art/{}.art", file),
        Err(_) => {
            // Return a BadRequest response if filename parameter is missing or incorrect
            return Ok(plain_response(StatusCode::BadRequest, "Invalid parameter"));
        }
    };

    let dataframe = match parse_art_file(filename.as_str()) {
        Ok(df) => df,
        Err(_) => {
            return Ok(plain_response(StatusCode::InternalServerError, ""));
        }
    };

    let column_string = query.columns.unwrap_or_default();
    let mut dataframe = match select_dataframe_columns(dataframe.lazy(), column_string.as_str()) {
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

    Ok(dataframe_to_json_response(&mut dataframe))
}
