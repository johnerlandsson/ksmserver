//use ksmparser::article::parse_art_file;
use chrono::NaiveDate;
use ksmparser::measurement::parse_dat_file;
use polars::prelude::*;
use polars_io::json::JsonWriter;
use serde::Deserialize;
use std::io::Cursor;
use tide::{Request, Response, StatusCode};

#[async_std::main]
async fn main() -> tide::Result<()> {
    tide::log::start();
    let mut server = tide::new();

    server.at("/measurement/:name").get(measurement);

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

enum KSMErrors {
    DateCreationError,
    DataFrameFilterError,
}

fn filter_dataframe_by_measure_time(
    dataframe: DataFrame,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<DataFrame, KSMErrors> {
    let start = match start_date.and_hms_opt(0, 0, 0) {
        Some(dt) => dt.and_utc().timestamp(),
        None => return Err(KSMErrors::DateCreationError),
    };
    let end = match end_date.and_hms_opt(23, 59, 59) {
        Some(dt) => dt.and_utc().timestamp(),
        None => return Err(KSMErrors::DateCreationError),
    };

    let start = col("measure_time1970").gt_eq(start);
    let end = col("measure_time1970").lt_eq(end);

    match dataframe.lazy().filter(start.and(end)).collect() {
        Ok(df) => Ok(df),
        Err(_) => Err(KSMErrors::DataFrameFilterError),
    }
}

#[derive(Deserialize, Debug)]
struct MeasurementQuery {
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    columns: Option<Vec<String>>,
}
async fn measurement(req: Request<()>) -> tide::Result {
    let query: MeasurementQuery = req.query()?;
    let filename: String = match req.param("name") {
        Ok(file) => format!("testdata/dat/{}.dat", file),
        Err(_) => {
            return Ok(plain_response(StatusCode::BadRequest, "Invalid parameter"));
        }
    };

    let dataframe = match parse_dat_file(filename.as_str()) {
        Ok(df) => df,
        Err(e) => {
            let body = format!("Error message: {e}.\nFile path: {filename}");
            return Ok(plain_response(StatusCode::NotFound, body.as_str()));
        }
    };

    println!("Before {}x{}", dataframe.height(), dataframe.width());
    let mut dataframe = match filter_dataframe_by_measure_time(
        dataframe,
        query.start_date.unwrap_or(NaiveDate::MIN),
        query.end_date.unwrap_or(NaiveDate::MAX),
    ) {
        Ok(df) => df,
        Err(_) => {
            return Ok(plain_response(
                StatusCode::InternalServerError,
                "Failed to filter result by \"measure_time1970\"",
            ))
        }
    };

    let columns: Vec<String> = query.columns.unwrap_or_else(
        dataframe
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
    );
    println!("{:?}", columns);

    println!("After {}x{}", dataframe.height(), dataframe.width());

    Ok(dataframe_to_json_response(&mut dataframe))
}
