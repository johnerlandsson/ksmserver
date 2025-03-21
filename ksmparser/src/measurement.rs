use super::{parse_folder, ParseError};
use crate::read_and_decode_lines;
use lazy_static::lazy_static;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::env;
use std::io;
use std::path::Path;

lazy_static! {
    static ref COLUMN_DTYPE: HashMap<&'static str, DataType> = {
        let mut m = HashMap::new();
        m.insert("centervalue", DataType::Float64);
        m.insert("check_wall_min_minlimit", DataType::Float64);
        m.insert("check_wall_min_nomlimit", DataType::Float64);
        m.insert("check_wall_mean_minlimit", DataType::Float64);
        m.insert("check_wall_mean_nomlimit", DataType::Float64);
        m.insert("diameter_outer_mean", DataType::Float64);
        m.insert("wall_extra_percent", DataType::Float64);
        m.insert("check_diameter_outer_mean_nomlimit", DataType::Float64);
        m.insert("area_outer", DataType::Float64);
        m.insert("diameter_outer_max", DataType::Float64);
        m.insert("ovality", DataType::Float64);
        m.insert("wall_min", DataType::Float64);
        m.insert("area_wall", DataType::Float64);
        m.insert("wall_mean", DataType::Float64);
        m.insert("measure_time1970", DataType::Int64);
        m
    };
}

/// Adds specified columns as null columns to a mutable DataFrame.
///
/// # Arguments
/// * `dataframe` - A mutable reference to a DataFrame to which the null columns will be added.
/// * `column_names` - A vector containing the names of the columns to be added.
///
/// # Returns
/// A PolarsResult indicating success or failure.
fn add_null_columns(dataframe: &mut DataFrame, column_names: &Vec<String>) -> PolarsResult<()> {
    for column_name in column_names {
        let dtype = match COLUMN_DTYPE.get(column_name.as_str()) {
            Some(t) => t,
            None => &DataType::String,
        };
        // Creating a full null column for each column name provided
        let null_column = Column::full_null(
            PlSmallStr::from_string(column_name.clone()),
            dataframe.height(),
            dtype,
        );
        // Appending the null column to the dataframe
        dataframe.with_column(null_column)?;
    }
    Ok(())
}

/// Ensures that both DataFrames have the same set of columns and inserts a row from one into another.
///
/// # Arguments
/// * `main_dataframe` - The main DataFrame that will be modified.
/// * `new_data_row` - The new DataFrame representing a single row to be inserted.
///
/// # Returns
/// A PolarsResult containing the modified DataFrame or an error.
fn align_dataframes_and_insert_row(
    main_dataframe: &mut DataFrame,
    new_data_row: &mut DataFrame,
) -> PolarsResult<DataFrame> {
    // Determine columns that need to be added to each DataFrame to align them
    let (columns_to_add_to_dataframe, columns_to_add_to_new_row) = find_column_name_differences(
        &new_data_row.get_column_names(),
        &main_dataframe.get_column_names(),
    );

    // Add necessary columns as null columns to each DataFrame
    add_null_columns(new_data_row, &columns_to_add_to_new_row)?;
    add_null_columns(main_dataframe, &columns_to_add_to_dataframe)?;
    // Align the new row with the main DataFrame and insert it
    align_and_insert_row(main_dataframe, new_data_row)
}

/// Vertically stacks a new single-row DataFrame onto another DataFrame after aligning their columns.
///
/// # Arguments
/// * `main_dataframe` - A mutable reference to the main DataFrame to be appended to.
/// * `new_data_row` - A reference to the DataFrame that represents the new row to insert.
///
/// # Returns
/// A PolarsResult containing the updated DataFrame or an error.
fn align_and_insert_row(
    main_dataframe: &DataFrame,
    new_data_row: &DataFrame,
) -> PolarsResult<DataFrame> {
    //select needs vec of owned PlSmallStr's
    //TODO is this the best way?
    let column_names: Vec<PlSmallStr> = main_dataframe
        .get_column_names()
        .iter()
        .map(|s| PlSmallStr::from_str(s.as_str()))
        .collect();
    let aligned_row = new_data_row.select(column_names)?;
    main_dataframe.vstack(&aligned_row)
}

/// Creates a DataFrame from a single pair of strings representing column names and their corresponding values.
///
/// This function expects two strings where each column name and its corresponding value are separated by tabs.
/// It splits these strings and pairs them up to construct a DataFrame containing a single row.
///
/// # Arguments
/// * `columns` - A string containing column names separated by tabs.
/// * `values` - A string containing corresponding values separated by tabs.
///
/// # Returns
/// * `PolarsResult<DataFrame>` - Returns a DataFrame constructed from the provided columns and values if successful.
///   Returns a `PolarsError` if the DataFrame could not be successfully constructed (e.g., mismatch in the number of columns and values).
pub fn create_dataframe_from_columns_and_values(
    columns: &str,
    values: &str,
) -> Result<DataFrame, ParseError> {
    let col_str_vec: Vec<&str> = columns.split("\t").collect();
    let val_str_vec: Vec<&str> = values.split("\t").collect();

    if col_str_vec.len() != val_str_vec.len() {
        return Err(ParseError::ColumnMismatchError);
    }

    let mut column_vec = Vec::new();
    for (column, value) in col_str_vec.into_iter().zip(val_str_vec.into_iter()) {
        let column = column.trim().trim_matches('"');
        let data_type = match COLUMN_DTYPE.get(column) {
            Some(t) => t,
            None => &DataType::String,
        };

        let column = parse_column(column, value, data_type);
        column_vec.push(column);
    }

    match DataFrame::new(column_vec) {
        Ok(df) => Ok(df),
        Err(e) => match e {
            PolarsError::Duplicate(_) => Err(ParseError::DuplicateColumns),
            _ => Err(ParseError::DataFrameCreationError),
        },
    }
}

/// Parses a string value into a Column of specified data type.
///
/// This function reads a string value and tries to parse and convert it into a Column
/// of a specific DataType. It covers parsing for common numerical types and dates.
///
/// # Arguments
/// * `column` - A string slice that holds the name of the column to which the value belongs.
/// * `value` - A string slice representing the value to be parsed into a Column.
/// * `data_type` - The desired DataType of the Column after parsing the value.
///
/// # Returns
/// A `Result` that is either:
/// - `Ok(Column)` - A new Column with the parsed value if successful.
/// - `Err(PolarsError)` - An error if the parsing fails.
fn parse_column(column: &str, value: &str, data_type: &DataType) -> Column {
    match data_type {
        DataType::Float64 => match value.parse::<f64>() {
            Ok(parsed_value) => Column::new(PlSmallStr::from_str(column), [parsed_value]),
            Err(_) => Column::full_null(PlSmallStr::from_str(column), 1, data_type),
        },
        // Similar parsing process for Float32.
        DataType::Float32 => match value.parse::<f32>() {
            Ok(parsed_value) => Column::new(PlSmallStr::from_str(column), [parsed_value]),
            Err(_) => Column::full_null(PlSmallStr::from_str(column), 1, data_type),
        },
        // Similar parsing process for Int64.
        DataType::Int64 => match value.parse::<i64>() {
            Ok(parsed_value) => Column::new(PlSmallStr::from_str(column), [parsed_value]),
            Err(_) => Column::full_null(PlSmallStr::from_str(column), 1, data_type),
        },
        // Similar parsing process for Int32.
        DataType::Int32 => match value.parse::<i32>() {
            Ok(parsed_value) => Column::new(PlSmallStr::from_str(column), [parsed_value]),
            Err(_) => Column::full_null(PlSmallStr::from_str(column), 1, data_type),
        },
        _ => Column::new(PlSmallStr::from_str(column), [value]),
    }
}

/// Finds the differences in column names between two string slices.
///
/// # Arguments
/// * `a` - First slice of string slices representing column names.
/// * `b` - Second slice of string slices representing column names.
///
/// # Returns
/// A tuple of vectors containing unique column names not found in each other's lists
fn find_column_name_differences(
    a: &Vec<&PlSmallStr>,
    b: &Vec<&PlSmallStr>,
) -> (Vec<String>, Vec<String>) {
    let set_a: HashSet<_> = a.iter().collect();
    let set_b: HashSet<_> = b.iter().collect();

    // Calculate unique columns to each list
    let unique_to_a: Vec<String> = set_a.difference(&set_b).map(|&s| s.to_string()).collect();
    let unique_to_b: Vec<String> = set_b.difference(&set_a).map(|&s| s.to_string()).collect();

    // Return the unique columns for both DataFrames
    (unique_to_a, unique_to_b)
}

/// Reads measurement entries from an iterator over `io::Result<String>` representing lines from a file.
///
/// This function processes pairs of lines from the given iterator, where the first line of each
/// pair represents column names and the second line represents the corresponding values. These
/// pairs are used to construct a DataFrame row by row. If any line read fails, or the pairs are
/// incomplete, it returns an error.
///
/// # Arguments
/// * `lines_res` - An iterator over `io::Result<String>` which yields lines from a file.
///
/// # Returns
/// * `Result<DataFrame, ParseError>` - On success, returns a DataFrame containing the data from all read entries.
///   Returns a `ParseError` on any form of reading or parsing failures, such as I/O errors, empty lines, or malformed entries.
///
/// # Errors
/// * `ParseError::IOError` - if there's an I/O error reading a line.
/// * `ParseError::MalformedEntry` - if there is a mismatch in the expected format, specifically if a value line is missing after a column line.
/// * `ParseError::GeneralError` - for errors during DataFrame construction or data alignment.
fn read_measurement_entries(
    mut lines_res: impl Iterator<Item = io::Result<String>>,
) -> Result<DataFrame, ParseError> {
    //Create dataframe to hold return data
    let mut dataframe = DataFrame::default();

    loop {
        //Read columns row into string
        let column_row = match lines_res.next() {
            Some(Ok(line)) => line,
            Some(Err(e)) => return Err(ParseError::IOError(e.to_string())),
            None => break,
        };

        //.dat file sometimes end with an empty line
        if column_row.is_empty() {
            break;
        }

        //Read values row into string
        let values_row = match lines_res.next().transpose() {
            Ok(Some(line)) => line,
            Ok(None) => {
                return Err(ParseError::MalformedEntry(String::from(
                    "No value row after column row",
                )))
            }
            Err(e) => return Err(ParseError::IOError(e.to_string())),
        };

        //Split the column and value stings and create a dataframe with a single row
        let mut new_row = match create_dataframe_from_columns_and_values(&column_row, &values_row) {
            Ok(df) => df,
            Err(e) => {
                match e {
                    //Ignore entries with duplicate column names
                    ParseError::DuplicateColumns => continue,
                    _ => return Err(e),
                }
            }
        };

        match align_dataframes_and_insert_row(&mut dataframe, &mut new_row) {
            Ok(df) => dataframe = df,
            Err(_) => return Err(ParseError::DataAlignmentError),
        }
    }
    let mut dataframe = add_local_datetime_column(dataframe)?;
    dataframe.shrink_to_fit(); // Not shrinking causes extreme bloating
    Ok(dataframe)
}

/// Converts the epoch time from the 'measure_time1970' column of a DataFrame
/// into a local DateTime based on the timezone specified in the environment variable 'TIMEZONE'.
/// Adds the resulting DateTime as a new column 'local_time' to the DataFrame.
///
/// # Parameters
/// - `dataframe`: A DataFrame containing a 'measure_time1970' column with epoch times.
///
/// # Returns
/// - `Result<DataFrame, ParseError>`: The modified DataFrame with the new 'local_time' column,
///   or a `ParseError` if there is an error during the conversion.
///
/// # Behavior
/// - The function reads the timezone from the 'TIMEZONE' environment variable,
///   defaults to "Europe/Stockholm" if not set, and converts the epoch time into a
///   local DateTime in the specified timezone.
fn add_local_datetime_column(mut dataframe: DataFrame) -> Result<DataFrame, ParseError> {
    //Read timezone from environment variable
    let timezone = env::var("TIMEZONE").unwrap_or(String::from("Europe/Stockholm"));

    //Create local_time column
    dataframe = match dataframe
        .lazy()
        .with_columns([(col("measure_time1970") * lit(1000))
            .cast(DataType::Datetime(
                TimeUnit::Milliseconds,
                Some("UTC".into()),
            ))
            .dt()
            .convert_time_zone(PlSmallStr::from_string(timezone))
            .alias("local_time")])
        .collect()
    {
        Ok(df) => df,
        Err(e) => return Err(ParseError::EpochToDatetime(e.to_string())),
    };

    Ok(dataframe)
}

/// Parses a .dat file at the specified path to construct a DataFrame.
///
/// This function leverages the `read_measurement_entries` to parse the file. It expects the file
/// to be encoded in a manner compatible with the encoding settings of `read_and_decode_lines`. Each
/// entry in the file should consist of consecutive lines; the first containing column names and the
/// second containing corresponding values.
///
/// # Type Parameters
/// * `P` - The type of the path which must implement `AsRef<Path>`, `Display`, and be copyable.
///
/// # Arguments
/// * `file_path` - A reference to the path of the .dat file.
///
/// # Returns
/// * `Result<DataFrame, ParseError>` where `DataFrame` contains the combined data from the file.
/// * `ParseError::InvalidFile` if the file cannot be opened or read.
/// * Errors inherited from `read_measurement_entries` function on parsing or DataFrame construction issues.
pub fn parse_dat_file<P: AsRef<Path>>(file_path: P) -> Result<DataFrame, ParseError> {
    match read_and_decode_lines(&file_path) {
        Ok(lines) => read_measurement_entries(lines),
        Err(_) => Err(ParseError::InvalidFile(
            file_path.as_ref().to_string_lossy().into_owned(),
        )),
    }
}

pub fn parse_dat_folder<P: AsRef<Path>>(dir: P) -> Result<HashMap<String, DataFrame>, ParseError> {
    parse_folder(dir, parse_dat_file, "dat")
}
