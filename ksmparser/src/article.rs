/// Module for parsing article parameters from `.art` files.
/// Provides functionality to read and validate article file content, ensuring
/// it conforms to the expected format and structure.
///
/// Functions included handle reading from files, parsing content, and validating
/// program names and key-value pair arrangements within the given article files.
use super::{ParseError, parse_folder};
use crate::read_and_decode_lines;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::io;

/// Parses article parameters from an iterator over lines of text, producing a DataFrame.
///
/// This function reads through lines of text provided by an `impl Iterator<Item = io::Result<String>>`
/// interpreting them to extract a number of predefined parameters into a `DataFrame`. The expected format
/// for the input includes specific key-value pairs divided by "=", and certain mandatory items such as the
/// program name.
///
/// # Parameters
/// - `mut line_res`: An iterator over IO results of strings, where each string represents a line of text
///   from the source input. The iterator can yield IO errors which are propagated as `ParseError`.
///
/// # Returns
/// This function returns a `Result<DataFrame, ParseError>`. If the parsing completes successfully without
/// encountering format or IO errors, it returns `Ok(DataFrame)` where `DataFrame` contains the parsed
/// data. If errors are encountered (e.g., IO issues, missing mandatory fields, malformatted entries),
/// a `ParseError` is returned to indicate the failure.
///
/// # Errors
/// The function can return the following errors:
/// - `ParseError::IOError(String)`: When the function encounters an IO error from the input iterator.
/// - `ParseError::MissingField(String)`: If a required field such as the "pgm_name" or a "None" terminator is missing.
/// - `ParseError::SeriesCreationError`: If there is an error adding a new Series to the DataFrame.
/// - `ParseError::MalformedEntry(String)`: If a line cannot be parsed into a valid key-value format.
fn read_article_parameters(
    mut line_res: impl Iterator<Item = io::Result<String>>,
) -> Result<DataFrame, ParseError> {
    let mut dataframe = DataFrame::default();

    // Check if the first line (expected to be the program name) exists and validate it
    match line_res.next() {
        Some(Ok(pgm_name)) => {
            // Trim whitespace from the program name
            let pgm_name = pgm_name.trim();
            // Store the valid program name in the parameters map and remove it from the lines
            //let series = Series::new(PlSmallStr::from_str("pgm_name"), &[pgm_name]);
            let column: Column = Column::new(PlSmallStr::from_str("pgm_name"), [pgm_name]);
            dataframe = dataframe
                .hstack(&[column])
                .map_err(|_| ParseError::SeriesCreationError)?;
        }
        Some(Err(e)) => return Err(ParseError::IOError(e.to_string())),
        None => return Err(ParseError::MissingField(String::from("pgm_name"))),
    }

    // Return an error if the "None" line is missing
    //TODO investigate what this is
    match line_res.next() {
        Some(Ok(line)) => {
            if line.trim() != "None" {
                return Err(ParseError::MissingField("None".to_string()));
            }
        }
        Some(Err(e)) => return Err(ParseError::IOError(e.to_string())),
        None => return Err(ParseError::IOError("Reading none line".to_string())),
    }

    // Iterate over the remaining lines to parse key-value pairs
    for line in line_res {
        let line = match line {
            Ok(l) => l,
            Err(e) => return Err(ParseError::IOError(e.to_string())),
        };
        //Ignore empty lines
        if line.trim().is_empty() {
            continue;
        }

        match line.split_once(" = ") {
            Some((key, value)) => {
                // Trim and insert the parsed key and value into the parameters map
                let column = Column::new(PlSmallStr::from_str(key.trim()), [value.trim()]);
                dataframe = dataframe
                    .hstack(&[column])
                    .map_err(|_| ParseError::SeriesCreationError)?;
            }
            None => {
                // Return an error if a line does not contain a valid key-value format
                return Err(ParseError::MalformedEntry(line.to_string()));
            }
        }
    }
    dataframe.shrink_to_fit();
    Ok(dataframe)
}

/// Parses an article file from the specified path.
///
/// This function reads and decodes content of a file at the given path, and attempts
/// to parse it using `read_article_parameters`. It ensures that the article content
/// adheres to the expected format and structure.
///
/// # Type Parameters
/// - `P`: The type of file path input, which must implement `AsRef<Path>`, `Display`, and `Copy`.
///
/// # Parameters
/// - `file_path`: A file path from which to read the article content.
///
/// # Returns
/// A `Result` which, on success, contains a `HashMap` of parsed parameters. On failure,
/// it returns a `ParseError` indicating issues either with file reading or content parsing.
///
/// # Errors
/// - `InvalidFile`: If the specified file could not be read or decoded.
/// - Additionally includes all errors thrown by `read_article_parameters`.
pub fn parse_art_file<P: AsRef<Path>>(
    file_path: P,
) -> Result<DataFrame, ParseError> {
    match read_and_decode_lines(&file_path) {
        // Attempt to read article parameters from the decoded lines
        Ok(lines) => read_article_parameters(lines),
        // Return an error if the file could not be read and decoded
        Err(_) => Err(ParseError::InvalidFile(file_path.as_ref().to_string_lossy().into_owned())),
    }
}

pub fn parse_art_folder<P: AsRef<Path>> (dir: P) -> Result<HashMap<String, DataFrame>, ParseError> {
    parse_folder(dir, parse_art_file, "art")
}
