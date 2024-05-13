/// Module for parsing article parameters from `.art` files.
/// Provides functionality to read and validate article file content, ensuring
/// it conforms to the expected format and structure.
///
/// Functions included handle reading from files, parsing content, and validating
/// program names and key-value pair arrangements within the given article files.

use super::ParseError;
use crate::read_and_decode_lines;
use std::collections::HashMap;
use std::path::Path;
use std::io;

/// Parses parameters from article content represented as an iterator over results of strings.
///
/// This function expects the first result to contain a valid program name from predefined
/// `PROGRAM_NAMES`. It then checks for a specific "None" result, followed by several
/// key-value pairs. Each key-value pair should be in the format "key = value".
///
/// # Parameters
/// - `line_res`: An iterator over `io::Result<String>`, where each item represents a line from the article,
///   potentially including I/O errors.
///
/// # Returns
/// A `Result` wrapping a `HashMap<String, String>` where each entry corresponds to a key-value pair found
/// in the article file. Returns `ParseError` on failure with an appropriate error message,
/// indicating whether a program name is invalid, a required line is missing, or a line is malformed.
///
/// # Errors
/// - `MissingField`: If the "None" line or the program name is missing.
/// - `MalformedEntry`: If any line doesn't comply with the "key = value" format.
/// - `IOError`: If there is an I/O error reading any line.
fn read_article_parameters(mut line_res: impl Iterator<Item = io::Result<String>>) -> Result<HashMap<String, String>, ParseError> {
    let mut parameters = HashMap::new(); // Create a new HashMap to store article parameters

    // Check if the first line (expected to be the program name) exists and validate it
    match line_res.next() {
        Some(Ok(pgm_name)) => {
            // Trim whitespace from the program name
            let pgm_name = pgm_name.trim();
            // Store the valid program name in the parameters map and remove it from the lines
            parameters.insert(String::from("pgm_name"), String::from(pgm_name));
        },
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
        },
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
                parameters.insert(key.trim().to_string(), value.trim().to_string());
            }
            None => {
                // Return an error if a line does not contain a valid key-value format
                return Err(ParseError::MalformedEntry(line.to_string()));
            }
        }
    }

    // Return the parsed parameters on success
    Ok(parameters)
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
pub fn parse_art_file<P: AsRef<Path> + std::fmt::Display + Copy>(
    file_path: P,
) -> Result<HashMap<String, String>, ParseError> {
    match read_and_decode_lines(file_path) {
        // Attempt to read article parameters from the decoded lines
        Ok(lines) => {
            read_article_parameters(lines)
        },
        // Return an error if the file could not be read and decoded
        Err(_) => Err(ParseError::InvalidFile(file_path.to_string()))
    }
}

