pub mod article;
pub mod measurement;
use encoding_rs::ISO_8859_10;
use encoding_rs_io::DecodeReaderBytesBuilder;
use polars::prelude::DataFrame;
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::Path;

/// Reads lines from a given file and decodes them using the ISO_8859_10 encoding.
///
/// The function opens a file specified by the `file_path` and decodes its content
/// from ISO_8859_10 to UTF-8, returning an iterator over the resulting lines.
/// Each line is wrapped in a `Result` to handle potential errors in reading or decoding.
fn read_and_decode_lines<P: AsRef<Path>>(
    file_path: P,
) -> io::Result<impl Iterator<Item = io::Result<String>>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    // Create a decoder that handles ISO_8859_10 encoding
    let decoder = DecodeReaderBytesBuilder::new()
        .encoding(Some(ISO_8859_10))
        .build(reader);
    Ok(BufReader::new(decoder).lines())
}

type ParseFunction<P> = fn(P) -> Result<DataFrame, ParseError>;
fn parse_folder<P: AsRef<Path>>(
    dir: P,
    parse_function: ParseFunction<&Path>,
) -> Result<HashMap<String, DataFrame>, ParseError> {
    let mut map = HashMap::new();
    for entry in fs::read_dir(dir).map_err(|_| ParseError::ReadFolderError)? {
        let file = entry.map_err(|_| ParseError::ReadFolderError)?;
        let path = file.path().clone();
        let key = file.file_name().to_string_lossy().to_string();
        if path.extension().unwrap_or_default() == "art" {
            map.insert(key, parse_function(file.path().as_ref())?);
        }
    }

    Ok(map)
}

// Define error types for parsing ksm data that can be displayed and formatted
#[derive(Debug)]
pub enum ParseError {
    /// Error for a non-readable file
    /// Includes the filename
    InvalidFile(String),

    /// Specifies that a required field is missing from the input.
    /// Includes the name of the missing field.
    MissingField(String),

    /// Specifies that an entry could not be parsed correctly.
    /// Includes the line with the malformed entry
    MalformedEntry(String),

    /// Specifies an unexpected error from the internal functions
    /// Includes a description of where the error happened
    GeneralError(String),

    /// Specifies that an error happened while reading a file
    /// Includes the error message from std::io
    IOError(String),

    DataFrameCreationError,

    SeriesCreationError,

    DataAlignmentError,

    ReadFolderError,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::InvalidFile(filename) => {
                write!(f, "Invalid file encountered: {}", filename)
            }
            ParseError::MissingField(field) => {
                write!(f, "Missing required field: {}", field)
            }
            ParseError::MalformedEntry(line) => {
                write!(f, "Malformed entry found: {}", line)
            }
            ParseError::GeneralError(line) => {
                write!(f, "General error from ksmparser: {}", line)
            }
            ParseError::IOError(line) => {
                write!(f, "Error when reading a file: {}", line)
            }
            ParseError::DataFrameCreationError => {
                write!(f, "Failed to create new row")
            }
            ParseError::SeriesCreationError => {
                write!(f, "Failed to create new column")
            }
            ParseError::DataAlignmentError => {
                write!(f, "Failed to align or insert row")
            }
            ParseError::ReadFolderError => {
                write!(f, "Error when interating entries in a folder")
            }
        }
    }
}
