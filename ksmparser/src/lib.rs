pub mod article;
pub mod measurement;
use encoding_rs::ISO_8859_10;
use encoding_rs_io::DecodeReaderBytesBuilder;
use polars::prelude::DataFrame;
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use regex::Regex;

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

fn parse_folder<P: AsRef<Path>>(
    dir: P,
    parse_function: fn(file_path: PathBuf) -> Result<DataFrame, ParseError>,
    file_extension: &str,
) -> Result<HashMap<String, DataFrame>, ParseError> {
    let mut map: HashMap<String, DataFrame> = HashMap::new();

    //Compile regex pattern for filename
    let pattern_string = format!(r"^\d{{3,5}}(-\d)?{}$", regex::escape(file_extension));
    let filename_pattern = Regex::new(&pattern_string).map_err(|_| ParseError::InvalidRegex)?;

    //Iterate files in directory
    for entry in fs::read_dir(dir).map_err(|_| ParseError::ReadFolderError)? {
        let path = entry.map_err(|_| ParseError::ReadFolderError)?.path();

        if let Some(file_name) = path.file_name().and_then(|name| name.to_str()) {
            //Check if filename matches pattern
            if filename_pattern.is_match(file_name) {
                let data_frame = parse_function(path.clone())?;
                map.insert(file_name.to_owned(), data_frame);
            }
        } else {
            return Err(ParseError::FileNameExtractionError);
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

    ColumnCreationError,

    DataAlignmentError,

    ReadFolderError,

    ReadMetadataError,

    FileNameExtractionError,

    TypeConversionError(String, String, String),

    ColumnMismatchError,

    DuplicateColumns,

    InvalidRegex,

    EpochToDatetime(String),
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
            ParseError::ColumnCreationError => {
                write!(f, "Failed to create new column")
            }
            ParseError::DataAlignmentError => {
                write!(f, "Failed to align or insert row")
            }
            ParseError::ReadFolderError => {
                write!(f, "Error when interating entries in a folder")
            }
            ParseError::ReadMetadataError => {
                write!(f, "Error when reading metadata for DirEntry")
            }
            ParseError::FileNameExtractionError => {
                write!(f, "Error when extracting file path")
            }
            ParseError::TypeConversionError(column, value, dtype) => {
                write!(f, "Error when converting column {} to {}: {}", column, dtype, value)
            }
            ParseError::ColumnMismatchError => {
                write!(f, "Column mismatch")
            }
            ParseError::DuplicateColumns => {
                write!(f, "Duplicate columns")
            }
            ParseError::InvalidRegex => {
                write!(f, "Invalid Regex")
            }
            ParseError::EpochToDatetime(line) => {
                write!(f, "Error converting epoch column to datetime: {}", line)
            }
        }
    }
}
