
use ksmparser::{ParseError, article, measurement};

#[test]
fn parse_article_invalid_filename() {
    let result = article::parse_art_file("invalid_file_name.abc");
    assert!(matches!(result, Err(ParseError::InvalidFile(_))));
}

#[test]
fn parse_missing_none_article() {
    let result = article::parse_art_file("testdata/missing_none.art");
    assert!(
        matches!(result, Err(ParseError::MissingField(_))),
        "Did not return ParseError::MissingField. Result: {:?}",
        result
    );
}

#[test]
fn parse_valid_article_parameters() {
    match article::parse_art_file("testdata/valid.art") {
        Ok(result) => {
            assert_eq!(result.get("pgm_name").unwrap(), "round_local");
            assert_eq!(result.get("cable_parts").unwrap(), "6");
            assert_eq!(result.get("check_wall_min_nomlimit").unwrap(), "0.13");
            assert_eq!(result.get("info1").unwrap(), "FKUX 105 0,25");
            assert_eq!(result.get("info6").unwrap(), "202");
            assert_eq!(result.get("material_core").unwrap(), "Cu");
        }
        Err(e) => {
            assert!(false, "parse_art_file returned Err({})", e);
        }
    }
}

#[test]
fn parse_valid_measurement_data() {
    match measurement::parse_dat_file("testdata/valid.dat") {
        Ok(result) => {
            assert_eq!(result.width(), 4, "Wrong number of columns");
            assert_eq!(result.height(), 4, "Wrong number of rows");
        },
        Err(e) => {
            assert!(false, "Error while parsing: {}", e);
        }
    }
}

#[test]
fn parse_uneven_rows_measurement_data() {
    match measurement::parse_dat_file("testdata/uneven_row.dat") {
        Ok(_) => {
            assert!(false, "Should return an error");
        },
        Err(e) => {
            match e {
                ParseError::MalformedEntry{ .. } => return,
                _ => assert!(false, "Should return MalformedEntry"),
            }
        }
    }
}

#[test]
fn parse_uneven_col_measurement_data() {
    match measurement::parse_dat_file("testdata/uneven_col.dat") {
        Ok(_) => {
            assert!(false, "Should return an error");
        },
        Err(e) => {
            match e {
                ParseError::MalformedEntry{ .. } => return,
                _ => assert!(false, "Should return MalformedEntry"),
            }
        }
    }
}
