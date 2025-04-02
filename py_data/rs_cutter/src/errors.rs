use thiserror::Error;

#[derive(Error, Debug)]
pub enum MyError {
    #[error("Index error: {0}")]
    IndexError(String),
    #[error("Column type error: {0}")]
    ColumnTypeError(String),
    #[error("Date error: {0}")]
    DateError(String),
}
