use std::error::Error;
use std::ops::Index;

type DataInteger = i32;
type DataFloat = f64;
type DataText = String;
type DataBool = bool;

#[derive(Debug)]
pub struct ColumnInteger {
    name: String,
    data: Vec<DataInteger>,
}

#[derive(Debug)]
pub struct ColumnFloat {
    name: String,
    data: Vec<DataFloat>,
}

#[derive(Debug)]
pub struct ColumnText {
    name: String,
    data: Vec<DataText>,
}

#[derive(Debug)]
pub struct ColumnBool {
    name: String,
    data: Vec<DataBool>,
}

#[derive(Debug)]
pub struct DataFrame {
    columns: Vec<DataColumn>,
}

impl Index<&str> for DataFrame {
    type Output = DataColumn;

    fn index(&self, name: &str) -> &Self::Output {
        for col in &self.columns {
            match col {
                DataColumn::IntegerDataColumn(c) => {
                    if c.name == name {
                        return &col;
                    }
                }
                DataColumn::TextDataColumn(c) => {
                    if c.name == name {
                        return &col;
                    }
                }
                DataColumn::FloatDataColumn(c) => {
                    if c.name == name {
                        return &col;
                    }
                }
                DataColumn::BoolDataColumn(c) => {
                    if c.name == name {
                        return &col;
                    }
                }
            }
        }
        panic!("unknown column name")
    }
}

#[derive(Debug)]
pub enum DataColumn {
    IntegerDataColumn(ColumnInteger),
    TextDataColumn(ColumnText),
    FloatDataColumn(ColumnFloat),
    BoolDataColumn(ColumnBool),
}

#[derive(Debug)]
struct DataFrameError {
    msg: String,
}

impl DataFrameError {
    fn create(msg: &str) -> Box<dyn Error> {
        Box::new(DataFrameError {
            msg: msg.to_owned(),
        })
    }
}

impl std::fmt::Display for DataFrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error for DataFrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl DataFrame {
    pub fn new(
        column_names: Vec<&str>,
        data: Vec<Vec<DataCell>>,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let num_cols = column_names.len();
        let mut column_types = vec![];

        // Figure out the column types from the data
        if data.len() > 0 {
            for i in 0..num_cols {
                if i >= data[0].len() {
                    // Default to integer
                    column_types.push(DataTypes::Integer);
                } else {
                    column_types.push(data[0][i].data_type());
                }
            }
        } else {
            for _ in 0..num_cols {
                column_types.push(DataTypes::Integer);
            }
        }

        // create columns based on column types
        let mut cols = Vec::<DataColumn>::new();
        for (i, v) in column_types.iter().enumerate() {
            match v {
                DataTypes::Integer => cols.push(DataColumn::IntegerDataColumn(ColumnInteger {
                    name: column_names[i].to_string(),
                    data: vec![],
                })),
                DataTypes::Text => cols.push(DataColumn::TextDataColumn(ColumnText {
                    name: column_names[i].to_string(),
                    data: vec![],
                })),
                DataTypes::Float => cols.push(DataColumn::FloatDataColumn(ColumnFloat {
                    name: column_names[i].to_string(),
                    data: vec![],
                })),
                DataTypes::Bool => cols.push(DataColumn::BoolDataColumn(ColumnBool {
                    name: column_names[i].to_string(),
                    data: vec![],
                })),
            }
        }

        // Go through each data cell and if they can be added to the appropriate column, do it
        for row in data.iter() {
            if row.len() != num_cols {
                return Err(DataFrameError::create(
                    "length of data provided did not match expected number of columns",
                ));
            }

            for (col_index, cell) in row.iter().enumerate() {
                match &mut cols[col_index] {
                    DataColumn::IntegerDataColumn(col) => match &cell {
                        DataCell::IntegerDataCell(val) => col.data.push(val.clone()),
                        _ => {
                            return Err(DataFrameError::create(
                                "data cell type did not match integer column type",
                            ))
                        }
                    },
                    DataColumn::TextDataColumn(col) => match &cell {
                        DataCell::TextDataCell(val) => col.data.push(val.clone()),
                        _ => {
                            return Err(DataFrameError::create(
                                "data cell type did not match text column type",
                            ))
                        }
                    },
                    DataColumn::FloatDataColumn(col) => match &cell {
                        DataCell::FloatDataCell(val) => col.data.push(val.clone()),
                        _ => {
                            return Err(DataFrameError::create(
                                "data cell type did not match float column type",
                            ))
                        }
                    },
                    DataColumn::BoolDataColumn(col) => match &cell {
                        DataCell::BoolDataCell(val) => col.data.push(val.clone()),
                        _ => {
                            return Err(DataFrameError::create(
                                "data cell type did not match bool column type",
                            ))
                        }
                    },
                }
            }
        }

        Ok(DataFrame { columns: cols })
    }
}

#[derive(Debug)]
enum DataTypes {
    Integer,
    Text,
    Float,
    Bool,
}

#[derive(Debug)]
pub enum DataCell {
    IntegerDataCell(DataInteger),
    TextDataCell(DataText),
    FloatDataCell(DataFloat),
    BoolDataCell(DataBool),
}

impl DataCell {
    fn data_type(&self) -> DataTypes {
        match self {
            DataCell::IntegerDataCell(_) => DataTypes::Integer,
            DataCell::TextDataCell(_) => DataTypes::Text,
            DataCell::FloatDataCell(_) => DataTypes::Float,
            DataCell::BoolDataCell(_) => DataTypes::Bool,
        }
    }
}

impl From<DataInteger> for DataCell {
    fn from(v: DataInteger) -> Self {
        DataCell::IntegerDataCell(v)
    }
}

impl From<DataText> for DataCell {
    fn from(v: DataText) -> Self {
        DataCell::TextDataCell(v)
    }
}

impl From<DataFloat> for DataCell {
    fn from(v: DataFloat) -> Self {
        DataCell::FloatDataCell(v)
    }
}

impl From<DataBool> for DataCell {
    fn from(v: DataBool) -> Self {
        DataCell::BoolDataCell(v)
    }
}

impl From<&str> for DataCell {
    fn from(v: &str) -> Self {
        DataCell::TextDataCell(v.to_owned())
    }
}

#[macro_export]
macro_rules! row {
    ( $( $x:expr ),* ) => {
        {
            let mut temp_vec = Vec::<DataCell>::new();
            $(
                temp_vec.push(DataCell::from($x));
            )*
            temp_vec
        }
    };
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_simple() {
        let dataframe = DataFrame::new(
            vec!["width", "height", "name", "in_stock", "count"],
            vec![
                row![0.4, 0.7, "book", true, 1],
                row![3.0, 4.7, "poster", true, 1],
            ],
        );
        assert_eq!(dataframe.is_ok(), true);
    }

    #[test]
    fn test_simple_col() -> Result<(), Box<dyn Error>> {
        let dataframe = DataFrame::new(
            vec!["width", "height", "name", "in_stock", "count"],
            vec![
                row![0.4, 0.7, "book", true, 1],
                row![3.0, 4.7, "poster", true, 1],
            ],
        )?;
        if let DataColumn::FloatDataColumn(widths) = &dataframe["width"] {
            assert_eq!(widths.data.len(), 2);
        } else {
            assert!(false, "wrong type")
        }
        Ok(())
    }
}