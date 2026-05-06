import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Container, Typography, Select, MenuItem, FormControl, InputLabel, Box, Card, CardContent } from '@mui/material';



function App() {

  const apiUrl = "http://localhost:8000"; // for loacalhost development

  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [explanation, setExplanation] = useState('');
  const [columns, setColumns] = useState([]);
  const [selectedColumn, setSelectedColumn] = useState('');
  const [columnMapping, setColumnMapping] = useState('');

  useEffect(() => {
    // שליפת רשימת הטבלאות מהשרת
    axios.get(`${apiUrl}/tables`)
    
      .then(response => {
        console.log("hello world1");
        setTables(response.data.tables);
      })
      .catch(error => {
        console.log("hello world2");
        console.error('There was an error fetching the tables!', error);
      });
  }, []);

  const handleTableSelect = (table) => {
    setSelectedTable(table);
    // שליפת ההסבר של הטבלה
    axios.get(`${apiUrl}/tables/${table}/explanation`)
      .then(response => {
        setExplanation(response.data.explanation);
      })
      .catch(error => {
        console.error('There was an error fetching the explanation!', error);
      });

    // שליפת העמודות של הטבלה
    axios.get(`${apiUrl}/tables/${table}/columns`)
      .then(response => {
        setColumns(response.data.columns);
      })
      .catch(error => {
        console.error('There was an error fetching the columns!', error);
      });
  };

  const handleColumnSelect = (column) => {
    setSelectedColumn(column);
    // שליפת המיפוי של העמודה
    axios.get(`${apiUrl}/tables/${selectedTable}/columns/${column}`)
      .then(response => {
        setColumnMapping(response.data.mapping);
      })
      .catch(error => {
        console.error('There was an error fetching the column mapping!', error);
      });
  };

  return (
    <Box sx={{ backgroundColor: '#e3f2fd', minHeight: '100vh', padding: '30px' }}>
      <Container maxWidth="md">
        <Card sx={{ padding: '20px', backgroundColor: '#f5f5f5', borderRadius: '10px' }}>
          <CardContent>
            <Typography variant="h3" gutterBottom align="center" sx={{ color: '#3f51b5' }}>
              MIMIC-IV to OMOP Mapping
            </Typography>

            {/* Dropdown לבחירת טבלה */}
            <FormControl fullWidth sx={{ marginBottom: '20px' }}>
              <InputLabel id="table-select-label">Select a Table</InputLabel>
              <Select
                labelId="table-select-label"
                value={selectedTable}
                label="Select a Table"
                onChange={(e) => handleTableSelect(e.target.value)}
              >
                {tables.map((table, index) => (
                  <MenuItem key={index} value={table}>{table}</MenuItem>
                ))}
              </Select>
            </FormControl>

            {/* הצגת ההסבר לטבלה שנבחרה */}
            {explanation && (
              <Box sx={{ marginBottom: '20px', padding: '10px', border: '1px solid #3f51b5', borderRadius: '5px', backgroundColor: '#e8eaf6' }}>
                <Typography variant="h5" sx={{ color: '#3f51b5' }}>Explanation</Typography>
                <Typography variant="body1" component="div">
                  <div dangerouslySetInnerHTML={{ __html: explanation }} />
                </Typography>
              </Box>
            )}


            {/* Dropdown לבחירת עמודה מתוך הטבלה שנבחרה */}
            {columns.length > 0 && (
              <FormControl fullWidth sx={{ marginBottom: '20px' }}>
                <InputLabel id="column-select-label">Select a Column</InputLabel>
                <Select
                  labelId="column-select-label"
                  value={selectedColumn}
                  label="Select a Column"
                  onChange={(e) => handleColumnSelect(e.target.value)}
                >
                  {columns.map((column, index) => (
                    <MenuItem key={index} value={column}>{column}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            )}

            {/* הצגת המיפוי של העמודה שנבחרה */}
            {columnMapping && (
              <Box sx={{ padding: '10px', border: '1px solid #3f51b5', borderRadius: '5px', backgroundColor: '#e8eaf6' }}>
                <Typography variant="h5" sx={{ color: '#3f51b5' }}>Column Mapping</Typography>
                <Typography variant="body1" component="div">
                  <div dangerouslySetInnerHTML={{ __html: columnMapping }} />
                </Typography>
              </Box>
            )}
          </CardContent>
        </Card>
      </Container>
    </Box>
  );
}

export default App;
