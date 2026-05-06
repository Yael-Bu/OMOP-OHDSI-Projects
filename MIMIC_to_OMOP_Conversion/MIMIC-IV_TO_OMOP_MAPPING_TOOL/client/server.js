const express = require('express');
const path = require('path');
const app = express();
const PORT = parseInt(process.env.PORT, 10) || 3000;


// משרת את הקבצים הסטטיים מתוך תיקיית build
app.use(express.static(path.join(__dirname, 'build')));

// כל בקשה אחרת מנותבת ל-index.html כדי לאפשר שימוש ב-React Router
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
