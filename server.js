const express = require('express');
const fs = require('fs');
const csv = require('csv-parser');
const app = express();
const PORT = process.env.PORT || 3000;

let dvfData = [];

fs.createReadStream('./data/dvf2024/75.csv')
  .pipe(csv({ separator: ',' }))
  .on('data', (row) => dvfData.push(row))
  .on('end', () => {
    console.log(`✅ Loaded ${dvfData.length} DVF records`);
  });

app.get('/api/dvf', (req, res) => {
  const { date_min, price_max, code_commune } = req.query;

  const filtered = dvfData.filter(d => {
    const matchDate = !date_min || d.date_mutation >= date_min;
    const matchPrice = !price_max || parseFloat(d.valeur_fonciere) <= parseFloat(price_max);
    const matchCommune = !code_commune || d.code_commune === code_commune;
    return matchDate && matchPrice && matchCommune;
  });

  res.json(filtered.slice(0, 1000)); // limit for now
});

app.listen(PORT, () => {
  console.log(`DVF API running: http://localhost:${PORT}`);
});
