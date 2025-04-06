const express = require('express');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());

let dvfData = [];

const dataRoot = path.join(__dirname, 'data');

/**
 * Load all CSVs from any subfolder under /data/
 */
function loadAllCSVFiles(rootDir) {
  fs.readdir(rootDir, (err, folders) => {
    if (err) return console.error('❌ Cannot read data directory:', err);

    folders.forEach((subfolder) => {
      const subDirPath = path.join(rootDir, subfolder);

      fs.stat(subDirPath, (err, stats) => {
        if (err || !stats.isDirectory()) return;

        fs.readdir(subDirPath, (err, files) => {
          if (err) return console.error(`❌ Failed to read ${subDirPath}:`, err);

          const csvFiles = files.filter(f => f.endsWith('.csv'));
          csvFiles.forEach(file => {
            const filePath = path.join(subDirPath, file);
            fs.createReadStream(filePath)
              .pipe(csv({ separator: ',' }))
              .on('data', (row) => {
                dvfData.push(row); // ✅ Keep all years and all rows
              })
              .on('end', () => {
                console.log(`✅ Loaded: ${filePath}`);
              });
          });
        });
      });
    });
  });
}

// Load everything at startup
loadAllCSVFiles(dataRoot);

/**
 * Serve filtered DVF data
 */
app.get('/api/dvf', (req, res) => {
  const {
    bbox,
    limit = 1000,
    year_min,
    year_max,
    price_min,
    price_max,
    price_m2_min,
    price_m2_max
  } = req.query;

  const filtered = dvfData.filter(d => {
    const lat = parseFloat(d.latitude);
    const lon = parseFloat(d.longitude);
    const date = new Date(d.date_mutation);
    const year = date.getFullYear();
    const price = parseFloat(d.valeur_fonciere);
    const surface = parseFloat(d.surface_reelle_bati);
    const priceM2 = surface > 0 ? price / surface : null;

    if (bbox) {
      const [minLng, minLat, maxLng, maxLat] = bbox.split(',').map(parseFloat);
      if (!(lat >= minLat && lat <= maxLat && lon >= minLng && lon <= maxLng)) {
        return false;
      }
    }

    return (!year_min || year >= parseInt(year_min)) &&
           (!year_max || year <= parseInt(year_max)) &&
           (!price_min || price >= parseFloat(price_min)) &&
           (!price_max || price <= parseFloat(price_max)) &&
           (!price_m2_min || (priceM2 !== null && priceM2 >= parseFloat(price_m2_min))) &&
           (!price_m2_max || (priceM2 !== null && priceM2 <= parseFloat(price_m2_max)));
  });

  res.json(filtered.slice(0, parseInt(limit)));
});

app.listen(PORT, () => {
  console.log(`✅ DVF API running: http://localhost:${PORT}`);
});
