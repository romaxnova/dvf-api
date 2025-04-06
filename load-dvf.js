const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { Pool } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const dataDir = path.join(__dirname, 'data');

async function insertBatch(records) {
  const query = `
    INSERT INTO dvf (code_commune, date_mutation, valeur_fonciere, surface_reelle_bati, type_local, latitude, longitude)
    VALUES ${records.map((_, i) => `($${i * 7 + 1}, $${i * 7 + 2}, $${i * 7 + 3}, $${i * 7 + 4}, $${i * 7 + 5}, $${i * 7 + 6}, $${i * 7 + 7})`).join(', ')}
  `;
  const flatValues = records.flat();
  await pool.query(query, flatValues);
}

async function loadCSV(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',' }))
      .on('data', (row) => {
        const record = [
          row.code_commune || null,
          row.date_mutation || null,
          parseFloat(row.valeur_fonciere) || null,
          parseFloat(row.surface_reelle_bati) || null,
          row.type_local || null,
          parseFloat(row.latitude) || null,
          parseFloat(row.longitude) || null
        ];
        rows.push(record);
      })
      .on('end', async () => {
        console.log(`📦 ${filePath} → ${rows.length} rows`);

        // Insert in batches of 500
        for (let i = 0; i < rows.length; i += 500) {
          const batch = rows.slice(i, i + 500);
          await insertBatch(batch);
        }

        resolve();
      })
      .on('error', reject);
  });
}

async function loadAll() {
  const folders = fs.readdirSync(dataDir);

  for (const folder of folders) {
    const fullFolderPath = path.join(dataDir, folder);
    if (!fs.statSync(fullFolderPath).isDirectory()) continue;

    const files = fs.readdirSync(fullFolderPath).filter(f => f.endsWith('.csv'));

    for (const file of files) {
      const filePath = path.join(fullFolderPath, file);
      await loadCSV(filePath);
    }
  }

  await pool.end();
  console.log('✅ All data loaded into NeonDB');
}

loadAll().catch(err => {
  console.error('❌ Error loading DVF data:', err);
});
