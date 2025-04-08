const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { Pool } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const dataDir = path.join(__dirname, 'data');

// Full field list from your schema (excluding the auto ID)
const FIELD_NAMES = [
  'id_mutation', 'date_mutation', 'numero_disposition', 'nature_mutation', 'valeur_fonciere',
  'adresse_numero', 'adresse_suffixe', 'adresse_nom_voie', 'adresse_code_voie',
  'code_postal', 'code_commune', 'nom_commune', 'code_departement',
  'ancien_code_commune', 'ancien_nom_commune', 'id_parcelle', 'ancien_id_parcelle',
  'numero_volume', 'lot1_numero', 'lot1_surface_carrez', 'lot2_numero', 'lot2_surface_carrez',
  'lot3_numero', 'lot3_surface_carrez', 'lot4_numero', 'lot4_surface_carrez',
  'lot5_numero', 'lot5_surface_carrez', 'nombre_lots',
  'code_type_local', 'type_local', 'surface_reelle_bati', 'nombre_pieces_principales',
  'code_nature_culture', 'nature_culture', 'code_nature_culture_speciale',
  'nature_culture_speciale', 'surface_terrain', 'longitude', 'latitude'
];

async function insertBatch(records) {
  const valuesClause = records.map((_, i) => {
    const offset = i * FIELD_NAMES.length;
    const placeholders = FIELD_NAMES.map((_, j) => `$${offset + j + 1}`);
    return `(${placeholders.join(', ')})`;
  }).join(', ');

  const flatValues = records.flat();
  const query = `INSERT INTO dvf (${FIELD_NAMES.join(', ')}) VALUES ${valuesClause}`;

  await pool.query(query, flatValues);
}

async function loadCSV(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];

    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',' }))
      .on('data', (row) => {
        const record = FIELD_NAMES.map(field => {
          const val = row[field];
          if (val === '') return null;
          if (['valeur_fonciere', 'surface_reelle_bati', 'lot1_surface_carrez', 'lot2_surface_carrez', 'lot3_surface_carrez', 'lot4_surface_carrez', 'lot5_surface_carrez', 'surface_terrain', 'latitude', 'longitude'].includes(field)) {
            return parseFloat(val.replace(',', '.')) || null;
          }
          if (['numero_disposition', 'nombre_lots', 'nombre_pieces_principales'].includes(field)) {
            return parseInt(val) || null;
          }
          return val || null;
        });

        rows.push(record);
      })
      .on('end', async () => {
        console.log(`📦 ${filePath} → ${rows.length} rows`);

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
