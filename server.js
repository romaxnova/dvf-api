const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const { Pool } = require('pg');

dotenv.config(); // Load .env file

const app = express();
const PORT = process.env.PORT || 3000;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

app.use(cors());

// === /api/dvf (flat rows) ===
app.get('/api/dvf', async (req, res) => {
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

  try {
    const conditions = [];
    const values = [];
    let idx = 1;

    if (year_min) {
      conditions.push(`EXTRACT(YEAR FROM date_mutation) >= $${idx++}`);
      values.push(parseInt(year_min));
    }

    if (year_max) {
      conditions.push(`EXTRACT(YEAR FROM date_mutation) <= $${idx++}`);
      values.push(parseInt(year_max));
    }

    if (price_min) {
      conditions.push(`valeur_fonciere >= $${idx++}`);
      values.push(parseFloat(price_min));
    }

    if (price_max) {
      conditions.push(`valeur_fonciere <= $${idx++}`);
      values.push(parseFloat(price_max));
    }

    if (price_m2_min) {
      conditions.push(`valeur_fonciere / NULLIF(surface_reelle_bati, 0) >= $${idx++}`);
      values.push(parseFloat(price_m2_min));
    }

    if (price_m2_max) {
      conditions.push(`valeur_fonciere / NULLIF(surface_reelle_bati, 0) <= $${idx++}`);
      values.push(parseFloat(price_m2_max));
    }

    if (bbox) {
      const [minLng, minLat, maxLng, maxLat] = bbox.split(',').map(parseFloat);
      conditions.push(`latitude BETWEEN $${idx} AND $${idx + 1}`);
      values.push(minLat, maxLat);
      idx += 2;
      conditions.push(`longitude BETWEEN $${idx} AND $${idx + 1}`);
      values.push(minLng, maxLng);
      idx += 2;
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql = `
      SELECT *
      FROM dvf
      ${whereClause}
      LIMIT $${idx}
    `;
    values.push(parseInt(limit));

    const result = await pool.query(sql, values);
    res.json(result.rows);
  } catch (err) {
    console.error('❌ DVF query failed:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// === /api/dvf/grouped (grouped by id_mutation) ===
app.get('/api/dvf/grouped', async (req, res) => {
  const { bbox, year_min, year_max, price_min, price_max } = req.query;

  const filters = [];
  const values = [];
  let idx = 1;

  if (bbox) {
    const [lngMin, latMin, lngMax, latMax] = bbox.split(',').map(parseFloat);
    filters.push(`latitude BETWEEN $${idx++} AND $${idx++}`);
    values.push(latMin, latMax);
    filters.push(`longitude BETWEEN $${idx++} AND $${idx++}`);
    values.push(lngMin, lngMax);
  }

  if (year_min) {
    filters.push(`EXTRACT(YEAR FROM date_mutation) >= $${idx++}`);
    values.push(year_min);
  }

  if (year_max) {
    filters.push(`EXTRACT(YEAR FROM date_mutation) <= $${idx++}`);
    values.push(year_max);
  }

  if (price_min) {
    filters.push(`valeur_fonciere >= $${idx++}`);
    values.push(price_min);
  }

  if (price_max) {
    filters.push(`valeur_fonciere <= $${idx++}`);
    values.push(price_max);
  }

  const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

  const query = `
    SELECT 
      id_mutation, 
      date_mutation,
      adresse_nom_voie, adresse_code_voie, adresse_numero, code_postal, nom_commune,
      valeur_fonciere,
      latitude, longitude,
      json_agg(json_build_object(
        'type_local', type_local,
        'surface_reelle_bati', surface_reelle_bati,
        'nombre_pieces_principales', nombre_pieces_principales
      )) AS lots
    FROM dvf
    ${whereClause}
    GROUP BY id_mutation, date_mutation, adresse_nom_voie, adresse_code_voie, adresse_numero, code_postal, nom_commune, valeur_fonciere, latitude, longitude
    ORDER BY date_mutation DESC
    LIMIT 1000;
  `;

  try {
    const { rows } = await pool.query(query, values);
    const result = rows.map(row => ({
      id_mutation: row.id_mutation,
      date_mutation: row.date_mutation,
      valeur_fonciere: row.valeur_fonciere,
      latitude: row.latitude,
      longitude: row.longitude,
      adresse: `${row.adresse_numero || ''} ${row.adresse_nom_voie || ''}, ${row.code_postal || ''} ${row.nom_commune || ''}`.trim(),
      lots: row.lots
    }));

    res.json(result);
  } catch (error) {
    console.error('❌ Grouped DVF API error:', error);
    res.status(500).json({ error: 'Failed to fetch grouped DVF data' });
  }
});

app.listen(PORT, () => {
  console.log(`✅ DVF API (PostgreSQL) running on http://localhost:${PORT}`);
});
