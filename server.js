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
        type_local, nombre_pieces_principales,
        surface_reelle_bati,
        lot1_numero, lot1_surface_carrez,
        lot2_numero, lot2_surface_carrez,
        lot3_numero, lot3_surface_carrez,
        lot4_numero, lot4_surface_carrez,
        lot5_numero, lot5_surface_carrez
      FROM dvf
      ${whereClause}
      ORDER BY date_mutation DESC
      LIMIT 1000;
    `;
  
    try {
      const { rows } = await pool.query(query, values);
  
      // Group by id_mutation
      const grouped = {};
  
      for (const row of rows) {
        const id = row.id_mutation;
        if (!grouped[id]) {
          grouped[id] = {
            id_mutation: id,
            date_mutation: row.date_mutation,
            valeur_fonciere: row.valeur_fonciere,
            latitude: row.latitude,
            longitude: row.longitude,
            adresse: `${row.adresse_numero || ''} ${row.adresse_nom_voie || ''}, ${row.code_postal || ''} ${row.nom_commune || ''}`.trim(),
            lots: []
          };
        }
  
        for (let i = 1; i <= 5; i++) {
            const numero = row[`lot${i}_numero`];
            const carrez = row[`lot${i}_surface_carrez`];
            const type_local = row.type_local || null;
            const surface_reelle_bati = row.surface_reelle_bati || null;
          
            if (numero || carrez || surface_reelle_bati) {
              grouped[id].lots.push({
                lot_numero: numero || null,
                Surface: surface_reelle_bati || null,
                Carrez: carrez || null,
                type_local,
                nombre_pieces_principales: row.nombre_pieces_principales || null
              });
            }
          }                   
      }
  
      const result = Object.values(grouped);
      res.json(result);
    } catch (error) {
      console.error('❌ Grouped DVF API error:', error);
      res.status(500).json({ error: 'Failed to fetch grouped DVF data' });
    }
  });  

app.listen(PORT, () => {
  console.log(`✅ DVF API (PostgreSQL) running on http://localhost:${PORT}`);
});
