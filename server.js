const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const { Pool } = require('pg');

dotenv.config(); // Load .env file
console.log("📦 Loaded DATABASE_URL:", process.env.DATABASE_URL);


const app = express();
const PORT = process.env.PORT || 3000;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

app.use(cors());

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
    // Build dynamic SQL query
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

app.listen(PORT, () => {
  console.log(`✅ DVF API (PostgreSQL) running on http://localhost:${PORT}`);
});
