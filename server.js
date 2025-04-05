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
  
    let results = dvfData.filter(d => {
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
  
    res.json(results.slice(0, parseInt(limit)));
  });
  