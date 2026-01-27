const API_BASE = "http://127.0.0.1:8000";

/* =====================================================
   🔥 TOP 10 VILLES LES PLUS CHAUDES
===================================================== */
fetch(`${API_BASE}/top10-hottest-cities`)
  .then(res => res.json())
  .then(data => {
    const labels = data.map(d => `${d.City} (${d.Country})`);
    const temps = data.map(d => d.avg_temperature);

    new Chart(document.getElementById("topCitiesChart"), {
      type: "bar",
      data: {
        labels: labels,
        datasets: [{
          label: "Température moyenne (°C)",
          data: temps,
          backgroundColor: "rgba(255, 99, 132, 0.7)"
        }]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { display: true }
        }
      }
    });
  })
  .catch(err => console.error("Erreur Top 10 villes :", err));


/* =====================================================
   📈 ÉVOLUTION ANNUELLE GLOBALE
===================================================== */
fetch(`${API_BASE}/global-yearly-trend`)
  .then(res => res.json())
  .then(data => {
    const years = data.map(d => d.year);
    const temps = data.map(d => d.global_avg_temperature);

    new Chart(document.getElementById("globalTrendChart"), {
      type: "line",
      data: {
        labels: years,
        datasets: [{
          label: "Température moyenne globale (°C)",
          data: temps,
          borderColor: "rgba(54, 162, 235, 1)",
          backgroundColor: "rgba(54, 162, 235, 0.2)",
          fill: true,
          tension: 0.25
        }]
      },
      options: {
        responsive: true,
        scales: {
          x: { title: { display: true, text: "Année" } },
          y: { title: { display: true, text: "Température (°C)" } }
        }
      }
    });
  })
  .catch(err => console.error("Erreur évolution annuelle :", err));


/* =====================================================
   🌍 TEMPÉRATURE MOYENNE PAR PAYS (TOP 15)
===================================================== */
fetch(`${API_BASE}/avg-temp-by-country?limit=15`)
  .then(res => res.json())
  .then(data => {
    const countries = data.map(d => d.Country);
    const temps = data.map(d => d.avg_temperature);

    new Chart(document.getElementById("countryChart"), {
      type: "bar",
      data: {
        labels: countries,
        datasets: [{
          label: "Température moyenne (°C)",
          data: temps,
          backgroundColor: "rgba(255, 206, 86, 0.7)"
        }]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        plugins: {
          legend: { display: true }
        }
      }
    });
  })
  .catch(err => console.error("Erreur par pays :", err));


/* =====================================================
   🏙️ ÉVOLUTION ANNUELLE D’UNE VILLE (DRILL-DOWN)
   (exemple statique pour la démo)
===================================================== */

const CITY = "Paris";
const COUNTRY = "France";

fetch(`${API_BASE}/city-yearly-trend?city=${CITY}&country=${COUNTRY}`)
  .then(res => res.json())
  .then(data => {
    if (data.length === 0) return;

    const years = data.map(d => d.year);
    const temps = data.map(d => d.avg_temperature);

    new Chart(document.getElementById("cityTrendChart"), {
      type: "line",
      data: {
        labels: years,
        datasets: [{
          label: `Température moyenne à ${CITY}`,
          data: temps,
          borderColor: "rgba(153, 102, 255, 1)",
          fill: false,
          tension: 0.2
        }]
      },
      options: {
        responsive: true
      }
    });
  })
  .catch(err => console.error("Erreur évolution ville :", err));
