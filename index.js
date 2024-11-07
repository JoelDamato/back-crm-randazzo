const fs = require('fs');
const csv = require('csv-parser');
const axios = require('axios');
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const stream = require('stream');

const app = express();
app.use(cors());

const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

// Token de Notion (directamente en el script)
const token = 'Bearer ntn_GG849748837abCQnsctJHEtwe9JNDxoKbjkD61zGuqO02D';

function parseCSV(buffer) {
  return new Promise((resolve, reject) => {
    const results = [];
    const bufferStream = new stream.PassThrough();
    bufferStream.end(buffer);

    bufferStream
      .pipe(csv())
      .on('data', (data) => {
        console.log('Fila leída del CSV:', data);
        results.push(data);
      })
      .on('end', () => {
        console.log('Parsing CSV completado. Filas totales:', results.length);
        resolve(results);
      })
      .on('error', (error) => {
        console.error('Error parseando CSV:', error);
        reject(error);
      });
  });
}

function cleanPhoneNumber(phone) {
  // Eliminar todos los caracteres excepto números y el símbolo +
  let cleaned = phone.replace(/[^0-9+]/g, '');

  // Asegurarse de que el número empiece con +
  if (!cleaned.startsWith('+')) {
    cleaned = `+${cleaned}`;
  }

  // Validar el formato internacional del número
  const validFormat = /^\+\d{10,15}$/;
  if (!validFormat.test(cleaned)) {
    console.error(`Número de teléfono no válido después de limpiar: "${cleaned}"`);
    return null;
  }

  console.log(`Número original: "${phone}", Número limpio: "${cleaned}"`);
  return cleaned;
}

async function obtenerNumerosExistentesEnNotion() {
  const url = 'https://api.notion.com/v1/databases/128032a62365817cb2aef2c4c2b20179/query';

  const headers = {
    'Authorization': token,
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json',
  };

  try {
    const response = await axios.post(url, { page_size: 100 }, { headers });
    const data = response.data.results;

    // Convertir todos los números de Notion a cadenas de texto para su comparación
    const numerosExistentes = data
      .map((entry) => {
        const telefono = entry.properties['Telefono']?.phone_number;
        return telefono ? telefono.toString() : null;
      })
      .filter(Boolean);

    console.log('Números ya existentes en Notion (limpios):', numerosExistentes);
    return numerosExistentes;
  } catch (error) {
    console.error('Error obteniendo los números existentes en Notion:', error.response?.data || error.message);
    return [];
  }
}

async function enviarANotion(telefono, grupo, retries = 3) {
  console.log(`Intentando enviar a Notion. Teléfono: ${telefono}, Grupo: ${grupo}`);
  const url = 'https://api.notion.com/v1/pages';

  const telefonoLimpio = cleanPhoneNumber(telefono);
  if (!telefonoLimpio) {
    console.error('Número de teléfono inválido:', telefono);
    return false;
  }

  const body = {
    parent: { database_id: "128032a62365817cb2aef2c4c2b20179" },
    properties: {
      "Nombre": {
        title: [
          {
            text: {
              content: "Nuevo Cliente"
            }
          }
        ]
      },
      "Telefono": { 
        phone_number: telefonoLimpio  // Enviar como phone_number en formato string
      },
      "Grupo Whatsapp": { select: { name: grupo } },
      "Metricas": {  // Relacionar con el ítem específico en la base de datos "Métricas Totales"
        relation: [
          {
            id: "128032a6236581f59b7bf8993198b037"  // ID del item en la base de datos "Métricas Totales"
          }
        ]
      }
    }
  };

  try {
    const headers = {
      'Authorization': token,
      'Notion-Version': '2022-06-28',
      'Content-Type': 'application/json'
    };
    const response = await axios.post(url, body, { headers });
    console.log('Respuesta de Notion:', response.status, response.statusText);
    console.log('Dato cargado correctamente:', telefonoLimpio);
    return true;
  } catch (error) {
    if (error.response?.status === 409 && retries > 0) {
      console.error('Conflicto al guardar en Notion. Reintentando...');
      await new Promise(resolve => setTimeout(resolve, 1000)); // Esperar 1 segundo antes de reintentar
      return enviarANotion(telefono, grupo, retries - 1);
    } else {
      console.error('Error cargando a Notion:', error.response?.data || error.message);
      return false;
    }
  }
}

app.post('/upload-file', upload.single('file'), async (req, res) => {
  console.log('Solicitud recibida en /upload-file');
  if (!req.file) {
    console.error('No se subió ningún archivo');
    return res.status(400).send('No se subió ningún archivo');
  }

  const grupo = req.body.grupo;
  if (!grupo) {
    console.error('El nombre del grupo es obligatorio');
    return res.status(400).send('El nombre del grupo es obligatorio');
  }

  console.log('Archivo recibido:', req.file.originalname);
  console.log('Grupo:', grupo);

  try {
    const data = await parseCSV(req.file.buffer);
    if (data.length === 0) {
      console.error('No se encontraron datos en el archivo');
      return res.status(400).send('No se encontraron datos en el archivo');
    }

    const numerosExistentes = await obtenerNumerosExistentesEnNotion();  // Obtener los números ya existentes en Notion

    // Filtrar los números que ya existen en Notion para no reenviarlos
    const numerosFiltrados = data.filter((row) => {
      const keys = Object.keys(row).map(key => key.toLowerCase());
      const telefonoKey = keys.find(key => key.includes('telefono'));
      const telefono = telefonoKey ? row[telefonoKey] : null;
      const telefonoLimpio = telefono ? cleanPhoneNumber(telefono) : null;
      return telefonoLimpio && !numerosExistentes.includes(telefonoLimpio);
    });

    let successCount = 0;
    for (const row of numerosFiltrados) {
      const telefono = row.telefono || row.Telefono || row.TELEFONO;
      if (telefono) {
        console.log('Procesando teléfono:', telefono);
        const success = await enviarANotion(telefono, grupo);
        if (success) successCount++;
      } else {
        console.error('Fila sin número de teléfono:', row);
      }
    }

    console.log(`Proceso completado. ${successCount} números enviados a Notion`);
    res.status(200).send(`Archivo procesado. ${successCount} números enviados a Notion`);
  } catch (error) {
    console.error('Error procesando el archivo:', error);
    res.status(500).send('Error procesando el archivo: ' + error.message);
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en http://localhost:${PORT}`);
});
