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
  // Eliminar todos los caracteres no numéricos
  const cleaned = phone.replace(/\D/g, '');
  console.log(`Número original: "${phone}", Número limpio: "${cleaned}"`);
  return cleaned;
}

async function obtenerNumerosExistentesEnNotion() {
  const url = 'https://api.notion.com/v1/databases/11b760f166c0819abc9dfdd6a51ba241/query';
  const token = 'Bearer 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';

  const headers = {
    'Authorization': token,
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json',
  };

  try {
    const response = await axios.post(url, {}, { headers });
    const data = response.data.results;

    // Convertir todos los números de Notion a cadenas de texto para su comparación
    const numerosExistentes = data
      .map((entry) => {
        const telefono = entry.properties['Telefono']?.number;
        return telefono ? telefono.toString() : null;  // Convertir números a string para comparar
      })
      .filter(Boolean);  // Asegurarse de no incluir valores undefined o null

    console.log('Números ya existentes en Notion (limpios):', numerosExistentes);
    return numerosExistentes;
  } catch (error) {
    console.error('Error obteniendo los números existentes en Notion:', error.response?.data || error.message);
    return [];
  }
}

async function enviarANotion(telefono, grupo) {
  console.log(`Intentando enviar a Notion. Teléfono: ${telefono}, Grupo: ${grupo}`);
  const url = 'https://api.notion.com/v1/pages';
  const token = 'Bearer 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';

  const telefonoLimpio = cleanPhoneNumber(telefono);
  if (!telefonoLimpio) {
    console.error('Número de teléfono inválido:', telefono);
    return false;
  }

  const telefonoNumero = parseInt(telefonoLimpio, 10);  // Convertir a número
  if (isNaN(telefonoNumero)) {
    console.error('No se pudo convertir el teléfono a número:', telefonoLimpio);
    return false;
  }

  const body = {
    parent: { database_id: "11b760f166c0819abc9dfdd6a51ba241" },
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
      "Telefono": { number: telefonoNumero },  // Enviar como número
      "Grupo Whatsapp": { select: { name: grupo } }
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
    console.log('Dato cargado correctamente:', telefonoNumero);
    return true;
  } catch (error) {
    console.error('Error cargando a Notion:', error.response?.data || error.message);
    return false;
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
      const telefono = row.telefono || row.Telefono || row.TELEFONO;
      const telefonoLimpio = cleanPhoneNumber(telefono);
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
