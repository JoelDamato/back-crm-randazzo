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

const notionAPIBase = 'https://api.notion.com/v1';
const token = 'Bearer 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';

// Definir IDs de las bases de datos de Notion
const databaseId = "14e482517a9581458d4bfefbcde4ea03"; // Base de datos de clientes
const interacciones_database_id = "14e482517a9581cbbfa7e9fc3dd61bae"; // Base de datos de interacciones
const metrics_id = "14e482517a9581f1ba44c86043cf23a0";

// Función para procesar el CSV
function parseCSV(buffer) {
  return new Promise((resolve, reject) => {
    const results = [];
    const bufferStream = new stream.PassThrough();
    bufferStream.end(buffer);

    bufferStream
      .pipe(csv())
      .on('data', (data) => {
        results.push(data);
      })
      .on('end', () => {
        resolve(results);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

// Limpiar número de teléfono
function cleanPhoneNumber(phone) {
  let cleaned = phone.replace(/[^0-9+]/g, '');
  if (!cleaned.startsWith('+')) cleaned = `+${cleaned}`;
  const validFormat = /^\+\d{10,15}$/;
  return validFormat.test(cleaned) ? cleaned : null;
}

// Verificar si el cliente ya existe
async function obtenerClientePorTelefono(telefono) {
  const url = `${notionAPIBase}/databases/${databaseId}/query`;

  try {
    const response = await axios.post(
      url,
      {
        filter: {
          property: 'Telefono',
          phone_number: { equals: telefono },
        },
      },
      {
        headers: {
          Authorization: token,
          'Notion-Version': '2022-06-28',
          'Content-Type': 'application/json',
        },
      }
    );
    return response.data.results[0]?.id || null;
  } catch (error) {
    console.error('Error verificando cliente en Notion:', error.message);
    return null;
  }
}

// Crear un cliente en la base de datos principal
async function crearCliente(full_name, telefono, instagram, grupo) {
  const url = `${notionAPIBase}/pages`;

  const body = {
    parent: { database_id: databaseId },
    properties: {
      Nombre: { title: [{ text: { content: full_name } }] },
      Telefono: telefono ? { phone_number: telefono } : undefined,
      Instagram: instagram ? { rich_text: [{ text: { content: instagram } }] } : undefined,
      'CSV': { select: { name: grupo } },
      "Metricas": {
        relation: [{ id: metrics_id }],
      },
    },
  };

  try {
    const response = await axios.post(url, body, {
      headers: {
        Authorization: token,
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json',
      },
    });
    console.log('Cliente creado exitosamente:', response.data.id);
    return response.data.id;
  } catch (error) {
    console.error('Error creando cliente en Notion:', error.message);
    return null;
  }
}

// Crear una interacción relacionada con un cliente
async function crearInteraccion(clienteId, full_name) {
  const url = `${notionAPIBase}/pages`;

  const body = {
    parent: { database_id: interacciones_database_id },
    properties: {
      Interaccion: { title: [{ text: { content: `Carga Masiva` } }] },
      'Nombre cliente': { relation: [{ id: clienteId }] },
      'Tipo contacto': { select: { name: "Carga Masiva" } },
      "Metricas": {
        relation: [{ id: metrics_id }],
      },
      "Estado interaccion": { select: { name: "Finalizada" } },

    },
  };

  console.log('Datos enviados a Notion para crear interacción:', JSON.stringify(body, null, 2));

  try {
    const response = await axios.post(url, body, {
      headers: {
        Authorization: token,
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json',
      },
    });
    console.log('Interacción creada y relacionada con el cliente:', response.data.id);
  } catch (error) {
    console.error('Error creando interacción en Notion:', error.message);
    console.error('Detalles del error:', error.response?.data);
  }
}


// Procesar cliente y crear interacción
// Procesar cliente y crear interacción
async function procesarCliente(full_name, telefono, instagram, grupo) {
  try {
    // Si el teléfono está presente, limpiar el número
    const telefonoLimpio = telefono ? cleanPhoneNumber(telefono) : null;

    // Verificar que al menos un campo válido esté presente
    if (!full_name && !telefonoLimpio && !instagram) {
      console.error('No se puede crear un cliente sin datos significativos.');
      return;
    }

    // Verificar si el cliente ya existe por teléfono si está disponible
    let clienteId = null;
    if (telefonoLimpio) {
      clienteId = await obtenerClientePorTelefono(telefonoLimpio);
    }

    if (clienteId) {
      console.log('Cliente ya existe. ID:', clienteId);
    } else {
      console.log('Cliente no encontrado. Creando nuevo cliente...');
      const nuevoClienteId = await crearCliente(full_name || 'Sin nombre', telefonoLimpio, instagram, grupo);
      if (!nuevoClienteId) {
        console.error('No se pudo crear el cliente.');
        return;
      }
      console.log('Cliente creado con éxito:', nuevoClienteId);

      // Crear interacción para el nuevo cliente
      await crearInteraccion(nuevoClienteId, full_name || 'Sin nombre');
      return;
    }

    // Crear interacción para el cliente existente
    await crearInteraccion(clienteId, full_name || 'Sin nombre');
  } catch (error) {
    console.error('Error procesando cliente:', error.message);
  }
}

// Endpoint para cargar CSV y procesar clientes
app.post('/upload-file', upload.single('file'), async (req, res) => {
  if (!req.file) {
    return res.status(400).send('No se subió ningún archivo');
  }

  const grupo = req.body.grupo; // Leer el nombre del grupo
  if (!grupo) {
    return res.status(400).send('El nombre del grupo es obligatorio');
  }

  try {
    const data = await parseCSV(req.file.buffer);
    if (data.length === 0) {
      return res.status(400).send('El archivo no contiene datos');
    }

    let successCount = 0;
    for (const row of data) {
      const keys = Object.keys(row).map((key) => key.toLowerCase());
      const telefonoKey = keys.find((key) => key.includes('telefono'));
      const instagramKey = keys.find((key) => key.includes('instagram'));
      const nombreKey = keys.find((key) => key.includes('nombre'));

      const telefono = telefonoKey ? row[telefonoKey] : null;
      const instagram = instagramKey ? row[instagramKey] : null;
      const full_name = nombreKey ? row[nombreKey] : null;

      // Verificar si al menos uno de los campos tiene datos
      if (!full_name && !telefono && !instagram) {
        console.log(`Fila inválida: ${JSON.stringify(row)}`);
        continue;
      }

      console.log('Procesando cliente:', { full_name, telefono, instagram });

      await procesarCliente(full_name, telefono, instagram, grupo);
      successCount++;
    }

    res.status(200).send(`Archivo procesado. ${successCount} clientes procesados.`);
  } catch (error) {
    console.error('Error procesando archivo:', error);
    res.status(500).send('Error procesando archivo: ' + error.message);
  }
});


const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en http://localhost:${PORT}`);
});
