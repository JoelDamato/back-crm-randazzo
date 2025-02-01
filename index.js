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

const databaseId = "14e482517a9581458d4bfefbcde4ea03";
const interacciones_database_id = "14e482517a9581cbbfa7e9fc3dd61bae";
const metrics_id = "14e482517a9581f1ba44c86043cf23a0";

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

function cleanPhoneNumber(phone) {
  let cleaned = phone.replace(/[^0-9+]/g, '');
  if (!cleaned.startsWith('+')) cleaned = `+${cleaned}`;
  const validFormat = /^\+\d{10,15}$/;
  return validFormat.test(cleaned) ? cleaned : null;
}

async function crearCliente(full_name, telefono, instagram, grupo, closer) {
  const closerName = Array.isArray(closer) ? closer[0] : closer;

  const url = `${notionAPIBase}/pages`;
  const body = {
      parent: { database_id: databaseId },
      properties: {
          Nombre: { title: [{ text: { content: full_name } }] },
          Telefono: telefono ? { phone_number: telefono } : undefined,
          Instagram: instagram ? { rich_text: [{ text: { content: instagram } }] } : undefined,
          CSV: { select: { name: grupo } },
          Masiva: closerName ? { select: { name: closerName } } : { select: { name: "Sin closer" } },
          Metricas: { relation: [{ id: metrics_id }] },
      },
  };

  try {
      const response = await axios.post(url, body, {
          headers: {
              Authorization: token,
              "Notion-Version": "2022-06-28",
              "Content-Type": "application/json",
          },
      });
      return response.data.id;
  } catch (error) {
      console.error("Error creando cliente en Notion:", error.response?.data);
      return null;
  }
}

async function crearInteraccion(clienteId, full_name, closer) {
  const closerName = Array.isArray(closer) ? closer[0] : closer;
  const url = `${notionAPIBase}/pages`;
  const body = {
    parent: { database_id: interacciones_database_id },
    properties: {
      Interaccion: { title: [{ text: { content: "Carga Masiva" } }] },
      "Nombre cliente": { relation: [{ id: clienteId }] },
      "Tipo contacto": { select: { name: "Carga Masiva" } },
      Masiva: { select: { name: closerName } },
      "Metricas": { relation: [{ id: metrics_id }] },
      "Estado interaccion": { select: { name: "Finalizada" } },
    },
  };

  try {
    await axios.post(url, body, {
      headers: {
        Authorization: token,
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
      },
    });
  } catch (error) {
    console.error("Error creando interacción en Notion:", error.response?.data);
  }
}

app.post("/upload-file", upload.single("file"), async (req, res) => {
  if (!req.file) return res.status(400).send("No se subió ningún archivo");

  const grupo = req.body.grupo;
  const closer = req.body.closer;
  if (!grupo || !closer) return res.status(400).send("El nombre del grupo y el closer son obligatorios");

  try {
    const data = await parseCSV(req.file.buffer);
    if (data.length === 0) return res.status(400).send("El archivo no contiene datos");

    for (const row of data) {
      const keys = Object.keys(row).reduce((acc, key) => {
        acc[key.toLowerCase().trim()] = key;
        return acc;
      }, {});

      const full_name = row[keys["nombre"]] || "Sin nombre";
      const telefono = row[keys["telefono"]] || null;
      const instagram = row[keys["instagram"]] || null;

      const clienteId = await crearCliente(full_name, telefono, instagram, grupo, closer);
      if (clienteId) {
        await crearInteraccion(clienteId, full_name, closer);
      }
    }
    res.status(200).send("Archivo procesado correctamente.");
  } catch (error) {
    console.error("Error procesando archivo:", error);
    res.status(500).send("Error procesando archivo");
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.error(`Servidor corriendo en http://localhost:${PORT}`);
});
