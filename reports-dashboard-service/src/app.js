import express from 'express';
import dotenv from 'dotenv';
import { dbConnection } from './config/db.js';

const app = express();
app.use(express.json());

dotenv.config();

app.get('/data', async (req, res) => {
    try {

      const db = dbConnection();
  
 
      const [rows] = await db.query("SELECT * FROM participant");
  
   
        console.log(rows);
        
        return res.status(200).json(rows)
    } catch (error) {
      console.error("Fel vid hämtning av data:", error);
      res.status(500).json({ error: "Kunde inte hämta data från databasen" });
    }
  });

app.listen(process.env.PORT, () => {
    dbConnection();
    console.log(`Servern är igång på port ${process.env.PORT}`); 

});

