import express from 'express';
import dotenv from 'dotenv';
import { dbConnection } from './config/db.js';
import { reportService } from './services/reportService.js';
import { dailyJob } from './jobs/dailyReports.js';

const app = express();
app.use(express.json());

dotenv.config();

const start = async () => {
  await dailyJob()
}
app.listen(process.env.PORT, async () => {
  await start()
    console.log(`Servern är igång på port ${process.env.PORT}`); 

});

