import express from "express";
import dotenv from "dotenv";
import { dailyJob } from "./jobs/dailyReports.js";


const app = express();
app.use(express.json());

dotenv.config();

app.listen(process.env.PORT, async () => {
  await dailyJob();
  console.log(`Servern är igång på port ${process.env.PORT}`);
});
