import express from "express";
import dotenv from "dotenv";
import { dbConnection } from "./config/db.js";
import { dailyJob } from "./jobs/dailyReports.js";
import { fetchReportData } from "./services/dashboard_report/fetchReportData.js";
import { collectDataForLeads } from "./services/Leads/collectDataForLeads.js";

const app = express();
app.use(express.json());

dotenv.config();

app.get("/databases", async (req, res) => {
  await fetchReportData()
  
});


app.delete('/delete', async (req, res) => {
  try {
    const { poolConnection, filteredDatabases } = await dbConnection();

    for (const db of filteredDatabases) {
      console.log(`Tar bort data från ${db}.dashboard_report...`);

      await poolConnection.query(`DELETE FROM ${db}.dashboard_report`);

      console.log(`Data borttagen från ${db}.dashboard_report`);
    
    }

    console.log("Rensning klar för alla databaser.");
  } catch (error) {
    console.error("Fel vid rensning av dashboard_report:", error);
    throw error;
  }
});

const start = async () => {
  await dailyJob();
};
app.listen(process.env.PORT, async () => {
  await start();
  console.log(`Servern är igång på port ${process.env.PORT}`);
});
