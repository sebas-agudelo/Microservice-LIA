import express from "express";
import dotenv from "dotenv";
import { dailyJob } from "./jobs/dailyReports.js";
import mergeData from "./services/Leads/insertLeadsData.js";
import { fetchReportData } from "./services/dashboard_report/fetchReportData.js";
import { dbConnection } from "./config/db.js";


const app = express();
app.use(express.json());

dotenv.config();

app.get("/databases", async (req, res) => {
  const { poolConnection } = await dbConnection();
  await fetchReportData();
  // await mergeData();

  await poolConnection.end();

});

app.delete('/delete', async (req, res) => {
  try {
    const { poolConnection, filteredDatabases } = await dbConnection();

    for (const db of filteredDatabases) {
      console.log(`Tar bort data från ${db}.dashboard_report...`);

      await poolConnection.query(`DELETE FROM ${db}.dashboard_report`);

      console.log(`Data borttagen från ${db}.dashboard_report`);

      console.log(`Tar bort data från ${db}.leads...`);

      await poolConnection.query(`DELETE FROM ${db}.leads`);

      console.log(`Data borttagen från ${db}.leads`);

      // await poolConnection.end();
    }

    console.log("Rensning klar för alla databaser.");
  } catch (error) {
    console.error("Fel vid rensning av dashboard_report:", error);
    throw error;
  }
});

app.listen(process.env.PORT, async () => {
  // await dailyJob();
  console.log(`Servern är igång på port ${process.env.PORT}`);
});
