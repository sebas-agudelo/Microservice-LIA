import express from "express";
import dotenv from "dotenv";
import { dbConnection } from "./config/db.js";
import { reportService } from "./services/reportService.js";
import { dailyJob } from "./jobs/dailyReports.js";
import { fetchReportData } from "./services/fetchReportData.js";

const app = express();
app.use(express.json());

dotenv.config();

app.get("/databases", async (req, res) => {
  // const { poolConnection } = await dbConnection();
  // const [databases] = await poolConnection.query(`SHOW DATABASES`);
  // const filtredDatabases = databases
  //   .map((db) => db.Database)
  //   .filter(
  //     (name) =>
  //       name !== "db_$DATA" &&
  //       name !== "cokecce_adoveo_com" &&
  //       name !== "danskebank_ratataa_se" &&
  //       name !== "tmp" &&
  //       name !== "sys" &&
  //       name !== "mysql" &&
  //       name !== "life_ratataa_se" &&
  //       name !== "innodb" &&
  //       name !== "information_schema" &&
  //       name !== "performance_schema" &&  
  //       name !== "sas_ratataa_se" &&
  //         name !== "db_clean_db"
  //   )
  //   .slice(0, 10);

  // return res.status(200).json(filtredDatabases);

  // await reportService()
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
