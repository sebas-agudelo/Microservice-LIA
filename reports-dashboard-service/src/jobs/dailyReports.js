import cron from "node-cron";
import { fetchReportData } from "../services/dashboard_report/fetchReportData.js";
import mergeData from "../services/Leads/insertLeadsData.js";
import { dbConnection } from "../config/db.js";

export const dailyJob = async () => {
    cron.schedule("0 0 * * *", async () => { 
    
      try {
        const { poolConnection } = await dbConnection();

        console.log("Börjar med dashboard_report");
        await fetchReportData();
        console.log("Klar med dashboard_report");

        console.log("Börjar med leads");
        await mergeData();
        console.log("Klar med leads");

        await poolConnection.end();  // Stäng anslutningarna i poolen

        console.log("Stänger databasen");
        
      } catch (error) {
        console.error("Fel under sammanslagningsjobbet:", error.message);
      }
    });
};
