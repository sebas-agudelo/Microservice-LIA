import cron from "node-cron";
import { fetchReportData } from "../services/dashboard_report/fetchReportData.js";
import { collectDataForLeads } from "../services/Leads/collectDataForLeads.js";

export const dailyJob = async () => {
    cron.schedule("* * * * *", async () => {
      console.log("Startar sammanslagningsjobbet (varje minut)...");
      try {
        // await fetchReportData();
        // await collectDataForLeads();
        

  
        console.log("Sammanslagningsjobb slutfört.");
      } catch (error) {
        console.error("Fel under sammanslagningsjobbet:", error.message);
      }
    });
  
    console.log("Cron-jobbet är startat och körs varje minut.");
  };