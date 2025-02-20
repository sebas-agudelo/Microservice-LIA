import cron from "node-cron";
import { reportService } from "../services/reportService.js";
import { fetchReportData } from "../services/fetchReportData.js";

export const dailyJob = async () => {
    cron.schedule("* * * * *", async () => {
      console.log("Startar sammanslagningsjobbet (varje minut)...");
      try {
        // await reportService();
        await fetchReportData();
  
        console.log("Sammanslagningsjobb slutfört.");
      } catch (error) {
        console.error("Fel under sammanslagningsjobbet:", error.message);
      }
    });
  
    console.log("Cron-jobbet är startat och körs varje minut.");
  };