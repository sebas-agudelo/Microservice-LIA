import { reportService } from "./reportService.js";
import { dbConnection } from "../../config/db.js";
import { createDataLine } from "./updateMergeData.js";

export const fetchReportData = async () => {
  try {
    const { poolConnection, filteredDatabases } = await dbConnection();

    if (!filteredDatabases || filteredDatabases.length === 0) {
      console.error("Inga databaser att bearbeta.");
      return;
    }

    const allResults = await reportService();
    const { filtredDate } = allResults[0];

    const mergeDataByLink = [];

    const mergeDataPerCampaign = [];


    allResults.forEach((results) => {
      const dbdb = results.db;

      // console.log("Alla  views", results.viewResult);
      
      // console.log("Alla leads", results.leadsResult);

      // Bearbeta view-rader för individuella länkar
      results.viewRows.forEach((row) => {
        const link = row.link || null; // Hantera fall där link är null
        const key = `${row.campaign_id}-${link}`;
        const campaignData = row.campaign_id;

        mergeDataByLink.push({
          db: dbdb,
          key: key,
          campaign_id: row.campaign_id,
          link: link,
          views: row.views
        })
      });

      results.viewResult.forEach((row) => {
        const key = row.campaign_id;

        mergeDataPerCampaign.push({
          db: dbdb,
          key: key,
          campaign_id: row.campaign_id,
          link: "Total links: " + row.link,
          views: row.views
        })
      });
    
      const combinedData = [
        ...results.leadsResult,
        ...results.paidleadsResult,
        ...results.uniqueLeadsResult,
        ...results.recuringLeadsResult,
        ...results.giftcardsSendResult,
        ...results.moneyReceivedResult,
        ...results.avaragePaymentResult,
        ...results.engagementTimeResult,
        ...results.answersPercentageResult
      ];
    
      combinedData.forEach((row) => {
        const campaign_id = row.campaign_id;
        const link = row.link || null; // Hantera fall där link är null
        // const leads = row.leads ?? 0;
        const leads = (row.leads !== undefined && row.leads !== null && !isNaN(row.leads)) ? row.leads : 0;
        const paid_leads = (row.paid_leads !== undefined && row.paid_leads !== null && !isNaN(row.paid_leads)) ? row.paid_leads : 0;
        const unique_leads = (row.unique_leads !== undefined && row.unique_leads !== null && !isNaN(row.unique_leads)) ? row.unique_leads : 0;
        const recuring_leads = (row.recuring_leads !== undefined && row.recuring_leads !== null && !isNaN(row.recuring_leads)) ? row.recuring_leads : 0;
        const giftcards_sent = (row.giftcards_sent !== undefined && row.giftcards_sent !== null && !isNaN(row.giftcards_sent)) ? row.giftcards_sent : 0;
        const money_received = (row.money_received !== undefined && row.money_received !== null && !isNaN(row.money_received)) ? row.money_received : 0;
        const avarage_payment = (row.avarage_payment !== undefined && row.avarage_payment !== null && !isNaN(row.avarage_payment)) ? row.avarage_payment : 0;
        const engagement_time = (row.engagement_time !== undefined && row.engagement_time !== null && !isNaN(row.engagement_time)) ? row.engagement_time : 0;
        const answers_percentage = (row.answers_percentage !== undefined && row.answers_percentage !== null && !isNaN(row.answers_percentage)) ? row.answers_percentage : 0;
        // Hitta rätt entry baserat på campaign_id och link (view_r)
        const existingEntry = mergeDataByLink.find(
          (entry) =>
            entry.db === dbdb &&
            entry.campaign_id === campaign_id &&
            entry.link === link
        );
    
        const existingEntryCampaign = mergeDataPerCampaign.find(
          (entry) =>
            entry.db === dbdb &&
            entry.campaign_id === campaign_id
        );

        if (existingEntry) {
          // Uppdatera leads och answers_percentage för rätt view_r

          if(leads !== 0 && (existingEntry.leads === undefined || existingEntry.leads === 0)){
            existingEntry.leads = leads; // Summera leads

          }
          if(paid_leads !== 0 && (existingEntry.paid_leads === undefined || existingEntry.paid_leads === 0)){
            
            existingEntry.paid_leads = paid_leads;
          }
          if(unique_leads !== 0 && (existingEntry.unique_leads === undefined || existingEntry.unique_leads === 0)){
            
            existingEntry.unique_leads = unique_leads;
          }
          if(recuring_leads !== 0 && (existingEntry.recuring_leads === undefined || existingEntry.recuring_leads === 0)){

            existingEntry.recuring_leads = recuring_leads;
          }
          if(giftcards_sent !== 0 && (existingEntry.giftcards_sent === undefined || existingEntry.giftcards_sent === 0)){

            existingEntry.giftcards_sent = giftcards_sent;
          }
          if(money_received !== 0 && (existingEntry.money_received === undefined || existingEntry.money_received === 0)){

            existingEntry.money_received = money_received;
          }
          if(avarage_payment !== 0 && (existingEntry.avarage_payment === undefined || existingEntry.avarage_payment === 0)){

            existingEntry.avarage_payment = avarage_payment;
          }
          if(engagement_time !== 0 && (existingEntry.engagement_time === undefined || existingEntry.engagement_time === 0)){

            existingEntry.engagement_time = engagement_time;
          }
          if(answers_percentage !== 0 && (existingEntry.answers_percentage === undefined || existingEntry.answers_percentage === 0)){

            existingEntry.answers_percentage = answers_percentage;
          }
        } 


        else if (existingEntryCampaign) {
         
          if(leads !== 0 && (existingEntryCampaign.leads === undefined || existingEntryCampaign.leads === 0)){
            existingEntryCampaign.leads = leads; // Summera leads

          }
          if(paid_leads !== 0 && (existingEntryCampaign.paid_leads === undefined || existingEntryCampaign.paid_leads === 0)){
            
            existingEntryCampaign.paid_leads = paid_leads;
          }
          if(unique_leads !== 0 && (existingEntryCampaign.unique_leads === undefined || existingEntryCampaign.unique_leads === 0)){
            
            existingEntryCampaign.unique_leads = unique_leads;
          }
          if(recuring_leads !== 0 && (existingEntryCampaign.recuring_leads === undefined || existingEntryCampaign.recuring_leads === 0)){
  
            existingEntryCampaign.recuring_leads = recuring_leads;
          }
          if(giftcards_sent !== 0 && (existingEntryCampaign.giftcards_sent === undefined || existingEntryCampaign.giftcards_sent === 0)){

            existingEntryCampaign.giftcards_sent = giftcards_sent;
          }
          if(money_received !== 0 && (existingEntryCampaign.money_received === undefined || existingEntryCampaign.money_received === 0)){

            existingEntryCampaign.money_received = money_received;
          }
          if(avarage_payment !== 0 && (existingEntryCampaign.avarage_payment === undefined || existingEntryCampaign.avarage_payment === 0)){

            existingEntryCampaign.avarage_payment = avarage_payment;
          }
          if(engagement_time !== 0 && (existingEntryCampaign.engagement_time === undefined || existingEntryCampaign.engagement_time === 0)){

            existingEntryCampaign.engagement_time = engagement_time;
          }
          if(answers_percentage !== 0 && (existingEntryCampaign.answers_percentage === undefined || existingEntryCampaign.answers_percentage === 0)){
  
            existingEntryCampaign.answers_percentage = answers_percentage;
          }
        } 
      });
    });

    // console.log(mergeDataByLink);
    
    
    // Infoga data i databasen
    for (const db of filteredDatabases) {
      const dataPerLinkAndPerCampaign = [];
    
      const dbEntries = mergeDataByLink.filter((entry) => entry.db === db);
      const dbEntries2 = mergeDataPerCampaign.filter((entry) => entry.db === db);

    
      dbEntries.forEach((data) => {
        dataPerLinkAndPerCampaign.push(createDataLine(data, filtredDate));
      });

      dbEntries2.forEach((data) => {
        dataPerLinkAndPerCampaign.push(createDataLine(data, filtredDate));
      });

      // console.log("Campaign data", mergeDataPerCampaign);
      

    
      if (dataPerLinkAndPerCampaign.length > 0) {
        dataPerLinkAndPerCampaign.sort((a, b) => {
          if (a[1] !== b[1]) {
            return a[1] - b[1];
          }
          return a[2] === null ? -1 : b[2] === null ? 1 : 0;
        });
        
        try {
          await poolConnection.query(
            `
            INSERT INTO ${db}.dashboard_report 
            (date, campaign_id, link, views, leads, paid_leads, unique_leads, recuring_leads, giftcards_sent, money_received, avarage_payment, engagement_time, answers_percentage, created)
            VALUES ?
          `,
            [dataPerLinkAndPerCampaign]
          );
    
          console.log(`Data inserted into ${db}`);
        } catch (insertError) {
          console.error(`Error during INSERT in ${db}:`, insertError);
        }
      } else {
        console.log(`No data to insert into ${db}`);
      }
    }
    
  } catch (error) {
    console.error("Error vid hämtning av rapportdata:", error);
  }
};
