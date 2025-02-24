import { reportService } from "./reportService.js";
import { dbConnection } from "../config/db.js";
import { createDataLine, updateMergeData } from "./updateMergeData.js";

export const fetchReportData = async () => {
  try {
    const { poolConnection, filteredDatabases } = await dbConnection();

    if (!filteredDatabases || filteredDatabases.length === 0) {
      console.error("Inga databaser att bearbeta.");
      return;
    }

    const allResults = await reportService();

    //En tom array för att samla all data per länk
    const mergeDataPerLink = [];

    //En tom array för att samla all data per kampanj
    const mergeDataPerCampaign = [];

    //En tom array för att samla alla result från sql querys
    const combinedData = [];

    allResults.forEach((results) => {
      console.log(results);
    
      const dbdb = results.db;

      combinedData.push(
        ...results.leadsResult,
        ...results.paidleadsResult,
        ...results.uniqueLeadsResult,
        ...results.recuringLeadsResult,
        ...results.giftcardsSendResult,
        ...results.moneyReceivedResult,
        ...results.avaragePaymentResult,
        ...results.engagementTimeResult,
        ...results.answersPercentageResult
      );

      results.viewRows.forEach((row) => {
        const locationKey =
          row.link === null
            ? "NULL_VALUE"
            : row.link === ""
            ? "EMPTY_STRING"
            : row.link;
        const key = `${row.campaign_id}-${locationKey}`;

        mergeDataPerLink.push({
          db: dbdb,
          key: key,
          campaign_id: row.campaign_id,
          link: row.link,
          views: row.views,
        });
      });

      results.viewRows2.forEach((row) => {
        const key = row.campaign_id;

        mergeDataPerCampaign.push({
          db: dbdb,
          key: key,
          campaign_id: row.campaign_id,
          link: "Totala länkar " + row.link,
          views: row.views,
        });
      });

      combinedData.forEach((row) => {
        const locationKey =
          row.location === null
            ? "NULL_VALUE"
            : row.location === ""
            ? "EMPTY_STRING"
            : row.location;

            const campaign_key = row.campaign_id;

        const existingEntry = mergeDataPerLink.find(
          (entry) =>
            entry.db === dbdb &&
            entry.campaign_id === row.campaign_id &&
            entry.link === locationKey
        );

        const existingEntry2 = mergeDataPerCampaign.find(
          (entry) =>
            entry.db === dbdb &&
            entry.campaign_id === campaign_key
        );

        if (existingEntry) {
          updateMergeData(existingEntry, row);
        }

        if (existingEntry2) {
          updateMergeData(existingEntry2, row);
        }
      });
    });

    for (const db of filteredDatabases) {

      const dataPerLinkAndPerCampaign = [];

      const dbEntries = mergeDataPerLink.filter((entry) => entry.db === db);
      const dbEntries2 = mergeDataPerCampaign.filter((entry) => entry.db === db);
      

      dbEntries.forEach((data) => {
        dataPerLinkAndPerCampaign.push(createDataLine(data))
      });

      dbEntries2.forEach((data) => {
        dataPerLinkAndPerCampaign.push(createDataLine(data));
      });

      dataPerLinkAndPerCampaign.sort((a, b) => {
        if (a[1] !== b[1]) {
          return a[1] - b[1];
        }
        return a[2] === null ? -1 : b[2] === null ? 1 : 0;
      });

      if (dataPerLinkAndPerCampaign.length > 0) {
  
        try {
          await poolConnection.query(
            `
            INSERT INTO ${db}.dashboard_report 
            (date, campaign_id, link, views, leads, paid_leads, unique_leads, recuring_leads, conversion_rate, giftcards_sent, money_received, avarage_payment, engagement_time, answers_percentage)
            VALUES ?
          `,
            [dataPerLinkAndPerCampaign]
          );

          console.log(`Data inserted into ${db}`);
        } catch (insertError) {
          console.error(`Error vid INSERT i ${db}:`, insertError);
        }
      } else {
        console.log(`No data to insert into ${db}`);
      }
    }
  } catch (error) {
    console.error("Error vid hämtning av rapportdata:", error);
  }
};
